/*
 * homa.c - support for Homa Protocol
 */

#include <string.h>

#include <base/log.h>
#include <base/hash.h>
#include <runtime/smalloc.h>
#include <runtime/rculist.h>
#include <runtime/sync.h>
#include <runtime/thread.h>
#include <runtime/homa.h>
#include <runtime/timer.h>

#include "defs.h"
#include "waitq.h"

#include <Homa/Shenango.h>

// Must be smaller than minimum ephemeral port (i.e., MIN_EPHEMERAL).
#define HOMA_MIN_CLIENT_PORT 49000


/* an opaque pointer to the homa transport instance */
void* homa_trans;

/* compile-time verification of mbuf layout compatibility */
typedef struct PacketSpec homapkt_t;
BUILD_ASSERT(offsetof(homapkt_t, payload) == offsetof(struct mbuf, data));
BUILD_ASSERT(offsetof(homapkt_t, length) == offsetof(struct mbuf, len));

/* a periodic background thread that handles timeout events */
static void homa_worker(void *arg)
{
    homaconn_t *c;
    uint64_t now;

    while (true) {
        now = microtime();

//        spin_lock_np(&tcp_lock);
//        list_for_each(&tcp_conns, c, global_link) {
//            if (load_acquire(&c->next_timeout) <= now)
//                tcp_handle_timeouts(c, now);
//        }
//        spin_unlock_np(&tcp_lock);

        timer_sleep(10 * ONE_MS);
    }
}

/**
 * homa_init_late - instantiates homa transport instance
 *
 * Returns 0 (always successful).
 */
int homa_init_late(void)
{
    uint32_t speed = 10 * 1000; // Mbits/second
    void *shim_drv = homa_driver_create(IPPROTO_HOMA, netcfg.addr,
            HOMA_MAX_PAYLOAD, speed, homa_tx_alloc_mbuf, net_tx_ip, mbuf_free);
    homa_trans = homa_trans_create(shim_drv, 0);
    log_info("homa driver payload size %u", homa_driver_max_payload(shim_drv));
    return 0;
}

static void homa_tx_release_mbuf(struct mbuf *m)
{
    if (atomic_dec_and_test(&m->ref))
        net_tx_release_mbuf(m);
}

void* homa_tx_alloc_mbuf(void)
{
    struct mbuf *m = net_tx_alloc_mbuf();
    m->release = homa_tx_release_mbuf;

    /* egress mbuf is initially shared between transport and driver */
    atomic_write(&m->ref, 2);
    return m;
}

/*
 * Homa Socket Support
 */

/* homa socket */
struct homaconn {
    struct trans_entry    c_e;    /* client entry in the global socket table */
    struct trans_entry    s_e;    /* server entry in the global socket table */
    bool            shutdown;   /* socket shutdown? */

    spinlock_t        lock;       /* protects TODO? */
    void*           mailbox;
};

/* handles ingress packets for Homa sockets */
static void homa_conn_recv(struct trans_entry *e, struct mbuf *m)
{
    // TODO: we are currently relying on Shenango's dispatch mechanism to
    // direct a packet its corresponding socket (e.g., you can obtain the
    // homaconn_t object from e); however, unlike tcp, the homa protocol
    // is really socket-agnostic and has no use for the socket right now
    // (as you can see, homa_trans_proc doesn't take homaconn_t as input).

    // As a result, the work in Shenango is wasted and homa_recv would have
    // to search the global Homa::Receiver::receivedMessages to find message
    // targeting a specific socket. Yes, we could try to partition the global
    // msg queue based on hash of socket addr but this sounds like a hacky
    // patch for our case? (well, perhaps this mechanism can be generally
    // useful? idk)

    homaconn_t *c = (e->laddr.port < HOMA_MIN_CLIENT_PORT) ?
            container_of(e, homaconn_t, s_e) :
            container_of(e, homaconn_t, c_e);
    homa_trans_proc(homa_trans, m, e->laddr.ip, c->mailbox);
}

/* handles network errors for Homa sockets */
static void homa_conn_err(struct trans_entry *e, int err)
{
    // TODO: do something?
    log_err("homa_conn_err: err = %d", err);
}

/* operations for Homa sockets */
const struct trans_ops homa_conn_ops = {
    .recv = homa_conn_recv,
    .err = homa_conn_err,
};

static void homa_mb_deliver(homaconn_t *c, void* in_msg)
{
    // TODO:
    log_info("homa_mb_deliver: received %lu-byte msg", homa_inmsg_len(in_msg));
}

static void homa_init_conn(homaconn_t *c)
{
    bzero(&c->c_e, sizeof(struct trans_entry));
    bzero(&c->s_e, sizeof(struct trans_entry));
    c->shutdown = false;
    spin_lock_init(&c->lock);
    c->mailbox = homa_mailbox_create(c, homa_mb_deliver);
}

static void homa_finish_release_conn(struct rcu_head *h)
{
    homaconn_t *c = container_of(h, homaconn_t, c_e.rcu);
    sfree(c);
}

static void homa_release_conn(homaconn_t *c)
{
    rcu_free(&c->c_e.rcu, homa_finish_release_conn);
    // FIXME(Yilong): how to free s_e.rcu?
//    rcu_free(&c->s_e.rcu, NULL);
}

/**
 * homa_open - creates a Homa socket for applications to initiate requests
 * as clients
 * @laddr: the local Homa address
 * @c_out: a pointer to store the Homa socket (if successful)
 *
 * Returns 0 if success, otherwise fail.
 */
int homa_open(struct netaddr laddr, homaconn_t **c_out)
{
    homaconn_t *c;
    int ret;

    /* only can support one local IP so far */
    if (laddr.ip == 0)
        laddr.ip = netcfg.addr;
    else if (laddr.ip != netcfg.addr)
        return -EINVAL;

    c = smalloc(sizeof(*c));
    if (!c)
        return -ENOMEM;

    homa_init_conn(c);
    trans_init_3tuple(&c->c_e, IPPROTO_HOMA, &homa_conn_ops, laddr);

    if (laddr.port == 0) {
        ret = trans_table_add_with_ephemeral_port(&c->c_e);
        laddr = c->c_e.laddr;
    } else
        ret = trans_table_add(&c->c_e);
    if (laddr.port < HOMA_MIN_CLIENT_PORT)
        return -EINVAL;

    if (ret) {
        sfree(c);
        return ret;
    }

    *c_out = c;
    return 0;
}

/**
 * homa_bind - binds a Homa socket to a server port to receive incoming requests
 * @c: the Homa socket
 * @port: the server port to bind to (must be less than HOMA_MIN_CLIENT_PORT)
 *
 * Returns 0 if success, otherwise fail.
 */
int homa_bind(homaconn_t *c, uint16_t port)
{
    struct netaddr laddr = c->c_e.laddr;
    laddr.port = port;

    /* TODO(Yilong): check a socket is not bound more than once */
    if (port >= HOMA_MIN_CLIENT_PORT)
        return -EINVAL;

    trans_init_3tuple(&c->s_e, IPPROTO_HOMA, &homa_conn_ops, laddr);

    return trans_table_add(&c->s_e);
}

/**
 * homa_client_addr - gets the local client address of the socket
 * @c: the Homa socket
 */
struct netaddr homa_client_addr(homaconn_t *c)
{
    return c->c_e.laddr;
}

/**
 * homa_server_addr - gets the local client address of the socket
 * @c: the Homa socket
 */
struct netaddr homa_server_addr(homaconn_t *c)
{
    return c->s_e.laddr;
}

// FIXME: this is the api of John's kernel impl.; it's a bit unnatural for
// user impl. (i.e., buf & len should be dropped; instead a msg object should
// be returned)
ssize_t homa_recv(homaconn_t *c, void *buf, size_t len, struct netaddr *raddr)
{
    // FIXME: grab an ingress msg from c->inq; blocked until there is one
    timer_sleep(300 * 1000000);
    return 0;
}

ssize_t homa_reply(homaconn_t *c, const void *buf, size_t len,
                const struct netaddr *raddr)
{
    return 0;
}

/**
 * homa_send - sends a request message using the Homa transport protocol
 * @c: the Homa socket
 * @buf: a buffer from which to load the payload
 * @len: the length of the payload
 * @raddr: the remote address of the message
 *
 * Returns 0 on success. If an error occurs, returns < 0 to indicate the error
 * code.
 */
int homa_send(homaconn_t *c, const void *buf, size_t len, struct netaddr raddr)
{
    // FIXME: remove this limitation to allow multi-packet messages
    if (len > HOMA_MAX_PAYLOAD)
        return -EMSGSIZE;

    void* out_msg = homa_trans_alloc(homa_trans, c->c_e.laddr.port);
    homa_outmsg_append(out_msg, buf, len);
    homa_outmsg_send(out_msg, raddr.ip, raddr.port);
    return 0;
}

void __homa_shutdown(homaconn_t *c)
{
    BUG_ON(c->shutdown);
    c->shutdown = true;
    homa_mailbox_free(c->mailbox);

    /* prevent ingress receive and error dispatch (after RCU period) */
    trans_table_remove(&c->c_e);
    trans_table_remove(&c->s_e);
}

/**
 * homa_shutdown - disables a Homa socket
 * @c: the socket to disable
 *
 * All blocking requests on the socket will return a failure.
 */
void homa_shutdown(homaconn_t *c)
{
    /* shutdown the Homa socket */
    __homa_shutdown(c);
}

/**
 * homa_close - frees a Homa socket
 * @c: the socket to free
 *
 * WARNING: Only the last reference can safely call this method. Call
 * homa_shutdown() first if any threads are sleeping on the socket.
 */
void homa_close(homaconn_t *c)
{
    if (!c->shutdown)
        __homa_shutdown(c);

    homa_release_conn(c);
}