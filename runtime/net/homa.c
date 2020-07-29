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

/* handle to the homa transport instance */
homa_trans homa;

/* compile-time verification of mbuf layout compatibility */
typedef struct PacketSpec homapkt_t;
BUILD_ASSERT(offsetof(homapkt_t, payload) == offsetof(struct mbuf, data));
BUILD_ASSERT(offsetof(homapkt_t, length) == offsetof(struct mbuf, len));

/* an ingress message */
struct in_msg {
    homa_inmsg          in_msg; /* handle to homa ingress message */
    struct netaddr      src;    /* source address of the message */
    struct list_node    link;   /* used to chain message in a list */
};

/* a periodic background thread that handles timeout events */
static void homa_worker(void *arg)
{
    uint64_t deadline_us;
    uint64_t next_tsc;
    while (true) {
        next_tsc = homa_trans_check_timeouts(homa);
        deadline_us = (next_tsc - start_tsc) / cycles_per_us;
        timer_sleep_until(deadline_us);
    }
}

// TODO: do we need a pacer thread to run sender-side SRPT logic?
// TODO: or should we embed the sender-side logic in some shenango mechanism? e.g., softirq?
// softirq is for low-level tasks? besides, changing softirq is more intrusive?
// also softirq is run on all cores; this might not be what we want

static void homa_pacer(void *arg)
{

}

static void homa_tx_release_mbuf(struct mbuf *m)
{
    if (atomic_dec_and_test(&m->ref))
        net_tx_release_mbuf(m);
}

static void *homa_tx_alloc_mbuf(void)
{
    struct mbuf *m = net_tx_alloc_mbuf();
    m->release = homa_tx_release_mbuf;

    /* egress mbuf is initially shared between transport and driver */
    atomic_write(&m->ref, 2);
    return m;
}

/**
 * homa_init_late - instantiates a homa transport instance
 *
 * Returns 0 (always successful).
 */
int homa_init_late(void)
{
    // TODO: read link_speed from netcfg?
    uint32_t speed = 10 * 1000; // Mbits/second
    homa_driver shim_drv = homa_driver_create(IPPROTO_HOMA, netcfg.addr,
            HOMA_MAX_PAYLOAD, speed, homa_tx_alloc_mbuf, net_tx_ip, mbuf_free);
    homa = homa_trans_create(shim_drv, 0);

    /* start the long-running Homa background thread */
    BUG_ON(thread_spawn(homa_worker, NULL));
    return 0;
}

/*
 * Homa Socket Support
 */

/* homa socket */
struct homaconn {
    struct trans_entry  e;          /* entry in the global socket table */
    spinlock_t          lock;       /* per-socket lock */
    homa_mailbox        mailbox;    /* used by homa lib to deliver msg */
    int                 rxq_len;    /* size of the ingress msg queue */
    struct list_head    rxq;        /* ingress messages to be processed */
    bool                shutdown;   /* socket shutdown? */
    waitq_t             wq;         /* uthreads waiting for ingress msgs */
};

/** handles ingress Homa packets */
/**
 * homa_conn_recv - handles an ingress Homa packet
 * @e: used to identify the destination socket of the packet
 * @m: the ingress packet
 */
static void homa_conn_recv(struct trans_entry *e, struct mbuf *m)
{
    homaconn_t *c =  container_of(e, homaconn_t, e);
    struct ip_hdr *iphdr = mbuf_network_hdr(m, *iphdr);

    /* run the packet through the homa protocol stack */
    homa_trans_proc(homa, m, ntoh32(iphdr->saddr), c->mailbox);
}

/* operations for Homa sockets */
const struct trans_ops homa_conn_ops = {
    .recv = homa_conn_recv,
    .err = NULL,
};

/**
 * homa_mb_deliver - callback function to deliver an ingress message to its
 * destination socket
 * @c: the homa socket
 * @in_msg: the ingress message
 * @ip: the source ip address
 * @port: the source port number
 */
static void homa_mb_deliver(homaconn_t *c, homa_inmsg in_msg, uint32_t ip,
                            uint16_t port)
{
    /* allocate a list node */
    struct in_msg *node = smalloc(sizeof(*node));
    node->in_msg = in_msg;
    node->src.ip = ip;
    node->src.port = port;

    spin_lock_np(&c->lock);

    /* add the message to the ingress queue */
    list_add_tail(&c->rxq, &node->link);
    c->rxq_len++;

    /* wake up a waiter */
    struct thread* th = waitq_signal(&c->wq, &c->lock);
    spin_unlock_np(&c->lock);

    waitq_signal_finish(th);
}

static void homa_init_conn(homaconn_t *c)
{
    bzero(&c->e, sizeof(struct trans_entry));
    spin_lock_init(&c->lock);
    c->mailbox = homa_mailbox_create(c, homa_mb_deliver);
    c->rxq_len = 0;
    list_head_init(&c->rxq);
    c->shutdown = false;
    waitq_init(&c->wq);
}

/* callback function used by rcu_free() to free the socket */
static void homa_finish_release_conn(struct rcu_head *h)
{
    homaconn_t *c = container_of(h, homaconn_t, e.rcu);
    sfree(c);
}

/* schedules the task of freeing the socket after RCU quiescent period */
static void homa_release_conn(homaconn_t *c)
{
    rcu_free(&c->e.rcu, homa_finish_release_conn);
}

/**
 * homa_open - creates a Homa socket for applications to send and receive
 * messages
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
    trans_init_3tuple(&c->e, IPPROTO_HOMA, &homa_conn_ops, laddr);

    /* port zero means assign an ephemeral port automatically */
    if (laddr.port == 0)
        ret = trans_table_add_with_ephemeral_port(&c->e);
    else
        ret = trans_table_add(&c->e);

    if (ret) {
        sfree(c);
        return ret;
    }

    *c_out = c;
    return 0;
}

/**
 * homa_local_addr - gets the local address of the socket
 * @c: the Homa socket
 */
struct netaddr homa_local_addr(homaconn_t *c)
{
    return c->e.laddr;
}

/**
 * homa_recvmsg - receives an ingress message from a Homa socket
 * @c: the Homa socket
 * @raddr: a pointer to store the source address (if successful)
 *
 * WARNING: This a blocking function. It will wait until the ingress message
 * arrives, or the socket is shutdown.
 *
 * Return the ingress message. If the socket has been shutdown, returns a
 * null handle.
 */
homa_inmsg homa_recvmsg(homaconn_t *c, struct netaddr *raddr)
{
    homa_inmsg in_msg = {NULL};

	spin_lock_np(&c->lock);

	/* block until there is an actionable event */
	while (list_empty(&c->rxq) && !c->shutdown)
		waitq_wait(&c->wq, &c->lock);

    /* is the socket drained and shutdown? */
    if (list_empty(&c->rxq) && c->shutdown) {
        spin_unlock_np(&c->lock);
        return in_msg;
	}

    /* pop an in_msg and deliver the payload */
    struct in_msg *node = list_pop(&c->rxq, struct in_msg, link);
    spin_unlock_np(&c->lock);
    in_msg = node->in_msg;
    *raddr = node->src;
    sfree(node);

    log_info("homa_recv: received %lu-byte msg", homa_inmsg_len(in_msg));
    return in_msg;
}

/* invoked when an egress message transitions to its end state */
static void homa_cb_outmsg_done(sema_t *sema)
{
    sema_up(sema);
}

/**
 * homa_sendmsg - sends a message using the Homa transport protocol
 * @c: the Homa socket
 * @buf: a buffer from which to copy the data
 * @len: the length of the data
 * @raddr: the remote address of the message
 *
 * WARNING: This a blocking function. It will wait until the egress message
 * is safely delivered to the target or a timeout occurs.
 *
 * Returns 0 if the message is delivered successfully. If an error occurs,
 * returns < 0 to indicate the error code.
 */
int homa_sendmsg(homaconn_t *c, const void *buf, size_t len,
        struct netaddr raddr, int *status)
{
    sema_t sema;
    homa_outmsg out_msg;

    // FIXME: remove this limitation to allow multi-packet messages
    if (len > HOMA_MAX_PAYLOAD)
        return -EMSGSIZE;

    sema_init(&sema, 0);
    out_msg = homa_trans_alloc(homa, c->e.laddr.port);
    homa_outmsg_register_cb(out_msg, homa_cb_outmsg_done, &sema);
    homa_outmsg_append(out_msg, buf, len);
    homa_outmsg_send(out_msg, raddr.ip, raddr.port);

    /* block until the message is acked or a timeout occurs */
    sema_down(&sema);
    *status = homa_outmsg_status(out_msg);

    homa_outmsg_release(out_msg);
    return 0;
}

void __homa_shutdown(homaconn_t *c)
{
    BUG_ON(c->shutdown);
    c->shutdown = true;
    homa_mailbox_free(c->mailbox);

    /* prevent ingress receive and error dispatch (after RCU period) */
    trans_table_remove(&c->e);
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