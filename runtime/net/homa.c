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

/* an ingress message */
struct in_msg {
    homa_inmsg          in_msg; /* handle to homa ingress message */
    struct netaddr      src;    /* source address of the message */
    struct list_node    link;   /* used to chain message in a list */
};

/* a periodic background thread that handles timeout events */
static __noreturn void homa_timeout_handler(__notused void *arg)
{
    uint64_t deadline_us;
    uint64_t next_tsc;
    while (true) {
        next_tsc = homa_trans_check_timeouts(homa);
        deadline_us = (next_tsc - start_tsc) / cycles_per_us;
        timer_sleep_until(deadline_us);
    }
}

/**
 * homa_pacer - starting method of the pacer thread which executes the sender-
 * side logic of Homa.
 */
static __noreturn void homa_pacer(__notused void *arg)
{
    bool more_work;
    uint64_t wait_until;
    sema_t sema;
    sema_init(&sema, 0);

    /* register a callback to wake us up when packets are ready to be sent */
    homa_trans_register_cb_send_ready(homa, (void (*)(void*))sema_up, &sema);

    /* drives the sender by calling try_send() repeatedly */
    while (1) {
        more_work = homa_trans_try_send(homa, &wait_until);
        if (more_work) {
            /* sleep to allow the NIC to drain its transmit queue */
            if (wait_until < start_tsc) {
                log_warn("wait_until %lu is too small!", wait_until);
                continue;
            }
            uint64_t deadline_us = (wait_until - start_tsc) / cycles_per_us;
            timer_sleep_until(deadline_us);
        } else {
            /* sleep until more work arrive */
            sema_down_all(&sema);
        }
    }
}

/**
 * homa_grantor - starting method of the grantor thread, which executes the
 * receiver-side logic of Homa.
 */
static __noreturn void homa_grantor(__notused void *arg)
{
    sema_t sema;
    sema_init(&sema, 0);

    /* register a callback to wake us up when messages are waiting for grants */
    homa_trans_register_cb_need_grants(homa, (void (*)(void*))sema_up, &sema);

    /* drives the receiver by calling try_send_grants() repeatedly */
    while (1) {
        homa_trans_try_grant(homa);

        /* sleep until some messages are waiting for grants */
        sema_down_all(&sema);
        /* but delay a bit to amortize the cost of sending grants */
        timer_sleep(5);
    }
}

static void homa_tx_release_mbuf(struct mbuf *m)
{
    if (atomic_dec_and_test(&m->ref))
        net_tx_release_mbuf(m);
}

/**
 * homa_tx_alloc_mbuf - allocates an mbuf for transmitting by the Homa transport
 *
 * @payload: set to the start address of the data buffer upon method return
 *
 * Returns an mbuf, or NULL if out of memory.
 */
static void *homa_tx_alloc_mbuf(void **payload)
{
    struct mbuf *m = net_tx_alloc_mbuf();
    m->release = homa_tx_release_mbuf;

    /* egress mbuf is initially owned only by the transport */
    atomic_write(&m->ref, 1);

    *payload = m->data;
    return m;
}

/**
 * homa_tx_ip - transmits an IP packet for the Homa transport
 * @desc: the packet descriptor which identifies the mbuf to transmit
 * @payload: the starting address of the payload
 * @len: the length of the payload
 * @proto: the transport protocol
 * @daddr: the destination IP address (in native byte order)
 * @prio: the packet priority
 *
 * This method is a thin wrapper around net_tx_ip() that helps convert a Homa
 * Driver::Packet to mbuf; see net_tx_ip() for more information.
 *
 * Returns 0 if successful.
 */
static int homa_tx_ip(uintptr_t desc, void* payload, int32_t len, uint8_t proto,
                      uint32_t daddr, uint8_t prio)
{
    int ref_cnt;
    int ret;

    struct mbuf *m = (struct mbuf*) desc;
    m->data = payload;
    m->len = len;

    /* egress mbuf is also owned by the driver while being transmitted */
    ref_cnt = atomic_fetch_and_add(&m->ref, 1);
    if (unlikely(!ref_cnt)) {
        /* transport already dropped the message; no need to transmit */
        mbuf_free(m);
        return -1;
    }

    // FIXME: pass packet priority to net_tx_ip; set ip priority!
    /* on success, the mbuf will be freed when the transmit completes */
    ret = net_tx_ip(m, proto, daddr);
    if (unlikely(ret)) {
        mbuf_free(m);
    }
    return ret;
}

static uint32_t homa_queued_bytes()
{
    // FIXME: how to implement this?
    return 0;
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
            HOMA_MAX_PAYLOAD, speed);
    homa_mailbox_dir dir = homa_mb_dir_create(IPPROTO_HOMA, netcfg.addr);
    homa = homa_trans_create(shim_drv, dir, 0);

    /* start the long-running Homa threads */
    BUG_ON(thread_spawn(homa_pacer, NULL));
    BUG_ON(thread_spawn(homa_grantor, NULL));
    BUG_ON(thread_spawn(homa_timeout_handler, NULL));
    return 0;
}

/*
 * Homa Socket Support
 */

/* homa socket */
struct homaconn {
    struct trans_entry  e;          /* entry in the global socket table */
    spinlock_t          lock;       /* per-socket lock */
    homa_sk             sk;         /* handle to Homa::Socket */
    int                 rxq_len;    /* size of the ingress msg queue */
    struct list_head    rxq;        /* ingress messages to be processed */
    bool                shutdown;   /* socket shutdown? */
    waitq_t             wq;         /* uthreads waiting for ingress msgs */
};

/* operations for Homa sockets */
const struct trans_ops homa_conn_ops = {
    /* homa_trans_proc is invoked directly from transport.c:trans_lookup() */
    .recv = NULL,
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
    bzero(&c->sk, sizeof(homa_sk));
    c->rxq_len = 0;
    list_head_init(&c->rxq);
    c->shutdown = false;
    waitq_init(&c->wq);
}

/* callback function used by rcu_free() to free the socket */
static void homa_finish_release_conn(struct rcu_head *h)
{
    homaconn_t *c = container_of(h, homaconn_t, e.rcu);
    homa_sk_close(c->sk);
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

    c->sk = homa_trans_open(homa, c->e.laddr.port);
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
    return in_msg;
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

    sema_init(&sema, 0);
    out_msg = homa_sk_alloc(c->sk);
    homa_outmsg_register_cb_end_state(out_msg, (void (*)(void*))sema_up, &sema);
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

    /* wake all blocked threads */
    waitq_release(&c->wq);
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

/* initialize function pointers that will be used in Homa/src/Shenango.cc */
#define INIT_HOMA_SHENANGO_FUNC(func) void *shenango_##func = func;
INIT_HOMA_SHENANGO_FUNC(smalloc)
INIT_HOMA_SHENANGO_FUNC(sfree)
INIT_HOMA_SHENANGO_FUNC(rcu_read_lock)
INIT_HOMA_SHENANGO_FUNC(rcu_read_unlock)
INIT_HOMA_SHENANGO_FUNC(homa_tx_alloc_mbuf)
INIT_HOMA_SHENANGO_FUNC(homa_tx_ip)
INIT_HOMA_SHENANGO_FUNC(homa_queued_bytes)
INIT_HOMA_SHENANGO_FUNC(mbuf_free)
INIT_HOMA_SHENANGO_FUNC(homa_mb_deliver)
INIT_HOMA_SHENANGO_FUNC(trans_table_lookup)
