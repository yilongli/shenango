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

/* number of egress bytes currently queued at the NIC by homa */
static atomic_t homa_tx_queued_bytes;

/* an ingress message */
struct in_msg {
    homa_inmsg          in_msg; /* handle to homa ingress message */
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
            BUG_ON(wait_until < start_tsc);
            uint64_t deadline_us = (wait_until - start_tsc) / cycles_per_us;
            timer_sleep_until(deadline_us);
        } else {
            /* sleep until more work arrive */
            sema_down_all(&sema);
        }
    }
}

static void homa_tx_release_mbuf(struct mbuf *m)
{
    if (atomic_dec_and_test(&m->ref)) {
        atomic_fetch_and_sub(&homa_tx_queued_bytes, (int)m->len);
        net_tx_release_mbuf(m);
    }
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
 * @prio: the packet priority (0 is the lowest); must be less than 8
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

    /* on success, the mbuf will be freed when the transmit completes */
    ret = net_tx_ip_prio(m, proto, daddr, prio);
    atomic_fetch_and_add(&homa_tx_queued_bytes, (int)m->len);
    if (unlikely(ret)) {
        mbuf_free(m);
    }
    log_debug("homa_tx_ip: sent %d-byte homa packet, %d bytes ahead", len,
            homa_tx_queued_bytes.cnt);
    return ret;
}

static uint32_t homa_queued_bytes()
{
    int ret = atomic_read(&homa_tx_queued_bytes);
    assert(ret >= 0);
    return ret;
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
 */
static void homa_mb_deliver(homaconn_t *c, homa_inmsg in_msg)
{
    /* allocate a list node */
    struct in_msg *node = smalloc(sizeof(*node));
    node->in_msg = in_msg;
    bzero(&node->link, sizeof(struct list_node));

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
 * @in_msg: a pointer to store the ingress message (if successful)
 *
 * WARNING: This a blocking function. It will wait until the ingress message
 * arrives, or the socket is shutdown.
 *
 * Returns 0 if the message is delivered successfully. If an error occurs,
 * returns < 0 to indicate the error code.
 */
int homa_recvmsg(homaconn_t *c, homa_inmsg *in_msg)
{
    spin_lock_np(&c->lock);

	/* block until there is an actionable event */
	while (list_empty(&c->rxq) && !c->shutdown)
		waitq_wait(&c->wq, &c->lock);

    /* is the socket drained and shutdown? */
    if (list_empty(&c->rxq) && c->shutdown) {
        spin_unlock_np(&c->lock);
        return -ESHUTDOWN;
	}

    /* pop an in_msg and deliver the payload */
    struct in_msg *node = list_pop(&c->rxq, struct in_msg, link);
    spin_unlock_np(&c->lock);
    *in_msg = node->in_msg;
    sfree(node);
    return 0;
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
        struct netaddr raddr)
{
    sema_t sema;
    homa_outmsg out_msg;
    int status;

    sema_init(&sema, 0);
    out_msg = homa_sk_alloc(c->sk);
    if (!out_msg.p) {
        return -ENOMEM;
    }

    homa_outmsg_register_cb_end_state(out_msg, (void (*)(void*))sema_up, &sema);
    homa_outmsg_append(out_msg, buf, len);
    homa_outmsg_send(out_msg, raddr.ip, raddr.port);

    /* block until the message is acked or a timeout occurs */
    sema_down(&sema);
    status = homa_outmsg_status(out_msg);
    homa_outmsg_release(out_msg);

    return (status == COMPLETED) ? 0 : -ETIMEDOUT;
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
