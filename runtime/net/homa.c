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

#include "defs.h"
#include "waitq.h"

// FIXME: use <...> instead of ""?
//#include <Homa/Shenango.h>
#include "../../homa/include/Homa/Shenango.h"

#define HOMA_IN_DEFAULT_CAP	512
#define HOMA_OUT_DEFAULT_CAP	2048
#define HOMA_MIN_CLIENT_PORT 60000

/* an opaque pointer to the homa transport instance */
void* homa_trans;

/**
 * homa_init_late - instantiates homa transport instance
 *
 * Returns 0 (always successful).
 */
int homa_init_late(void)
{
    void *shim_drv = homa_driver_create(IPPROTO_HOMA, netcfg.addr,
            HOMA_MAX_PAYLOAD, 10*1000, net_tx_alloc_mbuf, net_tx_ip, mbuf_free);
    homa_trans = homa_trans_create(shim_drv, 0);
    log_info("homa driver payload size %u", homa_driver_max_payload(shim_drv));
    return 0;
}

static int homa_send_raw(struct mbuf *m, size_t len,
			struct netaddr laddr, struct netaddr raddr)
{
	struct homa_hdr *homahdr;

	/* write Homa header */
	homahdr = mbuf_push_hdr(m, *homahdr);
	homahdr->src_port = hton16(laddr.port);
	homahdr->dst_port = hton16(raddr.port);
	homahdr->len = hton16(len + sizeof(*homahdr));
	homahdr->chksum = 0;

	/* send the IP packet */
	return net_tx_ip(m, IPPROTO_HOMA, raddr.ip);
}


/*
 * Homa Socket Support
 */

/* homa socket */
struct homaconn {
	struct trans_entry	c_e;    /* client entry in the global socket table */
	struct trans_entry	s_e;    /* server entry in the global socket table */
	bool			shutdown;   /* socket shutdown? */

	// FIXME(Yilong): remove all the members below?
	/* ingress support */
	spinlock_t		inq_lock;   /* protects ingress queue */
	int			    inq_cap;    /* ingress queue capacity */
	int			    inq_len;    /* size of the ingress queue */
	int			    inq_err;    /* error status */
	waitq_t			inq_wq;     /* condition var: ingress data available? */
	struct mbufq	inq;        /* ingress queue (ie. a linked-list of mbufs) */

	/* egress support */
	spinlock_t		outq_lock;  /* protects egress queue */
	bool			outq_free;  /* egress queue closed? */
	int			    outq_cap;   /* egress queue capacity */
	int			    outq_len;   /* size of the egress queue */
	waitq_t			outq_wq;    /* condition var: egress queue not full? */
};

/* handles ingress packets for Homa sockets */
static void homa_conn_recv(struct trans_entry *e, struct mbuf *m)
{
	homaconn_t *c = (e->laddr.port < HOMA_MIN_CLIENT_PORT) ?
	        container_of(e, homaconn_t, s_e) :
	        container_of(e, homaconn_t, c_e);
	thread_t *th;

	if (unlikely(!mbuf_pull_hdr_or_null(m, sizeof(struct homa_hdr)))) {
		mbuf_free(m);
		return;
	}

	spin_lock_np(&c->inq_lock);
	/* drop packet if the ingress queue is full */
	if (c->inq_len >= c->inq_cap || c->inq_err || c->shutdown) {
		spin_unlock_np(&c->inq_lock);
		mbuf_drop(m);
		return;
	}

	/* enqueue the packet on the ingress queue */
	mbufq_push_tail(&c->inq, m);
	c->inq_len++;

	/* wake up a waiter */
	th = waitq_signal(&c->inq_wq, &c->inq_lock);
	spin_unlock_np(&c->inq_lock);

	waitq_signal_finish(th);
}

/* handles network errors for Homa sockets */
static void homa_conn_err(struct trans_entry *e, int err)
{
	homaconn_t *c = (e->laddr.port < HOMA_MIN_CLIENT_PORT) ?
	        container_of(e, homaconn_t, s_e) :
	        container_of(e, homaconn_t, c_e);

	bool do_release;

	spin_lock_np(&c->inq_lock);
	do_release = !c->inq_err && !c->shutdown;
	c->inq_err = err;
	spin_unlock_np(&c->inq_lock);

	if (do_release)
		waitq_release(&c->inq_wq);
}

/* operations for Homa sockets */
const struct trans_ops homa_conn_ops = {
	.recv = homa_conn_recv,
	.err = homa_conn_err,
};

static void homa_init_conn(homaconn_t *c)
{
	c->shutdown = false;

	/* initialize ingress fields */
	spin_lock_init(&c->inq_lock);
	c->inq_cap = HOMA_IN_DEFAULT_CAP;
	c->inq_len = 0;
	c->inq_err = 0;
	waitq_init(&c->inq_wq);
	mbufq_init(&c->inq);

	/* initialize egress fields */
	spin_lock_init(&c->outq_lock);
	c->outq_free = false;
	c->outq_cap = HOMA_OUT_DEFAULT_CAP;
	c->outq_len = 0;
	waitq_init(&c->outq_wq);
}

static void homa_finish_release_conn(struct rcu_head *h)
{
	homaconn_t *c = container_of(h, homaconn_t, c_e.rcu);
	sfree(c);
}

static void homa_release_conn(homaconn_t *c)
{
	assert(waitq_empty(&c->inq_wq) && waitq_empty(&c->outq_wq));
	assert(mbufq_empty(&c->inq));
	rcu_free(&c->c_e.rcu, homa_finish_release_conn);
	// FIXME(Yilong): how to free s_e.rcu?
//	rcu_free(&c->s_e.rcu, NULL);
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
	if (laddr.port < HOMA_MIN_CLIENT_PORT)
	    return -EINVAL;

	c = smalloc(sizeof(*c));
	if (!c)
		return -ENOMEM;

	homa_init_conn(c);
	trans_init_3tuple(&c->c_e, IPPROTO_HOMA, &homa_conn_ops, laddr);

    ret = trans_table_add(&c->c_e);
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

/**
 * homa_set_buffers - changes send and receive buffer sizes
 * @c: the Homa socket
 * @read_mbufs: the maximum number of read mbufs to buffer
 * @write_mbufs: the maximum number of write mbufs to buffer
 *
 * Returns 0 if the inputs were valid.
 */
int homa_set_buffers(homaconn_t *c, int read_mbufs, int write_mbufs)
{
	c->inq_cap = read_mbufs;
	c->outq_cap = write_mbufs;

	/* TODO: free mbufs that go over new limits? */
	return 0;
}

/**
 * homa_read_from - reads from a Homa socket
 * @c: the Homa socket
 * @buf: a buffer to store the datagram
 * @len: the size of @buf
 * @raddr: a pointer to store the remote address of the datagram (if not NULL)
 *
 * WARNING: This a blocking function. It will wait until a datagram is
 * available, an error occurs, or the socket is shutdown.
 *
 * Returns the number of bytes in the datagram, or @len if the datagram
 * is >= @len in size. If the socket has been shutdown, returns 0.
 */
ssize_t homa_read_from(homaconn_t *c, void *buf, size_t len,
                      struct netaddr *raddr)
{
	ssize_t ret;
	struct mbuf *m;

	spin_lock_np(&c->inq_lock);

	/* block until there is an actionable event */
	while (mbufq_empty(&c->inq) && !c->inq_err && !c->shutdown)
		waitq_wait(&c->inq_wq, &c->inq_lock);

	/* is the socket drained and shutdown? */
	if (mbufq_empty(&c->inq) && c->shutdown) {
		spin_unlock_np(&c->inq_lock);
		return 0;
	}

	/* propagate error status code if an error was detected */
	if (c->inq_err) {
		spin_unlock_np(&c->inq_lock);
		return -c->inq_err;
	}

	/* pop an mbuf and deliver the payload */
	m = mbufq_pop_head(&c->inq);
	c->inq_len--;
	spin_unlock_np(&c->inq_lock);

	ret = min(len, mbuf_length(m));
	memcpy(buf, mbuf_data(m), ret);
	if (raddr) {
		struct ip_hdr *iphdr = mbuf_network_hdr(m, *iphdr);
		struct homa_hdr *homahdr = mbuf_transport_hdr(m, *homahdr);
		raddr->ip = ntoh32(iphdr->saddr);
		raddr->port = ntoh16(homahdr->src_port);
//		if (c->e.match == TRANS_MATCH_5TUPLE) {
//			assert(c->e.raddr.ip == raddr->ip &&
//			       c->e.raddr.port == raddr->port);
//		}
	}
	mbuf_free(m);
	return ret;
}

static void homa_tx_release_mbuf(struct mbuf *m)
{
	homaconn_t *c = (homaconn_t *)m->release_data;
	thread_t *th = NULL;
	bool free_conn;

	spin_lock_np(&c->outq_lock);
	c->outq_len--;
	free_conn = (c->outq_free && c->outq_len == 0);
	if (!c->shutdown)
		th = waitq_signal(&c->outq_wq, &c->outq_lock);
	spin_unlock_np(&c->outq_lock);
	waitq_signal_finish(th);

	net_tx_release_mbuf(m);
	if (free_conn)
		homa_release_conn(c);
}

ssize_t homa_recv(homaconn_t *c, void *buf, size_t len, struct netaddr *raddr)
{
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
	int ret;
	struct mbuf *m;
	void *payload;

	if (len > HOMA_MAX_PAYLOAD)
		return -EMSGSIZE;

	spin_lock_np(&c->outq_lock);

	/* is the socket shutdown? */
	if (c->shutdown) {
		spin_unlock_np(&c->outq_lock);
		return -EPIPE;
	}

	c->outq_len++;
	spin_unlock_np(&c->outq_lock);

	m = net_tx_alloc_mbuf();
	if (unlikely(!m))
		return -ENOBUFS;

	/* write datagram payload */
	payload = mbuf_put(m, len);
	memcpy(payload, buf, len);

	/* override mbuf release method */
	m->release = homa_tx_release_mbuf;
	m->release_data = (unsigned long)c;

	ret = homa_send_raw(m, len, c->c_e.laddr, raddr);
	if (unlikely(ret)) {
		net_tx_release_mbuf(m);
		return ret;
	}

	return 0;
}

void __homa_shutdown(homaconn_t *c)
{
	spin_lock_np(&c->inq_lock);
	spin_lock_np(&c->outq_lock);
	BUG_ON(c->shutdown);
	c->shutdown = true;
	spin_unlock_np(&c->outq_lock);
	spin_unlock_np(&c->inq_lock);

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

	/* wake all blocked threads */
	if (!c->inq_err)
		waitq_release(&c->inq_wq);
	waitq_release(&c->outq_wq);
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
	bool free_conn;

	if (!c->shutdown)
		__homa_shutdown(c);

	BUG_ON(!waitq_empty(&c->inq_wq));
	BUG_ON(!waitq_empty(&c->outq_wq));

	/* free all in-flight mbufs */
	while (true) {
		struct mbuf *m = mbufq_pop_head(&c->inq);
		if (!m)
			break;
		mbuf_free(m);
	}

	spin_lock_np(&c->outq_lock);
	free_conn = c->outq_len == 0;
	c->outq_free = true;
	spin_unlock_np(&c->outq_lock);

	if (free_conn)
		homa_release_conn(c);
}