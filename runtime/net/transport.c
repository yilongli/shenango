/*
 * transport.c - handles transport protocol packets (UDP and TCP)
 */

#include <base/stddef.h>
#include <base/hash.h>
#include <runtime/rculist.h>
#include <runtime/sync.h>
#include <runtime/net.h>
#include <net/ip.h>
#include <runtime/homa.h>

#include "defs.h"

/* size of the transport socket (hash)table */
#define TRANS_TBL_SIZE	16384

/* ephemeral port definitions (IANA suggested range) */
#define MIN_EPHEMERAL		49152
#define MAX_EPHEMERAL		65535

/* a seed value for transport handler table hashing calculations */
static uint32_t trans_seed;

/* a simple counter used to further randomize ephemeral ports */
static uint32_t ephemeral_offset;

static inline uint32_t trans_hash_3tuple(uint8_t proto, struct netaddr laddr)
{
	return hash_crc32c_one(trans_seed,
		(uint64_t)laddr.ip | ((uint64_t)laddr.port << 32) |
		((uint64_t)proto << 48));
}

static inline uint32_t trans_hash_5tuple(uint8_t proto, struct netaddr laddr,
				         struct netaddr raddr)
{
	return hash_crc32c_two(trans_seed,
		(uint64_t)laddr.ip | ((uint64_t)laddr.port << 32),
		(uint64_t)raddr.ip | ((uint64_t)raddr.port << 32) |
		((uint64_t)proto << 48));
}

static DEFINE_SPINLOCK(trans_lock);
/* transport socket (hash)table; each entry is an RCU list */
static struct rcu_hlist_head trans_tbl[TRANS_TBL_SIZE];

/**
 * trans_table_add - adds an entry to the match table
 * @e: the entry to add
 *
 * Returns 0 if successful, or -EADDRINUSE if a conflicting entry is already in
 * the table, or -EINVAL if the local port is zero.
 */
int trans_table_add(struct trans_entry *e)
{
	struct trans_entry *pos;
	struct rcu_hlist_node *node;
	uint32_t idx;

	/* port zero is reserved for ephemeral port auto-assign */
	if (e->laddr.port == 0)
		return -EINVAL;

	assert(e->match == TRANS_MATCH_3TUPLE ||
	       e->match == TRANS_MATCH_5TUPLE);
	if (e->match == TRANS_MATCH_3TUPLE)
		idx = trans_hash_3tuple(e->proto, e->laddr);
	else
		idx = trans_hash_5tuple(e->proto, e->laddr, e->raddr);
	idx %= TRANS_TBL_SIZE;

	spin_lock_np(&trans_lock);
	rcu_hlist_for_each(&trans_tbl[idx], node, true) {
		pos = rcu_hlist_entry(node, struct trans_entry, link);
		if (pos->match != e->match)
			continue;
		if (e->match == TRANS_MATCH_3TUPLE &&
		    e->proto == pos->proto &&
		    e->laddr.ip == pos->laddr.ip &&
		    e->laddr.port == pos->laddr.port) {
			spin_unlock_np(&trans_lock);
			return -EADDRINUSE;
		} else if (e->proto == pos->proto &&
			   e->laddr.ip == pos->laddr.ip &&
			   e->laddr.port == pos->laddr.port &&
			   e->raddr.ip == pos->raddr.ip &&
			   e->raddr.port == pos->raddr.port) {
			spin_unlock_np(&trans_lock);
			return -EADDRINUSE;
		}
	}
	rcu_hlist_add_head(&trans_tbl[idx], &e->link);
	store_release(&ephemeral_offset, ephemeral_offset + 1);
	spin_unlock_np(&trans_lock);

	return 0;
}

/**
 * trans_table_add_with_ephemeral_port - adds an entry to the match table
 * while automatically selecting the local port number
 * @e: the entry to add
 *
 * We use algorithm 3 from RFC 6056.
 *
 * Returns 0 if successful or -EADDRNOTAVAIL if all ports are taken.
 */
int trans_table_add_with_ephemeral_port(struct trans_entry *e)
{
	uint16_t offset, next_ephemeral = 0;
	uint16_t num_ephemeral = MAX_EPHEMERAL - MIN_EPHEMERAL + 1;
	int ret;

	// TODO: why the following check?
//	if (e->match != TRANS_MATCH_5TUPLE)
//		return -EINVAL;

	e->laddr.port = 0;
//	offset = trans_hash_5tuple(e->proto, e->laddr, e->raddr) +
//							load_acquire(&ephemeral_offset);
	offset = load_acquire(&ephemeral_offset);
	if (e->match == TRANS_MATCH_3TUPLE)
	    offset += trans_hash_3tuple(e->proto, e->laddr);
	else
        offset += trans_hash_5tuple(e->proto, e->laddr, e->raddr);
	while (next_ephemeral < num_ephemeral) {
		uint32_t port = MIN_EPHEMERAL +
				(next_ephemeral++ + offset) % num_ephemeral;
		e->laddr.port = port;
		ret = trans_table_add(e);
		if (!ret)
			return 0;
	}

	return -EADDRNOTAVAIL;
}

/**
 * trans_table_lookup - finds an entry from the match table
 * @proto: the transport protocol
 * @laddr: the local address
 * @raddr: the remote address
 *
 * Returns a transport entry if successful or NULL if no match is found.
 */
struct trans_entry *trans_table_lookup(uint8_t proto, struct netaddr laddr,
        struct netaddr raddr)
{
    struct trans_entry *e;
    struct rcu_hlist_node *node;
    uint32_t hash;

    assert(rcu_read_lock_held());

    /* attempt to find a 5-tuple match */
	hash = trans_hash_5tuple(proto, laddr, raddr);
	rcu_hlist_for_each(&trans_tbl[hash % TRANS_TBL_SIZE], node, false) {
		e = rcu_hlist_entry(node, struct trans_entry, link);
		if (e->match != TRANS_MATCH_5TUPLE)
			continue;
		if (e->proto == proto &&
		    e->laddr.ip == laddr.ip && e->laddr.port == laddr.port &&
		    e->raddr.ip == raddr.ip && e->raddr.port == raddr.port) {
			return e;
		}
	}

	/* attempt to find a 3-tuple match */
	hash = trans_hash_3tuple(proto, laddr);
	rcu_hlist_for_each(&trans_tbl[hash % TRANS_TBL_SIZE], node, false) {
		e = rcu_hlist_entry(node, struct trans_entry, link);
		if (e->match != TRANS_MATCH_3TUPLE)
			continue;
		if (e->proto == proto &&
		    e->laddr.ip == laddr.ip && e->laddr.port == laddr.port) {
			return e;
		}
	}

	return NULL;
}

/**
 * trans_table_remove - removes an entry from the match table
 * @e: the entry to remove
 *
 * The caller is responsible for eventually freeing the object with rcu_free().
 */
void trans_table_remove(struct trans_entry *e)
{
	spin_lock_np(&trans_lock);
	rcu_hlist_del(&e->link);
	spin_unlock_np(&trans_lock);
}

/* the first 4 bytes are identical for TCP and UDP */
struct l4_hdr {
	uint16_t sport, dport;
};

/**
 * trans_lookup - finds the target socket (i.e., recipient) of an ingress packet
 * @m: the ingress packet
 * @is_homa_pkt: set by the callee to indicate if the packet belongs to Homa
 *
 * Returns a transport entry if successful or NULL if no match is found or
 * @is_homa_pkt is set to true.
 */
static struct trans_entry *trans_lookup(struct mbuf *m, bool *is_homa_pkt)
{
	const struct ip_hdr *iphdr;
	const struct l4_hdr *l4hdr;
	struct netaddr laddr, raddr;

	assert(rcu_read_lock_held());
	*is_homa_pkt = false;

	/* set up the network header pointers */
	mbuf_mark_transport_offset(m);
	iphdr = mbuf_network_hdr(m, *iphdr);
	if (unlikely(iphdr->proto != IPPROTO_UDP &&
        iphdr->proto != IPPROTO_TCP && iphdr->proto != IPPROTO_HOMA))
		return NULL;
	l4hdr = (struct l4_hdr *)mbuf_data(m);
	if (unlikely(mbuf_length(m) < sizeof(*l4hdr)))
		return NULL;

	/* parse the source and destination network address */
	laddr.ip = ntoh32(iphdr->daddr);
	laddr.port = ntoh16(l4hdr->dport);
	raddr.ip = ntoh32(iphdr->saddr);
	raddr.port = ntoh16(l4hdr->sport);

	/*
	 * Unlike TCP, Homa schedules network packets globally across sockets;
	 * as a result, there is no point to dispatch an ingress packet to its
	 * socket before passing it through the Homa protocol stack. Besides,
	 * L4 headers of Homa control packets are currently zeroed, so they
	 * must be handled directly anyway.
	 */
	if (iphdr->proto == IPPROTO_HOMA) {
        *is_homa_pkt = true;
        homa_trans_proc(homa, (uintptr_t)m, m->data, m->len, raddr.ip);
        return NULL;
	}

	return trans_table_lookup(iphdr->proto, laddr, raddr);
}

/**
 * net_rx_trans - receive L4 packets
 * @ms: an array of mbufs to process
 * @nr: the size of the @ms array
 */
void net_rx_trans(struct mbuf **ms, const unsigned int nr)
{
	int i;
	const struct ip_hdr *iphdr;
	bool is_homa_pkt, has_homa_pkt;

	/* deliver each packet to a L4 protocol handler */
	has_homa_pkt = false;
	for (i = 0; i < nr; i++) {
		struct mbuf *m = ms[i];
		struct trans_entry *e;

		rcu_read_lock();
		e = trans_lookup(m, &is_homa_pkt);
		if (!is_homa_pkt) {
            if (unlikely(!e)) {
                rcu_read_unlock();
                iphdr = mbuf_network_hdr(m, *iphdr);
                if (iphdr->proto == IPPROTO_TCP)
                    tcp_rx_closed(m);
                mbuf_free(m);
                continue;
            }
            e->ops->recv(e, m);
		}
		has_homa_pkt |= is_homa_pkt;
		rcu_read_unlock();
	}

	/* invoke end-of-batch handlers of L4 protocols */
	if (has_homa_pkt) {
	    homa_trans_try_grant(homa);
	}
}

/**
 * trans_error - reports a network error to the L4 layer
 * @m: the mbuf that triggered the error
 * @err: the suggested ernno to report
 */
void trans_error(struct mbuf *m, int err)
{
	struct trans_entry *e;
	bool is_homa_pkt;

    rcu_read_lock();
	e = trans_lookup(m, &is_homa_pkt);
	if (e && e->ops->err)
		e->ops->err(e, err);
	rcu_read_unlock();
}

/**
 * trans_init - initializes transport protocol infrastructure
 *
 * Returns 0 (always successful).
 */
int trans_init(void)
{
	int i;

	spin_lock_init(&trans_lock);

	for (i = 0; i < TRANS_TBL_SIZE; i++)
		rcu_hlist_init_head(&trans_tbl[i]);

	trans_seed = rand_crc32c(0x48FA8BC1 ^ iok.key);
	return 0;
}
