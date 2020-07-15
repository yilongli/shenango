/*
 * lrpc.h - shared memory communication channels
 *
 * This design is inspired by Barrelfish, which in turn was based on Brian
 * Bershad's earlier LRPC work. The goal here is to minimize cache misses to
 * the maximum extent possible.
 */

#pragma once

#include <base/stddef.h>
#include <base/assert.h>
#include <base/atomic.h>

/* LRPC message format */
struct lrpc_msg {
	uint64_t	cmd;            /* command */
	unsigned long	payload;    /* payload: an integer or pointer */
};

#define LRPC_DONE_PARITY	(1UL << 63)
#define LRPC_CMD_MASK		(~LRPC_DONE_PARITY)


/*
 * Egress Channel Support
 *
 * The core data structure of an egress channel is a circular buffer.
 * This struct only holds the metadata of the egress channel; the circular
 * buffer is owned somewhere else.
 */

struct lrpc_chan_out {
	struct lrpc_msg	*tbl;       /* circular buffer holding outgoing lrpc msgs */
	uint32_t	*recv_head_wb;  /* next slot to dequeue a msg by the receiver */
	uint32_t	send_head;      /* next slot to enqueue a msg by the sender */
	uint32_t	send_tail;      /* cache of @recv_head_wb (often outdated) */
	uint32_t	size;           /* # slots in @tbl; must be power of two */
	uint32_t	pad;            /* pad to 4 cache lines */
};

extern bool __lrpc_send(struct lrpc_chan_out *chan, uint64_t cmd,
			unsigned long payload);

/**
 * lrpc_send - sends a message on the channel
 * @chan: the egress channel
 * @cmd: the command to send
 * @payload: the data payload
 *
 * Returns true if successful, otherwise the channel is full.
 */
static inline bool lrpc_send(struct lrpc_chan_out *chan, uint64_t cmd,
			     unsigned long payload)
{
	struct lrpc_msg *dst;

	assert(!(cmd & LRPC_DONE_PARITY));

	if (unlikely(chan->send_head - chan->send_tail >= chan->size))
		return __lrpc_send(chan, cmd, payload);

	dst = &chan->tbl[chan->send_head & (chan->size - 1)];
	cmd |= (chan->send_head++ & chan->size) ? 0 : LRPC_DONE_PARITY;
	dst->payload = payload;
	store_release(&dst->cmd, cmd);
	return true;
}

/**
 * lrpc_get_cached_send_window - retrieves the last known number of slots
 * available for sending
 * @chan: the egress channel
 *
 * This function doesn't cause coherence traffic but may return out of date
 * information. First call lrpc_poll_send_tail() to get the latest status.
 *
 * Returns the last known number of slots left available for sending.
 */
static inline uint32_t lrpc_get_cached_send_window(struct lrpc_chan_out *chan)
{
	return chan->size - chan->send_head + chan->send_tail;
}

/**
 * lrpc_get_cached_length - retrieves the number of queued messages
 * @chan: the egress channel
 *
 * This function doesn't cause coherence traffic but may return out of date
 * information. First call lrpc_poll_send_tail() to get the latest status.
 *
 * Returns the number of messages queued in the channel.
 */
static inline uint32_t lrpc_get_cached_length(struct lrpc_chan_out *chan)
{
	return chan->send_head - chan->send_tail;
}

/**
 * lrpc_poll_send_tail - gets the latest send tail (updating the channel)
 * @chan: the egress channel
 *
 * Returns the raw send tail.
 */
static inline uint32_t lrpc_poll_send_tail(struct lrpc_chan_out *chan)
{
	chan->send_tail = load_acquire(chan->recv_head_wb);
	return chan->send_tail;
}

extern int lrpc_init_out(struct lrpc_chan_out *chan, struct lrpc_msg *tbl,
			 unsigned int size, uint32_t *recv_head_wb);


/*
 * Ingress Channel Support
 */

struct lrpc_chan_in {
	struct lrpc_msg	*tbl;
	uint32_t	*recv_head_wb;
	uint32_t	recv_head;
	uint32_t	size;
};

/**
 * lrpc_recv - receives a message on the channel
 * @chan: the ingress channel
 * @cmd_out: a pointer to store the received command
 * @payload_out: a pointer to store the received payload
 *
 * Returns true if successful, otherwise the channel is empty.
 */
static inline bool lrpc_recv(struct lrpc_chan_in *chan, uint64_t *cmd_out,
			     unsigned long *payload_out)
{
        struct lrpc_msg *m = &chan->tbl[chan->recv_head & (chan->size - 1)];
        uint64_t parity = (chan->recv_head & chan->size) ?
			  0 : LRPC_DONE_PARITY;
	uint64_t cmd;

	cmd = load_acquire(&m->cmd);
        if ((cmd & LRPC_DONE_PARITY) != parity)
		return false;
	chan->recv_head++;

	*cmd_out = cmd & LRPC_CMD_MASK;
	*payload_out = m->payload;
	store_release(chan->recv_head_wb, chan->recv_head);
	return true;
}

/**
 * lrpc_empty - returns true if the channel has no available messages
 * @chan: the ingress channel
 */
static inline bool lrpc_empty(struct lrpc_chan_in *chan)
{
	struct lrpc_msg *m = &chan->tbl[chan->recv_head & (chan->size - 1)];
	uint64_t parity = (chan->recv_head & chan->size) ?
			  0 : LRPC_DONE_PARITY;
	return (ACCESS_ONCE(m->cmd) & LRPC_DONE_PARITY) != parity;
}

extern int lrpc_init_in(struct lrpc_chan_in *chan, struct lrpc_msg *tbl,
			unsigned int size, uint32_t *recv_head_wb);
