/*
 * homa.h - Homa sockets
 */

#pragma once

#include <base/types.h>
#include <runtime/net.h>
#include <sys/uio.h>

/*
 * Homa Socket API
 */

struct homaconn;
typedef struct homaconn homaconn_t;

extern void* homa_trans;

/* the maximum size of a Homa packet payload */
#define HOMA_MAX_PAYLOAD 1476

extern void* homa_tx_alloc_mbuf(void);

extern int homa_open(struct netaddr laddr, homaconn_t **c_out);
extern int homa_bind(homaconn_t *c, uint16_t port);
extern struct netaddr homa_client_addr(homaconn_t *c);
extern struct netaddr homa_server_addr(homaconn_t *c);
// TODO: do we really need the "id" argument in the following methods?
// how is the RPC layer implemented atop the msg layer in Collin's impl.?
extern ssize_t homa_recv(homaconn_t *c, void *buf, size_t len,
                         struct netaddr *raddr, uint64_t *id);
extern int homa_reply(homaconn_t *c, const void *buf, size_t len,
                      struct netaddr raddr, uint64_t id);
extern int homa_send(homaconn_t *c, const void *buf, size_t len,
                     struct netaddr raddr, uint64_t *id);
extern void homa_shutdown(homaconn_t *c);
extern void homa_close(homaconn_t *c);

// TODO: should we provide one or two sets of APIs (msg & RPC)? if just one, msg or RPC?