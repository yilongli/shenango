/*
 * homa.h - Homa sockets
 */

#pragma once

#include <base/types.h>
#include <runtime/net.h>
#include <sys/uio.h>
#include <Homa/Shenango.h>

/*
 * Homa Socket API
 */

struct homaconn;
typedef struct homaconn homaconn_t;

extern homa_trans homa;

/* the maximum size of a Homa packet payload */
#define HOMA_MAX_PAYLOAD 1476

extern int homa_open(struct netaddr laddr, homaconn_t **c_out);
extern struct netaddr homa_local_addr(homaconn_t *c);
extern void homa_shutdown(homaconn_t *c);
extern void homa_close(homaconn_t *c);

/*
 * High-level synchronous API
 */
extern int homa_sendmsg(homaconn_t *c, const void *buf, size_t len,
        struct netaddr raddr, int *status);
extern homa_inmsg homa_recvmsg(homaconn_t *c, struct netaddr *raddr);

// TODO: what about APIs corresponding to select/poll/epoll?

// TODO: implement RPC on top of msg-based API in another file homa_rpc.c