/*
 * homa.h - Homa sockets
 */

#pragma once

#include <base/types.h>
#include <net/homa.h>
#include <runtime/net.h>
#include <sys/uio.h>

// FIXME(Yilong): remove this
/* the maximum size of a HOMA payload */
#define HOMA_MAX_PAYLOAD 1472

/*
 * Homa Socket API
 */

struct homaconn;
typedef struct homaconn homaconn_t;

extern int homa_open(struct netaddr laddr, homaconn_t **c_out);
extern int homa_bind(homaconn_t *c, uint16_t port);
extern struct netaddr homa_client_addr(homaconn_t *c);
extern struct netaddr homa_server_addr(homaconn_t *c);
extern int homa_set_buffers(homaconn_t *c, int read_mbufs, int write_mbufs);
extern ssize_t homa_recv(homaconn_t *c, void *buf, size_t len,
			     struct netaddr *raddr);
extern ssize_t homa_reply(homaconn_t *c, const void *buf, size_t len,
			    const struct netaddr *raddr);
extern int homa_send(homaconn_t *c, const void *buf, size_t len,
                     struct netaddr raddr);
extern void homa_shutdown(homaconn_t *c);
extern void homa_close(homaconn_t *c);