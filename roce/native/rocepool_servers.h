/* rocepool_servers.h -- native C OST / MDT servers on top of roce_transport.
 * See rocepool_servers.c for wire formats and rationale. */
#ifndef ROCEPOOL_SERVERS_H
#define ROCEPOOL_SERVERS_H

#include <stddef.h>
#include <stdint.h>

#include "roce_transport.h"

#ifdef __cplusplus
extern "C" {
#endif

#define ROCEPOOL_NATIVE_OST 1
#define ROCEPOOL_NATIVE_MDT 2

typedef struct rocepool_native rocepool_native_t;

/* Start a native server of `kind` on `bind_address` ("host:port"). NULL on
 * failure (roce_last_error() says why). */
rocepool_native_t *rocepool_native_start(int kind, const char *bind_address, size_t max_frame_bytes);
int rocepool_native_local_address(rocepool_native_t *n, char *out, size_t out_len);
/* OST: stored objects/bytes; MDT: entries; both: requests served. */
void rocepool_native_stats(rocepool_native_t *n, uint64_t *objects, uint64_t *bytes, uint64_t *ops);
void rocepool_native_stop(rocepool_native_t *n);

#ifdef __cplusplus
}
#endif

#endif
