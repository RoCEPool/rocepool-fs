/* roce_transport.h -- minimal C API surface for the RoCE (RDMA over
 * Converged Ethernet) transport, called from Python via ctypes.
 *
 * DESIGN NOTE: all RDMA complexity (connection management, queue pairs,
 * memory registration, completion polling) lives in roce_transport.c,
 * written against the real librdmacm (<rdma/rdma_cma.h>) and libibverbs
 * (<infiniband/verbs.h>) headers -- not reimplemented via ctypes struct
 * layouts guessed from memory. Python only sees the handful of functions
 * below. This is deliberate: getting a struct's field order or padding
 * wrong in a hand-written ctypes binding is a silent, hard-to-diagnose
 * memory-corruption bug; writing ordinary C against the real headers and
 * letting the real compiler check it is the correct way to interface with
 * a low-level verbs API, and it's how virtually all real RDMA software
 * (rdma-core's own examples included) is actually written.
 *
 * Wire compatibility: every frame exchanged here is produced/consumed by
 * roce/protocol.py's Frame.encode()/decode() -- same 24-byte header, same
 * field layout, same bytes that roce/loopback_transport.py carries over
 * plain TCP. That is what makes this transport a drop-in replacement for
 * the loopback one from pool_node.py's point of view.
 *
 * Transport primitive used: two-sided SEND/RECV verbs (IBV_WR_SEND /
 * ibv_post_recv), not one-sided RDMA_WRITE/RDMA_READ. This matches the
 * loopback transport's request/response semantics exactly (a peer must
 * be actively listening to consume a SEND, same as accept()+recv() over
 * TCP), so pool_node.py's logic needs no transport-specific knowledge.
 * One-sided RDMA WRITE/READ for the bulk KV data path is a real, natural
 * next optimization (it is what lets a remote GPU pull a value without
 * interrupting the owning node's CPU at all) but is a deliberate
 * non-goal of this initial transport -- see docs/ROCE_DEPLOYMENT_NOTES.md.
 *
 * TESTING STATUS: written against the public RoCEv2 / rdma-core API
 * surface; compiled and run between two network nodes against softverbs
 * (a userspace RC emulator -- roce/native/softverbs/, driven by
 * tools/run_crossover_rdma.py), but NEVER run on a real RDMA NIC. The
 * authoring environment has no RDMA-capable NIC, no /dev/infiniband, no
 * rdma_rxe module and no rdma-core-dev package (confirmed by direct
 * inspection). Validate on real RoCE hardware before trusting it -- see
 * build.sh and docs/ROCE_DEPLOYMENT_NOTES.md.
 */
#ifndef ROCE_TRANSPORT_H
#define ROCE_TRANSPORT_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Every function returns 0 on success, a negative errno-shaped value on
 * failure. roce_last_error() returns a static, thread-local human-readable
 * string describing the most recent failure on the calling thread -- call
 * it immediately after a non-zero return, before any other roce_* call. */
const char *roce_last_error(void);

/* -------------------- client side -------------------- */

typedef struct roce_conn roce_conn_t;

/* Resolve `peer` (format "host:port"), establish an RC (Reliable
 * Connected) queue pair to it over RDMA_PS_TCP port space (this is what
 * makes the same port space work identically over RoCE or InfiniBand --
 * rdma_cma abstracts the fabric), and register a fixed-size bounce buffer
 * for SEND/RECV. `max_frame_bytes` bounds both directions; a frame larger
 * than this fails rather than silently truncating.
 *
 * Blocks until connected or `timeout_ms` elapses. Returns NULL and sets
 * roce_last_error() on failure. */
roce_conn_t *roce_client_connect(const char *peer, int timeout_ms, size_t max_frame_bytes);

/* Send `request` (request_len bytes, a fully-encoded roce.protocol.Frame)
 * and block for the matching response. On success, writes the response
 * into `*response_out` (caller must call roce_free_buffer on it) and its
 * length into `*response_len_out`. Returns 0 on success. */
int roce_client_send_request(roce_conn_t *conn, const uint8_t *request, size_t request_len,
                              uint8_t **response_out, size_t *response_len_out, int timeout_ms);

/* Returned by roce_client_send_request when the request provably never
 * reached the peer (post_send failed, or the SEND was flushed with
 * IBV_WC_WR_FLUSH_ERR) -- the only failure after which re-sending a
 * non-idempotent request on a NEW connection is safe. The connection is
 * dead either way; close it. Any other failure returns -1. */
#define ROCE_ERR_NOT_DELIVERED (-2)

void roce_client_close(roce_conn_t *conn);

/* 1 if nothing indicates the connection is dead, 0 if the peer has already
 * disconnected (pending CM event, or its posted RECV was flushed). Cheap and
 * non-blocking: connection pools call it before reusing an idle connection. */
int roce_client_is_alive(roce_conn_t *conn);

/* Like roce_client_connect, but for request/response use only
 * (roce_client_send_request): the connection owns no max_frame_bytes
 * buffers; each request borrows them from a process-wide registered pool.
 * Stream calls on such a connection fail. */
roce_conn_t *roce_client_connect_shared(const char *peer, int timeout_ms, size_t max_frame_bytes);

/* Process-wide registered buffer pool totals (all devices/frame sizes). */
void roce_bufpool_stats(uint64_t *registered_bytes, uint32_t *buffers, uint32_t *in_use);

/* Memory for a SERVER's buffer pool chunks (where request bulk lands and
 * replies are built), set before roce_server_start: alloc returns `len`
 * bytes (page aligned) or NULL to use anonymous memory; free releases what
 * alloc returned when the NIC refuses to register it; notify is told of
 * every chunk the server registers (whatever its origin). An OST uses them
 * for an NVMe's CMB (P2P: the NIC writes into the drive's own memory) and
 * for SPDK's DMA registration. */
typedef void *(*roce_chunk_alloc_fn)(size_t len, void *user);
typedef void (*roce_chunk_free_fn)(void *base, size_t len, void *user);
typedef void (*roce_chunk_notify_fn)(void *base, size_t len, void *user);
void roce_set_chunk_hooks(roce_chunk_alloc_fn alloc, roce_chunk_free_fn free_fn, roce_chunk_notify_fn notify, void *user);

/* -------------------- zero-copy request/response --------------------
 *
 * A request is `head` (copied into the SEND) followed by up to 4 segments.
 * A segment whose `mem` is a region from roce_mem_register is RDMA READ by
 * the server straight from where it is -- no copy on this side; one
 * without `mem` is copied into a registered buffer first. The reply comes
 * back in `reply` -- on a shared-pool connection the registered buffer the
 * server wrote it into (no copy), which the caller returns with
 * roce_reply_release. Registration is per RDMA device and lazy: a region
 * registered once serves every connection of the process. */
typedef struct roce_mem roce_mem_t;
roce_mem_t *roce_mem_register(void *addr, size_t len);
void roce_mem_release(roce_mem_t *mem);

/* Device (GPU) memory: `addr` is the device virtual address the
 * application uses (a CUDA device pointer), `len` bytes of it. With
 * dmabuf_fd >= 0 the range is the dma-buf `dmabuf_fd` from `dmabuf_offset`
 * (cuMemGetHandleForAddressRange(CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD)) and
 * is registered with ibv_reg_dmabuf_mr (verbs) / FI_MR_DMABUF (libfabric);
 * with -1 the device pointer itself is registered (verbs with the
 * nvidia-peermem module; libfabric FI_HMEM_CUDA). The NIC moves the bytes;
 * the CPU never touches the range: a request whose segment is device
 * memory always goes as a registered segment the server RDMA READs, never
 * copied inline, and an RDMA READ into it lands directly in the GPU.
 * cuda_device: the CUDA ordinal (libfabric needs it; verbs ignores it). */
roce_mem_t *roce_mem_register_device(void *addr, size_t len, int dmabuf_fd, uint64_t dmabuf_offset, int cuda_device);
int roce_mem_is_device(const roce_mem_t *mem);

typedef struct { const void *addr; size_t len; roce_mem_t *mem; } roce_seg_t;
typedef struct { uint8_t *data; size_t len; void *pool; void *buf; } roce_reply_t;

int roce_client_request(roce_conn_t *conn, const void *head, size_t head_len, const roce_seg_t *segs, int nsegs,
                        roce_reply_t *reply, int timeout_ms);
void roce_reply_release(roce_reply_t *reply);

/* -------------------- one-sided reads --------------------
 * The peer's CPU is not involved: the client RDMA READs remote memory the
 * server published (roce_server_mem_rkey) into its own registered memory.
 * Up to 3 remote ranges in one call, executed in order on the connection
 * (an RC QP performs its work requests in order, so a seqlock laid out as
 * header / data / trailer is read in that order) and completed as one.
 * Returns 0, or -1 with the error set (ROCE_ERR_NOT_DELIVERED when the
 * connection was already dead). */
typedef struct { uint64_t raddr; uint32_t rkey; uint32_t len; roce_mem_t *mem; void *local; } roce_read_t;
/* 1: the ranges of one roce_client_rdma_read are executed in order (RC
 * queue pairs: this transport); 0: concurrently, in no particular order
 * (reliable-datagram fabrics: ofi_transport.c on EFA). Callers that read a
 * seqlock check it accordingly (rocepool_client.c). */
int roce_transport_reads_ordered(void);
int roce_client_rdma_read(roce_conn_t *conn, const roce_read_t *reads, int n, int timeout_ms);
/* server side: the key a client needs to RDMA READ `mem` (registered on
 * the server's RDMA device on first use; -1 until a connection exists) */
struct roce_server;
int roce_server_mem_rkey(struct roce_server *server, roce_mem_t *mem, uint32_t *rkey);
/* the remote address a client must use for `va` inside `mem`: va itself on
 * fabrics that address registered memory virtually (RC verbs, EFA), the
 * offset into the region on those that do not (libfabric tcp) */
uint64_t roce_server_mem_addr(struct roce_server *server, roce_mem_t *mem, const void *va);

/* For server handlers: a reply buffer that, when larger than a SEND, is a
 * registered buffer the connection thread can RDMA WRITE to the client as
 * is. Every reply the handler returns must come from roce_reply_alloc or
 * malloc; roce_reply_free handles both. */
void *roce_reply_alloc(size_t len);
void roce_reply_free(void *ptr);

/* -------------------- raw duplex stream --------------------
 *
 * Added to back a byte-stream abstraction (roce/roce_transport.py's
 * RoCEStream) over the SAME connection roce_client_connect() already
 * established, so callers that don't fit the strict one-request/
 * one-response shape of roce_client_send_request() -- e.g.
 * wlp/wlp_client.py, which does its own independent sendall()/recv()
 * calls per the WLP wire protocol -- can still run over RDMA. Built
 * entirely from the same post_send/post_recv/poll_for_completion
 * primitives roce_client_send_request() already uses; no new rdma_cma or
 * ibverbs calls. Every SEND/RECV here preserves message boundaries (an
 * RDMA property TCP does not have); RoCEStream on the Python side is
 * responsible for turning that into byte-stream .recv(n) semantics. */

/* Sends `data` as one SEND and blocks for its completion. Returns 0 on
 * success. Does NOT wait for or consume a response -- pair with
 * roce_client_stream_recv() for that, called independently and in
 * whatever order the higher-level protocol needs. */
int roce_client_stream_send(roce_conn_t *conn, const uint8_t *data, size_t len, int timeout_ms);

/* Blocks for the next inbound message on this connection (consuming the
 * RECV that was posted after the previous call, or after connect) and
 * returns it. On success, writes a newly malloc()'d buffer into
 * *data_out (caller must free() it) and its length into *len_out, and
 * re-arms a RECV for the connection's next inbound message before
 * returning. Returns 0 on success. */
int roce_client_stream_recv(roce_conn_t *conn, uint8_t **data_out, size_t *len_out, int timeout_ms);

/* -------------------- server side -------------------- */

typedef struct roce_server roce_server_t;

/* Handler callback: given a decoded request frame's raw bytes, must
 * return a newly-allocated buffer (via roce_alloc_response, so it is
 * registered against the correct PD/MR for the connection it will be
 * sent back on) containing the response frame bytes, and set
 * *response_len to its length. Returning NULL is treated as an internal
 * error and closes that connection. */
typedef uint8_t *(*roce_request_handler)(const uint8_t *request, size_t request_len,
                                          size_t *response_len, void *user_data);

/* Bind to `bind_address` ("host:port", or "0.0.0.0:port" for any
 * interface) and start accepting RC connections in a background thread,
 * dispatching every request to `handler`. Returns NULL and sets
 * roce_last_error() on failure to bind/listen. Mirrors
 * roce.transport_base.Transport.serve()'s non-blocking contract. */
roce_server_t *roce_server_start(const char *bind_address, roce_request_handler handler,
                                  void *user_data, size_t max_frame_bytes);

/* Writes the actual bound "host:port" into `out` (caller-provided buffer
 * of at least `out_len` bytes) -- needed because bind_address may use
 * port 0 for an ephemeral port, same convention as
 * loopback_transport.py's ServerHandle.local_address(). */
int roce_server_local_address(roce_server_t *server, char *out, size_t out_len);

/* Stops accepting new connections, lets requests already received finish
 * sending their replies, then disconnects every live connection and waits
 * for their threads (each phase bounded by env ROCE_SERVER_DRAIN_MS, default
 * 2000), so the handler is not called after this returns. Safe to call once; not safe to call twice or to use
 * `server` afterward (same one-shot contract as
 * loopback_transport.ServerHandle.stop()). */
void roce_server_stop(roce_server_t *server);

/* Transport-level server counters, for a bottleneck analysis: writes them
 * as JSON object members ("\"k\": v, ...", no braces) into out and returns
 * the length; 0 when the backend keeps none. */
size_t roce_server_stats_json(roce_server_t *server, char *out, size_t out_len);

#ifdef __cplusplus
}
#endif

#endif /* ROCE_TRANSPORT_H */
