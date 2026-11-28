/* rocepool_ga.h -- the global address client library: the memory of all
 * the OSTs as one address space, read and written by address, straight
 * into and out of GPU memory.
 *
 * A region is memory an OST sets aside on request (rpga_alloc): it never
 * moves, is never evicted or written to the tier, and stays until freed or
 * its OST process ends -- it is NOT durable. Its bytes have global
 * addresses:
 *
 *     bits 63..56  OST (its place in the OST list given to rpga_open)
 *     bits 55..40  region (the OST's number for it)
 *     bits 39..0   byte offset in the region (regions up to 1 TiB)
 *
 * so 256 OSTs (128 nodes x 2) and 65535 regions each. Any process given the
 * same OST list, in the same order, reaches the same bytes by the same
 * address; a name (rpga_lookup) is how another process finds a region.
 *
 * rpga_read: one RDMA READ by this host's NIC from the OST's memory into
 * the registered buffer -- GPU memory (GPUDirect: dma-buf or
 * nvidia-peermem) or host memory -- with no lookup, no check and no CPU
 * on the OST. Region descriptors are cached per handle after first use.
 * rpga_write: one request whose data the OST pulls (RDMA READ) out of the
 * buffer and copies into the region. Where the NIC cannot reach the buffer,
 * or the transport cannot publish a region, both go through host memory
 * and RPCs instead (rpga_mem_direct, rpga_stats say which).
 *
 * Consistency is the caller's: there is no lock and no version. A read
 * racing a write to the same bytes may see a mix; ordering between
 * processes is by the caller's own signalling. A freed region is
 * deregistered first: a read of it fails (-ENOENT), it does not return
 * another region's bytes -- until its region number is handed out again,
 * after 65535 more allocations on that OST: an address must not be kept
 * past its region's free.
 *
 * Return values: 0, or a negative errno: -ENOENT (no such region), -EEXIST
 * (rpga_alloc exclusive, the name exists), -ERANGE (past the region's end,
 * or a size out of range), -ENOSPC (the OST's ROCEPOOL_OST_GA_MIB is used
 * up), -EINVAL, -EIO (transport/server failure). Thread-safe. */
#ifndef ROCEPOOL_GA_H
#define ROCEPOOL_GA_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t rpga_addr_t;
#define RPGA_OFFSET_BITS 40
#define RPGA_MAKE(ost, region, off) \
    (((uint64_t)(ost) << 56) | ((uint64_t)((region) & 0xffff) << RPGA_OFFSET_BITS) | ((uint64_t)(off) & ((1ull << RPGA_OFFSET_BITS) - 1)))
#define RPGA_OST(a) ((int)((a) >> 56))
#define RPGA_REGION(a) ((uint32_t)(((a) >> RPGA_OFFSET_BITS) & 0xffff))
#define RPGA_OFFSET(a) ((uint64_t)(a) & ((1ull << RPGA_OFFSET_BITS) - 1))

typedef struct rpga rpga_t;
typedef struct rpga_mem rpga_mem_t;

/* osts: "host:port,host:port,..." (at most 256) */
rpga_t *rpga_open(const char *osts);
void rpga_close(rpga_t *ga);
int rpga_osts(const rpga_t *ga);

/* a region of `size` bytes (zeroed) on OST `ost` (< 0: the OST its name
 * hashes to, or round robin when unnamed). name may be NULL; a named region
 * that exists is opened instead (-EEXIST with exclusive, -ERANGE if it is
 * smaller). *addr: its first byte. */
int rpga_alloc(rpga_t *ga, int ost, const char *name, uint64_t size, int exclusive, rpga_addr_t *addr);
/* the named region (ost < 0: where its name hashes to); size may be NULL */
int rpga_lookup(rpga_t *ga, int ost, const char *name, rpga_addr_t *addr, uint64_t *size);
/* the region containing addr; its address dies with it */
int rpga_free(rpga_t *ga, rpga_addr_t addr);
int rpga_region_size(rpga_t *ga, rpga_addr_t addr, uint64_t *size);

/* buffers: GPU memory (the device runtime of rocepool_devmem.h: CUDA, or
 * the emulator under ROCEPOOL_DEV_EMU=1) or host memory, registered with the
 * NIC once; NULL if base is not device memory / no runtime */
rpga_mem_t *rpga_register_dev(rpga_t *ga, void *base, size_t len);
rpga_mem_t *rpga_register_host(rpga_t *ga, void *base, size_t len);
void rpga_deregister(rpga_mem_t *m);
int rpga_mem_direct(const rpga_mem_t *m);   /* 1: the NIC reaches it; 0: transfers bounce */

/* len bytes between the global address and the buffer at byte offset off */
int rpga_read(rpga_t *ga, rpga_addr_t src, rpga_mem_t *m, size_t off, size_t len);
int rpga_write(rpga_t *ga, rpga_addr_t dst, rpga_mem_t *m, size_t off, size_t len);
/* many at once, every OST in parallel; status[i] per transfer. Returns 0,
 * or the first failure */
int rpga_mread(rpga_t *ga, size_t n, const rpga_addr_t *src, rpga_mem_t *m, const size_t *offs, const size_t *lens,
               int *status);
int rpga_mwrite(rpga_t *ga, size_t n, const rpga_addr_t *dst, rpga_mem_t *m, const size_t *offs, const size_t *lens,
                int *status);

/* bytes read one-sided / through RPCs, bytes written pulled by the OST /
 * through host memory, round trips */
void rpga_stats(rpga_t *ga, uint64_t *read_direct, uint64_t *read_rpc, uint64_t *write_direct, uint64_t *write_bounced,
                uint64_t *rpcs);

#ifdef __cplusplus
}
#endif
#endif
