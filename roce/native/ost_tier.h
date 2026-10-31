/* The OST's capacity tier: objects live on an NVMe (or any file / block
 * device) behind the in-memory index, RAM holds a bounded cache of them.
 *
 * Backends (ROCEPOOL_OST_TIER):
 *   file:PATH[:SIZE_GIB]   a regular file (grown to SIZE_GIB, default 16) or
 *                          a block device (its size), through the kernel:
 *                          O_DIRECT for page-aligned transfers -- the NIC's
 *                          buffer goes to the NVMe with no copy, and from a
 *                          P2P (CMB) buffer with no host memory at all --
 *                          buffered I/O otherwise
 *   spdk:TRADDR[:NSID]     SPDK's userspace NVMe driver (polled queue pair
 *                          per thread), built in only with -DROCEPOOL_SPDK
 *
 * Space is handed out in slabs of fixed size classes (4 KiB .. 16 MiB),
 * carved from 64 MiB zones assigned to a class on demand; freed slabs go
 * back to their class. One object = one slab; a partial write rewrites the
 * touched LBA range from the object's RAM copy. */
#ifndef OST_TIER_H
#define OST_TIER_H
#include <stddef.h>
#include <stdint.h>

typedef struct ost_tier ost_tier_t;

typedef struct {
    uint64_t off;    /* byte offset on the device */
    uint32_t cls;    /* size class index; capacity = ost_tier_class_bytes(cls) */
} tier_ext_t;

#define TIER_PAGE 4096u

ost_tier_t *ost_tier_open(const char *spec, char *err, size_t err_len);
void ost_tier_close(ost_tier_t *t);
const char *ost_tier_kind(const ost_tier_t *t);           /* "file" | "spdk" */
uint64_t ost_tier_capacity(const ost_tier_t *t);
uint64_t ost_tier_used(const ost_tier_t *t);
int ost_tier_direct(const ost_tier_t *t);                /* 1: page-aligned buffers go to the device without a copy */
size_t ost_tier_class_bytes(uint32_t cls);
uint32_t ost_tier_class_for(size_t size);                /* UINT32_MAX if too big for any class */

int ost_tier_alloc(ost_tier_t *t, size_t size, tier_ext_t *ext);   /* -ENOSPC when full */
void ost_tier_free(ost_tier_t *t, const tier_ext_t *ext);
/* write buf[0..len) at byte offset `off` of the object (within its slab);
 * any alignment -- the backend rounds to its LBA size, reading the edges
 * back when it must */
int ost_tier_write(ost_tier_t *t, const tier_ext_t *ext, uint64_t off, const void *buf, size_t len);
int ost_tier_read(ost_tier_t *t, const tier_ext_t *ext, uint64_t off, void *buf, size_t len);
/* page-aligned buffer, offset and length (multiples of TIER_PAGE): the
 * device's DMA engine takes/puts the bytes straight from/to `buf` */
int ost_tier_write_direct(ost_tier_t *t, const tier_ext_t *ext, uint64_t off, const void *buf, size_t len);
int ost_tier_read_direct(ost_tier_t *t, const tier_ext_t *ext, uint64_t off, void *buf, size_t len);

/* the descriptor whose fdatasync makes this tier's writes durable (the
 * file backend; -1 for SPDK, which has no sync here yet) */
int ost_tier_sync_fd(const ost_tier_t *t);
/* after a restart: mark these slabs (the objects the OST's log lists) as
 * used and rebuild the allocator around them. Zones below the highest one
 * used that hold none of them are not reused (their class is unknown). */
int ost_tier_restore(ost_tier_t *t, const tier_ext_t *used, size_t n);

/* DMA-able memory for the RAM copies: plain malloc for the file backend,
 * spdk_dma_malloc for SPDK (its NVMe driver can only move registered or
 * hugepage-backed memory) */
void *ost_tier_buf_alloc(ost_tier_t *t, size_t size);
void ost_tier_buf_free(ost_tier_t *t, void *p, size_t size);
/* tell the backend about memory it will see in direct transfers (the
 * transport's registered buffer pool): spdk_mem_register for SPDK */
void ost_tier_notify_chunk(ost_tier_t *t, void *base, size_t len);

#endif
