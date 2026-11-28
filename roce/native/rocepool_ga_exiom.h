/* rocepool_ga_exiom.h -- internal: the exiom backend of the global address
 * client library (rocepool_ga.c dispatches to it; see rocepool_ga.h). */
#ifndef ROCEPOOL_GA_EXIOM_H
#define ROCEPOOL_GA_EXIOM_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#include "roce_transport.h"
#include "rocepool_devmem.h"

/* a registered buffer (rpga_register_dev / rpga_register_host) */
struct rpga_mem {
    uint8_t *base;
    size_t len;
    roce_mem_t *mem;    /* NULL: transfers bounce */
    int fd;
    const rp_devrt_t *rt;
    atomic_int broken;
};

typedef struct gx gx_t;

/* "exiom://tenant@host:port|host:port..." -> NULL when not one */
gx_t *gx_open(const char *spec);
void gx_close(gx_t *g);
int gx_is_spec(const char *spec);

/* slots are the region numbers of the addresses (1..65535) */
int gx_alloc(gx_t *g, const char *name, uint64_t size, int exclusive, uint32_t *slot);
int gx_stage(gx_t *g, const char *name, uint64_t size, uint32_t *slot);
int gx_commit(gx_t *g, uint32_t slot);
int gx_lookup(gx_t *g, const char *name, uint32_t *slot, uint64_t *size);
int gx_free(gx_t *g, uint32_t slot);
int gx_size(gx_t *g, uint32_t slot, uint64_t *size);
/* *direct: 1 when the NIC moved the bytes straight to/from the buffer */
int gx_read(gx_t *g, uint32_t slot, uint64_t off, struct rpga_mem *m, size_t moff, size_t len, int *direct);
int gx_write(gx_t *g, uint32_t slot, uint64_t off, struct rpga_mem *m, size_t moff, size_t len, int *direct);
void gx_path(gx_t *g, int *endpoint, uint64_t *failovers, uint64_t *requests);

#endif
