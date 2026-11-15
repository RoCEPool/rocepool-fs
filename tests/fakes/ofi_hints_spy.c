/* TEST-ONLY LD_PRELOAD (tests/test_ofi_fabric.py): records every fi_getinfo hints the process asks for
 * (provider, fabric, caps, mr_mode) to $OFI_HINTS_SPY, then lets the real
 * fi_getinfo answer. What the transport asks the EFA provider for is checked
 * on a machine without one: fabric "efa" (never efa-direct), and FI_MR_HMEM
 * whenever FI_HMEM. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <rdma/fabric.h>
#include <rdma/fi_domain.h>

typedef int (*gi_t)(uint32_t, const char *, const char *, uint64_t, const struct fi_info *, struct fi_info **);

int fi_getinfo(uint32_t version, const char *node, const char *service, uint64_t flags,
               const struct fi_info *hints, struct fi_info **info) {
    const char *out = getenv("OFI_HINTS_SPY");
    if (out && hints) {
        FILE *f = fopen(out, "a");
        if (f) {
            fprintf(f, "{\"prov\": \"%s\", \"fabric\": %s%s%s, \"hmem\": %d, \"mr_hmem\": %d}\n",
                    hints->fabric_attr && hints->fabric_attr->prov_name ? hints->fabric_attr->prov_name : "",
                    hints->fabric_attr && hints->fabric_attr->name ? "\"" : "",
                    hints->fabric_attr && hints->fabric_attr->name ? hints->fabric_attr->name : "null",
                    hints->fabric_attr && hints->fabric_attr->name ? "\"" : "",
                    (hints->caps & FI_HMEM) != 0,
                    hints->domain_attr && (hints->domain_attr->mr_mode & FI_MR_HMEM) != 0);
            fclose(f);
        }
    }
    gi_t real = (gi_t)dlsym(RTLD_NEXT, "fi_getinfo");
    return real ? real(version, node, service, flags, hints, info) : -FI_ENOSYS;
}
