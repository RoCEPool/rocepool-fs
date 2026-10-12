/* Minimal stand-in for <rdma/rdma_cma.h> (librdmacm), covering only the
 * symbols roce_transport.c actually uses. Same disclaimer as
 * infiniband/verbs.h in this same test_stubs/ tree: this is a syntax-check
 * aid for an environment with no rdma-core packages (see
 * roce/native/build.sh), NOT a real implementation. A real build always
 * uses the system's real <rdma/rdma_cma.h>.
 */
#ifndef ROCEPOOL_STUB_RDMA_CMA_H
#define ROCEPOOL_STUB_RDMA_CMA_H

#include <stdint.h>
#include <sys/socket.h>
#include <infiniband/verbs.h>

#ifdef __cplusplus
extern "C" {
#endif

struct rdma_event_channel {
    int fd;
};

struct rdma_cm_id {
    struct ibv_context *verbs;
    struct ibv_qp *qp;
    struct rdma_event_channel *channel;
};

enum rdma_port_space {
    RDMA_PS_TCP = 0x0106,
};

enum rdma_cm_event_type {
    RDMA_CM_EVENT_ADDR_RESOLVED,
    RDMA_CM_EVENT_ADDR_ERROR,
    RDMA_CM_EVENT_ROUTE_RESOLVED,
    RDMA_CM_EVENT_ROUTE_ERROR,
    RDMA_CM_EVENT_CONNECT_REQUEST,
    RDMA_CM_EVENT_CONNECT_RESPONSE,
    RDMA_CM_EVENT_CONNECT_ERROR,
    RDMA_CM_EVENT_UNREACHABLE,
    RDMA_CM_EVENT_REJECTED,
    RDMA_CM_EVENT_ESTABLISHED,
    RDMA_CM_EVENT_DISCONNECTED,
    RDMA_CM_EVENT_DEVICE_REMOVAL,
};

struct rdma_cm_event {
    struct rdma_cm_id *id;
    enum rdma_cm_event_type event;
    int status;
};

struct rdma_conn_param {
    const void *private_data;
    uint8_t private_data_len;
    uint8_t responder_resources;
    uint8_t initiator_depth;
    uint8_t retry_count;
    uint8_t rnr_retry_count;
};

struct rdma_event_channel *rdma_create_event_channel(void);
void rdma_destroy_event_channel(struct rdma_event_channel *channel);

int rdma_create_id(struct rdma_event_channel *channel, struct rdma_cm_id **id,
                    void *context, enum rdma_port_space ps);
int rdma_destroy_id(struct rdma_cm_id *id);

int rdma_resolve_addr(struct rdma_cm_id *id, struct sockaddr *src_addr,
                       struct sockaddr *dst_addr, int timeout_ms);
int rdma_resolve_route(struct rdma_cm_id *id, int timeout_ms);

int rdma_create_qp(struct rdma_cm_id *id, struct ibv_pd *pd, struct ibv_qp_init_attr *qp_init_attr);
int rdma_destroy_qp(struct rdma_cm_id *id);

int rdma_connect(struct rdma_cm_id *id, struct rdma_conn_param *conn_param);
int rdma_disconnect(struct rdma_cm_id *id);

int rdma_bind_addr(struct rdma_cm_id *id, struct sockaddr *addr);
int rdma_listen(struct rdma_cm_id *id, int backlog);
int rdma_accept(struct rdma_cm_id *id, struct rdma_conn_param *conn_param);

struct sockaddr *rdma_get_local_addr(struct rdma_cm_id *id);

int rdma_get_cm_event(struct rdma_event_channel *channel, struct rdma_cm_event **event);
int rdma_ack_cm_event(struct rdma_cm_event *event);
const char *rdma_event_str(enum rdma_cm_event_type event);

int rdma_migrate_id(struct rdma_cm_id *id, struct rdma_event_channel *channel);

enum { RDMA_OPTION_ID = 0 };
enum { RDMA_OPTION_ID_TOS = 0, RDMA_OPTION_ID_ACK_TIMEOUT = 3 };
int rdma_set_option(struct rdma_cm_id *id, int level, int optname, void *optval, size_t optlen);

#ifdef __cplusplus
}
#endif

#endif /* ROCEPOOL_STUB_RDMA_CMA_H */
