/* Minimal stand-in for <infiniband/verbs.h> (libibverbs), covering only
 * the symbols roce_transport.c actually uses. This is NOT a real
 * implementation -- it exists solely so `gcc -fsyntax-only` can typecheck
 * roce_transport.c in an environment with no rdma-core packages installed
 * (see roce/native/build.sh and this project's README for why that's the
 * case here). Field names/order are matched to the real public API as
 * closely as this project's author could without the real header in hand;
 * ANY REAL BUILD MUST USE THE REAL <infiniband/verbs.h>, never this file
 * -- build.sh only ever compiles against the system headers.
 */
#ifndef ROCEPOOL_STUB_INFINIBAND_VERBS_H
#define ROCEPOOL_STUB_INFINIBAND_VERBS_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ibv_context;

struct ibv_pd {
    struct ibv_context *context;
};

struct ibv_comp_channel {
    struct ibv_context *context;
    int fd;
    int refcnt;
};

struct ibv_cq {
    struct ibv_context *context;
    struct ibv_comp_channel *channel;
    void *cq_context;
};

/* subset of rdma-core's struct ibv_device_attr (field names match; the
 * real struct has more fields and a different layout -- real builds use
 * the real header) */
struct ibv_device_attr {
    uint64_t max_mr_size;
    int max_qp_wr;
    int max_cqe;
    int max_mr;
    int max_srq;
    int max_srq_wr;
    int max_srq_sge;
    int max_qp_rd_atom;       /* outstanding RDMA READs a QP can have in flight */
    int max_qp_init_rd_atom;
};

struct ibv_mr {
    void *addr;
    size_t length;
    uint32_t lkey;
    uint32_t rkey;
};

struct ibv_qp_cap {
    uint32_t max_send_wr;
    uint32_t max_recv_wr;
    uint32_t max_send_sge;
    uint32_t max_recv_sge;
    uint32_t max_inline_data;
};

enum ibv_qp_type {
    IBV_QPT_RC = 2,
};

struct ibv_srq_attr {
    uint32_t max_wr;
    uint32_t max_sge;
    uint32_t srq_limit;
};

struct ibv_srq_init_attr {
    void *srq_context;
    struct ibv_srq_attr attr;
};

struct ibv_srq {
    struct ibv_context *context;
    void *srq_context;
    struct ibv_pd *pd;
};

struct ibv_qp_init_attr {
    void *qp_context;
    struct ibv_cq *send_cq;
    struct ibv_cq *recv_cq;
    struct ibv_srq *srq;
    struct ibv_qp_cap cap;
    enum ibv_qp_type qp_type;
    int sq_sig_all;
};

enum ibv_qp_state {
    IBV_QPS_RESET,
    IBV_QPS_INIT,
    IBV_QPS_RTR,
    IBV_QPS_RTS,
    IBV_QPS_SQD,
    IBV_QPS_SQE,
    IBV_QPS_ERR,
    IBV_QPS_UNKNOWN,
};

struct ibv_qp {
    struct ibv_context *context;
    struct ibv_pd *pd;
    uint32_t qp_num;
    enum ibv_qp_state state;
};

/* queue pairs connected without the CM (ibv_create_qp + ibv_modify_qp):
 * the subset of rdma-core's declarations roce_transport.c's raw queue
 * pairs use, field names as in the real header */
union ibv_gid {
    uint8_t raw[16];
    struct {
        uint64_t subnet_prefix;
        uint64_t interface_id;
    } global;
};

enum ibv_mtu {
    IBV_MTU_256 = 1,
    IBV_MTU_512 = 2,
    IBV_MTU_1024 = 3,
    IBV_MTU_2048 = 4,
    IBV_MTU_4096 = 5,
};

enum ibv_port_state {
    IBV_PORT_NOP = 0,
    IBV_PORT_DOWN = 1,
    IBV_PORT_INIT = 2,
    IBV_PORT_ARMED = 3,
    IBV_PORT_ACTIVE = 4,
};

struct ibv_port_attr {
    enum ibv_port_state state;
    enum ibv_mtu max_mtu;
    enum ibv_mtu active_mtu;
    int gid_tbl_len;
    uint8_t link_layer;
};

struct ibv_global_route {
    union ibv_gid dgid;
    uint32_t flow_label;
    uint8_t sgid_index;
    uint8_t hop_limit;
    uint8_t traffic_class;
};

struct ibv_ah_attr {
    struct ibv_global_route grh;
    uint16_t dlid;
    uint8_t sl;
    uint8_t src_path_bits;
    uint8_t static_rate;
    uint8_t is_global;
    uint8_t port_num;
};

struct ibv_qp_attr {
    enum ibv_qp_state qp_state;
    enum ibv_qp_state cur_qp_state;
    enum ibv_mtu path_mtu;
    uint32_t qkey;
    uint32_t rq_psn;
    uint32_t sq_psn;
    uint32_t dest_qp_num;
    unsigned int qp_access_flags;
    struct ibv_qp_cap cap;
    struct ibv_ah_attr ah_attr;
    uint16_t pkey_index;
    uint8_t max_rd_atomic;
    uint8_t max_dest_rd_atomic;
    uint8_t min_rnr_timer;
    uint8_t port_num;
    uint8_t timeout;
    uint8_t retry_cnt;
    uint8_t rnr_retry;
};

enum ibv_qp_attr_mask {
    IBV_QP_STATE = 1 << 0,
    IBV_QP_ACCESS_FLAGS = 1 << 3,
    IBV_QP_PKEY_INDEX = 1 << 4,
    IBV_QP_PORT = 1 << 5,
    IBV_QP_AV = 1 << 7,
    IBV_QP_PATH_MTU = 1 << 8,
    IBV_QP_TIMEOUT = 1 << 9,
    IBV_QP_RETRY_CNT = 1 << 10,
    IBV_QP_RNR_RETRY = 1 << 11,
    IBV_QP_RQ_PSN = 1 << 12,
    IBV_QP_MAX_QP_RD_ATOMIC = 1 << 13,
    IBV_QP_MIN_RNR_TIMER = 1 << 15,
    IBV_QP_SQ_PSN = 1 << 16,
    IBV_QP_MAX_DEST_RD_ATOMIC = 1 << 17,
    IBV_QP_DEST_QPN = 1 << 20,
};

struct ibv_sge {
    uint64_t addr;
    uint32_t length;
    uint32_t lkey;
};

/* numbering matches rdma-core's enum ibv_wr_opcode */
enum ibv_wr_opcode {
    IBV_WR_RDMA_WRITE = 0,
    IBV_WR_RDMA_WRITE_WITH_IMM = 1,
    IBV_WR_SEND = 2,
    IBV_WR_SEND_WITH_IMM = 3,
    IBV_WR_RDMA_READ = 4,
};

enum ibv_send_flags {
    IBV_SEND_SIGNALED = 1 << 1,
};

typedef uint32_t __be32;

struct ibv_send_wr {
    uint64_t wr_id;
    struct ibv_send_wr *next;
    struct ibv_sge *sg_list;
    int num_sge;
    enum ibv_wr_opcode opcode;
    unsigned int send_flags;
    union {
        __be32 imm_data;          /* *_WITH_IMM: immediate data, network byte order */
        uint32_t invalidate_rkey;
    };
    union {
        struct {
            uint64_t remote_addr;
            uint32_t rkey;
        } rdma;
    } wr;
};

struct ibv_recv_wr {
    uint64_t wr_id;
    struct ibv_recv_wr *next;
    struct ibv_sge *sg_list;
    int num_sge;
};

/* numbering matches rdma-core's enum ibv_wc_status */
enum ibv_wc_status {
    IBV_WC_SUCCESS = 0,
    IBV_WC_LOC_LEN_ERR = 1,
    IBV_WC_LOC_QP_OP_ERR = 2,
    IBV_WC_LOC_PROT_ERR = 4,
    IBV_WC_WR_FLUSH_ERR = 5,
    IBV_WC_REM_INV_REQ_ERR = 9,
    IBV_WC_REM_ACCESS_ERR = 10,
    IBV_WC_RETRY_EXC_ERR = 12,
    IBV_WC_RNR_RETRY_EXC_ERR = 13,
};

/* numbering matches rdma-core's enum ibv_wc_opcode */
enum ibv_wc_opcode {
    IBV_WC_SEND = 0,
    IBV_WC_RDMA_WRITE = 1,
    IBV_WC_RDMA_READ = 2,
    IBV_WC_RECV = 1 << 7,
    IBV_WC_RECV_RDMA_WITH_IMM = (1 << 7) + 1,
};

enum ibv_wc_flags {
    IBV_WC_WITH_IMM = 1 << 1,
};

struct ibv_wc {
    uint64_t wr_id;
    enum ibv_wc_status status;
    enum ibv_wc_opcode opcode;
    uint32_t vendor_err;
    uint32_t byte_len;
    union {
        __be32 imm_data;          /* valid when wc_flags & IBV_WC_WITH_IMM */
        uint32_t invalidated_rkey;
    };
    uint32_t qp_num;
    uint32_t src_qp;
    unsigned int wc_flags;
};

enum ibv_access_flags {
    IBV_ACCESS_LOCAL_WRITE = 1 << 0,
    IBV_ACCESS_REMOTE_WRITE = 1 << 1,
    IBV_ACCESS_REMOTE_READ = 1 << 2,
};

struct ibv_pd *ibv_alloc_pd(struct ibv_context *context);
int ibv_dealloc_pd(struct ibv_pd *pd);

struct ibv_cq *ibv_create_cq(struct ibv_context *context, int cqe, void *cq_context,
                              struct ibv_comp_channel *channel, int comp_vector);
int ibv_destroy_cq(struct ibv_cq *cq);
int ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc);

int ibv_query_device(struct ibv_context *context, struct ibv_device_attr *device_attr);

struct ibv_srq *ibv_create_srq(struct ibv_pd *pd, struct ibv_srq_init_attr *srq_init_attr);
int ibv_destroy_srq(struct ibv_srq *srq);
int ibv_post_srq_recv(struct ibv_srq *srq, struct ibv_recv_wr *recv_wr, struct ibv_recv_wr **bad_recv_wr);

struct ibv_comp_channel *ibv_create_comp_channel(struct ibv_context *context);
int ibv_destroy_comp_channel(struct ibv_comp_channel *channel);
int ibv_req_notify_cq(struct ibv_cq *cq, int solicited_only);
int ibv_get_cq_event(struct ibv_comp_channel *channel, struct ibv_cq **cq, void **cq_context);
void ibv_ack_cq_events(struct ibv_cq *cq, unsigned int nevents);

struct ibv_mr *ibv_reg_mr(struct ibv_pd *pd, void *addr, size_t length, int access);
int ibv_dereg_mr(struct ibv_mr *mr);

struct ibv_qp *ibv_create_qp(struct ibv_pd *pd, struct ibv_qp_init_attr *qp_init_attr);
int ibv_modify_qp(struct ibv_qp *qp, struct ibv_qp_attr *attr, int attr_mask);
int ibv_destroy_qp(struct ibv_qp *qp);
int ibv_query_port(struct ibv_context *context, uint8_t port_num, struct ibv_port_attr *port_attr);
int ibv_query_gid(struct ibv_context *context, uint8_t port_num, int index, union ibv_gid *gid);

int ibv_post_send(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad_wr);
int ibv_post_recv(struct ibv_qp *qp, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad_wr);

const char *ibv_wc_status_str(enum ibv_wc_status status);

#ifdef __cplusplus
}
#endif

#endif /* ROCEPOOL_STUB_INFINIBAND_VERBS_H */
