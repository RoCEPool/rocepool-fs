#!/usr/bin/env bash
# rocepool 一键运行：装依赖 -> 编译 -> 自检 -> 部署并跑官方 IO500 -> 报告。
#
#   sudo ./run.sh                                    有 cluster.conf：按文件里的全部节点跑（任意台数，见
#                                                    cluster.conf.example）；没有：本机单机（无 RoCE 网卡则 10 节点模拟）
#   sudo ./run.sh --conf 另一个.conf                  指定集群配置文件
#   sudo ./run.sh --peers root@n2=ip2,root@n3=ip3    N 台真机（本机 + 列出的各台），正式 300 s 跑
#   sudo ./run.sh --peers ... --quick                只跑 30 s 快速版
#   sudo ./run.sh --sim 10                           强制本机模拟 N 节点
#   sudo ./run.sh --skip-tests                       跳过自检
#   sudo ./run.sh --soft-roce [--peers ...]          没有 RDMA 网卡（如 AWS 普通机型）：自动建 Soft-RoCE（rxe0）后真机跑
#
# 网卡自动识别：RoCE 网卡 / Soft-RoCE(rxe) -> verbs 传输；AWS EFA -> libfabric(ofi) 传输。
# 在 AWS 上一条命令开集群、跑完、取回报告、释放机器：python3 tools/aws_cluster.py run --mode efa|rxe --nodes N
#
# 其余参数原样透传给 tools/roce_onsite.py（例如 --ip、--force）。
# 结果：onsite_report.json / onsite_io500.json / onsite_io500_valid.json（真机），
#       io500_report.json（模拟）；日志在 run.log。
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
LOG=run.log
exec > >(tee -a "$LOG") 2>&1
echo "== rocepool $(cat VERSION 2>/dev/null || echo ?) run $(date) =="

if [ "$(id -u)" != 0 ]; then echo "需要 root（FUSE 挂载、命名空间、RoCE 参数）：sudo ./run.sh ..."; exit 2; fi

SIM=""; QUICK=0; SKIP_TESTS=0; PEERS=""; SOFT=0; PASS=(); CONF=cluster.conf
while [ $# -gt 0 ]; do
  case "$1" in
    --sim) SIM="$2"; shift 2;;
    --quick) QUICK=1; shift;;
    --skip-tests) SKIP_TESTS=1; shift;;
    --peers) PEERS="$2"; shift 2;;
    --conf) CONF="$2"; shift 2;;
    --soft-roce) SOFT=1; PASS+=(--soft-roce); shift;;
    *) PASS+=("$1"); shift;;
  esac
done

# ---- 1. 依赖（有包管理器就装，装不上继续，后面的步骤会说明缺什么）
echo "== 1/4 依赖"
PKGS_DEB="python3 gcc g++ make git cmake curl pkg-config autoconf automake libtool fuse3 util-linux iproute2 openssh-client libibverbs-dev librdmacm-dev ibverbs-providers rdma-core"
PKGS_RPM="python3 gcc gcc-c++ make git cmake curl pkgconfig autoconf automake libtool fuse3 util-linux iproute openssh-clients rdma-core-devel libibverbs-utils librdmacm-utils"
if command -v apt-get >/dev/null; then
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends $PKGS_DEB >/dev/null 2>&1 || echo "   (apt 部分包未装上，继续)"
elif command -v dnf >/dev/null; then dnf install -y $PKGS_RPM >/dev/null 2>&1 || echo "   (dnf 部分包未装上，继续)"
elif command -v yum >/dev/null; then yum install -y $PKGS_RPM >/dev/null 2>&1 || echo "   (yum 部分包未装上，继续)"
fi
for t in python3 gcc make git; do command -v $t >/dev/null || { echo "缺少 $t，装上后重跑"; exit 1; }; done

# ---- 2. 有没有 RDMA 设备
HAVE_RDMA=0
if [ -z "$SIM" ] && [ $SOFT = 1 ] && ! ls /sys/class/infiniband/ 2>/dev/null | grep -q .; then
  echo "   --soft-roce：本机没有 RDMA 设备，建 Soft-RoCE（rxe0）"
  python3 tools/roce_preflight.py --fix --soft-roce --env-out /dev/null | grep -E "soft_roce|FIXED" || true
fi
if [ -z "$SIM" ] && ls /sys/class/infiniband/ 2>/dev/null | grep -q .; then HAVE_RDMA=1; fi
if [ -z "$SIM" ] && [ $HAVE_RDMA = 0 ]; then
  echo "   本机没有 RDMA 设备（/sys/class/infiniband 为空）：改跑本机 10 节点模拟"
  SIM=10
fi

# ---- 3. 编译 + 自检
echo "== 2/4 编译 + 自检"
python3 - <<'EOF' || exit 1
import sys; sys.path.insert(0, ".")
from roce.native.build_lib import ensure_softverbs_lib, ensure_fuse_client, ensure_intercept_lib, ensure_kv_lib
ensure_softverbs_lib(); ensure_fuse_client(); ensure_intercept_lib(); ensure_kv_lib()
print("   模拟器版库、FUSE 客户端、拦截库、KV 库：已编译")
EOF
if [ $SKIP_TESTS = 0 ]; then
  echo "   自检（约 3 分钟）..."
  if python3 -m unittest discover -s tests -t . > selftest.log 2>&1; then
    echo "   自检通过：$(grep -E '^Ran' selftest.log)"
  else
    echo "   自检有失败（selftest.log），继续往下跑："; grep -E '^(FAIL|ERROR):' selftest.log | head -20
    echo "   失败原因（摘要）："
    grep -E '^[A-Za-z_.]*(Error|Exception|Exit)[A-Za-z]*: |error: ' selftest.log | cut -c1-220 | sort | uniq -c | sort -rn | head -8
  fi
fi

# ---- 3b. 集群：cluster.conf（或 --peers）里的每一台，root 免密登录 + 主机名不重名
# 全部并行、自动；已经能免密的节点不动。需要装公钥时用 cluster.conf [login] 里的
# 用户和密码；文件里没写密码、又是在终端里运行时，只问一次（所有节点通用）。
if [ -z "$PEERS" ] && [ -f "$CONF" ] && [ -z "$SIM" ]; then CONF_MODE=1; else CONF_MODE=0; fi
if [ -z "$SIM" ] && { [ -n "$PEERS" ] || [ $CONF_MODE = 1 ]; }; then
  echo "== 集群节点：root 免密登录、主机名"
  if [ $CONF_MODE = 1 ]; then OUT=$(python3 tools/cluster_conf.py prepare --conf "$CONF")
  else OUT=$(python3 tools/cluster_conf.py prepare --peers "$PEERS"); fi
  if [ $? != 0 ]; then echo "   集群准备失败（原因见上），停止"; exit 1; fi
  PEERS=$(echo "$OUT" | sed -n 's/^PEERS //p' | tail -1)
  LOCAL_IP=$(echo "$OUT" | sed -n 's/^LOCAL //p' | tail -1)
  if [ -n "$LOCAL_IP" ] && [[ " ${PASS[*]:-} " != *" --ip "* ]]; then PASS+=(--ip "$LOCAL_IP"); fi
  [ -n "$PEERS" ] && echo "   节点就绪：$PEERS"
fi
[ -n "$PEERS" ] && PASS+=(--peers "$PEERS")

# ---- 4. 跑
echo "== 3/4 运行"
if [ -n "$SIM" ]; then
  python3 tools/io500_run.py --nodes "$SIM" --report io500_report.json
  RC=$?
  echo "== 4/4 完成（rc=$RC）：报告 io500_report.json，原始输出 .bench_cache/io500_run/"
else
  BENCH=full; [ $QUICK = 1 ] && BENCH=quick
  [ -n "$PEERS" ] && echo "   节点：本机 + $PEERS" || echo "   单机（网卡回环）"
  if ls -d /sys/class/infiniband/efa* >/dev/null 2>&1; then echo "   网卡：AWS EFA -> libfabric(ofi) 传输后端"
  elif ls -d /sys/class/infiniband/rxe* >/dev/null 2>&1; then echo "   网卡：Soft-RoCE(rxe) -> verbs 传输后端"
  else echo "   网卡：RoCE -> verbs 传输后端"; fi
  python3 tools/roce_onsite.py --bench $BENCH --report onsite_report.json "${PASS[@]}"
  RC=$?
  echo "== 4/4 完成（rc=$RC）：$(ls onsite_report.json onsite_io500.json onsite_io500_valid.json 2>/dev/null | tr '\n' ' ')"
  [ -f onsite_io500.json ] || echo "   注意：没有 IO500 结果（看 onsite_report.json 里 io500 / cluster 步骤的原因）"
  [ -f onsite_io500_valid.json ] && python3 -c "
import json; r=json.load(open('onsite_io500_valid.json')); s=r.get('score',{})
print('   正式分数:', s.get('score'), ' 带宽 GiB/s:', s.get('bw_gib_s'), ' 元数据 kIOPS:', s.get('md_kiops'), ' (WEKA Afton 105.94 / 33.28 / 337.29)')"
fi

# ---- 把这次运行的全部结果打成一个包，直接发回即可分析
RES="rocepool-results-$(hostname -s)-$(date +%Y%m%d-%H%M%S).tar.gz"
FILES=$(ls -d run.log selftest.log io500_report.json onsite_*.json roce_env.sh .bench_cache/io500_run/*.log \
            .bench_cache/io500_run/*.out .bench_cache/io500_run/results \
            .bench_cache/io500_run/probe_*/io500.out 2>/dev/null)
if [ -n "$FILES" ]; then
  tar czf "$RES" $FILES 2>/dev/null && echo "== 结果包：$(pwd)/$RES（把这个文件发回来即可）"
fi
exit $RC
