"""Buffer pool at its cap (ROCE_BUFPOOL_MAX=2, set by the caller): large
requests need two buffers each; taking them one at a time let concurrent
callers each hold one and wait forever for the second. argv: ROOT port.
Prints RESULT <json>."""
import json
import os
import sys
import threading

sys.path.insert(0, sys.argv[1])
from roce.protocol import Frame, Op  # noqa: E402
from roce.roce_transport import RoCETransport  # noqa: E402
from roce.transport_base import PeerUnreachable  # noqa: E402

a = f"127.0.0.1:{int(sys.argv[2])}"
srv = RoCETransport().serve(a, lambda f: Frame(op=Op.MPI_SEND_OK, request_id=f.request_id, key="", value=f.value[:10]))
ok, errs = [], []


def worker(w):
    t = RoCETransport(pool=True)
    for j in range(10):
        try:
            r = t.send_request(a, Frame(op=Op.MPI_SEND, request_id=w * 100 + j, key="k", value=b"q" * 200_000),
                               timeout_s=20)
            ok.append(r.value == b"q" * 10)
        except PeerUnreachable as e:
            errs.append(str(e))
ts = [threading.Thread(target=worker, args=(w,)) for w in range(8)]
[t.start() for t in ts]
[t.join(60) for t in ts]
print("RESULT " + json.dumps({"ok": sum(ok), "all_matched": all(ok), "errors": errs[:3],
                              "stuck": sum(t.is_alive() for t in ts),
                              "pool": RoCETransport().bufpool_stats()}), flush=True)
os._exit(0)
