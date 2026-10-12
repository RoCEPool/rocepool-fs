"""Connection-pool behaviour of RoCETransport over the softverbs library,
in its own process (the transport library is loaded once per process).
argv: ROOT base_port. Prints one line: RESULT <json>.
Used by tests/test_conn_pool.py."""
import json
import os
import sys
import threading
import time

sys.path.insert(0, sys.argv[1])

from roce.protocol import Frame, Op  # noqa: E402
from roce.roce_transport import RoCEStream, RoCETransport  # noqa: E402
from roce.transport_base import PeerUnreachable  # noqa: E402

port = int(sys.argv[2])
HOST = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1"  # the RoCE IP when run on site
res = {}


def addr():
    global port
    port += 1
    return f"{HOST}:{port}"


def echo(f):
    return Frame(op=Op.MPI_SEND_OK, request_id=f.request_id, key=f.key, value=f.value)


def req(i, key="k", value=b""):
    return Frame(op=Op.MPI_SEND, request_id=i, key=key, value=value)


try:
    srv_t = RoCETransport()
    a = addr()
    h = srv_t.serve(a, echo)

    # 1. reuse: 200 sequential requests ride one connection
    t = RoCETransport(pool=True)
    ok = all(t.send_request(a, req(i, value=bytes([i % 256]) * i)).value == bytes([i % 256]) * i for i in range(1, 201))
    res["pooled"] = {"ok": ok, "connects": t.connects}

    # 2. pool disabled -> one connection per request (old behaviour)
    t2 = RoCETransport(pool=False)
    for i in range(1, 51):
        t2.send_request(a, req(i))
    res["unpooled_connects"] = t2.connects

    # 3. concurrent callers never share a connection: 8 threads x 50, all
    #    replies matched, pool holds at most max_idle afterwards
    t3 = RoCETransport(pool=True)
    bad = []

    def worker(w):
        for j in range(50):
            rid = 100_000 * (w + 1) + j
            r = t3.send_request(a, req(rid, key=f"w{w}", value=str(rid).encode()))
            if r.request_id != rid or r.value != str(rid).encode():
                bad.append(rid)
    ts = [threading.Thread(target=worker, args=(w,)) for w in range(8)]
    [x.start() for x in ts]
    [x.join() for x in ts]
    res["concurrent"] = {"bad": bad, "connects": t3.connects,
                         "idle": sum(len(v) for v in t3._pool._idle.values())}

    # 4. server restarted on the same address: stale pooled connection is
    #    detected and the request goes through on a fresh one
    b = addr()
    hb = srv_t.serve(b, echo)
    t4 = RoCETransport(pool=True)
    t4.send_request(b, req(1))
    hb.stop()
    time.sleep(0.3)
    hb = srv_t.serve(b, echo)
    time.sleep(0.1)
    try:
        r = t4.send_request(b, req(2, value=b"after-restart"))
        res["restart"] = {"ok": r.value == b"after-restart", "connects": t4.connects}
    except PeerUnreachable as e:
        res["restart"] = {"ok": False, "error": str(e), "connects": t4.connects}

    # 5. mismatched reply request_id is never returned as a success, and the
    #    connection it came on is discarded
    c = addr()
    srv_t.serve(c, lambda f: Frame(op=Op.MPI_SEND_OK, request_id=f.request_id + 1, key="", value=b""))
    t5 = RoCETransport(pool=True)
    try:
        t5.send_request(c, req(7))
        res["mismatch"] = {"raised": False}
    except PeerUnreachable as e:
        res["mismatch"] = {"raised": True, "msg": str(e),
                           "idle": sum(len(v) for v in t5._pool._idle.values())}

    # 6. stop() drains: a request already inside the handler when the
    #    server is stopped still gets its reply
    d = addr()
    entered = threading.Event()

    def slow(f):
        entered.set()
        time.sleep(0.4)
        return echo(f)
    hd = srv_t.serve(d, slow)
    t6 = RoCETransport(pool=True)
    out = {}

    def call():
        try:
            out["v"] = t6.send_request(d, req(9, value=b"drained")).value
        except PeerUnreachable as e:
            out["err"] = str(e)
    th = threading.Thread(target=call)
    th.start()
    entered.wait(5)
    t_stop = time.monotonic()
    hd.stop()
    res["drain"] = {"reply": (out.get("v") or b"").decode(), "err": out.get("err"),
                    "stop_waited_s": round(time.monotonic() - t_stop, 2)}
    th.join(5)
    res["drain"]["reply"] = (out.get("v") or b"").decode()
    res["drain"]["err"] = out.get("err")

    # 7. idle connections older than idle_close_s are closed, not reused
    t7 = RoCETransport(pool=True)
    t7._pool.idle_close_s = 0.05
    t7.send_request(a, req(1))
    time.sleep(0.15)
    t7.send_request(a, req(2))
    res["idle_expiry_connects"] = t7.connects
    # 8. receive memory is bounded by the SRQ, not by open connections:
    #    64 clients each keep a pooled connection open and send 1 MiB
    big = b"x" * (1 << 20)
    e = addr()
    srv_t.serve(e, echo)
    before = t.bufpool_stats()
    clients = [RoCETransport(pool=True) for _ in range(64)]
    for i, c in enumerate(clients):
        assert c.send_request(e, req(i, value=big)).value == big
    after = t.bufpool_stats()
    res["bounded"] = {"open_connections": sum(c.connects for c in clients),
                      "buffers_before": before["buffers"], "buffers_after": after["buffers"],
                      "in_use_after": after["in_use"]}

    # 9. SRQ exhausted -> RNR back-pressure, not failures: 2 receive
    #    buffers, 12 concurrent callers into a slow handler
    os.environ["ROCE_SRQ_DEPTH"] = "2"
    f_addr = addr()

    def slowish(fr):
        time.sleep(0.02)
        return echo(fr)
    srv_t.serve(f_addr, slowish)
    errs, oks = [], []

    def hammer(w):
        tr = RoCETransport(pool=True)
        for j in range(10):
            try:
                r = tr.send_request(f_addr, req(w * 1000 + j, value=b"y" * 100_000), timeout_s=30)
                oks.append(r.request_id == w * 1000 + j)
            except PeerUnreachable as ex:
                errs.append(str(ex))
    ts = [threading.Thread(target=hammer, args=(w,)) for w in range(12)]
    [x.start() for x in ts]
    [x.join() for x in ts]
    del os.environ["ROCE_SRQ_DEPTH"]
    res["srq_backpressure"] = {"ok": sum(oks), "all_matched": all(oks), "errors": errs[:3]}
    # 10. a pipelining peer that disconnects mid-stream must not leak SRQ
    #     buffers: 4-buffer SRQ, 12 rounds of "3 frames, close without
    #     reading" (receives stashed behind a reply), then a normal request
    os.environ["ROCE_SRQ_DEPTH"] = "4"
    g_addr = addr()

    def slow50(fr):
        time.sleep(0.05)
        return echo(fr)
    srv_t.serve(g_addr, slow50)  # (the SRQ is created at the first connection: env stays set until then)
    stalled_rounds = 0
    for rnd in range(12):
        st = RoCEStream.connect(g_addr, timeout_ms=3000)
        try:
            for i in range(3):
                st.sendall(req(rnd * 10 + i, value=b"z" * 100).encode())
        except Exception:  # noqa: BLE001 -- a SEND stuck in RNR: the SRQ ran dry
            stalled_rounds += 1
        time.sleep(0.02)
        st.close()
    del os.environ["ROCE_SRQ_DEPTH"]
    time.sleep(0.5)
    try:
        r = RoCETransport(pool=True).send_request(g_addr, req(4242, value=b"still-serving"), timeout_s=5)
        res["srq_after_disconnects"] = {"ok": r.value == b"still-serving" and stalled_rounds == 0,
                                        "stalled_rounds": stalled_rounds}
    except PeerUnreachable as ex:
        res["srq_after_disconnects"] = {"ok": False, "error": str(ex), "stalled_rounds": stalled_rounds}
except Exception:  # noqa: BLE001
    import traceback
    res["error"] = traceback.format_exc()
print("RESULT " + json.dumps(res), flush=True)
os._exit(0)
