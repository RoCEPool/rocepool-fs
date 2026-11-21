#!/usr/bin/env python3
"""A stand-in for vLLM + LMCache's server, for rehearsing tools/kvcache_e2e.py
without GPUs. It serves the same HTTP calls the harness makes:

  vLLM port:      GET /health, POST /tokenize, POST /v1/completions (streamed)
  LMCache port:   GET /healthcheck, GET /status, POST /cache/clear

and keeps KV cache the way LMCache does: 256-token chunks keyed by a chained
hash of the tokens (the same key on every host), an L1 of chunk slots in one
arena, and RoCEPool as L2 through the real roce.lmcache_connector -- the arena
registered with the NIC (zero copy) unless --no-register. A request uses the
longest prefix of chunks found in L1, then in RoCEPool (loaded into L1), and
"computes" the rest (--prefill-us per token); new full chunks go to L1 and,
asynchronously, to RoCEPool (write-through, like LMCache's default policy).

The answer is derived from the bytes of every KV chunk used plus the
uncached tokens: KV that comes back wrong changes the answer, as with a real
model. Fault injection: --corrupt-loads flips a byte of every chunk loaded
from RoCEPool; --no-register leaves the arena unregistered (transfers copy).
"""
from __future__ import annotations

import argparse
import collections
import ctypes
import hashlib
import json
import mmap
import os
import random
import select
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from roce.lmcache_connector import RoCEPoolConnector  # noqa: E402

CHUNK = 256


class Engine:
    def __init__(self, a):
        self.a = a
        self.chunk_bytes = CHUNK * a.kv_bytes_per_token
        self.slots = max(4, (a.l1_mb << 20) // self.chunk_bytes)
        self.arena = mmap.mmap(-1, self.slots * self.chunk_bytes)
        self.base = ctypes.addressof((ctypes.c_char * len(self.arena)).from_buffer(self.arena))
        self.conn = RoCEPoolConnector(osts=a.osts, namespace=a.namespace, part_mb=a.part_mb)
        self.registered = False if a.no_register else self.conn.register_region(self.base, len(self.arena))
        self.l1: collections.OrderedDict[str, int] = collections.OrderedDict()   # key -> slot (LRU order)
        self.free = list(range(self.slots))
        self.lock = threading.Lock()          # L1 bookkeeping
        self.engine_lock = threading.Lock()   # one request at a time, like one GPU
        self.waiters: dict[int, tuple[threading.Event, list]] = {}
        self.wlock = threading.Lock()
        threading.Thread(target=self._completions, daemon=True).start()

    # -- connector completions ------------------------------------------------
    def _completions(self):
        fd = self.conn.event_fd()
        while True:
            select.select([fd], [], [], 1.0)
            for fid, ok, err, res in self.conn.drain_completions():
                with self.wlock:
                    w = self.waiters.pop(fid, None)
                if w:
                    w[1].extend([ok, err, res])
                    w[0].set()

    def _submit(self, fn, *args):
        ev, out = threading.Event(), []
        with self.wlock:   # registered before the completion can be drained
            fid = fn(*args)
            self.waiters[fid] = (ev, out)
        return ev, out

    def _wait(self, handle, timeout=300):
        ev, out = handle
        if not ev.wait(timeout):
            raise TimeoutError("RoCEPool operation timed out")
        return out

    # -- L1 -------------------------------------------------------------------
    def view(self, slot: int) -> memoryview:
        o = slot * self.chunk_bytes
        return memoryview(self.arena)[o:o + self.chunk_bytes]

    def alloc(self) -> int:
        with self.lock:
            if not self.free:
                _, slot = self.l1.popitem(last=False)   # evict the least recently used
                self.free.append(slot)
            return self.free.pop()

    def clear(self):
        with self.lock:
            self.free.extend(self.l1.values())
            self.l1.clear()

    def kv_for(self, key: str) -> bytes:
        """What 'prefill' produces for a chunk: deterministic in its key."""
        seed = hashlib.sha256(key.encode()).digest()
        return (seed * (self.chunk_bytes // len(seed) + 1))[:self.chunk_bytes]

    # -- a request ----------------------------------------------------------------
    def keys(self, toks: list[str]) -> list[str]:
        out, h = [], b"fake-model"
        for i in range(len(toks) // CHUNK):
            h = hashlib.sha256(h + " ".join(toks[i * CHUNK:(i + 1) * CHUNK]).encode()).digest()
            out.append("fake-model@" + h.hex())
        return out

    def serve(self, prompt: str) -> tuple[list[str], float]:
        toks = prompt.split()
        keys = self.keys(toks)
        with self.engine_lock:
            used: list[int] = []
            # L1
            i = 0
            with self.lock:
                while i < len(keys) and keys[i] in self.l1:
                    self.l1.move_to_end(keys[i])
                    used.append(self.l1[keys[i]])
                    i += 1
            # RoCEPool: the longest run of chunks present, loaded into L1
            if i < len(keys):
                _, err, present = self._wait(self._submit(self.conn.submit_batch_exists, keys[i:]))
                run = 0
                while run < len(present) and present[run]:
                    run += 1
                if run:
                    slots = [self.alloc() for _ in range(run)]
                    ok, err, res = self._wait(self._submit(self.conn.submit_batch_get, keys[i:i + run],
                                                           [self.view(s) for s in slots]))
                    got = 0
                    while got < run and res[got]:
                        got += 1
                    with self.lock:
                        for k in range(run):
                            if k < got:
                                if self.a.corrupt_loads:
                                    v = self.view(slots[k])
                                    v[7] = v[7] ^ 0xFF
                                self.l1[keys[i + k]] = slots[k]
                                used.append(slots[k])
                            else:
                                self.free.append(slots[k])
                    i += got
            # compute the rest
            hit_tokens = i * CHUNK
            time.sleep((len(toks) - hit_tokens) * self.a.prefill_us / 1e6)
            new_keys, new_slots = [], []
            for k in range(i, len(keys)):
                s = self.alloc()
                self.view(s)[:] = self.kv_for(keys[k])
                with self.lock:
                    self.l1[keys[k]] = s
                used.append(s)
                new_keys.append(keys[k])
                new_slots.append(s)
            # the answer: from every chunk's KV bytes and the uncached tail
            h = hashlib.sha256()
            for s in used:
                h.update(hashlib.sha256(self.view(s)).digest())
            h.update(" ".join(toks[len(keys) * CHUNK:]).encode())
            rng = random.Random(h.digest())
            answer = [rng.choice(("alpha", "beta", "gamma", "delta", "omega", "sigma")) for _ in range(32)]
            ttft_ready = time.perf_counter()
        # write-through to RoCEPool, after answering (LMCache stores asynchronously)
        if new_keys:
            self._submit(self.conn.submit_batch_set, new_keys, [self.view(s) for s in new_slots])
        return answer, ttft_ready

    def status(self) -> dict:
        return {"is_healthy": True, "engine_type": "fake",
                "storage_manager": {"l2_adapters": [{"is_healthy": True, "type": "rocepool",
                                                     "zero_copy_l1": bool(self.registered),
                                                     "rocepool": self.conn.stats()}]}}


def make_handler(eng: Engine):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _json(self, code, obj):
            data = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(n)) if n else {}

        def do_GET(self):
            if self.path in ("/health", "/healthcheck"):
                self._json(200, {"status": "ok"})
            elif self.path == "/status":
                self._json(200, eng.status())
            else:
                self._json(404, {"detail": "not found"})

        def do_POST(self):
            body = self._body()
            if self.path == "/tokenize":
                self._json(200, {"count": len(str(body.get("prompt", "")).split())})
            elif self.path == "/cache/clear":
                eng.clear()
                self._json(200, {"status": "ok", "cleared": {"tier": "l1"}})
            elif self.path == "/v1/completions":
                answer, _ = eng.serve(str(body.get("prompt", "")))
                n = min(int(body.get("max_tokens", 16)), len(answer))
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()

                def send(obj):
                    data = ("data: " + (obj if isinstance(obj, str) else json.dumps(obj)) + "\n\n").encode()
                    self.wfile.write(b"%x\r\n%s\r\n" % (len(data), data))
                    self.wfile.flush()
                for k in range(n):
                    send({"choices": [{"index": 0, "text": (" " if k else "") + answer[k]}]})
                send({"choices": [], "usage": {"prompt_tokens": len(str(body.get("prompt", "")).split()),
                                               "completion_tokens": n}})
                send("[DONE]")
                self.wfile.write(b"0\r\n\r\n")
            else:
                self._json(404, {"detail": "not found"})
    return H


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--lmcache-http-port", type=int, required=True)
    ap.add_argument("--osts", required=True)
    ap.add_argument("--namespace", default="lmcache/")
    ap.add_argument("--kv-bytes-per-token", type=int, default=4096)
    ap.add_argument("--l1-mb", type=int, default=256)
    ap.add_argument("--part-mb", type=float, default=0.5)
    ap.add_argument("--prefill-us", type=float, default=60.0)
    ap.add_argument("--corrupt-loads", action="store_true")
    ap.add_argument("--no-register", action="store_true")
    a = ap.parse_args()
    eng = Engine(a)
    servers = [ThreadingHTTPServer(("127.0.0.1", p), make_handler(eng)) for p in (a.port, a.lmcache_http_port)]
    for s in servers[1:]:
        threading.Thread(target=s.serve_forever, daemon=True).start()
    print(f"fake engine on {a.port} / {a.lmcache_http_port}, zero copy {eng.registered}", flush=True)
    servers[0].serve_forever()


if __name__ == "__main__":
    main()
