"""Many threads writing one file at once at different offsets, out of order --
how DuckDB 2.0's asynchronous writer writes (src/common/serializer/
async_write_queue.cpp: up to 4 x the host's cores of writer threads per
DuckDB, each a pwrite on the shared fd) -- many files and processes at once.

    python3 tools/fuse_pwrite_stress.py DIR [--procs 8] [--files 16] [--threads 64] [--chunks 32]

Every failed pwrite is reported with what the same fd and path say right
after it (fstat, stat): EIO from both means the kernel marked the inode
bad; from the pwrite alone, the write itself failed. Short writes are
counted and continued, as DuckDB does. Every file is read back and
compared. Prints one JSON line; exit 1 on any error."""
from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import random
import threading
import time

SIZES = [26, 22, 4096, 45185, 204799, 330637, 1 << 20]


def one_file(root: str, tag: str, fi: int, nthreads: int, nchunks: int, out: dict, lock) -> None:
    rnd = random.Random(f"{tag}-{fi}")
    sizes = [rnd.choice(SIZES) for _ in range(nchunks)]
    offs, o = [], 0
    for s in sizes:
        offs.append(o)
        o += s
    data = [bytes([(fi + k) % 251 + 1]) * s for k, s in enumerate(sizes)]
    path = f"{root}/{tag}-f{fi:05d}.bin"
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    order = list(range(nchunks))
    rnd.shuffle(order)

    def writer(ks):
        for k in ks:
            done = 0
            while done < sizes[k]:
                try:
                    n = os.pwrite(fd, memoryview(data[k])[done:], offs[k] + done)
                except OSError as e:
                    after = {}
                    for what, fn in (("fstat", lambda: os.fstat(fd)), ("stat", lambda: os.stat(path))):
                        try:
                            st = fn()
                            after[what] = f"ok size {st.st_size}"
                        except OSError as e2:
                            after[what] = e2.strerror
                    with lock:
                        out["errors"].append({"path": path, "offset": offs[k] + done, "size": sizes[k] - done,
                                              "error": e.strerror, "then": after})
                    return
                if n < sizes[k] - done:
                    with lock:
                        out["short"] += 1
                done += n
    ts = [threading.Thread(target=writer, args=(order[i::nthreads],)) for i in range(nthreads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    try:
        os.close(fd)
    except OSError as e:
        with lock:
            out["errors"].append({"path": path, "error": f"close: {e.strerror}"})
    try:
        with open(path, "rb") as f:
            same = f.read() == b"".join(data)
    except OSError as e:
        same = False
        with lock:
            out["errors"].append({"path": path, "error": f"read back: {e.strerror}"})
    if not same:
        with lock:
            out["mismatch"] += 1


def proc(args) -> dict:
    root, tag, nfiles, nthreads, nchunks = args
    out, lock = {"errors": [], "short": 0, "mismatch": 0}, threading.Lock()
    fs = [threading.Thread(target=one_file, args=(root, tag, i, nthreads, nchunks, out, lock)) for i in range(nfiles)]
    for t in fs:
        t.start()
    for t in fs:
        t.join()
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dir")
    ap.add_argument("--procs", type=int, default=8)
    ap.add_argument("--files", type=int, default=16, help="per process, written at the same time")
    ap.add_argument("--threads", type=int, default=64, help="writer threads per file")
    ap.add_argument("--chunks", type=int, default=32, help="writes per file")
    ap.add_argument("--tag", default=os.uname().nodename)
    a = ap.parse_args()
    os.makedirs(a.dir, exist_ok=True)
    t0 = time.time()
    with multiprocessing.Pool(a.procs) as pool:
        res = pool.map(proc, [(a.dir, f"{a.tag}-p{p}", a.files, a.threads, a.chunks) for p in range(a.procs)])
    errs = [e for r in res for e in r["errors"]]
    rep = {"host": a.tag, "files": a.procs * a.files, "writer_threads": a.procs * a.files * a.threads,
           "seconds": round(time.time() - t0, 1), "errors": len(errs), "short_writes": sum(r["short"] for r in res),
           "mismatches": sum(r["mismatch"] for r in res), "first_errors": errs[:8]}
    print(json.dumps(rep))
    return 1 if errs or rep["mismatches"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
