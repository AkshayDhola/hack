#!/usr/bin/env python3
"""Speed Demon 2026, Track D (Traverse), optimized implementation.

    python3.13 d_traverse.py [--data /data] [--out /out]

Reads graph.txt and queries.txt and writes distances.txt (specs/d-traverse.md): for
each query, in input order, the shortest directed s -> t distance, or -1.

Three phases on raw forked children (no multiprocessing: its import and pool cost more
than they save). Bulk data lives in shared anonymous maps, so children read it in place:
it is never copied on write and only the pages written are charged.
1. Parse: child c parses one line-aligned range of edge lines in pieces and buckets every
   edge by node range, once by tail (forward graph) and once by head (reverse graph), into
   its own slot of a scratch map. An edge is one int, weight << SHIFT | other end.
2. Build: child j counts then scatters bucket j into CSR rows (offsets + edges) of both
   graphs in the graph map. The scratch map is then freed.
3. Query: children pull query indexes from a prefilled pipe (dynamic load balance) and
   run bidirectional Dijkstra on heap keys distance << SHIFT | node: expand the side with
   the smaller heap, stop once the two heap tops sum to at least the best path seen. An
   exhausted side means every path has been seen, so unreachable targets are cheap.
"""

import gc
import marshal
import mmap
import os
from array import array
from heapq import heappop, heappush
from itertools import accumulate

GRAPH_FILE = "graph.txt"
QUERIES_FILE = "queries.txt"
OUTPUT_FILE = "distances.txt"
MAX_LINE = 64
MIN_LINE = 6  # "u v w\n": bounds the edge count of a range by its bytes
PIECE = 1 << 20  # parse a range in pieces: small transient token lists
INF = 1 << 128  # above every heap key: distances are below 2^64, node ids below 2^64
QUEUE_SLOTS = 1 << 14  # 4-byte queue records that fit an empty 64 KiB pipe

# Inherited by forked children.
_fd = -1
_n = 0
_shift = 1
_span = 1  # nodes per build bucket
_slots = []  # per parse range: (fwd nodes, fwd edges, rev nodes, rev edges) views into scratch
_counts = []  # per parse range: [fwd edges per bucket, rev edges per bucket]
_bases = ([], [])  # per direction, first edge slot of each bucket in the graph map
_csr = ()  # (fwd offsets, fwd edges, rev offsets, rev edges): views into the graph map
_tasks = []  # distinct (s, t) with s != t
_per = 1  # tasks per queue record
_queue = -1


def fork_map(func, items):
    """[func(item) for item in items], one forked child per item, results via marshal."""
    pipes = []
    for item in items:
        r, w = os.pipe()
        if not os.fork():
            code = 1
            try:
                os.close(r)
                with open(w, "wb") as f:
                    marshal.dump(func(item), f)
                code = 0
            finally:
                os._exit(code)
        os.close(w)
        pipes.append(r)
    out = []
    for r in pipes:
        with open(r, "rb") as f:
            out.append(marshal.load(f))
    for _ in pipes:
        if os.wait()[1]:
            raise RuntimeError("worker failed")
    return out


def spans(fd, start, end, parts):
    """Cut [start, end) into about `parts` line-aligned ranges."""
    bounds = [start]
    for i in range(1, parts):
        pos = max(start + (end - start) * i // parts, bounds[-1])
        nl = os.pread(fd, MAX_LINE, pos).find(b"\n")
        if nl < 0:
            break
        bounds.append(pos + nl + 1)
    bounds.append(end)
    return [(a, b) for a, b in zip(bounds, bounds[1:]) if a < b]


def parse(job):
    """Phase 1: range c of edge lines -> its scratch slot, bucket after bucket; -> counts."""
    c, (start, end) = job
    size, shift = _span, _shift
    count = -(-_n // size)
    fk, fe, bk, be = ([array(code) for _ in range(count)] for code in "IQIQ")
    fka, fea, bka, bea = ([a.append for a in arrays] for arrays in (fk, fe, bk, be))
    rest = b""
    while start < end:
        n = min(PIECE, end - start)
        buf = rest + os.pread(_fd, n, start)
        start += n
        cut = buf.rfind(b"\n") + 1 if start < end else len(buf)
        rest = buf[cut:]
        it = map(int, buf[:cut].split())
        for u, v, w in zip(it, it, it):
            w <<= shift
            j, k = divmod(u, size)
            fka[j](k)
            fea[j](w | v)
            j, k = divmod(v, size)
            bka[j](k)
            bea[j](w | u)
    for view, arrays in zip(_slots[c], (fk, fe, bk, be)):
        at = 0
        for a in arrays:
            view[at:at + len(a)] = a
            at += len(a)
    return [list(map(len, fk)), list(map(len, bk))]


def build(j):
    """Phase 2: bucket j -> its CSR rows of both graphs, counted then scattered in place."""
    lo = j * _span
    rows = min(_n, lo + _span) - lo
    for d, offsets, edges in ((0, _csr[0], _csr[1]), (1, _csr[2], _csr[3])):
        parts = []
        for slot, counts in zip(_slots, _counts):
            at = sum(counts[d][:j])
            parts.append((slot[2 * d][at:at + counts[d][j]], slot[2 * d + 1][at:at + counts[d][j]]))
        degree = [0] * rows
        for nodes, _ in parts:
            for k in nodes:
                degree[k] += 1
        # Row starts; the last one is the next bucket's base, which it writes too.
        pos = list(accumulate(degree, initial=_bases[d][j]))
        offsets[lo:lo + rows + 1] = array("I", pos)
        for nodes, encoded in parts:
            for k, e in zip(nodes, encoded):
                edges[pos[k]] = e
                pos[k] += 1
    return 0


def distance(s, t):
    """Bidirectional Dijkstra: shortest s -> t distance, or -1 if t is unreachable."""
    if s == t:
        return 0
    fo, fe, bo, be = _csr
    shift = _shift
    mask = (1 << shift) - 1
    df = {s: s}  # node -> best heap key (distance << shift | node) from s
    db = {t: t}  # node -> best heap key to t
    gf = df.get
    gb = db.get
    hf = [s]
    hb = [t]
    mu = INF
    while hf and hb:
        if (hf[0] >> shift) + (hb[0] >> shift) >= mu:
            break
        if len(hf) <= len(hb):
            x = heappop(hf)
            u = x & mask
            if df[u] != x:
                continue  # stale entry
            base = x ^ u  # distance << shift: + edge gives the neighbour's key
            for e in fe[fo[u]:fo[u + 1]]:
                k = base + e
                v = e & mask
                if k < gf(v, INF):
                    df[v] = k
                    heappush(hf, k)
                    o = gb(v)
                    if o is not None and (c := (k >> shift) + (o >> shift)) < mu:
                        mu = c
        else:
            x = heappop(hb)
            u = x & mask
            if db[u] != x:
                continue
            base = x ^ u
            for e in be[bo[u]:bo[u + 1]]:
                k = base + e
                v = e & mask
                if k < gb(v, INF):
                    db[v] = k
                    heappush(hb, k)
                    o = gf(v)
                    if o is not None and (c := (k >> shift) + (o >> shift)) < mu:
                        mu = c
    return -1 if mu == INF else mu


def answer(_):
    """Phase 3: take queue records until the queue is empty -> [(task, distance)]."""
    out = []
    while rec := os.read(_queue, 4):
        i = int.from_bytes(rec, "little") * _per
        for task in range(i, min(i + _per, len(_tasks))):
            out.append((task, distance(*_tasks[task])))
    return out


def main(data_dir, out_dir):
    global _fd, _n, _shift, _span, _slots, _counts, _bases, _csr, _tasks, _per, _queue
    gc.disable()  # no reference cycles: skip collector passes over millions of objects

    _fd = os.open(os.path.join(data_dir, GRAPH_FILE), os.O_RDONLY)
    try:
        size = os.fstat(_fd).st_size
        head = os.pread(_fd, MAX_LINE, 0)
        start = head.index(b"\n") + 1
        n, _ = map(int, head[:start].split())
        workers = max(1, len(os.sched_getaffinity(0)))
        _n = n
        _shift = max(1, (n - 1).bit_length())
        _span = -(-n // workers)
        ranges = spans(_fd, start, size, workers)
        # Scratch slot per range, sized for its most possible edges: 4 + 8 bytes per edge
        # and direction (an even cap keeps the 8-byte views aligned).
        caps = [((b - a) // MIN_LINE + 2) & ~1 for a, b in ranges]
        scratch = mmap.mmap(-1, max(1, 24 * sum(caps)))
        view = memoryview(scratch)
        at = 0
        for cap in caps:
            _slots.append((view[at:at + 4 * cap].cast("I"), view[at + 4 * cap:at + 12 * cap].cast("Q"),
                           view[at + 12 * cap:at + 16 * cap].cast("I"), view[at + 16 * cap:at + 24 * cap].cast("Q")))
            at += 24 * cap
        _counts = fork_map(parse, enumerate(ranges))
    finally:
        os.close(_fd)
        _fd = -1

    buckets = -(-n // _span)
    _bases = tuple([0, *accumulate(sum(c[d][j] for c in _counts) for j in range(buckets))] for d in (0, 1))
    m = _bases[0][-1]
    # Graph map: offsets (n + 1 uint32, padded to 8 bytes) then edges (m uint64), per direction.
    off_bytes = -(-(n + 1) * 4 // 8) * 8
    half = off_bytes + 8 * m
    graph = memoryview(mmap.mmap(-1, 2 * half))
    _csr = (graph[:off_bytes].cast("I"), graph[off_bytes:half].cast("Q"),
            graph[half:half + off_bytes].cast("I"), graph[half + off_bytes:].cast("Q"))
    fork_map(build, range(buckets))
    _slots = []
    view.release()
    scratch.close()  # every child that mapped it has exited: its pages are freed

    with open(os.path.join(data_dir, QUERIES_FILE), "rb") as f:
        nums = list(map(int, f.read().split()))
    queries = list(zip(nums[0::2], nums[1::2]))
    # Each distinct (s, t) with s != t is answered once; s == t is 0.
    index = {}
    for q in queries:
        if q[0] != q[1] and q not in index:
            index[q] = len(index)
    _tasks = list(index)
    answers = []
    if _tasks:
        _per = -(-len(_tasks) // QUEUE_SLOTS)
        records = -(-len(_tasks) // _per)
        _queue, w = os.pipe()
        os.write(w, array("I", range(records)).tobytes())  # fits the empty pipe: no blocking
        os.close(w)
        found = [0] * len(_tasks)
        for part in fork_map(answer, range(min(workers, records))):
            for task, d in part:
                found[task] = d
        os.close(_queue)
        answers = [found[index[q]] if q in index else 0 for q in queries]

    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, OUTPUT_FILE), "wb") as f:
        f.write("".join(f"{d}\n" for d in answers).encode())


if __name__ == "__main__":
    import argparse  # CLI only: the container calls main() directly, so skip this import there

    parser = argparse.ArgumentParser(description="Track D (Traverse) optimized implementation.")
    parser.add_argument("--data", default="/data", help="directory holding graph.txt and queries.txt")
    parser.add_argument("--out", default="/out", help="directory to write distances.txt into")
    args = parser.parse_args()
    main(args.data, args.out)
