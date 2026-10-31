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
import json
import marshal
import mmap
import os
from array import array
from heapq import heappop, heappush
from itertools import accumulate, pairwise

GRAPH_FILE = "graph.txt"
QUERIES_FILE = "queries.txt"
OUTPUT_FILE = "distances.txt"
MAX_LINE = 64
MIN_LINE = 6  # "u v w\n": bounds the edge count of a range by its bytes
PIECE = 512 << 10  # parse a range in pieces: small transients (512 KB: -10% memory vs 1 MB)
COMMAS = bytes.maketrans(b" \n", b",,")  # edge lines -> one JSON array of ints
INF = 1 << 128  # above every heap key: distances are below 2^64, node ids below 2^64
QUEUE_SLOTS = 1 << 14  # 4-byte queue records that fit an empty 64 KiB pipe

# Inherited by forked children.
_fd = -1
_n = 0
_shift = 1
_span = 1  # nodes per build bucket
_slots = []  # per parse range: (fwd nodes, fwd edges, rev nodes, rev edges) views into scratch
_counts = []  # per parse range: [fwd edges per bucket, rev edges per bucket]
_bases = [[], []]  # per direction, first edge slot of each bucket in the graph map
_csr = ()  # (fwd offsets, fwd edges, rev offsets, rev edges): views into the graph map
_tasks = []  # distinct (s, t) with s != t
_per = 1  # tasks per queue record
_queue = -1


def fork_map(func, items):
    """[func(item) for item in items], one forked child per item, results via marshal."""
    pipes = []
    for item in items:
        r, w = os.pipe()
        if not os.fork():  # child: send the result; exit 1 if func raised
            try:
                with open(w, "wb") as f:
                    marshal.dump(func(item), f)
                os._exit(0)
            finally:
                os._exit(1)
        os.close(w)
        pipes.append(r)
    out = []
    for r in pipes:
        with open(r, "rb") as f:
            out.append(marshal.loads(f.read()))  # one read: load(f) reads per object
    if any(os.wait()[1] for _ in pipes):
        raise RuntimeError("worker failed")
    return out


def views(buf, cuts):
    """(uint32, uint64, uint32, uint64) views of buf between consecutive byte offsets."""
    return [buf[a:b].cast(t) for a, b, t in zip(cuts, cuts[1:], "IQIQ")]


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
    return [(a, b) for a, b in pairwise(bounds) if a < b]


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
        # json's C scanner builds the ints straight from the text: faster than split()
        # then int() on every token (-8% parse).
        it = iter(json.loads(b"[%s]" % buf[:cut].translate(COMMAS).rstrip(b",")))
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
        for a in arrays:  # copy straight into the slot: no joined temporary
            view[at : at + len(a)] = a
            at += len(a)
    return [list(map(len, fk)), list(map(len, bk))]


def build(j):
    """Phase 2: bucket j -> its CSR rows of both graphs, counted then scattered in place."""
    lo = j * _span
    rows = min(_n, lo + _span) - lo
    for d, offsets, edges in ((0, _csr[0], _csr[1]), (1, _csr[2], _csr[3])):
        # This bucket's (node, edge) pairs in every parse slot.
        at = [(sum(c[d][:j]), sum(c[d][: j + 1])) for c in _counts]
        parts = [(s[2 * d][a:b], s[2 * d + 1][a:b]) for s, (a, b) in zip(_slots, at)]
        degree = array("I", bytes(4 * rows))
        for nodes, _ in parts:
            for k in nodes:
                degree[k] += 1
        # Row starts; the last one is the next bucket's base, which it writes too.
        pos = array("I", accumulate(degree, initial=_bases[d][j]))
        offsets[lo : lo + rows + 1] = pos
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
        # Stop once no unsettled pair can beat mu (skipped until a path is known: -4%).
        if mu < INF and (hf[0] >> shift) + (hb[0] >> shift) >= mu:
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


def main(data_dir="/data", out_dir="/out"):
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
            _slots.append(views(view, [at + x * cap for x in (0, 4, 12, 16, 24)]))
            at += 24 * cap
        _counts = fork_map(parse, enumerate(ranges))
    finally:
        os.close(_fd)
        _fd = -1

    buckets = -(-n // _span)
    sizes = [[sum(c[d][j] for c in _counts) for j in range(buckets)] for d in (0, 1)]
    _bases = [[0, *accumulate(b)] for b in sizes]
    m = _bases[0][-1]
    # Graph map: offsets (n + 1 uint32, padded to 8 bytes) then edges (m uint64), per direction.
    off_bytes = -(-(n + 1) * 4 // 8) * 8
    half = off_bytes + 8 * m
    graph = memoryview(mmap.mmap(-1, 2 * half))
    _csr = views(graph, [0, off_bytes, half, half + off_bytes, 2 * half])
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
    answers = [0] * len(queries)
    if _tasks:
        _per = -(-len(_tasks) // QUEUE_SLOTS)
        records = -(-len(_tasks) // _per)
        _queue, w = os.pipe()
        # Fits the empty pipe, so this never blocks.
        os.write(w, array("I", range(records)).tobytes())
        os.close(w)
        found = [0] * len(_tasks)
        for part in fork_map(answer, range(min(workers, records))):
            for task, d in part:
                found[task] = d
        os.close(_queue)
        answers = (found[index[q]] if q in index else 0 for q in queries)

    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, OUTPUT_FILE), "wb") as f:
        f.write("".join(f"{d}\n" for d in answers).encode())


if __name__ == "__main__":
    import sys

    flags = dict(zip(sys.argv[1::2], sys.argv[2::2]))  # --data DIR --out DIR
    main(flags.get("--data", "/data"), flags.get("--out", "/out"))
