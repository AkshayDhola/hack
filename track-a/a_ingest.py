#!/usr/bin/env python3
"""Speed Demon 2026, Track A (Ingest), optimized implementation.

    python3.13 a_ingest.py [--data /data] [--out /out]

Reads requests.log and writes summary.txt: for every (UTC hour, endpoint) pair that
has at least one request, the request count, the p50/p95/p99 latency (nearest rank) and
the number and rate of 5xx responses. specs/a-ingest.md is the contract.

Two phases on raw forked children (no multiprocessing: its import and pool cost more
than the work they save), results back over pipes with marshal:
1. Each child parses one line-aligned file range with os.pread and packs latencies per
   key into uint32 arrays, split into one partition per worker by key hash.
2. Child j merges partition j from every phase-1 result (inherited copy-on-write),
   computes nearest-rank percentiles and formats its lines; the parent sorts lines.
"""

import gc
import marshal
import os
from array import array

INPUT_NAME = "requests.log"
OUTPUT_NAME = "summary.txt"

SUB_CHUNK = 256 << 10
MAX_LINE = 256

_fd = -1
_workers = 1
_results = []  # phase-1 output, inherited by the phase-2 fork


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
            out.append(marshal.loads(f.read()))
    for _ in pipes:
        if os.wait()[1]:
            raise RuntimeError("worker failed")
    return out


def work(span):
    """Phase 1: parse one file range -> per partition, marshalled ({key: packed latencies}, {key: 5xx})."""
    start, end = span

    lats = {}
    errs = {}
    rest = b""

    while start < end:
        n = min(SUB_CHUNK, end - start)
        buf = rest + os.pread(_fd, n, start)
        start += n

        cut = buf.rfind(b"\n") + 1

        # Process a final unterminated line, if present.
        if start == end:
            cut = len(buf)

        rest = buf[cut:]
        fields = buf[:cut].split()
        del buf

        local = {}

        # Fields per record: timestamp, method, endpoint, status, latency.
        # The timestamp's first 13 bytes identify the hour.
        it = iter(fields)
        for ts, _, ep, st, lat in zip(it, it, it, it, it):

            key = ts[:13] + ep

            try:
                local[key].append(lat)
            except KeyError:
                local[key] = [lat]

            if st[0] == 53:  # ASCII '5': 5xx status codes.
                errs[key] = errs.get(key, 0) + 1

        for key, values in local.items():
            try:
                lats[key].extend(map(int, values))
            except KeyError:
                lats[key] = array("I", map(int, values))

    # Forked children share the parent's hash seed, so partitions agree across workers.
    parts = _workers
    lat_parts = [{} for _ in range(parts)]
    err_parts = [{} for _ in range(parts)]
    for key, values in lats.items():
        j = hash(key) % parts
        lat_parts[j][key] = values.tobytes()
        if key in errs:
            err_parts[j][key] = errs[key]
    return [marshal.dumps(p) for p in zip(lat_parts, err_parts)]


def finish(j):
    """Phase 2: merge partition j from every phase-1 result, then summarize it."""
    latency_parts = {}
    errors = {}
    for blobs in _results:
        part_lats, part_errors = marshal.loads(blobs[j])
        for key, packed in part_lats.items():
            try:
                latency_parts[key].append(packed)
            except KeyError:
                latency_parts[key] = [packed]
        for key, count in part_errors.items():
            errors[key] = errors.get(key, 0) + count

    out = []
    for key, parts in latency_parts.items():
        values = array("I", b"".join(parts)).tolist()
        values.sort()

        count = len(values)
        errs = errors.get(key, 0)

        out.append(
            b"%s:00:00Z %s %d %d %d %d %d %d\n"
            % (
                key[:13],
                key[13:],
                count,
                values[(50 * count + 99) // 100 - 1],
                values[(95 * count + 99) // 100 - 1],
                values[(99 * count + 99) // 100 - 1],
                errs,
                errs * 1_000_000 // count,
            )
        )
    # Sorted runs make the parent's final sort a cheap merge.
    out.sort()
    return b"".join(out)


def spans(fd, size, parts):
    """Divide the file into approximately equal, line-aligned ranges."""
    bounds = [0]

    for i in range(1, parts):
        pos = size * i // parts

        nl = os.pread(fd, MAX_LINE, pos).find(b"\n")

        if nl < 0:
            break

        bounds.append(pos + nl + 1)

    bounds.append(size)

    return [(start, end) for start, end in zip(bounds, bounds[1:]) if start < end]


def main(data_dir, out_dir):
    global _fd, _workers, _results
    gc.disable()  # no reference cycles: skip collector passes over millions of objects

    _fd = os.open(os.path.join(data_dir, INPUT_NAME), os.O_RDONLY)
    output = b""
    try:
        size = os.fstat(_fd).st_size
        if size:
            _workers = max(1, len(os.sched_getaffinity(0)))
            _results = fork_map(work, spans(_fd, size, _workers))
            blobs = fork_map(finish, range(_workers))
            _results = []
            # Space (0x20) sorts below every endpoint byte, so sorting whole lines
            # gives the spec order: hour, then endpoint with a proper prefix first.
            lines = b"".join(blobs).splitlines(keepends=True)
            lines.sort()
            output = b"".join(lines)
    finally:
        os.close(_fd)
        _fd = -1

    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, OUTPUT_NAME), "wb") as f:
        f.write(output)


if __name__ == "__main__":
    import argparse  # CLI only: the container calls main() directly, so skip this import there

    parser = argparse.ArgumentParser(description="Track A (Ingest) optimized implementation.")
    parser.add_argument("--data", default="/data", help="directory holding requests.log")
    parser.add_argument("--out", default="/out", help="directory to write summary.txt into")
    args = parser.parse_args()
    main(args.data, args.out)
