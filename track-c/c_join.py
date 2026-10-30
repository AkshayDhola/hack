#!/usr/bin/env python3.13
"""Speed Demon 2026, Track C (Join), optimized.

    python3.13 c_join.py [--data /data] [--out /out]

Reads records.csv and regions.csv and writes aggregates.txt (specs/c-join.md).
One regex parses whole records in C. The file is cut into one range per CPU at
record starts; forked workers read their range in small pieces, bucket records
by country and aggregate each bucket with C-level sum/min/max/set. The parent
merges per-country partials.
"""
import gc
import marshal
import os
import re
from itertools import repeat

RECORDS_FILE = "records.csv"
REGIONS_FILE = "regions.csv"
OUTPUT_FILE = "aggregates.txt"
UNMATCHED = b"_UNMATCHED"
PIECE = 1 << 20
SPLIT_MIN = 1 << 20

# id, name, note, region_id, amount. Spec 2.2: a non-empty name or note is
# always quoted, so a text field is either empty or quoted and one group holds
# its still-escaped inner bytes. Ids, region ids and amounts are never quoted and
# have no leading zeros, so region ids are compared as bytes and an amount
# without its dot is its cents.
# Possessive quantifiers (*+, ++): no backtracking into a matched field or number.
FIELD = rb'(?:"([^"]*+(?:""[^"]*+)*+)"|)'
RECORD = re.compile(rb"(?:\xef\xbb\xbf)?([0-9]++)," + FIELD + b"," + FIELD
                    + rb",([0-9]++),(-?[0-9]++\.[0-9]{2})(?:\r?\n|\Z)")
# A record starts at a line start with a digit or a BOM. Continuation lines of a
# multi-line note are lowercase words, so they never look like one.
RECORD_START = re.compile(rb"\n(?=[0-9]|\xef\xbb\xbf)")

_fd = -1  # inherited by forked workers
# Region id -> country index (0: unmatched). A flat table, not a dict: dict lookups write
# refcounts on its 400k key/value objects, copying those pages into every forked worker.
_table = b""
_blobs = {}  # phase 2: country -> name blobs from every worker


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


def last_record_start(buf):
    """Offset of the last record start in buf, or -1. Notes span at most four lines."""
    for lo in (max(0, len(buf) - 4096), 0):
        pos = -1
        for m in RECORD_START.finditer(buf, lo):
            pos = m.end()
        if pos >= 0:
            return pos
    return -1


def aggregate(recs):
    """Records of one country -> partial aggregates; names stay one NUL-joined blob."""
    ids, names, notes, _, amounts = zip(*recs)
    cents = list(map(int, map(bytes.replace, amounts, repeat(b"."), repeat(b""))))
    notes = b"\0".join(notes)
    joined = b"\0".join(names)
    n = len(recs)
    # Escaped length minus one byte per "" pair; \0 separators keep pairs apart.
    return [n, sum(map(int, ids)), sum(cents), min(cents), max(cents),
            len(joined) - (n - 1) - joined.count(b'""'),
            len(notes) - (n - 1) - notes.count(b'""'), [joined]]


def merge(totals, part):
    """Fold one partial-aggregates dict into another, in place."""
    for key, p in part.items():
        t = totals.get(key)
        if t is None:
            totals[key] = p
            continue
        t[0] += p[0]
        t[1] += p[1]
        t[2] += p[2]
        t[3] = min(t[3], p[3])
        t[4] = max(t[4], p[4])
        t[5] += p[5]
        t[6] += p[6]
        if len(p) > 7:
            t[7] += p[7]


def work(span):
    start, end = span
    totals = {}
    rest = b""
    table = _table
    while start < end:
        n = min(PIECE, end - start)
        buf = rest + os.pread(_fd, n, start)
        start += n
        cut = len(buf) if start >= end else last_record_start(buf)
        if cut <= 0:
            rest = buf
            continue
        rest = buf[cut:]
        buckets = {}
        for rec in RECORD.findall(buf, 0, cut):
            try:
                key = table[int(rec[3])]
            except IndexError:  # id above the lookup's range
                key = 0
            try:
                buckets[key].append(rec)
            except KeyError:
                buckets[key] = [rec]
        merge(totals, {k: aggregate(v) for k, v in buckets.items()})
    # One \0-joined blob per country pickles far faster than a set of bytes.
    # Names are deduplicated once per country in phase 2, not in growing per-worker sets.
    names = {k: b"\0".join(t.pop()) for k, t in totals.items()}
    return totals, names


def count_names(keys):
    """Phase 2: per country in keys, the number of distinct names across all workers' blobs."""
    out = []
    for k in keys:
        names = set()
        for blob in _blobs[k]:
            names.update(blob.split(b"\0"))
        out.append(len(names))
    return out


def spans(fd, start, end, parts):
    """Cut [start, end) into about `parts` ranges, each starting at a record start."""
    bounds = [start]
    # Small files: one range, so a quoted LF+BOM (spec 2.1, never generated) can't be cut.
    for i in range(1, parts if end - start > SPLIT_MIN else 1):
        pos = max(start + (end - start) * i // parts, bounds[-1])
        m = RECORD_START.search(os.pread(fd, 1 << 16, pos))
        if m is None:
            break
        bounds.append(pos + m.end())
    bounds.append(end)
    return [(a, b) for a, b in zip(bounds, bounds[1:]) if a < b]


def load_regions(path):
    """regions.csv -> (table: region id -> country index, 0 = unmatched; countries by index)."""
    with open(path, "rb") as f:
        head, _, body = f.read().partition(b"\n")
    if head != b"region_id,country":
        raise ValueError("unexpected regions.csv header")
    tokens = body.replace(b",", b" ").split()
    ids = list(map(int, tokens[0::2]))
    codes = tokens[1::2]
    countries = [UNMATCHED, *set(codes)]
    size = max(ids, default=0) + 1
    # One byte per id while indexes fit, else two.
    table = bytearray(size) if len(countries) <= 256 else memoryview(bytearray(2 * size)).cast("H")
    any(map(table.__setitem__, ids, map({c: i for i, c in enumerate(countries)}.__getitem__, codes)))
    return table, countries


def format_cents(cents):
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    return f"{sign}{cents // 100}.{cents % 100:02d}"


def main(data_dir, out_dir):
    gc.disable()  # no reference cycles: skip collector passes over millions of objects
    global _fd, _table, _blobs
    _table, countries = load_regions(os.path.join(data_dir, REGIONS_FILE))
    _fd = os.open(os.path.join(data_dir, RECORDS_FILE), os.O_RDONLY)
    size = os.fstat(_fd).st_size
    header = os.pread(_fd, 64, 0)
    skip = header.find(b"\n") + 1
    if header[:skip].removeprefix(b"\xef\xbb\xbf").rstrip(b"\r\n") != b"id,name,note,region_id,amount":
        raise ValueError("unexpected records.csv header")
    workers = len(os.sched_getaffinity(0))
    totals = {}
    for part, names in fork_map(work, spans(_fd, skip, size, workers)):
        merge(totals, part)
        for k, blob in names.items():
            _blobs.setdefault(k, []).append(blob)
    totals = {countries[k]: t for k, t in totals.items()}
    _blobs = {countries[k]: b for k, b in _blobs.items()}
    keys = sorted(totals)
    # Biggest countries first, each to the least-loaded worker; children inherit _blobs.
    loads = [0] * workers
    chunks = [[] for _ in range(workers)]
    for k in sorted(keys, key=lambda k: -sum(map(len, _blobs[k]))):
        j = loads.index(min(loads))
        loads[j] += sum(map(len, _blobs[k]))
        chunks[j].append(k)
    chunks = [c for c in chunks if c]
    distinct = dict(zip([k for c in chunks for k in c],
                        [n for counts in fork_map(count_names, chunks) for n in counts]))
    _blobs = {}
    lines = []
    for key in keys:
        n, id_sum, amt, lo, hi, name_bytes, note_bytes = totals[key]
        lines.append(f"{key.decode()} {n} {id_sum} {format_cents(amt)} {format_cents(lo)} "
                     f"{format_cents(hi)} {distinct[key]} {name_bytes} {note_bytes}\n")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, OUTPUT_FILE), "wb") as f:
        f.write("".join(lines).encode())


if __name__ == "__main__":
    import argparse  # CLI only: the container calls main() directly, so skip this import there

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data", default="/data")
    ap.add_argument("--out", default="/out")
    args = ap.parse_args()
    main(args.data, args.out)
