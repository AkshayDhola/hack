#!/usr/bin/env python3.13
"""Speed Demon 2026, Track C (Join), optimized.

    python3.13 c_join.py [--data /data] [--out /out]

Reads records.csv and regions.csv and writes aggregates.txt (specs/c-join.md).
The file is cut into one range per CPU at record starts; forked workers read their
range in small pieces and parse each piece with numpy (vendored in ./vendor), no
per-record Python: quote parity marks the commas and LFs outside quotes, the
numbers are read with vectorized digit arithmetic and the per-country stats are
summed with bincount and ufunc.at. The parent merges per-country partials; the
distinct names are counted per country in a second forked phase.
"""
import gc
import marshal
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "vendor"))
import numpy as np  # noqa: E402

RECORDS_FILE = "records.csv"
REGIONS_FILE = "regions.csv"
OUTPUT_FILE = "aggregates.txt"
HEADER = b"id,name,note,region_id,amount"
UNMATCHED = b"_UNMATCHED"
PIECE = 256 << 10  # piece size: 256 KB beat 512 KB and 1 MB (time and memory)
SPLIT_MIN = 1 << 20

POW10 = 10 ** np.arange(19, dtype=np.int64)
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


def last_record_start(buf):
    """Offset of the last record start in buf, or -1. Notes span at most four lines."""
    for lo in (max(0, len(buf) - 4096), 0):
        pos = -1
        for m in RECORD_START.finditer(buf, lo):
            pos = m.end()
        if pos >= 0:
            return pos
    return -1


def number(a, s, e):
    """Decimal digits a[s:e] -> int64 per row, right to left: one gather per digit place."""
    n = e - s
    v = np.zeros(len(s), np.int64)
    for k in range(int(n.max(initial=0))):
        # Inactive rows may index an earlier field (or wrap within the piece).
        # Their weight is zero; active rows always index an ASCII digit.
        digit = a[e - 1 - k] - np.uint8(48)
        v += digit * np.where(n > k, POW10[k], 0)
    return v


def scan(piece, table, acc, names):
    """Whole records -> per-country stats added into acc, inner name bytes into names."""
    a = np.frombuffer(piece, np.uint8)
    # Quotes so far. Spec 2.1: a quote only occurs in a quoted field, so a byte is
    # outside quotes when the count is even. uint16 wraps, harmlessly: only the parity
    # and differences across one field are used.
    quotes = np.cumsum(a == 34, dtype=np.uint16)
    outside = (quotes & 1) == 0
    lf = np.flatnonzero((a == 10) & outside)  # record terminators
    if piece[-1:] != b"\n":
        lf = np.append(lf, len(a))  # the final record may lack its terminator
    start = np.concatenate(([0], lf[:-1] + 1))
    start += 3 * (a[start] == 0xEF)  # a BOM can only start a record (ids are digits)
    commas = np.flatnonzero((a == 44) & outside)
    if len(commas) != 4 * len(lf):
        raise ValueError("malformed record in records.csv")
    c0, c1, c2, c3 = commas.reshape(-1, 4).T
    end = lf - (a[lf - 1] == 13)  # drop the CR of a CRLF
    neg = a[c3 + 1] == 45
    cents = number(a, c3 + 1 + neg, end - 3) * 100 + number(a, end - 2, end)
    cents = np.where(neg, -cents, cents)
    region = number(a, c2 + 1, c3)
    key = np.where(region < len(table), table[np.minimum(region, len(table) - 1)], 0)
    key = key.astype(np.intp)

    def unescaped(s, e):  # quoted: minus both delimiters and one byte per "" pair
        q = (quotes[e - 1] - quotes[s - 1]).astype(np.int64)
        return e - s - q // 2 - (q > 0)

    count, ids, total, lo, hi, name_bytes, note_bytes = acc
    count += np.bincount(key, minlength=len(count))
    np.add.at(ids, key, number(a, start, c0))
    np.add.at(total, key, cents)
    np.minimum.at(lo, key, cents)
    np.maximum.at(hi, key, cents)
    np.add.at(name_bytes, key, unescaped(c0 + 1, c1))
    np.add.at(note_bytes, key, unescaped(c1 + 1, c2))
    # Inner (still escaped) name bytes, \0-separated and grouped by country, in one gather.
    quoted = a[c0 + 1] == 34
    order = np.argsort(key, kind="stable")
    s, e, key = (c0 + 1 + quoted)[order], (c1 - quoted)[order], key[order]
    size = e - s + 1  # the name and its \0
    ends = np.cumsum(size)
    idx = np.repeat(s - (ends - size), size) + np.arange(ends[-1])
    idx[ends - 1] = len(a)  # each \0: the zero byte appended below
    blob = np.append(a, np.uint8(0))[idx].tobytes()
    cuts = [0, *(np.flatnonzero(key[1:] != key[:-1]) + 1).tolist(), len(key)]
    for i, j in zip(cuts, cuts[1:]):
        names.setdefault(int(key[i]), []).append(blob[ends[i] - size[i] : ends[j - 1] - 1])


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


def work(span):
    """Phase 1: one range -> ({country: [count, id sum, cents sum, min, max, name bytes,
    note bytes]}, {country: \0-joined inner name bytes})."""
    start, end = span
    table = np.frombuffer(_table, np.uint16 if isinstance(_table, memoryview) else np.uint8)
    k = int(table.max()) + 1
    big = 1 << 62
    acc = [np.zeros(k, np.int64) for _ in range(3)]
    acc += [np.full(k, big, np.int64), np.full(k, -big, np.int64)]
    acc += [np.zeros(k, np.int64) for _ in range(2)]
    names = {}
    rest = b""
    while start < end:
        n = min(PIECE, end - start)
        buf = rest + os.pread(_fd, n, start)
        start += n
        cut = len(buf) if start >= end else last_record_start(buf)
        if cut <= 0:
            rest = buf
            continue
        rest = buf[cut:]
        scan(buf[:cut], table, acc, names)
    totals = {int(c): [int(x[c]) for x in acc] for c in np.flatnonzero(acc[0])}
    # One \0-joined blob per country pickles far faster than a set of bytes.
    # Names are deduplicated once per country in phase 2, not in growing per-worker sets.
    packed = {}
    while names:
        c, blobs = names.popitem()
        packed[c] = b"\0".join(blobs)
    return totals, packed


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
    table = bytearray(size)
    if len(countries) > 256:
        table = memoryview(bytearray(2 * size)).cast("H")
    index = {c: i for i, c in enumerate(countries)}
    any(map(table.__setitem__, ids, map(index.__getitem__, codes)))
    return table, countries


def format_cents(c):
    return f"{'-' if c < 0 else ''}{abs(c) // 100}.{abs(c) % 100:02d}"


def main(data_dir="/data", out_dir="/out"):
    gc.disable()  # no reference cycles: skip collector passes over millions of objects
    global _fd, _table, _blobs
    _table, countries = load_regions(os.path.join(data_dir, REGIONS_FILE))
    _fd = os.open(os.path.join(data_dir, RECORDS_FILE), os.O_RDONLY)
    size = os.fstat(_fd).st_size
    header = os.pread(_fd, 64, 0)
    skip = header.find(b"\n") + 1
    if header[:skip].removeprefix(b"\xef\xbb\xbf").rstrip(b"\r\n") != HEADER:
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
    distinct = dict(zip(sum(chunks, []), sum(fork_map(count_names, chunks), [])))
    _blobs = {}
    lines = []
    for key in keys:
        n, ids, amount, lo, hi, name_bytes, note_bytes = totals[key]
        head = f"{key.decode()} {n} {ids} {format_cents(amount)} {format_cents(lo)} {format_cents(hi)}"
        lines.append(f"{head} {distinct[key]} {name_bytes} {note_bytes}\n")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, OUTPUT_FILE), "wb") as f:
        f.write("".join(lines).encode())


if __name__ == "__main__":
    import sys

    flags = dict(zip(sys.argv[1::2], sys.argv[2::2]))  # --data DIR --out DIR
    main(flags.get("--data", "/data"), flags.get("--out", "/out"))
