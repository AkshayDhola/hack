#!/usr/bin/env python3.13
"""Speed Demon 2026, Track B (Index), optimized.

    python3.13 b_index.py [--data /data] [--out /out]

Reads docs.txt and queries.txt and writes results.txt (specs/b-index.md).

Two phases, each on a forked pool:

1. Documents, cut into one range per CPU. A worker normalizes its range in bulk
   (lower + translate, in C), builds posting lists for the BOOL terms and the
   phrase anchors only, answers every PHRASE query on its own documents (a
   phrase needs the text, which never leaves the worker), and returns the BOOL
   postings as packed arrays of global ids.
2. Queries. The parent joins each term's postings (ranges are in id order, so
   the result is sorted), forks again so the joined postings are shared
   copy-on-write, and splits the distinct BOOL queries across workers: each one
   is evaluated exactly once, on whole-corpus sets.
"""
import gc
import heapq
import marshal
import os
from array import array
from itertools import repeat

DOCS_FILE = "docs.txt"
QUERIES_FILE = "queries.txt"
RESULTS_FILE = "results.txt"
EMPTY = frozenset()

# One translate pass: A-Z lowercased, every byte not a-z, 0-9, TAB or LF becomes a space.
KEEP = b"abcdefghijklmnopqrstuvwxyz0123456789\t\n"
TABLE = bytes(c if (c := bytes([b]).lower()[0]) in KEEP else 0x20 for b in range(256))
SAMPLE = 2 << 20  # corpus bytes used to estimate document frequency
PIECE = 4 << 20  # read and normalize a range in pieces: small transient buffers
# A BOOL query with a term above this estimated document share is "heavy": its
# sets are big, so it is answered per document range where sets are small.
HEAVY_DF = 0.01


def consume(iterator):
    """Run an iterator of None results to the end in C."""
    any(iterator)

# Inherited by forked workers.
_fd = -1
_bool_terms = frozenset()
_phrases = []  # (anchor, terms) per distinct phrase
_anchors = frozenset()
_heavy = []  # distinct BOOL query indexes answered in phase 1
_light_terms = frozenset()
_postings = {}  # phase 2: term -> packed ascending global ids
_bools = []  # phase 2: distinct BOOL word lists


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


def normalize(raw):
    """docs.txt bytes -> lines of "<id>\\t<text>", every separator byte in the text now a space."""
    return raw.translate(TABLE)


def sample_df(fd, terms):
    """Document frequency of each query term in the first SAMPLE bytes, and the sample's doc count."""
    lines = normalize(os.pread(fd, SAMPLE, 0)).split(b"\n")[:-1]
    df = dict.fromkeys(terms, 0)
    hits = frozenset(terms).intersection
    for line in lines:
        for term in hits(line.split()[1:]):
            df[term] += 1
    return df, max(1, len(lines))


def parse_queries(data):
    """-> per line (is_bool, index into the distinct BOOL or PHRASE list), and those lists as words."""
    order = []
    bools = {}
    phrases = {}
    for line in data.split(b"\n"):
        if not line:
            continue
        kind, _, rest = line.partition(b" ")
        if kind == b"BOOL":
            order.append((True, bools.setdefault(rest, len(bools))))
        elif kind == b"PHRASE" and rest.startswith(b'"') and rest.endswith(b'"'):
            order.append((False, phrases.setdefault(rest[1:-1], len(phrases))))
        else:
            raise ValueError(f"bad query {line!r}")
    return order, [q.split(b" ") for q in bools], [p.split(b" ") for p in phrases]


def phrase_match(text, terms):
    """True if `terms` occur as consecutive tokens of a "<id>\t<text>" line's text (tokens
    separated by runs of spaces)."""
    first = terms[0]
    end = len(text)
    start = text.find(b"\t") + 1
    pos = text.find(first, start)
    while pos >= 0:
        if pos == start or text[pos - 1] == 32:
            j = pos + len(first)
            for term in terms[1:]:
                if j >= end or text[j] != 32:
                    break
                while j < end and text[j] == 32:
                    j += 1
                if not text.startswith(term, j):
                    break
                j += len(term)
            else:
                if j == end or text[j] == 32:
                    return True
        pos = text.find(first, pos + 1)
    return False


def index_range(span):
    """Phase 1: one document range -> (phrase answers, BOOL postings as packed global ids)."""
    start, end = span
    lines = []
    rest = b""
    while start < end:
        n = min(PIECE, end - start)
        part = normalize(rest + os.pread(_fd, n, start)).split(b"\n")
        start += n
        rest = part.pop()  # partial last line (b"" when the piece ends on LF); normalize is idempotent
        lines += part
    if rest:
        lines.append(rest)
    first = int(lines[0].partition(b"\t")[0])
    lists = {t: array("I") for t in _bool_terms | _anchors}  # 4 bytes per posting
    hits = frozenset(lists).intersection
    postings_of = lists.__getitem__
    for doc, line in enumerate(lines, first):
        # Append doc to the posting list of every indexed term in its text (split()[0] is
        # the id), looping in C.
        consume(map(array.append, map(postings_of, hits(line.split()[1:])), repeat(doc)))
    n = len(lines)
    phrase_answers = []
    # Stream phrase matches instead of allocating a potentially large list for
    # every phrase. Candidate postings are in ascending document-ID order.
    matches = phrase_match
    for anchor, terms in _phrases:
        count = 0
        total = 0
        smallest = []
        for doc in lists[anchor]:
            if matches(lines[doc - first], terms):
                count += 1
                total += doc
                if len(smallest) < 10:
                    smallest.append(doc)
        phrase_answers.append((count, total, smallest))
    del lines
    # Heavy queries have big sets: use one int per term as a bitset over this
    # range (bit k = doc first + k), so AND/OR/NOT run on machine words in C.
    bits = {}

    left = use_counts(_bools[i] for i in _heavy)

    def get(term):
        try:
            x = bits[term]
        except KeyError:
            ids = lists[term]
            if len(ids) >= 1000:  # long list: set flag bytes in C, then pack 8 strided slices
                flags = bytearray(n)
                consume(map(flags.__setitem__, map(int.__sub__, ids, repeat(first)), repeat(1)))
                x = 0
                for j in range(8):  # flags[j::8][i] is doc 8i+j -> bit 8i+j
                    x |= int.from_bytes(flags[j::8], "little") << j
            else:
                packed = bytearray(nbytes)
                for k in map(int.__sub__, ids, repeat(first)):
                    packed[k >> 3] |= 1 << (k & 7)
                x = int.from_bytes(packed, "little")
        left[term] -= 1
        if left[term]:
            bits[term] = x
        else:
            bits.pop(term, None)  # last use: keep only sets still needed
        return x

    # masks[b] has bit k set iff bit b of k is set: id sum = sum of popcount(x & masks[b]) << b.
    nbytes = (n + 7) // 8
    masks = [int.from_bytes(bytes([p]) * nbytes, "little") for p in (0xAA, 0xCC, 0xF0)]
    for b in range(max(0, n.bit_length() - 3)):
        block = 1 << b
        pattern = (bytes(block) + b"\xff" * block) * (nbytes // (2 * block) + 1)
        masks.append(int.from_bytes(pattern[:nbytes], "little"))
    heavy_answers = [evaluate_bits(_bools[i], get, first, masks) for i in _heavy]
    return phrase_answers, heavy_answers, {t: ids.tobytes()
                                          for t in _light_terms if (ids := lists[t])}


def use_counts(word_lists):
    """term -> how many times the queries use it, so a cached set can be freed after its last use."""
    left = {}
    for words in word_lists:
        for term in words[0::2]:
            left[term] = left.get(term, 0) + 1
    return left


def evaluate_bits(words, get, first, masks):
    """BOOL words on int bitsets -> (count, id sum, 10 smallest ids)."""
    x = get(words[0])
    for j in range(1, len(words), 2):
        op = words[j]
        y = get(words[j + 1])
        if op == b"AND":
            x &= y
        elif op == b"OR":
            x |= y
        elif op == b"NOT":
            x &= ~y
        else:
            raise ValueError(f"unknown operator {op!r}")
    n = x.bit_count()
    total = first * n
    for b, mask in enumerate(masks):
        total += (x & mask).bit_count() << b
    smallest = []
    for _ in range(min(n, 10)):
        low = x & -x
        smallest.append(first + low.bit_length() - 1)
        x ^= low
    return n, total, smallest


def evaluate(words, get):
    """BOOL words -> (count, id sum, 10 smallest ids), operators applied left to right."""
    result = get(words[0])
    for j in range(1, len(words), 2):
        op = words[j]
        postings = get(words[j + 1])
        if op == b"AND":
            result = result & postings
        elif op == b"OR":
            result = result | postings
        elif op == b"NOT":
            result = result - postings
        else:
            raise ValueError(f"unknown operator {op!r}")
    n = len(result)
    smallest = sorted(result)[:10] if n <= 64 else heapq.nsmallest(10, result)
    return n, sum(result), smallest


def answer_bools(indices):
    """Phase 2: evaluate some distinct BOOL queries on whole-corpus posting sets."""
    sets = {}
    left = use_counts(_bools[i] for i in indices)

    def get(term):
        try:
            s = sets[term]
        except KeyError:
            packed = _postings.get(term)
            s = frozenset(array("I", packed)) if packed else EMPTY
        left[term] -= 1
        if left[term]:
            sets[term] = s
        else:
            sets.pop(term, None)  # last use: keep only sets still needed
        return s

    return [evaluate(_bools[i], get) for i in indices]


def combine(per_range, n):
    """Per-range answers (ranges in id order) -> whole-corpus answers."""
    if not per_range:
        return [(0, 0, [])] * n
    out = []
    for answers in zip(*per_range):
        count = 0
        total = 0
        smallest = []
        # Ranges are ordered by document ID, so local top-10 lists can be
        # concatenated only until the global first 10 IDs have been collected.
        for answer in answers:
            count += answer[0]
            total += answer[1]
            if len(smallest) < 10:
                smallest.extend(answer[2][:10 - len(smallest)])
        out.append((count, total, smallest))
    return out


def spans(fd, size, parts):
    """Cut [0, size) into about `parts` ranges ending on line boundaries."""
    bounds = [0]
    for i in range(1, parts):
        pos = max(size * i // parts, bounds[-1])
        while True:
            chunk = os.pread(fd, 1 << 16, pos)
            nl = chunk.find(b"\n")
            if nl >= 0 or not chunk:
                break
            pos += len(chunk)
        if nl < 0:
            break
        bounds.append(pos + nl + 1)
    bounds.append(size)
    return [(a, b) for a, b in zip(bounds, bounds[1:]) if a < b]


def main(data_dir="/data", out_dir="/out"):
    gc.disable()  # no reference cycles: skip collector passes over millions of objects
    global _fd, _bool_terms, _phrases, _anchors, _postings, _bools, _heavy, _light_terms
    _fd = os.open(os.path.join(data_dir, DOCS_FILE), os.O_RDONLY)
    with open(os.path.join(data_dir, QUERIES_FILE), "rb") as f:
        order, _bools, phrase_words = parse_queries(f.read())
    df, sample_docs = sample_df(_fd, {w for words in _bools for w in words[0::2]}.union(*phrase_words))
    # A phrase only needs candidates from one of its terms: index the one rarest in the sample.
    _phrases = [(min(terms, key=df.__getitem__), terms) for terms in phrase_words]
    _bool_terms = frozenset(w for words in _bools for w in words[0::2])
    _anchors = frozenset(a for a, _ in _phrases)
    limit = HEAVY_DF * sample_docs
    is_heavy = [max(df.get(w, 0) for w in words[0::2]) > limit for words in _bools]
    _heavy = [i for i, h in enumerate(is_heavy) if h]
    light = [i for i, h in enumerate(is_heavy) if not h]
    _light_terms = frozenset(w for i in light for w in _bools[i][0::2])
    workers = len(os.sched_getaffinity(0))
    ranges = spans(_fd, os.fstat(_fd).st_size, workers)

    parts = fork_map(index_range, ranges)
    # Ranges are in id order: summed counts and sums, concatenated smallest ids, joined postings.
    phrase_results = combine([p[0] for p in parts], len(_phrases))
    bool_results = [None] * len(_bools)
    for i, r in zip(_heavy, combine([p[1] for p in parts], len(_heavy))):
        bool_results[i] = r
    _postings = {t: b"".join(p[2].get(t, b"") for p in parts) for t in _light_terms}
    del parts

    chunks = [c for c in (light[j::workers] for j in range(workers)) if c]
    for chunk, results in zip(chunks, fork_map(answer_bools, chunks)):
        for i, r in zip(chunk, results):
            bool_results[i] = r

    lines = []
    for is_bool, i in order:
        n, total, smallest = (bool_results if is_bool else phrase_results)[i]
        lines.append(" ".join(map(str, [n, total] + smallest)) + "\n")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, RESULTS_FILE), "w", encoding="ascii", newline="\n") as f:
        f.write("".join(lines))


if __name__ == "__main__":
    import sys

    flags = dict(zip(sys.argv[1::2], sys.argv[2::2]))  # --data DIR --out DIR
    main(flags.get("--data", "/data"), flags.get("--out", "/out"))
