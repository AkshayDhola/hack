# Track A: Ingest

The task, the input files and the exact output format are in the [spec](https://github.com/hackathon-raptors/speed-demon-2026/blob/main/specs/a-ingest.md).

## Approach

The work is one pass over 239 MB of text, so the solution is about doing as little per line as possible and doing it on every core.

**Two phases on forked workers.** The parent cuts `requests.log` into one line-aligned range per CPU and forks one child per range: plain `os.fork`, a pipe and `marshal`, because importing `multiprocessing` and starting a pool cost more than the work they would parallelise.

1. **Parse.** Each child reads its range with `os.pread` in 128 KB pieces and lets `bytes.split()` tokenise a piece in C. The timestamp is never parsed. The first 13 bytes (`2026-10-30T18`) already name the hour, so the group key is just that prefix plus the endpoint bytes. Each latency goes into its key's `array('I')`, 4 bytes per value instead of a 28-byte int object in a list. A 5xx is a status whose first byte is `5`. At the end the child splits its keys into one partition per worker by `hash(key)` and marshals the packed arrays. Forked children share the parent's hash seed, so every child puts a key in the same partition.
2. **Summarise.** Child *j* merges partition *j* from every phase-1 result. Those results are inherited copy-on-write, so nothing is sent to it. It sorts each key's latencies with C's timsort, reads the nearest-rank p50/p95/p99 by index, and formats its lines. The parent concatenates and sorts the lines. A space sorts below every endpoint byte, so a plain byte sort gives the spec's order (hour, then endpoint, a proper prefix first).

**Environment.**
- The image runs Debian's CPython 3.13, copied out of the allowlisted `gcc:15` image into `debian:13-slim`. Its interpreter links libpython statically and measured 18% faster here than `python:3.13-slim`.
- The standard library is precompiled with unchecked-hash `.pyc` files, and the run uses `-I -S`.
- The garbage collector is off: nothing here makes reference cycles.

## Dependencies

None: the Python standard library only.

## With another 24 hours

A profile of a worker shows where the rest of the time goes:

| share | what |
|---|---|
| 22% | the interpreter loop |
| 22% | object allocation |
| 11% | `bytes.split` |
| 6% | SipHash on the dict keys |
| 4% | int parsing |

All of that is the cost of one Python object per token. We tried selection instead of sorting, regex and space-split parsing, numpy, StringZilla, malloc tuning and CPU pinning (see PERF.md); none helped. The next real step is a compiled per-line kernel, which we chose not to write, to keep the entry Python.
