# Track D: performance log

Every step from the reference to the final version, each with the live-board run ID that measured it. Rows without a run ID were measured on our laptop (i7-8665U, 4 cores and 8 threads, in the same container the board uses), as interleaved A/B runs with medians.

| Step | Change | Run ID | T (s) | M (MB) | Points |
|---|---|---|---|---|---|
| 0 | The reference, unchanged | | 72.69 | 893 | 0 |
| 1 | Parse, build and query on forked workers; graph as CSR in shared mmaps; bidirectional Dijkstra on packed int keys | RUN_142bc06 | 3.511 | 353 | 31.1 |
| 2 | Typed arrays in the build; stop test skipped until a path exists | RUN_9e288ac | | | |
| 3 | Debian's statically linked CPython 3.13; JSON-scanner int parsing; 512 KB pieces | run-3105f90c1a55 | 2.691 | 320 | 34.7 |

## What each step did

**1. The design (3.511 s on the board).** The reference keeps the graph as Python dicts of lists and runs one Dijkstra per query.

**Graph storage:**
- **Shared memory:** the graph lives in anonymous `mmap`s created before the fork. Children read it in place, it is never copied on write, and only written pages count as memory.
- **Parse:** each child parses a range of edge lines into its own slot of a scratch map, bucketed by node range, once by tail (forward graph) and once by head (reverse graph). An edge is one int, `weight << shift | other end`.
- **Build:** child *j* counts and scatters bucket *j* into CSR rows. Then the scratch map is freed.

**Queries:**
- **Deduplication:** duplicate queries and `s == t` are answered once.
- **Load balance:** workers pull query indexes from a pre-filled pipe, so long and short searches even out.
- **Search:** bidirectional Dijkstra on heap keys `distance << shift | node`. Adding an encoded edge to `distance << shift` gives the neighbour's key in one addition, and `heapq` compares plain ints.
- **Stop rule:** stop when the two heap tops sum to at least the best path. An exhausted side means every path has been seen.

**2. Typed arrays and the stop test (laptop: container memory 338 → about 275 MB; query kernel −4.2%).**
- `build()` kept its per-node degree and position counters in Python lists of ints; `array('I')` saves about 5 MB per worker at the build peak.
- The stop test is only evaluated once a path is known. Before that it can never fire, and skipping it saved 4.2% of query time.

**3. Interpreter, int parsing and piece size (laptop, in container).**
- **Interpreter:** Debian's statically linked python3.13 was 13–20% faster than `python:3.13-slim` (see track A's log).
- **Int parsing:** spaces and newlines are translated to commas and each piece is read as one JSON array, so the C JSON scanner builds the ints without making a `bytes` token for each one. One range: 0.395 → 0.361 s (−8.5%). Container memory: 260–276 → 241–261 MB.
- **Piece size:** 512 KB pieces instead of 1 MB keep the time (median 5.63 vs 5.74 s) and cut memory 265 → 240 MB.

## Tried and reverted (laptop measurements)

| Experiment | Result | Why it lost |
|---|---|---|
| Prune heap pushes: push a node only if its distance plus the other side's top beats the best path | 2.09 vs 1.96 s on 400 queries, same answers | the extra test on every relaxation costs more than the pushes it saves |
| Bucket by shift and mask (power-of-two node ranges) instead of `divmod` | 0.394 vs 0.395 s | equal |
| numpy parse and CSR build (vectorised digits; `bincount`, `cumsum` and stable `argsort` per bucket) | parse 1.65× faster, build 2.4× faster (byte-identical CSR), ≈ −18% of the whole run | one dependency costs about 2.3 points, which cancels it |
| orjson (Rust) / msgspec (C) instead of `json.loads` | parse 1.43× / 1.38× faster | about −5% of the whole run, less than a dependency costs |
| glibc malloc tunables | page faults down, no clear time win, +4–6% memory | |
| `os._exit` instead of teardown; `-OO` | no difference | |
| Landmarks (ALT) or contraction hierarchies | not built | they need full-graph Dijkstras, seconds each in Python, more than the whole query phase (2.4 s of 4.1 s on the laptop) |

Where the time goes now (laptop): parse 0.9 s, build 0.7 s, queries 2.4 s. Each settled node costs a heap pop, a few dict lookups and a slice of the edge view.
