# Track A: performance log

Every step from the reference to the final version, each with the live-board run ID that measured it. Rows without a run ID were measured on our laptop (i7-8665U, 4 cores and 8 threads, in the same container the board uses); the laptop is noisy, so those use interleaved A/B runs and medians.

| Step | Change | Run ID | T (s) | M (MB) | Points |
|---|---|---|---|---|---|
| 0 | The reference, unchanged | | 32.05 | 228 | 0 |
| 1 | Forked workers over line-aligned ranges, `bytes.split`, hour as a byte prefix, `array('I')` latencies, partitioned merge | RUN_142bc06 | 0.715 | 127 | 40.5 |
| 2 | Latency `int` straight into the key's array, no per-piece lists | RUN_9e288ac | | | |
| 3 | Debian's statically linked CPython 3.13; 128 KB pieces | RUN_bed61e9 | | | |

## What each step did

**1. The design (0.715 s on the board).** The reference spends most of its time in `datetime.strptime`.
- **Hour without parsing:** the first 13 bytes of the timestamp already name the hour, so the group key is `ts[:13] + endpoint`, built from tokens `bytes.split()` makes in C.
- **Parallelism:** one forked child per CPU parses one line-aligned range. A second fork merges one hash partition per child.
- **Why not `multiprocessing`:** its import and pool cost more than they saved, so it's raw `os.fork` + pipe + `marshal`.
- **Memory:** latencies are packed in `array('I')`, 4 bytes each instead of a 28-byte int in an 8-byte list slot. The garbage collector is off.

**2. No temporary lists (laptop, in container: 1035–1076 → 812–835 ms, −22%).** The parse loop first collected each piece's latencies as `bytes` in a per-piece dict of lists, then converted them with `map(int, …)`. Appending `int(lat)` straight to the key's array removes that dict, its lists and the second pass.
- One worker's loop alone: 0.243 → 0.207 s (−15%).
- In the full run it gained more: the per-piece lists also churned memory across all workers.

**3. Interpreter and piece size (laptop, in container: 823–854 → 678–702 ms, −18%).**
- **Interpreter:** `python:3.13-slim` is built with PGO and LTO, but its interpreter calls into a shared libpython. Debian's python3.13 has neither PGO nor LTO, but links libpython statically, and it was faster on all four tracks (A −18%, B −16%, C −14%, D −13 to −20%). The image copies it out of the allowlisted `gcc:15` image, which is too big to be the runtime itself (2.18 GB against the 2 GB cap), into `debian:13-slim`.
- **Piece size:** 128 KB pieces beat 256 KB (median 742 vs 866 ms) and 64 KB (792 ms), probably because a piece and its tokens stay in cache.

## Tried and reverted (laptop measurements)

| Experiment | Result | Why it lost |
|---|---|---|
| Quickselect for the three percentiles instead of sorting | 4.8× slower (0.357 vs 0.075 s on 36k keys) | Python has no selection written in C; a Python loop loses to C timsort, and the lists average 24 values |
| `re.findall` to pull out only the needed fields | 0.229 vs 0.076 s for `split()` | `split()` is already a C tokenizer |
| `split(b" ")`: 4 tokens per line, latency inside the next line's first token | 0.246 vs 0.208 s | the extra `find` and slices cost more than one fewer token |
| C-level `map` pipeline; caching the hour prefix | 0.25 vs 0.24 s; 0.48 vs 0.47 s | the cost is object creation, not bytecode |
| numpy vectorised parse (newline/space positions, digit arithmetic) | parse alone 0.155 s + 70 ms import, vs 0.207 s for the whole Python loop with grouping | grouping by variable-length endpoint is still left to do; numpy would also cost a dependency |
| StringZilla SIMD split | lazy split 0.05 ms, but making the tokens `bytes` 0.31–0.77 ms vs `bytes.split` 0.26 ms per 128 KB | dict keys need real `bytes` |
| Zero-copy reads (re-`pread` the partial line instead of `rest + piece` and a slice) | 0.221 vs 0.216 s | two 128 KB copies per piece are negligible |
| glibc malloc tunables (`top_pad`, `trim_threshold`, `mmap_threshold`) | page faults 148k → 45k, time −3%, memory +10% | neutral on K |
| `os._exit` instead of interpreter teardown | no difference | the parent holds little at exit |
| `-OO` with `-o 2` bytecode | 681 vs 664 ms | no asserts in the hot code |
| Pinning each forked worker to one CPU | within noise (slower on track C) | the scheduler already spreads them |
| Fewer workers to save memory | each worker costs about 0.5 MB beyond its data | K weighs time 0.5 and memory 0.2, so more workers win |

Where the time goes now (perf, one worker):

| share | what |
|---|---|
| 22% | the interpreter loop |
| 22% | allocation |
| 11% | `bytes.split` |
| 6% | SipHash of the keys |
| 4% | `int()` |

That is one Python object per token, and we found no way to avoid it in Python.
