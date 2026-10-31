# Track C: performance log

Every step from the reference to the final version, each with the live-board run ID that measured it. Rows without a run ID were measured on our laptop (i7-8665U, 4 cores and 8 threads, in the same container the board uses), as interleaved A/B runs with medians.

| Step | Change | Run ID | T (s) | M (MB) | Points |
|---|---|---|---|---|---|
| 0 | The reference, unchanged | | 14.44 | 367 | 0 |
| 1 | One regex per record, forked workers over record-aligned ranges, flat region table, distinct names in a second phase | RUN_142bc06 | 1.016 | 243 | 28.4 |
| 2 | Debian's statically linked CPython 3.13; 256 KB pieces | RUN_bed61e9 | | | |
| 3 | numpy: whole pieces parsed vectorised, no per-record Python | RUN_numpy | | | |

## What each step did

**1. The design (1.016 s on the board).**
- **Parser:** one compiled regex matches a whole record, with possessive quantifiers so it never backtracks into a field. Records are bucketed by country, and each bucket is aggregated with C-level `sum`/`min`/`max`.
- **Region table:** region id → country is a flat `bytearray`, not a dict. Workers share the table copy-on-write, and dict lookups would write reference counts and copy every page of a 200k-entry dict into each worker.
- **Distinct names:** each worker ships its names as one `\0`-joined blob per country. A second fork counts distinct names per country in a `set`, with the countries spread over workers by size.
- **A trap we found:** keeping the previous piece's record tuples alive while the regex parsed the next piece cost 22–29%. Each piece's objects are now dropped before the next is parsed.

**2. Interpreter and piece size (laptop, in container: 1.79–1.85 → 1.54–1.59 s, then 256 KB pieces: median 1994 → 1510 ms, 187 → 165 MB).**
- **Interpreter:** Debian's python3.13 links libpython statically and was 14% faster here than `python:3.13-slim` (see track A's log).
- **Piece size:** 256 KB pieces beat 512 KB and 1 MB on both time and memory.

**3. numpy (laptop, in container: median 1410 → 965 ms, −32%; 166 → 145 MB, −13%).** A profile of the regex version showed:

| share | what |
|---|---|
| 22% | the regex engine |
| 12% | allocation |
| 11% | building and freeing the per-record tuples |
| 6% | `int()` |

So the per-record Python had to go. Each piece is now handled as a `uint8` array:
- **Quotes:** a running count of `"` bytes marks which bytes are inside quotes. Spec 2.1 puts quotes only in quoted fields, so a byte is outside when the count is even. The count is `uint16`, which wraps harmlessly: only its parity and short differences are used.
- **Records and fields:** an LF outside quotes ends a record; the byte before it is dropped if it is a CR, and a record starting with `EF` skips its BOM. Exactly four commas outside quotes per record split the fields; anything else is an error.
- **Numbers:** read right to left with one gather per digit place across all records at once.
- **Stats:** count, sums, min and max per country are `bincount` and `ufunc.at`. An unescaped length is the field length minus two delimiters minus one per `""` pair, which is half the quote count.
- **Names:** gathered, still escaped, into one `\0`-separated blob per country in a single fancy-index. Phase 2 is unchanged.

One worker's range: 0.35 → 0.16 s. numpy is our only dependency. It costs about 2.3 points on the D axis and adds 40 code lines; with the time and memory gains, K still improves by about 9%. It is vendored in `vendor/` exactly as pip installed it, because builds have no network.

## Tried and reverted (laptop measurements)

| Experiment | Result | Why it lost |
|---|---|---|
| A 10 MB region table (no out-of-range check) plus C-level bucketing with `map`/`deque` | the bucket loop alone −17%, but the full run +2.7% time and +14 MB | the bigger table costs cache and pages in every worker |
| glibc `top_pad` | first run −6%; on re-runs median 1565 vs 1570 ms with more memory | it was noise |
| Pinning each forked worker to one CPU | median 1706 vs 1585 ms | slower |
| `os._exit` instead of interpreter teardown; `-OO` | no difference (1524 vs 1505 ms for `-OO`) | nothing to save |
| numpy prototype, first version (masked digit loop, `int64` cumsum, `xor` accumulate) | 1.28× faster | fixed by a `uint16` cumsum that gives both parity and counts, and `np.where` digits: 2.2× |
