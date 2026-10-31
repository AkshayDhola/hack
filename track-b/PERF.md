# Track B: performance log

Every step from the reference to the final version, each with the live-board run ID that measured it. Rows without a run ID were measured on our laptop (i7-8665U, 4 cores and 8 threads, in the same container the board uses), as interleaved A/B runs with medians.

| Step | Change | Run ID | T (s) | M (MB) | Points |
|---|---|---|---|---|---|
| 0 | The reference, unchanged | | 56.35 | 2,200 | 0 |
| 1 | Index only the query terms, on forked workers; phrases answered per range; heavy BOOL queries as bitsets; light ones on whole-corpus sets | RUN_142bc06 | 4.901 | 912 | 26.3 |
| 2 | Debian's statically linked CPython 3.13 | RUN_bed61e9 | | | |

## What each step did

**1. The design (4.901 s on the board).**

**Planning:**
- The queries are readable up front, so only the terms they use get posting lists.
- Each phrase is anchored on its rarest word, estimated from a 2 MB sample, so only documents containing that word are checked for the phrase.

**Phase 1, per document range:**
- **Text:** one `bytes.translate` lowercases the range and turns separators into spaces.
- **Postings:** `array('I')` posting lists are filled through a `frozenset` intersection and C-level `map`, with no per-token Python loop.
- **Phrases:** answered on the worker's own documents, so the text never leaves the worker.
- **Heavy BOOL queries** (a term in over 1% of documents): one Python int per term with one bit per document, so AND, OR and NOT run on machine words in C. Id sums come from popcounts against bit masks.

**Phase 2:** a second fork evaluates each distinct light BOOL query once, on whole-corpus `frozenset`s shared copy-on-write.

**Time split on the laptop:** index 60%, heavy bitsets 27%, phrases 13%.

**2. Interpreter (laptop, in container: 8.35–8.57 → 7.03–7.23 s, −16%).** Debian's python3.13 links libpython statically (see track A's log).

## Tried and reverted (laptop measurements)

| Experiment | Result | Why it lost |
|---|---|---|
| Reading worker results with streaming `marshal.load(f)` | +17% time | it reads per object; `marshal.loads(f.read())` reads once |
| Keep only phrase-candidate documents in phase 1 to cut the 730 MB peak | candidates are 84% of documents, 96% of bytes | nothing to save |
| 2 MB or 1 MB pieces instead of 4 MB | medians 7.85 s and 8.14 s vs 7.65 s, memory −2% | slower |
| `os._exit` instead of teardown; `-OO` | no difference | |

B is our weakest track, so it is dropped from the overall (best three of four). Dependencies the spec allows here (Roaring bitmaps, Aho-Corasick) only pay off if B could overtake our C track.
