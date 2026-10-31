# Track B: Index

The task, the input files and the exact output format are in the [spec](https://github.com/hackathon-raptors/speed-demon-2026/blob/main/specs/b-index.md).

## Approach

The queries are known up front, so only the terms they use are indexed.

**Planning in the parent.**
- It parses `queries.txt`, removes duplicate queries and estimates each term's document frequency from the first 2 MB of the corpus.
- A phrase only needs candidate documents from one of its words, so each phrase is anchored on the word that is rarest in that sample.
- A BOOL query with any term above 1% of documents is "heavy": its sets are big, so it is answered per document range, where the sets are small.

**Phase 1, one forked worker per document range.**
- **Text:** the worker lowercases and normalises its text with a single `bytes.translate` (in C), then splits it into lines.
- **Postings:** for every document, the indexed terms it contains are appended to `array('I')` posting lists (4 bytes per posting), with a `frozenset` intersection and C-level `map` doing the loop.
- **Phrases:** the worker answers every phrase on its own documents by checking each candidate document with `bytes.find`, so the text never leaves the worker.
- **Heavy BOOL queries:** these use bitsets. Each term becomes one Python int with one bit per document in the range, so AND, OR and NOT run on machine words in C. Id sums come from popcounts against bit masks.

**Phase 2.**
- The parent joins each light term's postings across ranges; the ranges are in id order, so the result stays sorted.
- It forks again, so the joined postings are shared copy-on-write.
- The distinct light BOOL queries are split across workers and each is evaluated exactly once on whole-corpus `frozenset`s.

**Environment.** Debian's statically linked CPython 3.13 on `debian:13-slim`, precompiled stdlib, `-I -S`, garbage collector off.

## Dependencies

None: the Python standard library only.

## With another 24 hours

Memory peaks in phase 1 (730 MB), because every worker keeps its documents' text for phrase matching. We checked whether only the phrase-candidate documents could be kept: they are 84% of the documents and 96% of the bytes, so that saves nothing.

The spec allows Roaring bitmaps for the light BOOL sets and a multi-pattern matcher (Aho-Corasick) for the phrases. Both would be dependencies, and they only pay off if B could overtake our C track. With B as our weakest track, it is dropped from the overall anyway.
