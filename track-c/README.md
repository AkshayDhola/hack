# Track C: Join

The task, the input files and the exact output format are in the [spec](https://github.com/hackathon-raptors/speed-demon-2026/blob/main/specs/c-join.md).

## Approach

The cost is parsing 200 MB of hostile CSV correctly, so the parser has no per-record Python at all.

**Ranges and pieces.**
- The parent loads `regions.csv` into a flat table, region id → country index, one byte per id (two bytes if there were over 255 countries). A dict would be shared copy-on-write with every worker, but its reference counts would copy its pages into each one.
- The parent cuts `records.csv` into one range per CPU at record starts. A record starts at a line start with a digit or a BOM; a note's continuation lines are lowercase words, so they never look like one.
- Forked workers read their range in 256 KB pieces, each ending at a record start.

**One piece, vectorised with numpy.**
- **Quotes:** a running count of quote bytes (`cumsum`) marks which bytes are inside quotes. Quotes only occur in quoted fields (spec 2.1), so a byte is outside when the count is even.
- **Records:** an LF outside quotes ends a record, the byte before it is dropped if it is a CR, and a record starting with `EF` starts with a BOM, which is skipped. Exactly four commas outside quotes per record split the five fields; any other count is an error.
- **Numbers:** ids, region ids and amounts are read with digit arithmetic, one gather per digit place across all records at once. Amounts become integer cents.
- **Countries and stats:** a region id indexes the table (ids beyond it are `_UNMATCHED`). Count, sums, min and max per country are `bincount` and `ufunc.at` over the whole piece.
- **Byte lengths:** the unescaped length of a quoted field is its length minus its two delimiters minus one per `""` pair, which is half its quote count.
- **Names:** the inner, still-escaped name bytes are gathered country by country into one `\0`-separated blob in a single fancy-index.

**Distinct names.** Escaping is one-to-one, so escaped bytes compare like unescaped ones. A second forked phase gives each worker a share of the countries (largest first, to the least loaded worker), and it counts the distinct names in a `set`.

**Environment.** Same as the other tracks: Debian's statically linked CPython 3.13 on `debian:13-slim`, precompiled stdlib, `-I -S`, garbage collector off.

## Dependencies

numpy 2.5.4 (`requirements.txt`), vendored in `vendor/` as pip installed it, with its `.dist-info`: builds run without network. It parses the CSV pieces vectorized (quote parity, digit arithmetic, `bincount`/`ufunc.at` sums); no dataframe, join or group-by library is used.

## With another 24 hours

- The digit loop and the per-field `ufunc.at` calls are now the largest numpy costs. They could be replaced with one sort by country and `reduceat`.
- The phase-2 name sets could become sorted numpy arrays of fixed-width names, with `unique` per country.
- Both need measuring: piece size and memory moved together every time we changed them here.
