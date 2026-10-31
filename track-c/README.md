# Track C: Join

The task, the input files and the exact output format are in the [spec](https://github.com/hackathon-raptors/speed-demon-2026/blob/main/specs/c-join.md).

## Approach

How your solution works, in plain prose.

## Dependencies

numpy 2.5.4 (`requirements.txt`), vendored in `vendor/` as pip installed it, with its `.dist-info`: builds run without network. It parses the CSV pieces vectorized (quote parity, digit arithmetic, `bincount`/`ufunc.at` sums); no dataframe, join or group-by library is used.

## With another 24 hours

What you would do next.
