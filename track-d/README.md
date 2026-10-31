# Track D: Traverse

The task, the input files and the exact output format are in the [spec](https://github.com/hackathon-raptors/speed-demon-2026/blob/main/specs/d-traverse.md).

## Approach

There are three phases on forked workers. The graph lives in shared anonymous `mmap`s, so children read it in place, nothing is copied on write, and only pages actually written are charged as memory.

1. **Parse.** The edge lines are cut into one line-aligned range per CPU.
   - Each child turns a 512 KB piece into integers with the C JSON scanner: spaces and newlines are translated to commas and the piece is read as one JSON array. That measured 8% faster than `split()` plus `int()`.
   - Each edge is stored twice, once under its tail for the forward graph and once under its head for the reverse graph. It goes into the bucket of the node range it belongs to, in the child's own slot of a scratch map.
   - An edge is one integer, `weight << shift | other end`.
2. **Build.** Child *j* owns node bucket *j*. It counts that bucket's edges per node, writes the row offsets, and scatters the edges into compressed sparse rows of both graphs in the shared graph map. The scratch map is then freed.
3. **Query.** Duplicate queries and `s == t` are answered once. Children pull query indexes from a pre-filled pipe, so a child that gets short queries takes more of them. Each query runs a bidirectional Dijkstra:
   - Heap keys are `distance << shift | node`, so a plain int heap orders by distance and carries the node. Adding an encoded edge to `distance << shift` gives the neighbour's key in one addition.
   - The side with the smaller heap is expanded.
   - The search stops once the two heap tops sum to at least the best path found. That test only starts once a path exists, which measured −4%.
   - If either side runs out of nodes, every path has been seen, so unreachable targets cost little.

**Environment.** Debian's statically linked CPython 3.13 on `debian:13-slim`, precompiled stdlib, `-I -S`, garbage collector off.

## Dependencies

None: the Python standard library only. numpy would make parsing 1.65× and the build 2.4× faster (measured). That is about 18% of D's time, which the dependency penalty cancels exactly, so we left it out.

## With another 24 hours

Queries are about 60% of the time, and every settled node costs a heap pop, a few dict lookups and a slice. The next steps would be:
- a cheaper meeting test;
- trying a radix heap (allowed by the spec) against `heapq`.

Contraction hierarchies or landmarks would cut each search a lot. But building them means full-graph Dijkstras, which take seconds in Python, more than the whole query phase.
