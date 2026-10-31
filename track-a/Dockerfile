# Interpreter: Debian's CPython 3.13 from gcc:15 (too big for the 2 GB image cap as the
# runtime itself). Its python3.13 links libpython statically and measured ~18% faster
# here than python:3.13-slim's shared-library build.
FROM gcc:15@sha256:eb5e64286ad47187b32bda75c6b6916166ecb41da56559b8b09e0001748d2cce AS py
FROM debian:13-slim@sha256:62bd423aeee912639ca8ee681c2b762cb3e25137e0a92b20be455898a9f034ad
COPY --from=py /usr/bin/python3.13 /usr/bin/
COPY --from=py /usr/lib/python3.13 /usr/lib/python3.13
COPY --from=py /usr/lib/x86_64-linux-gnu/libexpat.so.1 /usr/lib/x86_64-linux-gnu/
COPY a_ingest.py /app/
# The run is read-only and the image ships no stdlib bytecode: compile everything now, so no
# module is compiled on every start. unchecked-hash: no source stat per import.
RUN python3.13 -m compileall -q --invalidation-mode unchecked-hash /usr/lib/python3.13 /app
# -I: isolated (no env, no user site); -S: no site import. -c imports the cached module
# directly (a script run as __main__ is always recompiled).
ENTRYPOINT ["python3.13", "-I", "-S", "-c", "import sys;sys.path.insert(0,'/app');from a_ingest import main;main('/data','/out')"]
