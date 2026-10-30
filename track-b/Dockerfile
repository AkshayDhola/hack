FROM python:3.13-slim@sha256:d98cc29d3266e535dd9da1099688d9cb3ca4753813571e071763d17725460962
COPY b_index.py /app/
# The run is read-only and the image ships no stdlib bytecode: compile everything now, so no
# module is compiled on every start. unchecked-hash: no source stat per import.
RUN python3 -m compileall -q --invalidation-mode unchecked-hash /usr/local/lib/python3.13 /app
# -I: isolated (no env, no user site); -S: no site import. -c imports the cached module
# directly (a script run as __main__ is always recompiled).
ENTRYPOINT ["python3", "-I", "-S", "-c", "import sys;sys.path.insert(0,'/app');from b_index import main;main('/data','/out')"]
