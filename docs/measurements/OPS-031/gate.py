"""Reproducible capture research; never produces a qualified peak.

Requires /tmp/ops031-capture cross-built from capture/main_linux.go.
Output must be new; all Docker resources are disposable and labelled.
"""
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid

HERE = Path(__file__).resolve().parent
OUT = Path(sys.argv[1]).resolve()
OUT.mkdir(parents=True, exist_ok=False)
PROJECT = "binance-ops031-" + uuid.uuid4().hex[:10]
LABEL = "com.docker.compose.project=" + PROJECT
IMAGE = "postgres:18.6-bookworm@sha256:bbf83594798e144bff365a9c191c5d779f181fb3135902064ab8ff11918020fc"
DATA = "/var/lib/postgresql/18/docker"
report = {"project": PROJECT, "commands": [], "status": "running",
          "temp_peak_live_bytes": None, "qualification_blocked": True}
clients = []


def run(args, check=True, timeout=60, **kwargs):
    report["commands"].append(args)
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout, **kwargs)
    if check and p.returncode:
        raise RuntimeError((args, p.returncode, p.stderr[-2000:]))
    return p.stdout


def ex(*args, **kwargs):
    return run(["docker", "exec", PROJECT, *args], **kwargs)


def sql(query):
    return ex("psql", "-XAt", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", query)


def collect(name, remote):
    run(["docker", "cp", PROJECT + ":" + remote, str(OUT / name)])


def process_evidence():
    return ex("sh", "-c", "for p in /proc/[0-9]*/status; do if test \"$(sed -n 's/^Name:[[:space:]]*//p' \"$p\")\" = postgres; then sed -n '/^Name:/p;/^Pid:/p;/^PPid:/p;/^TracerPid:/p;/^Threads:/p' \"$p\"; fi; done")


HELPER = r'''set -eu
mkdir -p /tmp/work
f=/tmp/work/pgsql_tmp1.0
exec 3<>"$f"
dd if=/dev/zero bs=4096 count=4 >&3 2>/dev/null
printf 'grown '; stat -Lc '%d %i %s %h' "$f"
ln "$f" "$f.alias"
printf 'alias '; stat -Lc '%d %i %s %h' "$f.alias"
rm "$f"
printf 'linked_alias '; stat -Lc '%d %i %s %h' "$f.alias"
rm "$f.alias"
printf 'unlinked_open '; stat -Lc '%d %i %s %h' "/proc/$$/fd/3"
truncate -s 4096 "/proc/$$/fd/3"
printf 'shrunken '; stat -Lc '%d %i %s %h' "/proc/$$/fd/3"
truncate -s 12288 "/proc/$$/fd/3"
printf 'regrown '; stat -Lc '%d %i %s %h' "/proc/$$/fd/3"
exec 4>&3
exec 3>&-
printf 'dup_open '; stat -Lc '%d %i %s %h' "/proc/$$/fd/4"
exec 4>&-
exec 3<>"$f"
dd if=/dev/zero bs=4096 count=2 >&3 2>/dev/null
printf 'reused '; stat -Lc '%d %i %s %h' "$f"
rm "$f"
exec 3>&-
test -z "$(ls -A /tmp/work)"
printf 'closed_empty\n'
'''


try:
    (OUT / "capture-source.go.txt").write_bytes((HERE / "capture/main_linux.go").read_bytes())
    (OUT / "gate-source.py.txt").write_bytes(Path(__file__).read_bytes())
    report["binary_sha256"] = hashlib.sha256(Path("/tmp/ops031-capture").read_bytes()).hexdigest()
    report["source_sha256"] = hashlib.sha256((HERE / "capture/main_linux.go").read_bytes()).hexdigest()
    report["image"] = run(["docker", "image", "inspect", "--format", "{{.Id}}", IMAGE]).strip()
    run(["docker", "volume", "create", "--label", LABEL, PROJECT + "-data"])
    run(["docker", "run", "-d", "--name", PROJECT, "--label", LABEL,
         "--network", "none", "--cpus", "1", "--memory", "1g",
         "--cap-add", "SYS_PTRACE", "--security-opt", "seccomp=unconfined",
         "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-v", PROJECT + "-data:/var/lib/postgresql",
         "--entrypoint", "sh", IMAGE, "-c",
         "docker-entrypoint.sh postgres -c io_method=sync & wait; sleep infinity"])
    for _ in range(200):
        logs = run(["docker", "logs", PROJECT], check=False)
        if "init process complete" in logs:
            ready = ex("pg_isready", "-U", "postgres", check=False)
            if "accepting connections" in ready:
                break
        time.sleep(.1)
    else:
        raise RuntimeError("postgres readiness timeout")
    report["settings"] = json.loads(sql("SELECT json_object_agg(name,setting) FROM pg_settings WHERE name IN ('server_version','io_method','fsync','synchronous_commit','full_page_writes','data_directory','temp_tablespaces');"))
    report["tablespaces"] = json.loads(sql("SELECT json_agg(t) FROM (SELECT oid,spcname,pg_tablespace_location(oid) AS location FROM pg_tablespace) t"))
    assert report["settings"]["io_method"] == "sync"
    assert all(not t["location"] for t in report["tablespaces"])
    sql("CREATE TABLE gate_input AS SELECT i,repeat(md5(i::text),4) payload FROM generate_series(1,20000) i; ANALYZE gate_input;")
    ex("gosu", "postgres", "pg_ctl", "-D", DATA, "stop", "-m", "fast", "-w")
    run(["docker", "cp", "/tmp/ops031-capture", PROJECT + ":/tmp/capture"])
    ex("chmod", "755", "/tmp/capture")
    run(["docker", "exec", "-i", PROJECT, "sh", "-c", "cat >/tmp/helper.sh"], input=HELPER)
    started = time.monotonic()
    (OUT / "filesystem.txt").write_text(ex("/tmp/capture", "/tmp/work", "/tmp/fs.gz", "--", "sh", "/tmp/helper.sh"))
    report["filesystem_traced_seconds"] = time.monotonic() - started
    collect("filesystem.jsonl.gz", "/tmp/fs.gz")
    started = time.monotonic()
    ex("sh", "/tmp/helper.sh")
    report["filesystem_untraced_seconds"] = time.monotonic() - started
    # Trace the final postmaster from exec, not an attachment after its children exist.
    p = subprocess.Popen(["docker", "exec", "--user", "postgres", PROJECT,
        "/tmp/capture", DATA + "/base/pgsql_tmp", "/tmp/pg.gz", "--",
        "postgres", "-D", DATA, "-c", "io_method=sync", "-c", "log_temp_files=0"],
        stdout=open(OUT / "postgres.stdout", "w"), stderr=open(OUT / "postgres.stderr", "w"))
    clients.append(p)
    for _ in range(300):
        if p.poll() is not None:
            raise RuntimeError("capture exited during startup")
        if "accepting connections" in ex("pg_isready", "-U", "postgres", check=False):
            break
        time.sleep(.1)
    else:
        raise RuntimeError("traced postgres readiness timeout")
    query = "SET work_mem='64kB'; SET max_parallel_workers_per_gather=0; SELECT count(*) FROM (SELECT * FROM gate_input ORDER BY payload OFFSET 0) t;"
    started = time.monotonic()
    report["ordinary_result"] = sql(query)
    report["ordinary_traced_seconds"] = time.monotonic() - started
    (OUT / "ordinary-processes.txt").write_text(process_evidence())
    query = "SET work_mem='64kB'; SET max_parallel_workers_per_gather=2; SET min_parallel_table_scan_size=0; SET parallel_setup_cost=0; SET parallel_tuple_cost=0; EXPLAIN (ANALYZE, FORMAT JSON) SELECT count(*) FROM gate_input a JOIN gate_input b USING (i);"
    started = time.monotonic()
    (OUT / "parallel-plan.txt").write_text(sql(query))
    report["parallel_traced_seconds"] = time.monotonic() - started
    (OUT / "after-parallel-processes.txt").write_text(process_evidence())
    ex("gosu", "postgres", "pg_ctl", "-D", DATA, "stop", "-m", "fast", "-w", timeout=120)
    report["capture_exit"] = p.wait(timeout=30)
    collect("postgres.jsonl.gz", "/tmp/pg.gz")
    report["unavailable"] = ex("/tmp/missing-ops031-observer", check=False)
    ex("/tmp/capture", "/tmp/work", "/tmp/killed.gz", "--", "sh", "-c", "kill -KILL $$")
    collect("killed-tracee.jsonl.gz", "/tmp/killed.gz")
    ex("cp", "/lib/aarch64-linux-gnu/libc.so.6", "/tmp/work/preload.so")
    ex("/tmp/capture", "/tmp/work", "/tmp/mmap.gz", "--", "env", "LD_PRELOAD=/tmp/work/preload.so", "/bin/true")
    collect("unsupported-mmap.jsonl.gz", "/tmp/mmap.gz")
    # Real tracer interruption, not a manufactured missing footer.
    interrupted = subprocess.Popen(["docker", "exec", PROJECT, "sh", "-c",
        "echo $$ >/tmp/interrupted.pid; exec /tmp/capture /tmp/work /tmp/interrupted.gz -- sleep 30"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    clients.append(interrupted)
    for _ in range(100):
        if ex("sh", "-c", "test -s /tmp/interrupted.gz && echo ready", check=False).strip() == "ready":
            break
        time.sleep(.05)
    else:
        raise RuntimeError("interruption control did not start")
    ex("sh", "-c", "kill -KILL \"$(cat /tmp/interrupted.pid)\"")
    report["interrupted_exit"] = interrupted.wait(timeout=10)
    collect("interrupted.jsonl.gz", "/tmp/interrupted.gz")
    report["kernel"] = ex("uname", "-a").strip()
    report["status"] = "capture-research-not-qualified"
except Exception as error:
    report["status"] = "failed"
    report["error"] = repr(error)
    if isinstance(error, subprocess.TimeoutExpired):
        report["timeout_stderr"] = str(error.stderr)
    for name, remote in (("incomplete-filesystem.jsonl.gz", "/tmp/fs.gz"),
                         ("incomplete-postgres.jsonl.gz", "/tmp/pg.gz")):
        run(["docker", "cp", PROJECT + ":" + remote, str(OUT / name)], check=False)
    raise
finally:
    run(["docker", "rm", "-f", PROJECT], check=False)
    for p in clients:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
    run(["docker", "volume", "rm", PROJECT + "-data"], check=False)
    report["cleanup"] = {kind: run(["docker", kind, "ls", *(["-a"] if kind == "container" else []),
        "-q", "--filter", "label=" + LABEL]).splitlines() for kind in ("container", "volume", "network")}
    report["sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in OUT.iterdir() if p.is_file() and p.name != "report.json"}
    (OUT / "report.json").write_text(json.dumps(report, indent=2) + "\n")
print(report["status"])
