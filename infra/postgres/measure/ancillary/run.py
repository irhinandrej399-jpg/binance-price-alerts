"""Reproducible capture research; never produces a qualified peak.

Requires /tmp/ops033-capture cross-built from capture/main_linux.go.
Output must be new; all Docker resources are disposable and labelled.
"""
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

OPS040 = os.environ.get("OPS040_REFERENCES") == "1"
HERE = Path(__file__).resolve().parent
OUT = Path(sys.argv[1]).resolve()
OUT.mkdir(parents=True, exist_ok=False)
PROJECT = "binance-ops033-" + uuid.uuid4().hex[:10]
LABEL = "com.docker.compose.project=" + PROJECT
IMAGE = "postgres:18.6-bookworm@sha256:bbf83594798e144bff365a9c191c5d779f181fb3135902064ab8ff11918020fc"
DATA = "/var/lib/postgresql/18/docker"
report = {"project": PROJECT, "commands": [], "status": "running",
          "temp_peak_live_bytes": None, "qualification_blocked": True}
clients = []
if OPS040:
    report.update(lower_bytes=None, upper_bytes=None, mode="ops040-references-v1")


def run(args, check=True, timeout=60, **kwargs):
    report["commands"].append(args)
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout, **kwargs)
    if check and p.returncode:
        raise RuntimeError((args, p.returncode, p.stderr[-2000:]))
    return p.stdout


def ex(*args, **kwargs):
    return run(["docker", "exec", PROJECT, *args], **kwargs)


def sql(query):
    return ex("psql", "-XAt", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", query, timeout=180 if OPS040 else 60)


def collect(name, remote):
    run(["docker", "cp", PROJECT + ":" + remote, str(OUT / name)])


def process_evidence():
    return ex("sh", "-c", "for p in /proc/[0-9]*/status; do if test \"$(sed -n 's/^Name:[[:space:]]*//p' \"$p\")\" = postgres; then sed -n '/^Name:/p;/^Pid:/p;/^PPid:/p;/^TracerPid:/p;/^Threads:/p' \"$p\"; fi; done")




try:
    for name in ("main_linux.go", "message_linux.go", "inventory_linux.go", "lifecycle_linux.go", "control/main_linux.go", "control/references_linux.go"):
        (OUT / (name.replace("/", "-") + ".txt")).write_bytes((HERE / name).read_bytes())
    (OUT / "gate-source.py.txt").write_bytes(Path(__file__).read_bytes())
    (OUT / "verifier-source.py.txt").write_bytes((HERE / "verify.py").read_bytes())
    if OPS040:
        dependencies = {}
        for ticket, run_name in (("OPS-038", "run-c"), ("OPS-039", "run-f")):
            directory = Path("docs/measurements") / ticket / run_name
            dependencies[ticket] = {"directory": str(directory), "report_sha256": hashlib.sha256((directory / "report.json").read_bytes()).hexdigest()}
        (OUT / "dependencies.json").write_text(json.dumps(dependencies, indent=2) + "\n")
        for source, target in (("inventory", "inventory-verifier.py.txt"), ("lifecycle", "lifecycle-verifier.py.txt"), ("references", "references-verifier.py.txt")):
            (OUT / target).write_bytes((HERE.parent / source / "verify.py").read_bytes())
        env = ["env", "GOCACHE=/tmp/binance-ops038-gocache", "GOMODCACHE=/tmp/binance-ops039-modcache", "GOOS=linux", "GOARCH=arm64", "CGO_ENABLED=0", "GOTOOLCHAIN=local"]
        for name, source in (("capture", "./infra/postgres/measure/ancillary"), ("control", "./infra/postgres/measure/ancillary/control")):
            run(env + ["go", "build", "-o", "/tmp/ops033-" + name, source])
    report["binary_sha256"] = hashlib.sha256(Path("/tmp/ops033-capture").read_bytes()).hexdigest()
    report["source_sha256"] = hashlib.sha256((HERE / "main_linux.go").read_bytes()).hexdigest()
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
    if OPS040:
        ex("mkdir", "-p", "/tmp/work/custom")
        ex("chown", "-R", "postgres:postgres", "/tmp/work")
        sql("CREATE TABLESPACE ops040_custom LOCATION '/tmp/work/custom'")
        report["tablespaces"] = json.loads(sql("SELECT json_agg(t) FROM (SELECT oid,spcname,pg_tablespace_location(oid) AS location FROM pg_tablespace) t"))
        witness = dict(io_method=report["settings"]["io_method"], settings=report["settings"], tablespaces=report["tablespaces"])
        (OUT / "pg-witness.json").write_text(json.dumps(witness))
        run(["docker", "cp", str(OUT / "pg-witness.json"), PROJECT + ":/tmp/pg-witness.json"])
    capture_env = ["-e", "OPS038_INVENTORY=1", "-e", "OPS039_LIFECYCLE=1", "-e", "OPS038_PATH_SCOPE=/tmp/work", "-e", "OPS038_PG_WITNESS=/tmp/pg-witness.json"] if OPS040 else []
    row_count = 2000 if OPS040 else 20000
    report["row_count"] = row_count
    sql(f"CREATE TABLE gate_input AS SELECT i,repeat(md5(i::text),4) payload FROM generate_series(1,{row_count}) i; ANALYZE gate_input;")
    ex("gosu", "postgres", "pg_ctl", "-D", DATA, "stop", "-m", "fast", "-w")
    run(["docker", "cp", "/tmp/ops033-capture", PROJECT + ":/tmp/capture"])
    ex("chmod", "755", "/tmp/capture")
    run(["docker", "cp", "/tmp/ops033-control", PROJECT + ":/tmp/control"])
    ex("chmod", "755", "/tmp/control")
    started = time.monotonic()
    mode = ["references040"] if OPS040 else []
    (OUT / "filesystem.txt").write_text(run(["docker", "exec", *capture_env, PROJECT, "/tmp/capture", "/tmp/work", "/tmp/fs.gz", "--", "/tmp/control", *mode], timeout=180 if OPS040 else 60))
    report["filesystem_traced_seconds"] = time.monotonic() - started
    collect("filesystem.jsonl.gz", "/tmp/fs.gz")
    if not OPS040:
        started = time.monotonic()
        ex("/tmp/control")
        report["filesystem_untraced_seconds"] = time.monotonic() - started
    report["control_binary_sha256"] = hashlib.sha256(Path("/tmp/ops033-control").read_bytes()).hexdigest()
    # Trace the final postmaster from exec, not an attachment after its children exist.
    pg_command = ["docker", "exec", *capture_env, "--user", "postgres", PROJECT,
        "/tmp/capture", DATA + "/base/pgsql_tmp", "/tmp/pg.gz", "--",
        "postgres", "-D", DATA, "-c", "io_method=sync", "-c", "log_temp_files=0"]
    report["commands"].append(pg_command)
    p = subprocess.Popen(pg_command,
        stdout=open(OUT / "postgres.stdout", "w"), stderr=open(OUT / "postgres.stderr", "w"))
    clients.append(p)
    for _ in range(600 if OPS040 else 300):
        if p.poll() is not None:
            raise RuntimeError("capture exited during startup")
        if OPS040:
            ready = "database system is ready to accept connections" in (OUT / "postgres.stderr").read_text()
        else:
            ready = "accepting connections" in ex("pg_isready", "-U", "postgres", check=False)
        if ready:
            break
        time.sleep(.5 if OPS040 else .1)
    else:
        raise RuntimeError("traced postgres readiness timeout")
    query = "SET work_mem='64kB'; SET max_parallel_workers_per_gather=0; SELECT count(*) FROM (SELECT * FROM gate_input ORDER BY payload OFFSET 0) t;"
    started = time.monotonic()
    if OPS040:
        query = "SET temp_tablespaces=''; " + query
    report["ordinary_result"] = sql(query)
    report["ordinary_traced_seconds"] = time.monotonic() - started
    (OUT / "ordinary-processes.txt").write_text(process_evidence())
    query = "SET work_mem='64kB'; SET max_parallel_workers_per_gather=2; SET min_parallel_table_scan_size=0; SET parallel_setup_cost=0; SET parallel_tuple_cost=0; EXPLAIN (ANALYZE, FORMAT JSON) SELECT count(*) FROM gate_input a JOIN gate_input b USING (i);"
    started = time.monotonic()
    if OPS040:
        query = "SET temp_tablespaces=ops040_custom; " + query.replace("SELECT count(*) FROM gate_input a JOIN gate_input b USING (i)", "SELECT * FROM gate_input ORDER BY payload")
    (OUT / "parallel-plan.txt").write_text(sql(query))
    report["parallel_traced_seconds"] = time.monotonic() - started
    (OUT / "after-parallel-processes.txt").write_text(process_evidence())
    ex("gosu", "postgres", "pg_ctl", "-D", DATA, "stop", "-m", "fast", "-w", timeout=120)
    report["capture_exit"] = p.wait(timeout=30)
    collect("postgres.jsonl.gz", "/tmp/pg.gz")
    report["kernel"] = ex("uname", "-a").strip()
    report["status"] = "ancillary-captured-not-qualified"
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
    if OPS040 and report["status"] == "ancillary-captured-not-qualified":
        sys.path.insert(0, str(HERE.parent / "references"))
        from artifacts import seal
        seal(OUT, report)
    (OUT / "report.json").write_text(json.dumps(report, indent=2) + "\n")
print(report["status"])
