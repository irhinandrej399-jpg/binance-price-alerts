"""Verify saved capture evidence, NOT completeness of an observer.

All reports stay unknown, including reports whose diagnostic snapshot maxima
match filesystem controls. No numerical bounds are certified by this verifier.
"""
import argparse
from collections import Counter
import copy
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import stat
import tempfile


def require(condition, message):
    if not condition:
        raise ValueError(message)


def replay(rows):
    require(rows and rows[0]["kind"] == "header", "missing header")
    require(rows[0]["schema"] == "ops031-ptrace-gate-v1", "unknown schema")
    require(rows[-1]["kind"] == "footer", "missing footer")
    alive, pending, serial, issues = set(), {}, {}, set()
    active, previous_ns, peak, snapshots = None, -1, 0, 0
    root = None
    seen = set()
    generations = Counter()
    states = []
    events = Counter()
    for seq, row in enumerate(rows, 1):
        require(row["seq"] == seq, "sequence gap")
        require(row["ns"] >= previous_ns, "nonmonotonic clock")
        previous_ns = row["ns"]
        kind, pid = row["kind"], row.get("pid")
        events[kind] += 1
        if kind == "root":
            require(root is None and not alive, "duplicate root")
            root = pid
            alive.add(pid)
        elif kind == "event":
            require(pid in alive, "event for unregistered process")
            if row["event"] in (1, 2, 3):
                require(row["message"] not in alive, "duplicate child")
                alive.add(row["message"])
        elif kind in ("child_stop", "signal"):
            require(pid in alive, "stop for unregistered process")
        elif kind == "enter":
            require(pid in alive and pid not in pending, "unmatched entry")
            pending[pid] = row["nr"]
            serial[pid] = row["serial"]
        elif kind == "dispatch":
            require(active is None and serial.get(pid), "overlapping dispatch")
            active = pid
        elif kind == "leave":
            require(pending.pop(pid, None) == row["nr"], "unmatched exit")
        elif kind == "release":
            require(active == pid and pid not in pending, "unmatched release")
            active = None
        elif kind == "exit":
            require(pid in alive, "exit for unregistered process")
            alive.remove(pid)
            pending.pop(pid, None)  # exit/exit_group have no syscall return
        elif kind == "snapshot":
            require(set(row["tids"] or []) == alive, "incomplete registered task inventory")
            by_inode = {}
            for f in row["files"] or []:
                require(stat.S_ISREG(f["mode"]), "non-regular file counted")
                require(f["size"] >= 0 and f["nlink"] >= 0, "negative file metadata")
                require(not f.get("pid") or f["pid"] in alive, "FD belongs to missing process")
                identity = (f["dev"], f["ino"])
                metadata = (f["size"], f["nlink"], f["mode"])
                require(identity not in by_inode or by_inode[identity] == metadata,
                        "inconsistent inode evidence during gate snapshot")
                by_inode[identity] = metadata
            for identity in by_inode.keys() - seen:
                generations[identity] += 1
            seen = set(by_inode)
            total = sum(x[0] for x in by_inode.values())
            peak = max(peak, total)
            snapshots += 1
            states.append({"seq": seq, "phase": row["phase"], "bytes": total,
                           "inodes": len(by_inode)})
        elif kind == "unsupported":
            issues.add(row["reason"])
    require(not alive and not pending and active is None, "incomplete process lifecycle")
    footer = rows[-1]
    require(footer["status"] == "unknown" and footer["temp_peak_live_bytes"] is None
            and footer["qualification_blocked"] is True, "unqualified capture advertised as measured")
    require(set(footer["issues"]) == issues, "issue journal/footer mismatch")
    require("complete syscall classification and monotonic-per-operation proof pending" in issues,
            "missing unresolved proof gate")
    return {"status": "unknown", "temp_peak_live_bytes": None,
            "lower_bytes": None, "upper_bytes": None, "qualification_blocked": True,
            "diagnostic_snapshot_max_bytes": peak, "snapshots": snapshots,
            "inode_generations": sum(generations.values()), "events": dict(events),
            "issues": sorted(issues)}


def load(path):
    with gzip.open(path, "rt") as f:
        return [json.loads(line) for line in f]


def verify(directory):
    report = json.loads((directory / "report.json").read_text())
    require(report["status"] == "capture-research-not-qualified", "gate did not finish")
    require(report["temp_peak_live_bytes"] is None and report["qualification_blocked"], "false qualification")
    require(all(not value for value in report["cleanup"].values()), "resources remain")
    for name, expected in report["sha256"].items():
        require(hashlib.sha256((directory / name).read_bytes()).hexdigest() == expected,
                "hash mismatch: " + name)
    require(report["source_sha256"] == report["sha256"]["capture-source.go.txt"], "source mismatch")
    settings = report["settings"]
    require(settings["io_method"] == "sync", "wrong IO profile")
    require(all(settings[k] == "on" for k in ("fsync", "synchronous_commit", "full_page_writes")), "durability")
    require(all(not t["location"] for t in report["tablespaces"]), "custom tablespace")
    traces = {name: load(directory / (name + ".jsonl.gz")) for name in
              ("filesystem", "postgres", "killed-tracee", "unsupported-mmap")}
    results = {name: replay(rows) for name, rows in traces.items()}
    control = [line.split() for line in (directory / "filesystem.txt").read_text().splitlines()]
    require([r[0] for r in control] == ["grown", "alias", "linked_alias", "unlinked_open",
        "shrunken", "regrown", "dup_open", "reused", "closed_empty"], "control labels")
    require(len({tuple(row[1:3]) for row in control[:7]}) == 1, "inode aliases disagree")
    require([int(r[3]) for r in control[:-1]] == [16384, 16384, 16384, 16384, 4096, 12288, 12288, 8192], "sizes")
    require([int(r[4]) for r in control[:-1]] == [1, 2, 1, 0, 0, 0, 0, 1], "link counts")
    require(results["filesystem"]["diagnostic_snapshot_max_bytes"] == 16384, "filesystem maximum")
    require("tracee killed by signal" in results["killed-tracee"]["issues"], "lost tracee not detected")
    require("work inode mmap" in results["unsupported-mmap"]["issues"], "unsupported mmap not detected")
    pg_files = [f for r in traces["postgres"] if r["kind"] == "snapshot" for f in r["files"] or []]
    require(any(".fileset/" in f["path"] for f in pg_files), "no shared fileset")
    require(any(".fileset" not in f["path"] for f in pg_files), "no ordinary spill")
    # Actual workers must be in both the plan and ptrace ancestry.
    plan_text = (directory / "parallel-plan.txt").read_text()
    plan = json.loads(plan_text[plan_text.index("["):])
    def walk(value):
        if isinstance(value, dict):
            yield value
            for child in value.values():
                yield from walk(child)
        elif isinstance(value, list):
            for child in value:
                yield from walk(child)
    require(any(node.get("Workers Launched", 0) >= 2 for node in walk(plan)), "parallel workers not launched")
    require(len({f["pid"] for f in pg_files if f.get("pid")}) >= 3, "missing worker FD evidence")
    checks = []
    def reject(name, action):
        try:
            action()
        except (ValueError, EOFError, gzip.BadGzipFile):
            checks.append(name)
        else:
            raise ValueError("negative accepted: " + name)
    original = traces["postgres"]
    reject("missing-footer", lambda: replay(original[:-1]))
    damaged = copy.deepcopy(original)
    del damaged[next(i for i, r in enumerate(damaged) if r["kind"] == "enter")]
    reject("missing-entry", lambda: replay(damaged))
    damaged = copy.deepcopy(original)
    del damaged[next(i for i, r in enumerate(damaged) if r["kind"] == "event" and r["event"] in (1,2,3))]
    # Renumbering cannot hide missing ancestry.
    for i, row in enumerate(damaged, 1):
        row["seq"] = i
    reject("missing-process-lineage", lambda: replay(damaged))
    damaged = copy.deepcopy(original)
    row = next(r for r in damaged if r["kind"] == "snapshot" and r["files"])
    row["files"].append(dict(row["files"][0], size=row["files"][0]["size"] + 1))
    reject("conflicting-inode", lambda: replay(damaged))
    damaged = copy.deepcopy(original)
    damaged[-1]["temp_peak_live_bytes"] = 0
    reject("false-zero", lambda: replay(damaged))
    reject("actual-tracer-interruption", lambda: load(directory / "interrupted.jsonl.gz"))
    require(report["interrupted_exit"] != 0, "tracer not interrupted")
    require("missing-ops031-observer" in report["unavailable"], "unavailable control not recorded")
    # Run the actual artifact verifier on an altered copy, leaving historical
    # raw untouched. It must fail the saved hash before attempting replay.
    with tempfile.TemporaryDirectory(prefix="ops031-tampered-") as tmp:
        changed = Path(tmp) / "copy"
        shutil.copytree(directory, changed)
        raw = changed / "postgres.jsonl.gz"
        raw.write_bytes(raw.read_bytes() + b"changed")
        reject("tampered-raw-hash", lambda: verify(changed))
    return {"results": results, "negative_checks": checks,
            "qualification_blocked": True, "temp_peak_live_bytes": None}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--record", action="store_true")
    args = parser.parse_args()
    result = verify(args.directory)
    if args.record:
        (args.directory / "checks.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
