# OPS-041 — shared-fileset PostgreSQL capture

6 November 2026. Full acceptance verifier: **PASS**. Consistency unknown,
qualification blocked; `temp_peak_live_bytes`, `lower_bytes`, `upper_bytes` null.
This is evidence for OPS-034, not its lifetime reducer or bounds qualification.

## Reproduce

From the repository root with Docker and the pinned PG image available:

```sh
OPS040_REFERENCES=1 OPS041_SCENARIO=shared-fileset \
  python3 infra/postgres/measure/ancillary/run.py /tmp/ops041-new
PYTHONDONTWRITEBYTECODE=1 python3 infra/postgres/measure/references/verify.py \
  /tmp/ops041-new --record
PYTHONDONTWRITEBYTECODE=1 python3 infra/postgres/measure/references/verify.py \
  docs/measurements/OPS-041/run-c
```

The last command replays preserved acceptance artifacts, including the linked
OPS-038/run-c and OPS-039/run-f controls. It also generates new rehashed,
resequenced negative artifacts and calls the same full verifier. Allow several
GiB temporary disk and several minutes for the full PG lifecycle/census replay
and negative controls. The [contract](../../../infra/postgres/measure/references/README.md)
describes the lossless ordered 32 MiB gzip chunks and disk-backed JSONL view.

## Profile and captured result

[report.json](run-c/report.json) records every command, exact SQL, source/raw/
binary hashes, runtime settings and the empty labelled container/volume/network
cleanup. Linux arm64 LP64, PG18.6, io_method=sync, fsync/full_page_writes/
synchronous_commit on; fresh unique volume, network none, 1 CPU/1 GiB. The final
postmaster is traced from exec through shutdown with OPS-039 lifecycle and
OPS-038 FD/maps/path/stat/identity plus independent process/thread censuses.
No new capture, scheduler or phase harness was added.

The fixture derives from OPS-033/run-c's self join on unique integer `i` keys:
8000 rows with 132 B payload. Ordinary/default external sort returns 8000;
shared/custom self join uses work_mem=64kB, hash_mem_multiplier=1,
max_parallel_workers_per_gather=2 and zero scan/parallel thresholds.
[parallel-plan.txt](run-c/parallel-plan.txt) saves backend PID then EXPLAIN
ANALYZE VERBOSE JSON; actual workers and batch count must pass the full verifier.
A separate serial self join checks aggregate result=8000. This is a separate
execution, not the aggregate value emitted by EXPLAIN.

`OPS041_SCENARIO=parallel-sort` remains the default OPS-040 ordinary/default plus
parallel/custom sort profile. The shared mode selects the self join in the
existing runner and preserves the original scenarios. Default/custom locations,
mounts/namespaces/work roots and independent PG witness are retained in raw.
Debug5 and `%p`/`%b` PostgreSQL logs retain exact backend/worker roles and
statements; timings include tracing/debug overhead, not application latency.

## Verification and input to OPS-034

[checks.json](run-c/checks.json) and [final-replay.json](final-replay.json):
**PASS**, 314 FS/6782 PG checkpoints; 11 fileset paths, 11 identities, 6 cross-owner identities; backend 224, workers [225, 226]; 992 FD/path identity checks. Fifteen existing reference controls plus five PG raw
controls were rejected after hash/sequence regeneration: no filesets, removed
worker owners, missing stat, missing lineage, substituted inode. The no-fileset
mutation fails the explicit fileset gate. The old OPS-040 raw independently
[fails that same gate](preflight/old-fileset-gate.json), while its full historical
contract and 15 controls still [pass regression](ops040-regression.json).

The final full replay also passes after tightening successful-open tracking to
forget closed/reused FD numbers; its output equals the acceptance result.
Read errors remain explicit: PG FD=47131, maps=5125488,
census read errors=0, changed census checkpoints=12.
These counts are observations, not proof of completeness. [preflight](preflight/README.md) preserves the sandbox startup
failure and run-b's missing worker-role log evidence. Original OPS-033/038/039/
040 raw and reports are unchanged. Old OPS-040 has 5660 inventories but no
`.fileset/`; old OPS-033 has 31 fileset paths but no new inventory/lifecycle.
Their gate failure was reproduced before the new shared capture.

Use [run-c/report.json](run-c/report.json), all its hashed chunks/snapshots and
linked dependency reports together. The full verifier binds fileset FD dev/inode
and readable pathname stat, successful open return, PID/starttime census,
postmaster-child lineage and independently logged backend/worker roles. Distinct
PG owners must observe shared identities. These are positive reference witnesses;
no inode-generation continuity or last-reference conclusion follows solely from
numeric dev/inode/path/FD equality. Replay must retain every ambiguity.

For example, dev=64/inode=208046 is first observed at seq=55449 under
backend 224/starttime=3999232 (FD22, size=0, nlink=1), then at seq=59643
under worker 225/starttime=3999483 (FD13, size=65536, nlink=1), at the same
custom fileset pathname. Corresponding successful open returns are seq=55448
and seq=59642; both ptrace births link to postmaster 180. This is a concrete
cross-owner reference witness; it is not a reducer's lifetime conclusion.

## Limits

All procfs FD/maps/census errors and races remain explicit; even matching censuses
are not an all-owner barrier. Shared FD/VM state, queues/kernel references,
outside-namespace/external aliases and custom tablespace owner completeness are
unproved. No lifetime, peak, lower/upper bound, observer gate or quota is qualified.
Historical captures are not requalified. The unaccepted local OPS-034 draft is
preserved and is not part of this ticket's implementation or acceptance evidence.
