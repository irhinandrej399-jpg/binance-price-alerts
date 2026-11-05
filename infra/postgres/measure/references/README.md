# OPS-040 reference evidence and PostgreSQL

This extends the **existing OPS-033 runner/control** with
`OPS040_REFERENCES=1`. Capture remains OPS-039 Linux arm64 LP64 lifecycle
plus OPS-038 inventories; no new capture, phase harness or lifetime reducer.
Evidence is incomplete, consistency unknown, qualification blocked;
`temp_peak_live_bytes`, `lower_bytes`, `upper_bytes` stay null.

From the repository root, Docker with the pinned image already present:

```sh
OPS040_REFERENCES=1 python3 infra/postgres/measure/ancillary/run.py /tmp/ops040-new
python3 infra/postgres/measure/references/verify.py /tmp/ops040-new --record
```

Runner cross-builds capture/control, starts a unique labelled Linux container
and fresh PG18.6 volume, network none, 1 CPU/1 GiB, SYS_PTRACE and seccomp
unconfined. No working volumes or business fixture. It traces final postmaster
from exec through fast shutdown, saves commands, kernel/image, ABI, settings,
source/binary/raw SHA-256, helper output, plans, procfs and cleanup. Finally
removes only its own resources and records labelled container/volume/network
inventories. SIGKILL can bypass finally; exact project label is in report.json.
Full verifier rejects failed or unavailable cleanup, partial gzip or missing footer.

## References and independent witnesses

`control references040` runs sequential receive, drop and mapping controls.
Each creates 8192 B at `/tmp/work/reused`, fstats it, unlinks and saves nlink=0.
For each queue interval it sends one byte and one SCM_RIGHTS FD over a private
SOCK_DGRAM socketpair, then closes the source FD **before** the helper checkpoint.
Independent ioctl FIONREAD reports one queued byte; helper procfs shows no source
FD/path/mapping. At the subsequent getpgid stop, complete inventories cover all
visible owners, including external processes; no inventoried FD holds the inode.
This is caller synchronization, not an all-owner barrier.

Receive returns the byte/right and independently fstats the delivered FD.
Drop receives with zero control capacity and checks MSG_CTRUNC; no FD is delivered.
Separate getpgid checkpoints precede both operations and follow discharge.
Capture preserves send/receive headers, control bytes, returns and per-right stat.
Verifier binds socket identities, successful send, sender close, queue size,
no-FD interval, delivered inode or truncated drop and empty queue to helper witnesses.
Queue-only absence of FD never discharges a reference or completes a lifetime.
The raw unsupported queue obligation remains mandatory and blocks qualification.

Mapping control mmaps, closes/unlinks, writes/checks byte 42, records independent
maps and the saved prior fstat, then unmaps. Verifier binds the mapping-only
inventory to helper maps despite no remaining FD/path; map_files failures remain
explicit. Saved fstat is historical, not a new stat after close. All three controls
recreate the same pathname and exact FD number with 4096 B and new independent
fstat. `inode_reused` reports actual dev/ino equality; no inode generation or open
file description identity follows from this equality.

Rename outside root, growth, unlink-open, external owner, dup/fcntl/reopen,
inheritance/shared FD/VM and nonleader exec reuse the **preserved** direct controls
OPS-038/run-c and OPS-039/run-f. `dependencies.json` binds their report hashes;
full artifact verification invokes their full verifiers, not a summary/checksum
alone. Historical raw and source snapshots are unchanged.

## PostgreSQL profile

Two thousand 132 B rows, work_mem=64kB: ordinary external sort with parallelism
zero and default temp tablespace; then forced parallel sort with two requested
workers and custom `/tmp/work/custom` tablespace. EXPLAIN ANALYZE must show actual
workers. Both default `DATA/base/pgsql_tmp` and custom temporary FDs must occur in
full inventories. Backend/worker lineage is compared to independently enumerated
process/thread census; inventories retain every visible owner, mountinfo,
namespace, cwd/root, all FDs/maps and path walk `/tmp/work`.

`pg-witness.json` records io_method=sync, durability settings, data_directory and
pg_default/pg_global/custom tablespaces; raw environment reads and binds that file.
Per-query settings and SQL are in commands/plan. Custom files are observed, but
external aliases/custom tablespace owner completeness is **not certified**.
Matching censuses do not prove atomicity; procfs races/errors block completeness.

## Artifact transport and verification

Large original PG gzip bytes are split into ordered 32 MiB `.part-NNN` files.
`report.archives` retains order/size/encoding and original raw SHA-256 in
`report.sha256`; each part has its own hash. Concatenation reconstructs the exact
original gzip. No observations are filtered or normalized. DiskRows spools JSONL
to a temporary indexed file for full lifecycle/inventory replay with bounded RAM;
allow several GiB temporary disk and time for the complete verification.
Inventory verifier retains per-checkpoint summaries rather than whole inventories,
without dropping validation. `full-*` snapshots record post-capture verifier
refinements separately; original capture-time source snapshots remain intact.

At final exit, a proven Z -> X procfs transition with stable prior PID/starttime
and stat PPID=0 can disagree with the immediately preceding zombie status read.
Full lifecycle verifier records it explicitly as `terminal_identity_races` and
increments incomplete evidence. Other identity contradictions still fail. This
never converts the two reads into a consistent identity checkpoint.

Full artifact verifier validates every PG/FS lifecycle and inventory, mandatory
helper stages/identities, isolation/census and independent references, tablespace
bindings, original/archive/source hashes and cleanup. Rehashed/resequenced tamper
calls this same verifier. Hashes demonstrate preservation, not authenticity if
all independent witnesses and manifests are replaced together.

## Input to OPS-034

Use [OPS-038 inventory](../inventory/README.md),
[OPS-039 lifecycle](../lifecycle/README.md) and this artifact contract together.
The [recorded report](../../../../docs/measurements/OPS-040/README.md) maps every
original OPS-037 criterion to evidence and remaining gaps. Journal sequence/time
are tracer processing order/time, never a kernel-wide atomic timeline. PID/starttime,
raw arguments, clone flags, former TIDs, FD/maps stat and historical helper fstat
are evidence, not already reduced generations/lifetimes. Unsupported queue/kernel,
shared-state/clear_tid, external aliases, unreadable procfs and inconsistent
checkpoints must remain unresolved. Inherited child-return stops were not observed
in OPS-039; no new success is claimed here. Last reference, complete owners,
observer gate, numeric bounds and quota qualification remain subsequent work.
