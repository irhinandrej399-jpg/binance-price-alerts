# OPS-039 lifecycle evidence

Additive Linux arm64 mode of the OPS-033 capture using the OPS-038 inventories.
`OPS039_LIFECYCLE=1 OPS038_INVENTORY=1` selects
`ops039-ptrace-lifecycle-v1`. Legacy modes and saved raw stay unchanged.
This journal is evidence, with **consistency unknown, qualification blocked**;
peak/lower/upper remain null. It does not reduce reference lifetimes.

## Reproduce

From repository root with Docker and the pinned PostgreSQL image present:

```sh
python3 infra/postgres/measure/lifecycle/run.py /tmp/ops039-new
python3 infra/postgres/measure/lifecycle/verify.py /tmp/ops039-new --record
python3 docs/measurements/OPS-039/check.py
```

The output directory must not exist. Runner cross-builds Go/assembly controls
and capture, saves source snapshots, source/raw/binary SHA-256, commands,
kernel/image, independent helper output and cleanup. It uses a uniquely labelled
container, no network, 1 CPU/512 MiB, private 16 MiB `/probe` tmpfs and ptrace
permission. It starts no PostgreSQL and touches no working volume. Finally
removes its container and checks container/volume/network labels. SIGKILL can
bypass finally; manual cleanup must use only the exact recorded label/project.
A timeout or missing footer fails verification; a partial gzip is not a complete
capture. Per-record gzip flush retains diagnostic stops even before timeout.

## Contract for OPS-040

- Existing entry/return/message and inventory records retain their meaning;
  inventory checkpoints are still caller-only, never an all-owner barrier.
  Sequence/time are tracer processing order and elapsed time, not kernel time.
- `wait_stop` retains raw wait status, `/proc/TID/status` and `stat` (or errors),
  every pending operation, active gate and queued tasks **before** handling the
  stop. These are independent process witnesses, not identities inferred from
  the task map. Exit can make procfs unreadable; the error remains evidence.
- `syscall_info` retains returned size/errno and the full 88 B capture buffer.
  Only returned bytes are authoritative. Entry needs 80 B, exit 33 B, and the
  arm64 audit ABI is checked. Unknown/short/error/op=NONE stops are incomplete;
  the full verifier rejects unsupported shapes or retains blocked diagnostics.
- Every entry has `linux-arm64-lifecycle-v1` data: scalar six-argument syscall
  record plus bounded NUL-terminated pathname bytes for openat/openat2,
  execve/execveat, unlinkat/symlinkat/linkat/renameat/renameat2 and path probes.
  openat2 retains pointed-to open_how; unsupported size or read error blocks.
  dup/dup3/fcntl arguments and kernel returns remain explicit. Path is evidence,
  not an inode identity; use the adjacent FD/map inventories as well.
- clone keeps scalar flags; clone3 retains exact entry bytes at its pointer
  and size. Supported Linux clone_args sizes are 64/80/88 B. Decode flags only
  from saved bytes, never from pointer values. Bad pointers/sizes retain raw
  attempts and explicit incomplete. Parent-side PIDFD/PARENT_SETTID outputs, if
  requested, are read using pointers saved at entry. Child TID/clear_tid lifetime
  semantics remain explicitly unresolved. Shared VM/FD observations lack an
  all-owner barrier and stay blocked, including successful fixture variants.
- Fork/vfork/clone ptrace events link the pending parent operation to child TID.
  The CLONE_THREAD flag determines task group membership. Initial child SIGSTOP
  can arrive before the parent event; it stays stopped until lineage is known.
  `OPS039_EARLY_CHILD=1` selects the real child's wait status first after reading
  the parent's event message, exercising this ordering without inventing stops.
  A newborn can produce an inherited return or begin with a new syscall entry;
  both paths are checked. These runs observed new entries, **zero inherited
  syscall-return stops**; no real inherited-return test is claimed.
- `exec_transition` binds PTRACE_EVENT_EXEC message/former TID to the new leader,
  preserves source pending syscall/arguments and abandoned siblings' operations,
  records retired tasks, procfs identity and before/after gate/queue state.
  It moves the exec operation/gate to leader TID and removes retired siblings
  and former TID from the queue. Subsequent exec return belongs to the new TID.
  Normal exits preserve their pending operation in the preceding stop witness.
- Full verifier replays pending/newborn/group/queue/gate transitions against
  kernel syscall-info, raw stop identities, independent helper points and the
  complete OPS-038 census/inventory verifier. Lost records, mismatched returns,
  flags, former TID, siblings or stop identities fail. Read failures, census
  races and shared-state ambiguity never authorize completeness. Rehashed and
  resequenced controls run through the full artifact verifier, not hash checks
  alone. Hashes do not authenticate replacement of all independent witnesses.

## Actual controls and limits

Independent Go helper locks its OS thread. Assembly children execute only raw
syscalls, on separate stacks, and never enter the Go runtime. Legacy clone and
clone3 both exercise isolated and shared FD/VM creation. Child closes the
inherited FD and stores 42 into its memory copy; parent waits, checks exit status,
FD presence and its own memory value. Shared variants use CLONE_VFORK to hold
the parent until child exit; this is a fixture synchronization, not a global
inventory barrier. Census and child FD stats independently bind inherited inode.
Helper confirms fcntl duplication, dup3 and procfs reopen. Another control execs
from a nonleader thread into a new helper that independently records PID/TID/
starttime. Exact original `/bin/true` preflight is retained separately.

Observed clone3 success and failure paths apply only to the recorded kernel.
A denied clone3 on another kernel is reported as untested creation, never a
successful variant. Linux fork/vfork are arm64 clone flags/events, not separate
arm64 fork syscalls. Restart returns remain in the journal; fixture matching
allows restart entries before the final parent result.

Reference generations, last reference, queued-only owners, external aliases,
PG ordinary/parallel coverage, bounds and phase integration are subsequent work.
OPS-040 must use this lifecycle verifier plus its own reference/PG controls;
passing this journal does not establish their coverage.

[Recorded results](../../../../docs/measurements/OPS-039/README.md).
The former-TID transition follows the Linux documented
[ptrace exec semantics](https://man7.org/linux/man-pages/man2/ptrace.2.html).
