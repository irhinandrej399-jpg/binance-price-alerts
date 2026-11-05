# OPS-038 inventory checkpoints

Additive mode of the OPS-033 Linux arm64 capture. Old raw artifacts and the old
ancillary schema/replay remain unchanged. This is evidence collection, with
**consistency unknown, incomplete inventories, qualification blocked**, never
an inode-peak measurement. Results keep peak/lower/upper null.

## Reproduce

From repository root, with Docker and the pinned PG18.6 image already present:

```sh
python3 infra/postgres/measure/inventory/run.py /tmp/ops038-new
python3 infra/postgres/measure/inventory/verify.py /tmp/ops038-new --record
env GOCACHE=/tmp/binance-ops038-gocache GOOS=linux GOARCH=arm64 \
  GOTOOLCHAIN=local go vet ./infra/postgres/measure/ancillary/... \
  ./infra/postgres/measure/inventory/...
```

Output directory must not exist. Runner builds Linux arm64 binaries, creates a
unique labelled container, network none, 1 CPU/512 MiB, SYS_PTRACE/seccomp
unconfined, private 16 MiB tmpfs `/probe`; removes its container in finally and
records container/volume/network cleanup. No working PG volume or business fixture
is used. SIGKILL of runner can bypass finally; only its exact project/label may be
used for manual cleanup. Source snapshots, source/raw SHA-256, binary SHA-256,
image/kernel and exact commands are in report.json. Binaries are removed from
host output after hashing. The image supplies the environment; PG is not started.

Direct capture opts in with `OPS038_INVENTORY=1`. Set `OPS038_PATH_SCOPE` to the
path tree to enumerate, `OPS038_READ_PROBE` to an explicit procfs read probe, and
`OPS038_PG_WITNESS` to the input witness file. The control requests a genuinely
nonexistent `/proc/2147483647/status` and retains the resulting read error.

## Raw contract and OPS-039 input

Header `ops038-ptrace-inventory-v1`, inventory `ops038-inventory-v1`. Existing
ancillary entry/return bytes and ptrace events retain their OPS-033 interpretation.
`seq` is tracer journal order; `ns` is elapsed processing time, not kernel time.
The new mode replaces legacy path-filtered snapshots with `inventory` records:

- Initial after the root exec stop, before/after every existing gated operation,
  and final before footer. The inherited scheduler/classifier is unchanged.
- `census_before` and `census_after` independently enumerate `/proc` numeric PIDs,
  then each `/proc/PID/task`; retain directory names/errors and raw status/stat
  for every TID. Parse PID/TID/TGID/PPID/starttime from raw witnesses. Compare
  against ptrace lineage; `external` includes tracer, namespace init, helper
  peers and unknown tasks. PID alone is never a lifetime identity.
- Every enumerated thread has **all** FD names, readlink results and stat results,
  all raw maps lines and one `map_files/START-END` readlink/stat attempt per line.
  No work-root, file type or ptrace-membership filter. stat is one syscall:
  failure leaves stat null and its error applies to dev/ino/mode/size/nlink.
  Successful records contain all five values. Anonymous mappings also retain
  their failed map_files lookup, rather than claiming no memory reference.
- Each owner retains mountinfo, namespaces, cwd/root links. Path scope has a
  recursive walk with directory listings, target stat/readlink and explicit walk
  errors. Symlink targets are recorded but not recursively followed. A failed
  read is distinct from an empty file. All observations are ordered reads,
  without any assertion that they share one instant.
- `inventory_environment` retains Linux kernel, ABI, mounts, namespace IDs,
  capabilities/status, work/path scope and isolation limitation. Visibility stops
  at the container PID/mount boundary; host references, bind aliases, kernel
  queues and FD references hidden from procfs remain unresolved. nlink/pathname
  cannot establish owner completeness or last reference.
- `pg_witness` is a read witness for JSON `{io_method, settings, tablespaces}`.
  Missing/unreadable inputs are retained as errors. These witnesses alone do not
  prove PG coverage; custom tablespaces without inventory/isolation coverage
  block the experiment. Full PG runs and that coverage gate belong to OPS-040.

Consistency is **always** `{status: unknown, complete: false, reason: ...}`.
This mode has no all-owner stop/barrier for shared FD tables, memory and kernel
references. Two matching censuses do not prove atomicity. The verifier validates
structure and independent fixture bindings, reports census changes/errors and
still returns blocked; a valid artifact never authorizes bounds. FD absence
never proves absence of mappings or queued/kernel references. OPS-039 must add
lifecycle/shared-state evidence without reinterpreting these checkpoints as
atomic; nonleader exec/clone3 scheduler repair is explicitly outside this mode.

## Independent control and verification

An untraced process creates/holds a 12288 B external inode. The traced helper
creates 4096 B, renames outside work root, grows to 8192 B, unlinks, mmaps and
closes the FD; the mapping remains writable, then is unmapped. Finally the
external owner exits. Each stage saves its own fstat, maps and task identities,
then calls getpgid(0), gated only in the new capture mode. The main helper thread
performs no reference mutation between its witness and this checkpoint. Other
owners remain live, so this is a fixture binding, **not** a global barrier.

The full verifier checks sequence, inherited ancillary/lifecycle replay,
mandatory initial/operation/final checkpoints, raw census membership/identities,
FD listing coverage, maps-line coverage/stat identities, path walk coverage,
namespace/mount witnesses and seven independent helper bindings. Source/raw
hashes demonstrate preservation, not authenticity against replacing all
independent witnesses. Rehashed/renumbered tamper controls cover omitted or
substituted census, FD, maps, paths, namespaces, errors and completeness.
For unanchored live observations a consistent replacement cannot always be
detected; they retain the unconditional incomplete/unknown status. No completeness
claim follows from passing the verifier.

[Recorded results](../../../../docs/measurements/OPS-038/README.md).
Procfs semantics and process-ID reuse limitations:
[Linux kernel proc documentation](https://docs.kernel.org/filesystems/proc.html).
