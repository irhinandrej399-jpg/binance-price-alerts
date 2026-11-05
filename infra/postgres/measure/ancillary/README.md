# OPS-033: ancillary raw capture/replay

Linux arm64 LP64 only, ptrace from exec in a disposable PID/filesystem namespace.
This extends the OPS-031 research capture in a separate implementation; the old
capture and gate-g are unchanged. There is no observer qualification or phase API.

## Reproduce

From the repository root, with the pinned PostgreSQL image already available:

```sh
env GOCACHE=/tmp/binance-ops033-gocache GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
  GOTOOLCHAIN=local go build -o /tmp/ops033-capture ./infra/postgres/measure/ancillary
env GOCACHE=/tmp/binance-ops033-gocache GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
  GOTOOLCHAIN=local go build -o /tmp/ops033-control ./infra/postgres/measure/ancillary/control
python3 infra/postgres/measure/ancillary/run.py /tmp/ops033-new
python3 infra/postgres/measure/ancillary/verify.py /tmp/ops033-new --record
env GOCACHE=/tmp/binance-ops033-gocache GOOS=linux GOARCH=arm64 \
  GOTOOLCHAIN=local go vet ./infra/postgres/measure/ancillary/...
```

Output directory must not exist. Runner creates only its unique labelled
container/volume, network none, 1 CPU/1 GiB, SYS_PTRACE, seccomp unconfined.
It initializes a fresh PG18.6 cluster, checks sync IO/durability/default tablespaces,
then traces the postmaster from exec through shutdown. No application collector,
business fixture, working volume, or OPS-023 API is used. Report records exact
commands, image/kernel/settings, binary/source/raw SHA-256 and cleanup inventories.
Finally removes its own resources on success and exception; SIGKILL of the runner
can bypass finally, so use only its report project/label for manual cleanup.

## Versioned contract for OPS-034

JSONL gzip header schema `ops033-ptrace-ancillary-v1`, message version
`linux-arm64-ancillary-v1`. Linux little-endian arm64 audit arch is checked at
every syscall stop. `seq` is total journal order, `ns` is tracer monotonic elapsed
nanoseconds at stop processing. These are not exact kernel entry/return times.
Task ancestry, enter/leave args/return, signals and exit remain in the journal.

Every entry **and** return of arm64 211 sendmsg, 212 recvmsg, 243 recvmmsg and
269 sendmmsg has mandatory `messages: {version, items, error}`. Single-call flags
are args[2]; multi-call flags args[3], vlen args[2]. recvmmsg timeout pointer remains
args[4]; timeout contents are not interpreted as file-reference evidence.

`items[i].header` is a memory witness `{address,length,hex,error}`. Linux
`user_msghdr` is 56 bytes; `mmsghdr` stride is 64 (msg_len at offset 56).
The control pointer is offset 32, capacity/output length offset 40 (uint64),
msg_flags offset 48 (uint32). All header bytes, including pointers, are saved.
Receiver input saves capacity, not uninitialized control contents. Sender input
saves every attempted vector element, including an unsent suffix. A successful
multi return N saves precisely N output headers/msg_len values. Negative returns
have zero output items: unchanged or partly written userspace output is not
advertised as a completed transfer. A single successful zero-byte return is
retained; it must not be inferred to discharge a socket-reference obligation.

`control` is another memory witness: sender at entry, receiver on successful
return only. It is null at the other phase. Zero length is an explicit empty
witness. cmsghdr is 16 bytes (uint64 len, int32 level/type), next aligned to 8.
SOL_SOCKET/SCM_RIGHTS (1/1) payload is int32 FD array. Each FD requires
`{fd,stat,error}`. Stat is independently obtained through `/proc/PID/fd/FD`,
with dev/ino/size/nlink/mode, PID/FD and readlink target; it is not derived from
pathname, the control bytes, or the old work-root-filtered snapshots. Path is
only diagnostic. Mode distinguishes regular work candidates from sockets/pipes.
FD numbers differ between endpoints. dev/ino alone does not establish lifetime
or open-file-description identity; OPS-034 must model generations and references.

Memory failures preserve partial bytes and an error. Limits are 1024 vector
entries and 1 MiB per memory read; over-limit/wrapped vector is explicit
incomplete evidence. Missing fields or short successful reads are rejected.
Malformed cmsghdr, unknown ancillary types, failed FD stat and MSG_CTRUNC are
unresolved, not empty reference lists. Truncated messages retain delivered FDs;
the discarded suffix cannot be reconstructed. Failed sends are attempts, never
successful enqueues. Partial mmsg leaves its unsent/unreceived suffix incomplete.
Kernel restart returns -512/-513/-514/-516 have empty output and remain separate
attempts; a later re-entry is recorded anew. Do not count restarted entry twice
as a transfer or pretend an interrupted call returned usable control data.

Successful send is evidence of a possible queued reference, not evidence that
it reached a receiver. Socket queue identity, pending references after sender
close, receive/drop/close/GC and external peers are explicit OPS-034 obligations;
the footer includes `socket queue reference history pending OPS-034` for message
calls. No queue pairing or last-reference proof is provided here. Stopping a
thread does not freeze shared memory/FD tables in other threads; these races
need the reference/completeness model, and failed evidence stays unresolved.

Replay validates the mandatory entry/return association, ancestry, sequence,
ABI offsets, memory lengths/addresses, control parsing and FD stat binding.
For controls it additionally checks independent helper fstat evidence, exact
sender and receiver counts/FD numbers, input capacities and output flags/lengths.
The cross-process fixture proves different sender/receiver PIDs. Ten mutations
are rejected by the full artifact verifier after rewriting raw SHA-256 and
renumbering seq. Hashes show preservation, not authenticity against replacement
of every independent witness. A custom artifact without the specific control
fixture can use `replay(rows)`; `verify(directory)` requires the runner fixture.

All replay results retain temp_peak_live_bytes/lower_bytes/upper_bytes=null and
qualification_blocked=true. The inherited incomplete classifier/gate and
snapshot diagnostics cannot qualify an inode peak. Unknown is never measured zero.

[Results](../../../../docs/measurements/OPS-033/README.md).
ABI/semantics: [Linux user_msghdr/mmsghdr](https://github.com/torvalds/linux/blob/v6.12/include/linux/socket.h),
[SCM_RIGHTS and truncation](https://man7.org/linux/man-pages/man7/unix.7.html),
[recvmsg](https://man7.org/linux/man-pages/man2/recvmsg.2.html),
[sendmmsg partial success](https://man7.org/linux/man-pages/man2/sendmmsg.2.html),
[recvmmsg](https://man7.org/linux/man-pages/man2/recvmmsg.2.html).

OPS-038 adds an opt-in [inventory mode](../inventory/README.md), with a separate
raw schema, independent process/thread census and unfiltered FD/maps evidence.
Legacy mode and historical raw retain their old contract. New runner source
snapshots also include `inventory_linux.go`, required by the capture build.

OPS-039 adds an opt-in [lifecycle journal](../lifecycle/README.md) over the
OPS-038 inventories, including clone3/shared-state evidence and nonleader exec
TID/gate transitions. Legacy schemas and historical raw remain unchanged.

OPS-040 adds [reference controls/full artifact verification](../references/README.md)
and ordinary/parallel PG via `OPS040_REFERENCES=1` in this same runner. The raw
OPS-039/038 schemas and all legacy capture modes remain unchanged.
