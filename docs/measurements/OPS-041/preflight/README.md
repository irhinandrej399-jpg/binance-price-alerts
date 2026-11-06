# OPS-041 preflight

6 November 2026. These diagnostics are not acceptance evidence.

- Run-a failed during image inspect: sandbox Python could not access Docker
  socket. It created no Docker resource. Unique attempted project:
  `binance-ops033-8803c46e90`; cleanup queries were unavailable in that sandbox.
- Run-b: PG18.6/io_method=sync, 8000 rows, Workers Launched=2, Hash Batches=4,
  fourteen fileset paths. Full verifier rejected `PG role/worker witnesses
  missing`: default PostgreSQL log recorded only the client backend deleting
  filesets, not the worker role witnesses. Preserved report includes commands,
  exact source/raw/chunk hashes and empty labelled cleanup. This rejection was
  observed after full lifecycle/inventory replay of that diagnostic capture.
- Run-c repeats the same fixture with debug5 and `%p`/`%b` log fields. Its full
  verifier and raw are the acceptance input, never a merge with run-b.

Unaccepted run-a snapshots and run-b raw remain locally under
`/tmp/binance-ops041-run-a` and `/tmp/binance-ops041-run-b`; they are not needed
for acceptance replay. No old OPS-033/038/039/040 raw was modified.
