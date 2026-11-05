# Matcher continuation (OPS-025)

Run sequentially from the repository root with Docker and the local Go toolchain.
Each output must be absent. Source is exactly OPS-024/run-a2, never mixed with
run-b2. Source evidence is hashed in the manifest and checked by the verifier.

```sh
GOCACHE=/tmp/binance-ops025-build python3 scripts/measure_joint.py --output docs/measurements/OPS-025/run-a3 --manifest infra/measure/matcher.json --timeout 120 --command python3 infra/measure/matcher_adapter.py docs/measurements/OPS-025/run-a3/identities.json docs/measurements/OPS-024/run-a2/identities.json
GOCACHE=/tmp/binance-ops025-build python3 scripts/measure_joint.py --output docs/measurements/OPS-025/run-b3 --manifest infra/measure/matcher.json --timeout 120 --command python3 infra/measure/matcher_adapter.py docs/measurements/OPS-025/run-b3/identities.json docs/measurements/OPS-024/run-a2/identities.json
python3 scripts/verify_matcher_measurement.py docs/measurements/OPS-025/run-a3 docs/measurements/OPS-025/run-b3
```

The compiled opt-in driver reuses the OPS-024 testing.TB TLS helper and OPS-023
JSON-lines protocol. No new runtime, collector, coordinator, migration or business
transition is introduced. Regular Go tests skip this infrastructure driver.

Setup creates a real empty API snapshot/pin before importing the frozen source
journal through API-owned journal.Append. No canonical subscription/business
rows are reconstructed: this API dataset serves only the recorded journal.
Import checks byte equality for all 19 original events. The original
read_committed prefix (20 records including one real duplicate) is republished
transactionally and consumed read_committed before baseline. Original offsets
and new disposable topic/offsets are separate fields in evidence. Topic creation,
source publication and TLS establishment are setup, never matcher operation cost.
Market-data database has no writer. Original candidate quote times are preserved;
this is storage replay, not a new fresh-price eligibility decision.

API RPC servers own api_runtime pool. Snapshot/replay clients authenticate as
matcher over loopback TLS 1.3 with explicit method grants. All matcher writes
use matcher_runtime and existing snapshotload/projection/fencing/pricework stores.
No matcher code reads API SQL. Final diagnostic reads use only matcher-owned
state; collectors independently retain schemas and relation/WAL/IO snapshots.

## Operation coverage

| Window | Existing adapter / durable writes |
| --- | --- |
| empty | no operation; diagnostic/background baseline, never subtracted |
| snapshot_load/repeat | empty immutable generation, page progress and complete; same real RPC request and resource |
| assign/repeat | assignment request ledger, scope, grant, projection membership; exact request replay |
| initialize/repeat | complete empty snapshot origin and projection checkpoint; exact repeat |
| replay_first/repeat | mTLS Begin/Read/ack plus one complete ApplyReplay; repeat of original source bytes |
| live_sql_duplicate | first Kafka identity/NextOffset for the already replayed C=1 |
| live_first/physical_duplicate | first version at C=2 and same source event at another physical offset |
| observe_first/duplicate | separate diagnostic Walk windows; same C/images and greater durable NextOffset after duplicate |
| live_creates | remaining three initial versions, C=5 |
| work_accept/repeat | one pricework input and four candidate rows, original owner/subscription/version and candidate quote |
| work_results/repeat | four source canonical workflow outcomes and completed count, exact repeats |
| work_complete/repeat | completed job; Latest has no invented price offset/Prefix |
| live_tail | 14 newer after-images, versions, workflow, cancellation/delivery tombstones, final C=19 |
| live_old_version | old full transaction cannot replace final image or rewind checkpoint |
| takeover/stale_write/revoke | next token and ledger/membership; stale ApplyReplay rejected; conditional revoke |

Durable matcher tables: `matcher_snapshot_generations` (covered empty generation),
`matcher_snapshot_rows` (nonempty rows uncovered), `matcher_checkpoints` and
`matcher_subscriptions` (projection/origin/C/transport), `matcher_assignments`,
`matcher_assignment_requests`, `matcher_projection_grants` (fencing),
`matcher_price_work` and `matcher_price_candidates` (job/results). Gate locking
uses `matcher_fence_gate`; `matcher_price_checkpoints` remains empty because
this fixture uses Latest, with no asserted price Prefix. Schema/migration hashes
are retained in both reports. The fixture max_batch=4 for work windows denotes
the accepted candidate set size; each Record is a separate single-candidate call.
There is no product maximum of four candidates.

Each business call runs only in its named operation window. PostgreSQL operation,
checkpoint, inclusive and background deltas are distinct; API-side pin/RPC writes
remain attributed to the API database. Kafka physical file/internal-topic growth
is collected independently and is not all attributed to matcher. Empty is not
a correction factor. The manifest's size is adapter calls, with four candidate
rows per work input; this fixture does not establish a maximum candidate count.

## Bounds and missing classes

Snapshot page rows: 1..1000; fixture has zero rows. Replay pages: 1..1000 whole
transactions and 3 MiB protobuf payload budget; fixture reads one transaction.
Each source transaction contains one event, pricework contains four candidates.
Price numeric has 18 integer/18 fraction digits; source is 101. ID strings,
pricework candidate count, total snapshot size, journal transactions and several
trusted string inputs have no shared finite product bound. Producer buffers do
not establish a bound on a SQL transaction or total projection.

Uncovered: nonempty snapshot pages, coordinator activation/restart, Kafka group
assignment/OffsetCommit, price transport Prefix, full pricehandler and live
RequestDelivery, superseded/error/rollback paths, maximal payloads/batches/TOAST.
Canonical results supplied to Record are original API evidence, not new RPC
outcomes measured here. This cannot price a complete matcher runtime path.

Reports remain collected-not-qualified. Missing classes/bounds, PostgreSQL temp
peak/device IO and Kafka fsync unsupported block OPS-018. These are physical
fixture windows, not a load test or a qualified capacity/2x/ledger estimate.
Full functional/race suites and load fixtures must not run alongside them.
