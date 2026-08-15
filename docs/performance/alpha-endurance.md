# Alpha endurance harness

The alpha endurance gate is a closed, remote application test. It is not a
benchmark-only storage loop. A qualifying run must keep Rust, Go, TypeScript,
and Python clients active through public named operations while exercising
reads, writes, workflows, durable events, live queries, hot and cold keys, and
multiple tenants.

The checked workload and safety bounds live in
`fixtures/endurance/alpha-manifest-v1.json`. A release-specific action manifest
supplies exact argument arrays for the four installed clients, the sampler,
conformance checks, and every lifecycle operation. The runner defaults to the
reviewed `release/evidence/endurance-actions-v1.json` manifest used by the
release gate. Set `RIFFDB_ENDURANCE_ACTION_MANIFEST` only to select another
explicitly reviewed campaign manifest. Commands are executed directly without a
shell, so the action manifest cannot inject an arbitrary command line through
quoting or interpolation. Fault controls belong to the external orchestrator;
they are never added to the production application protocol.

The checked manifest fixes the client count, total rate ceiling, deterministic
seed for each language, tenant inventory, hot and cold key cardinalities, and
operation weights. An action manifest cannot override those values: each
worker repeats them alongside its exact command, and the outer harness rejects
drift before starting the controller. The outer harness also hashes the exact
action manifest and rejects a raw receipt that is not bound to that hash.
Each worker publishes per-language progress inside the 30-second sampling
interval. The validator rejects a stalled language frontier even when other
workers keep the aggregate operation frontier moving.

This release campaign is an endurance gate, not a throughput benchmark. Its
closed ceiling is four logical operations per second in total: one per language
worker across sixteen closed-loop language/tenant sessions. That still exceeds
one million operations over 72 hours while keeping disk exhaustion from
masquerading as a soak failure. Throughput evidence remains owned by the
separate application benchmark suite.

## Fast regression gate

Run this during ordinary development:

```bash
./scripts/alpha-endurance --self-test
./scripts/endurance-conformance --self-test
cargo +1.97.0 test -p riffdb-testkit --test endurance_harness --all-features
```

The test is synthetic and deliberately short. It proves that the receipt
validator rejects early exit, host interference, missing lifecycle coverage,
missing journal generations, resource leaks, superlinear lifecycle work,
unbounded queues, starvation, and hidden retries. It does not claim endurance
evidence.

## Real run

The 24-hour form is a rehearsal. Only an uninterrupted 72-hour run can satisfy
the alpha release gate.

```bash
export RIFFDB_ENDURANCE_RELEASE_ARTIFACT_SHA256='<sha256 of the installed release bundle>'

./scripts/alpha-endurance \
  --duration-hours 24 \
  --all-domains \
  --all-languages \
  --remote \
  --require-idle-host \
  --require-all-lifecycles \
  --artifact-root target/alpha-endurance/rehearsal \
  --output release/evidence/alpha-endurance-rehearsal-v1.json
```

The action manifest schema is `riffdb.alpha-endurance-actions/v1`. It contains:

- one bounded setup command and one bounded teardown command for the isolated
  remote environment; both emit retained, typed action receipts;
- exactly four long-lived worker commands, one for each supported language;
- the fixed seed, client count, per-worker rate ceiling, tenants, and complete
  workload coverage for each worker;
- one bounded sampler command that prints a single JSON resource observation;
- one periodic conformance command;
- exactly one staggered scheduled command for each required lifecycle; and
- exactly one staggered orchestrator command for every required crash or
  network fault.

The reviewed release campaign places all lifecycle and fault actions on one
20-minute cadence. Initial offsets are unique and at least 75 seconds apart,
so supported offline operations are exercised repeatedly without turning the
first campaign wave into one artificial, continuously unavailable maintenance
window. The runner validates the exact interval and offset assigned to every
action before setup; a clustered or independently shortened schedule is an
invalid action manifest rather than evidence with a larger retry budget.

The first semantic conformance checkpoint waits 900 seconds. The slowest
closed language worker needs one complete 100-slot schedule before every
declared workload class is observable, and recovery or rotation pauses can
extend that first cycle. Later checkpoints remain five minutes apart. The
warmup is an exact checked part of the action manifest: pre-coverage worker
evidence is not misclassified as an application conformance failure.

Each command is a nonempty JSON string array. Secrets remain in protected
environment/configuration files and must not appear in the manifest, process
inventory, observations, or receipt.

The first-party environment action is `scripts/endurance-environment`. Setup
creates a fresh, artifact-root-confined TicketDesk installation with a direct
TLS listener and two named databases whose data and backup roots are siblings.
Setup requires a clean source revision and builds the two periodic semantic
test executables once in release mode. It copies those executables, the exact
adapter fixture closure, and the conformance runner into the run root, then
seals every file digest in
`riffdb.alpha-endurance-conformance-probes/v1`. Later checkpoints execute only
that installed bundle. Editing, regenerating, or temporarily breaking the live
developer checkout after setup cannot change or fail an in-flight endurance
run; changing any installed probe byte fails closed instead.
The `default` alias carries the complete event-bearing four-language workload.
The `retention` alias deploys the same exact application lock but receives only
event-free history commands through its own `TicketDeskSeeder` binding, so the
campaign can exercise successful retention without pretending the primary
database's pending outbox work is delivered. The main workload still uses
three separately bound least-authority roles: `TicketDeskSeeder`,
`TicketDeskApplication`, and `TicketDeskAgent`. Setup builds and installs the
checked Rust, Go, TypeScript, and Python application prerequisites, starts one
bounded driver pool per role, and publishes a protected state file plus a typed
setup receipt. There is no broad endurance credential and no cleartext
fallback. Teardown stops every recorded driver and server process without
deleting either database, logs, or receipts.

The installed `scripts/endurance-worker` dispatcher selects one compiled
worker for `rust`, `go`, `typescript`, or `python`. Every worker independently
revalidates the closed tenant inventory, workload coverage, four-client count,
seed, per-language rate ceiling, and exact 32-KiB modeled durable-operation
charge before opening a session. The charge is a conservative accounting unit
for every operation that may persist authoritative, audit, event, outbox,
consumer, or provenance state; it is deliberately not an estimate of encoded
payload bytes. Page reads and live-query snapshots carry no durable-operation
charge. Each worker uses
the generated TicketDesk facade with one transport attempt per logical
operation, separate seeder/application/agent authority, four tenant-owned
client loops, and atomic bounded metric snapshots under
`environment-v1/metrics`. The language implementations deliberately exercise
the same weighted page read, comment write, contextual reaction, durable event
acknowledgement, live-query snapshot, and cold-ticket growth shapes; none may
substitute a kernel read or benchmark-only mutation.

Each worker permits at most three outer retries for the closed transient error
set: storage unavailable, outcome uncertainty, overload, deadline expiry, and
transport interruption. The same logical input and idempotency identity are
retained across attempts. Every failed attempt increments both
`transport_attempts` and `declared_retries`; successful public operations
increment `logical_operations` and `transport_attempts`. Therefore
`transport_attempts == logical_operations + declared_retries` is exact and a
maintenance interruption cannot turn retries into hidden work.

`scripts/endurance-sample` merges the four atomic worker snapshots with the
installed daemon/driver process inventory, authenticated health frontier, and
closed storage/backup/lifecycle artifacts. Per-operation latency uses one
identical 16-bucket microsecond histogram in every language, so aggregate p50
and p99 remain bounded and mergeable without retaining individual samples.
On Linux, `allocator_bytes` is deliberately the sum of `VmData` for the closed
process inventory: it is a conservative writable-data envelope rather than an
allocator-private counter. This may reject a healthy run by over-counting, but
cannot hide growth by under-counting known process data. The observation names
the authoritative application frontier separately and reports projection zero
for TicketDesk because this workload deploys no projection.

Journal generations are read from both checksummed extent-header slots; an
absent, stale, malformed, or checksum-invalid header cannot be replaced by a
harness counter. The four workers separately report newly emitted events and
durably acknowledged normal/contextual deliveries. Consumer backlog is the
closed TicketDesk relation `(two consumers × emitted TicketCreated) − durable
acknowledgements`, and the checkpoint observation is the acknowledgement
frontier rather than the number of event RPCs attempted.
Contextual reaction execution and contextual acknowledgement are separate
public operations: every worker records both, and advances its acknowledgement
counter only after the exact leased item is durably acknowledged.

`scripts/endurance-lifecycle` serializes action state under a protected file
lock and refuses every lifecycle name until that operation has a real evidence
implementation. Checkpoint and restart actions gracefully drain the installed
daemon, inspect the stopped database's proof-carrying checkpoint, parse the
exact maximum deferred writer queue across the one shutdown-evidence line
emitted for each configured database, start a fresh installed daemon process,
and require authenticated readiness at or after the checkpoint frontier.
Journal-recycle actions wait for a checksummed on-disk generation advance;
reactive-consumer actions require exact durable acknowledgement progress.
Evidence files are atomically written and only their SHA-256 enters the bounded
action result.

Recovery actions are deliberately different from clean restarts: they observe
the authenticated application frontier and checksummed journal generation,
terminate the daemon with `SIGKILL`, start a new installed process, and require
journal recovery to publish at least both observed frontiers. Only that real
unclean reopen increments the recovery counter.

`scripts/endurance-fault` owns the five closed orchestrator-only fault cells and
serializes them against lifecycle administration with the same protected lock.
Process-kill and journal-recycle-crash cells kill the installed daemon and
require recovery at or beyond the observed application and journal frontiers.
The checkpoint-crash cell stops the daemon, leaves its graceful-termination
checkpoint signal pending, kills it, inspects the unchanged stopped database,
and requires suffix recovery. If the bounded deterministic workload is in a
read-only segment when that cell becomes due, the fault first uses the
installed generated application client to commit one identity-unique symbolic
seed event. The receipt distinguishes that public-client seed from an ambient
workload suffix; the harness never manufactures a storage record or edits a
checkpoint to satisfy the precondition. The network-interrupt cell stops only
the server process, proves a public TLS health operation is unavailable,
resumes that same process, and requires authenticated recovery.

The consumer fault uses the installed generated Python client as a disposable
public-surface process. It receives a contextual item, commits its declared
idempotent reaction, publishes bounded evidence, and is killed without an ack.
After the fixed 60-second contextual lease expires, a second generated-client
process must receive the same event at a higher attempt, recover the persisted
command outcome as a replay, and acknowledge the exact redelivery. No seek,
nack, kernel read, or harness-authored checkpoint substitutes for lease expiry.

Backup actions start and poll one identity-stable public remote maintenance
operation, require the exact four-file immutable backup inventory, and retain
the checksum and size of every artifact. Only the newest two verified
`endurance-*` backup directories remain on disk. The action verifies the new
backup completely before retiring an older one through the installed
`riffdb backup retire` public command, polls its stable operation identity to a
terminal V2 receipt, and records both retained inventories and exact retirement
receipt evidence. Direct directory or maintenance-receipt deletion is pinned as
an architecture failure. Capability-rotation actions issue a new
least-authority health credential and revoke the preceding rotation credential;
their frontier is the real administration sequence returned by those public
operations. Neither action substitutes a harness counter for the durable server
or filesystem result.

Deploy-under-load actions invoke the exact checked application package through
the public deploy command and bind their receipt to both manifest and lock
hashes. Retention actions first add one event-free symbolic command to the
dedicated `retention` alias, then drain and inspect the installed daemon. The
event-bearing `default` alias must report `UndeliveredOutboxLowWater` below its
application head and is never pruned. Only the retention alias is pruned to its
reported maximum permissible watermark. The reopened daemon must preserve the
primary frontier, and every later retention cycle must advance the dedicated
lane before pruning again. A failed offline action still attempts a bounded
restart; it cannot report success or advance lifecycle state.
Authenticated lifecycle probes accept the documented `ready` or `degraded`
serving states only when authoritative storage, the catalog, and the commit
coordinator are all explicitly healthy. A degraded projection or outbox does
not hide an unhealthy authoritative component and does not prevent safe
offline maintenance.

The first installed split-lane rehearsal exposed and now regression-tests the
ADR-0085 Amendment 4 boundary: offline prune previously changed redb while the
durability journal still named the pre-prune suffix, so the next open returned
`CorruptData`. Retention now performs ordinary journal recovery, durably rebases
to an empty newer generation, and holds an internal preparation witness before
checkpoint deletion or any prune transaction. No release receipt qualifies
unless two installed prune/restart cycles pass. The harness never deletes the
journal, marks pending intents delivered, or weakens startup validation to make
that evidence green.

Environment setup creates one short-lived test CA and two distinct leaf/key
pairs before starting RiffDB. Certificate rotation atomically replaces the
configured leaf and key with the inactive pair, gracefully restarts the
installed daemon, and proves authenticated readiness through the unchanged CA
root. Receipts expose only public certificate hashes—never private-key bytes or
digests—and alternate between the two leaves on successive actions.

`RIFFDB_ENDURANCE_ARTIFACT_ROOT` must be an absolute, non-symlink path and must
name a fresh run root. The script never reuses or erases an existing
`environment-v1` directory. The server's stdin is held open for the complete
environment lifetime because EOF is a supported clean-shutdown request; a
daemon launched with `/dev/null` is therefore not a valid endurance setup.

Every lifecycle and fault command must print one bounded
`riffdb.alpha-endurance-action-result/v1` JSON object. It names the exact
action, contains only an evidence SHA-256 and numeric before/after frontiers,
and confirms recovery. Lifecycle actions must advance their frontier. The
controller retains these safe action results and derives the counters from
them; a successful process exit alone is not lifecycle evidence.

The periodic conformance command likewise prints one bounded
`riffdb.alpha-endurance-conformance-result/v2` object. It must prove all four
adapter domains, current row-policy probes, exact data reconciliation, zero
silent loss, a content digest, the immutable probe-bundle digest, and the
installed release-artifact digest. Every checkpoint in a receipt must carry
the same probe identity. The controller retains every result. Exit status
alone cannot assert conformance or policy correctness.

The first-party `scripts/endurance-conformance` checkpoint reconciles every
worker's logical-operation, transport-attempt, workload, and tenant totals;
reads all sixteen language/tenant hot tickets through the generated Python
client over verified TLS; and proves the seeder role cannot read one. It also
runs the sealed four-domain and row-policy semantic binaries and binds their
outputs to authenticated serving health, the application lock, the installed
release identity, the probe-bundle identity, and the four atomic worker
snapshots. The retained evidence is content addressed; only its digest enters
the controller result.

The controller writes an untrusted raw receipt. The outer harness adds separate
preflight and postflight host inventories, binds the receipt to the canonical
workload manifest, exact action manifest, and release digest, and validates it
before publishing the requested output. A failed or interrupted run remains an artifact for diagnosis but
cannot be relabeled as passing.

The release gate revalidates the retained 72-hour receipt independently:

```bash
./scripts/alpha-endurance \
  --verify-release-receipt release/evidence/alpha-endurance-v1.json
```

This stricter mode requires all four adapter domains, green policy and data
reconciliation, and content-addressed references to the exact durable-format,
export/reimport, destructive-recovery, and adapter-conformance evidence. Each
referenced file must be a bounded, non-symlink file below its closed release
prefix, match its declared SHA-256, and carry the expected receipt schema. The
environment and complete observation array are also bound by canonical digest.
Merely copying a 72-hour raw controller receipt into `release/evidence` cannot
pass release verification.

Release evidence is attached with a checked path inventory rather than by
hand-editing the raw receipt:

```bash
./scripts/alpha-endurance \
  --bind-release-receipt release/evidence/alpha-endurance-v1.json \
  --release-evidence-inventory release/evidence/alpha-endurance-inventory-v1.json \
  --output release/evidence/alpha-endurance-v1.json
```

The input is the outer harness receipt, not the controller's diagnostic
`raw-receipt-v1.json`: only the outer receipt carries the independently sampled
preflight and postflight host-validity records. Binding may atomically replace
that same outer receipt after the adapter evidence phases have passed.

The inventory uses schema
`riffdb.alpha-endurance-release-inventory/v1`, fixes the durable-format path to
`release/durable-format-manifest-v1.json`, and supplies export/reimport,
disaster-recovery, and conformance receipt paths for exactly `openfga`,
`mlflow`, `better-auth`, and `woodpecker`. Payload remains a post-alpha
regression fixture and does not occupy one of the four release receipt slots.
The binder derives all hashes itself,
validates each referenced schema, adds canonical environment and observation
digests, re-runs full 72-hour validation, and publishes through an atomic
same-directory replacement. Paths outside the closed release prefixes or
through symlinks are rejected.

## Receipt guarantees

`riffdb.alpha-endurance-receipt/v1` is bounded to 4 MiB and at most 10,000
observations. A passing receipt proves:

- requested wall time was completed on an idle inventoried host;
- all four language workers remained active;
- every language, workload class, and tenant made measured operation progress;
- every required lifecycle reached its minimum and its durable frontier moved;
- every required external fault actually ran its minimum number of times;
- operation and transport-attempt counts reconcile, including declared
  retries;
- latency, throughput, errors, queues, consumers, projections, retention,
  checkpoint, journal, database, backup, RSS, and allocator observations are
  present;
- the maximum process count is taken from resource samples rather than a
  controller constant, and matches the bounded process inventory;
- memory and file growth fit declared fixed plus retained-data bounds;
- lifecycle cost does not show repeated superlinear history growth; and
- no unsupported `storage_unavailable`, conformance failure, silent loss,
  starvation, or unbounded diagnostic/process inventory occurred.

The harness does not weaken durability, bypass commands, expose kernel reads,
or grant test authority to application clients. Backup/restore and destructive
faults remain operator operations outside the application credential.
