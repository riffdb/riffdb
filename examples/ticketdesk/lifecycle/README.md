# Sustainable TicketDesk preparation

This separate application lineage preserves the existing TicketDesk example
and the alpha endurance workload. It extends the TicketDesk domain with a
stable-population `UpdateTicket` command, explicit expected-revision evidence,
a `TicketUpdated` event, and the `TicketUpdates` counting projection. Named
`GetTicket` results include revision; generated event consumers and the
`TicketQueueWatch` live query retain the normal public authorization path.

Regenerate or check the compiler-owned closure from the repository root:

```bash
./scripts/generate-ticketdesk-lifecycle
./scripts/generate-ticketdesk-lifecycle --check
```

The check also compiles a missing-fence counterexample and requires both writes
to be rejected at their source spans. Generated Rust, Go, TypeScript, Python
and MCP artifacts are source artifacts; never edit them manually.

Build and run the real public TLS preparation smoke in a fresh private root:

```bash
cargo build -p riffdb-server --bin riffdbd -p riffdb-cli --bin riffdb \
  -p riffdb-ticketdesk --bin lifecycle -p riffdb-driver-host --bin riffdb-driverd
./scripts/ticketdesk-lifecycle-smoke \
  --root /tmp/riffdb-lifecycle-preparation \
  --server target/debug/riffdbd --cli target/debug/riffdb \
  --worker target/debug/lifecycle --driver target/debug/riffdb-driverd
```

The script creates one owned preparation database, bootstraps an operator,
deploys the exact application, and binds narrowly declared roles. The Rust
client checks original-command replay, stale-fence refusal, named state reads,
partition isolation, event consumption/acknowledgement and a live-query snapshot.
The script stops its daemon through the production shutdown signal. Logs and
credentials remain private; use a new root for each attempt.

The Rust `seed` mode creates exactly 65,536 tickets across 64 organizations
through 16 clients and generated compiled commands. It requires the protected
`RIFFDB_LIFECYCLE_SEED_CREDENTIAL`, `RIFFDB_LIFECYCLE_CA`, and verified HTTPS
`RIFFDB_LIFECYCLE_ENDPOINT` environment configuration. Original seed identities
are deterministic so an operator can repeat the same preparation safely.

These are preparation tools. The sustained workload, fault controller,
independent campaign reconciliation, and real rehearsal
remain WP-825 work. No preparation output is a qualifying campaign receipt.
See the [milestone handoff](../../../docs/architecture/SUSTAINABLE-TICKETDESK.md).

Use `--restart-recovery graceful_restart` or `--restart-recovery process_kill`
in a fresh preparation root to interrupt an already connected generated client.
The preparation proves a failed submission, then requires the same command input
and key to settle, replay exactly, and reconcile state within sixty seconds.
These modes run separately from population and observer preparation. Their
private observations are not campaign receipts or pruning-recovery evidence.

The `--archive-witness` preparation checks two actual archived events against
independent acknowledgement evidence after production shutdown. Population
preparation writes bounded seed/update witnesses and performs the same offline
archive reconciliation for every event. The checker uses native exclusive
custody and envelope validation and never emits event payloads. Its witness
files are private test evidence, not authoritative storage or campaign receipts.

Population preparation observes explicit public outbox readiness before offered
load and before shutdown, recording both drain times separately. These waits
stop offered writes and cannot satisfy the campaign's continuing-load outage
gate. Add `--online-retention --observe` to a population preparation to select
the existing bounded retention lane and record actual sealed-file removals
alongside production deletion/reuse and hold counters. Counts are measurements;
this preparation does not qualify the full rehearsal.

A fresh full-population preparation can also run a short offered-load probe:

```bash
./scripts/ticketdesk-lifecycle-smoke \
  --root /tmp/riffdb-lifecycle-population-preparation \
  --server target/release/riffdbd --cli target/release/riffdb \
  --worker target/release/lifecycle --population-probe-seconds 300
```

Build those binaries with `cargo build --release` first. This mode seeds the
whole population, offers 100 updates and 400 named reads each second, checks
the measured rates and latency thresholds, and independently reads every
seeded ticket afterward. The load lane admits at most 28 concurrent requests,
reserving four slots for later campaign observations/control. Queues and
latency histograms are bounded. Windows exclude post-duration drain completions;
slow tail overflow is disclosed and cannot become a fast latency sample.
This preparation has no lifecycle faults and cannot qualify the milestone.

Population preparation also samples public native health and the selected
projection during load. One auxiliary lane issues these calls sequentially,
at most once every five seconds, alongside the two observer lanes and one
control lane. The query requests the current available frontier without waiting
for a requested sequence. Each private sample binds the projection ID, plan
hash, generation and actual applied frontier to the earlier sampled application
head; their nonnegative difference is the observed sequence backlog. It does
not claim a simultaneous current head or a byte count. Missing, unavailable,
inconsistent or regressing evidence fails preparation, and every load window
requires an actual completed sample. Final aggregate reconciliation remains a
separate check. This sampler has unit evidence; host measurement is pending.

The public CLI encodes health sequences as canonical decimal strings. The
projection sampler and sink oracle validate that native representation before
converting it to bounded numeric observations. JSON numbers, leading zeros,
negative values, missing fields and overflow fail observation. The first host
attempt exposed an incorrect integer assumption in the harness; its failed
preparation is preserved and does not qualify the milestone.

The optional `--driver` smoke compiles retained TypeScript bindings against the
native runtime, then checks named reads, partition isolation, a refused stale
update and its exact replay, unchanged state, and a live-query snapshot. It
creates no durable event consumer that could hold future campaign history.
Use the already installed pinned TypeScript runtime dependencies.

Add `--python-native target/debug/lib_native.so` with `--driver` to include
actual Go and Python generated calls. Build that first-party extension separately
with `cargo build --locked -p riffdb-client-python-native`. Go compilation uses
private caches inside the owned preparation root. Both helpers check exact
stale-command replay, organization isolation, unchanged state and a bounded
live snapshot. With `--additive-deploy`, the same checks run for V1 and V2.
These boolean preparation observations are not hash-bound campaign conformance.

Use `--archive-witness --capability-rotation` in a separate fresh root to replace
an application bearer for the same principal. Revoked reads and command replay
must refuse; the replacement must replay the original result and reconcile
state. The preparation runs after the initial write and does not cover rotation
under continuing offered load.

The initial durable-SSD host preparation completed all 30,000 updates and
120,000 reads in its five-minute window, with p95 73.830 ms and p99 84.490 ms,
zero retries, and no post-window completions. All 65,536 ticket states were
reconciled. This is a load preparation result, with no lifecycle fault coverage;
it does not qualify WP-825 or the 24-hour campaign.

The optional `--observe` registers a separate campaign consumer and live query
before seeding, and waits for their explicit ready message. The observer uses
two reserved slots, keeps a bounded 1,024-ticket event oracle for organization
zero, checks every observed creation/update and exact redelivery, acknowledges
through generated methods, and checks monotonic consumer/live frontiers. It
refuses gaps, substituted events, unexpected incarnations and unplanned resets.
The preparation also queries `TicketUpdates` through the public CLI with its
compiler-bound identity and a freshness fence, then checks the exact count.
This observer currently refuses lifecycle interruptions; fault-aware restart,
restore and rotation handling remain campaign work. Its positive smoke and
hostile event/projection oracle tests are preparation evidence only.

The `--additive-deploy` one-ticket preparation first checks V1, stops its
observer, deploys the exact-parent V2 application, binds its declared role, and
checks Rust and optional TypeScript operations under V2. The positive V2 update
retains its expected-revision fence and original-key replay. This mode cannot
be combined with a population probe. The full campaign must quiesce outstanding
V1 operations, account queued offered work, and reconnect current observers
across the scheduled deployment; that controller remains incomplete.

Generate or check the V2 compatibility closure with
`scripts/generate-ticketdesk-lifecycle-successor`. Generation uses an owned
verified-TLS preparation daemon and the public parent-aware application-lock
workflow. The V2 fixture adds one compiled version command and preserves every
parent entity and command identity. No schema transformation is required.

Full-population preparation keeps the consumer registered throughout seeding,
then admits the live query after an explicit seed-completion gate. It verifies
that initial view before starting offered load. Seeding runs faster than the
campaign rate and can exceed the native bounded notification buffer; seeding
is outside the measured load windows. The observer's private `consumer.pause`
marker omits consumer calls while leaving live draining active. The measured
outage preparation described below checks that effect and times recovery.

Live observation renews its bounded request every twenty seconds using the
opaque native cursor. Each renewed snapshot must preserve partition, canonical
ticket identities, ordering and a monotonic frontier; renewals are counted in
observations. Unexpected terminal refusal or cursor reset fails preparation.
The one-ticket check waits for a real renewal before succeeding.

`--population-probe-seconds 900 --observe --consumer-outage` runs a separate
fifteen-minute preparation with a five-minute consumer pause requested five
seconds after offered load starts. Its full duration begins when the observer
confirms quiescence. Consumer event count and checkpoint must
freeze while live progress continues, and the missing events must drain within
ten minutes after resumption. All three load windows and every final ticket are
checked. Timings and logs stay in the private preparation root. This covers one
fault only and cannot produce a campaign qualification receipt.

The separate `--population-probe-seconds 900 --observe --sink-outage`
preparation removes access to its owned private archive directory for five
minutes. The native connector must report degraded delivery while application
writes and the live query keep progressing. Recovery restores access to the
same directory and archive file and requires public delivery-drained health
within ten minutes while the offered load continues. Cleanup restores the pinned
directory even after a preparation failure. It changes no database record,
quota, retry policy, durability boundary or offered rate. Sink and consumer
outages use separate preparation roots; neither is a campaign receipt.

Latency limits apply to normal preparation windows. Only windows overlapping
the measured pause and backlog drain receive the campaign checker's fault
exception; all offered/completed counts, errors, concurrency and final state
checks remain binding. At least one complete normal window is required.
Online-retention preparations also require the native lane's normal cancelled
shutdown state. A refused lane, missing terminal observation or malformed census
fails preparation even if workload latency and reconciliation pass. Measured
extent observations remain available in the private failed root.

The Rust load driver supports private quiescence and release controls for the
fault controller. Admission stops while the offered clock keeps filling the
bounded queue. A release can change only after every admitted original
operation settles; each worker then constructs the exact generated V1 or V2
facade with the selected protected credential. The controller must supervise
the public deployment and observer handover separately.

Measured update/read retries use one native generated transport attempt per
submission and preserve the original input until recovery settles. Probe
`transport_attempts` counts those workload calls, including explicit retries.
Consumer, live-query and operator calls belong to separate auxiliary accounting.
The generated application SDK's automatic outcome recovery remains unchanged.

Population preparations also record private `memory-samples.jsonl` diagnostics
for the controller, daemon, Rust load worker and Rust observer. They require
real observations in every load window and refuse process exit, replacement,
missing memory fields and overflow. `allocator_bytes` is labeled as the whole
virtual address space upper bound, including mappings, stacks and reservations;
it is not live allocator ownership. The scope excludes transient CLI children
and peaks between samples, and every record sets `qualifies_resource_model`
to false. Complete frozen campaign resource accounting is still required.
