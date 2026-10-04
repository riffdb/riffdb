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

The optional `--driver` smoke compiles retained TypeScript bindings against the
native runtime, then checks named reads, partition isolation, a refused stale
update and its exact replay, unchanged state, and a live-query snapshot. It
creates no durable event consumer that could hold future campaign history.
Use the already installed pinned TypeScript runtime dependencies.

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
marker omits consumer calls while leaving live draining active. The actual
five-minute outage and recovery drill still requires the campaign controller.
