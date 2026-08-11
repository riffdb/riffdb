# Alpha endurance harness

The alpha endurance gate is a closed, remote application test. It is not a
benchmark-only storage loop. A qualifying run must keep Rust, Go, TypeScript,
and Python clients active through public named operations while exercising
reads, writes, workflows, durable events, live queries, hot and cold keys, and
multiple tenants.

The checked workload and safety bounds live in
`fixtures/endurance/alpha-manifest-v1.json`. A release-specific action manifest
supplies exact argument arrays for the four installed clients, the sampler,
conformance checks, and every lifecycle operation. Select it with
`RIFFDB_ENDURANCE_ACTION_MANIFEST`. Commands are executed directly without a
shell, so the action manifest cannot inject an arbitrary command line through
quoting or interpolation. Fault controls belong to the external orchestrator;
they are never added to the production application protocol.

The checked manifest fixes the client count, total rate ceiling, deterministic
seed for each language, tenant inventory, hot and cold key cardinalities, and
operation weights. An action manifest cannot override those values: each
worker repeats them alongside its exact command, and the outer harness rejects
drift before starting the controller. The outer harness also hashes the exact
action manifest and rejects a raw receipt that is not bound to that hash.

## Fast regression gate

Run this during ordinary development:

```bash
./scripts/alpha-endurance --self-test
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
export RIFFDB_ENDURANCE_ACTION_MANIFEST="$PWD/release/evidence/endurance-actions-v1.json"
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

- exactly four long-lived worker commands, one for each supported language;
- the fixed seed, client count, per-worker rate ceiling, tenants, and complete
  workload coverage for each worker;
- one bounded sampler command that prints a single JSON resource observation;
- one periodic conformance command;
- exactly one staggered scheduled command for each required lifecycle; and
- exactly one staggered orchestrator command for every required crash or
  network fault.

Each command is a nonempty JSON string array. Secrets remain in protected
environment/configuration files and must not appear in the manifest, process
inventory, observations, or receipt.

Every lifecycle and fault command must print one bounded
`riffdb.alpha-endurance-action-result/v1` JSON object. It names the exact
action, contains only an evidence SHA-256 and numeric before/after frontiers,
and confirms recovery. Lifecycle actions must advance their frontier. The
controller retains these safe action results and derives the counters from
them; a successful process exit alone is not lifecycle evidence.

The controller writes an untrusted raw receipt. The outer harness adds separate
preflight and postflight host inventories, binds the receipt to the canonical
workload manifest, exact action manifest, and release digest, and validates it
before publishing the requested output. A failed or interrupted run remains an artifact for diagnosis but
cannot be relabeled as passing.

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
