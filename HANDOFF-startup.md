# The ADR-0156 clean-close fast path does not engage after a daemon restart

## Symptom

A `riffdbd` started against a cleanly shut down ~1.1M-row database (4.7 GB,
`--production` tier) takes the **complete** startup validation pass. Measured on
a GCP N1 VM (8 vCPU, Intel Xeon @2.30GHz):

- more than **20 minutes** at 99.9% CPU without becoming ready
- about **10 GB** resident memory
- essentially no physical disk reads (12 KB total), so this is CPU work over
  page-cached data, not IO

`perf` on that daemon, flat profile:

```text
16.83%  sha2::sha256::soft::unroll::compress
 6.28%  <riffdb_proto::wire::Cursor>::next
 5.61%  riffdb_proto::durable_wire::preflight
 4.06%  crc::crc32::update_table::<16>
 2.93%  malloc
 2.81%  prost::encoding::varint::encode_varint
 2.35%  cfree
 1.51%  prost::encoding::varint::decode_varint
```

That is a full re-parse and re-validation of every durable record. Which is
exactly what ADR-0156 exists to avoid on a clean start.

## What is NOT the cause — both were investigated and eliminated

1. **The daemon does write the certificate.** `riffdbd`'s graceful shutdown
   calls it: `write_shutdown_validated_prefix_checkpoint` in
   `crates/riffdb-server/src/process_graph.rs:1095` calls
   `storage.write_clean_close_lifecycle()`, deliberately after the optional
   checkpoint so CLEAN is the final authoritative write.

2. **The benchmark harness was dirtying the database, and fixing that was not
   sufficient.** `restart_for_measurement`
   (`examples/app-baseline/riffdb/src/server.rs`) called
   `authoritative_table_inventory_after_reopen_v1`, which opens a full
   `RedbStore` and drops it. Opening transitions the lifecycle record to dirty
   (`store.rs`, `successor_dirty`) and nothing in `Drop` writes it back, so that
   reopen discarded the certificate the seed daemon's shutdown had just written.
   That was a real defect and is fixed (the call site now uses the read-only
   `authoritative_table_inventory_v1`). **The restart is still slow after the
   fix**, so there is at least one more reason the gate closes.

## Where the gate is

`crates/riffdb-storage-redb/src/startup.rs:774` — the fast path is admitted for
exactly one reason:

```rust
let verified_clean_lifecycle = self.shared.verified_clean_close_lifecycle(...)?;
if let Some(lifecycle) = verified_clean_lifecycle {
    ...
    clean_close_fast: true,
```

`verified_clean_close_lifecycle` (`store.rs:365`) returns `None` — silently, with
no diagnostic and no counter — on **any** of:

1. `engine_repaired_at_open` is set
2. `META_CLEAN_CLOSE_LIFECYCLE` absent
3. the record does not decode
4. `database_id` mismatch
5. `history_incarnation` mismatch
6. state is not `CleanCloseState::Clean(_)`
7. the journal clean-close header digest does not verify
   (`verify_clean_close_header_digest_with_media`, `journal.rs:2800`)
8. application/administration frontier disagreement (`read_commit_tail` /
   `read_administration_tail`)

Every one of these produces the same observable behaviour: a very slow start.

## Deliverable one is the diagnostic, not the fix

**Land a reason code before changing any admission logic.** Right now the only
way to tell these eight cases apart is to patch the engine and re-run a
twenty-five-minute benchmark, which is why this has been misdiagnosed twice.

Make `verified_clean_close_lifecycle` return *why* it declined — a closed enum,
reported the way the startup refusal discriminant already is (that diagnostic is
what made the previous startup ceiling findable at all). Then a single start says
which precondition failed.

Note the two `let _ =` sites in `write_shutdown_validated_prefix_checkpoint`:
both write failures are deliberately ignored per ADR-0019 A1 semantics, which is
defensible, but it means a *write*-side failure is also invisible. If the reason
code says the record is absent or not CLEAN, look there next —
`note_checkpoint_write_failure` has a counter for the checkpoint but check
whether the lifecycle write has an equivalent.

## Then find the actual precondition and fix it

Two hypotheses worth testing first, both cheap once the reason code exists:

- **The digest or frontier check is scale-sensitive.** A `full`-scale database
  (19,220 commands) may pass where 1.1M does not; if so the check itself is the
  bug, not the shutdown.
- **The harness's second inventory call site.** `shutdown_with_evidence`
  (`server.rs`, around line 602) still uses the reopen variant. It runs after
  measurement so it cannot affect the same run, but confirm no earlier phase
  reaches it.

## Reproduction

Cheap first: assert in a test that a store which writes
`write_clean_close_lifecycle()` and is dropped reports
`clean_close_fast_startup() == true` on the next open. If that passes, add the
daemon and the harness one layer at a time until it fails. `measure_clean_startup`
and `measure_clean_startup_linear` in `benchmark_support.rs` already exist for
timing.

Expensive but end-to-end, on an idle host:

```bash
RIFFDB_STOP_TIMEOUT_SECS=1800 RIFFDB_START_TIMEOUT_SECS=1800 \
benchmarks/run-app-baseline --production \
  --load interactive --load-clients 32 --load-duration-secs 60 --reps 1
```

Seeding is about 8 minutes at ~2,260 ops/s on an N1 VM and produces roughly
1,112,350 commands. The restart is the thing under test.

## Why this matters beyond the benchmark

- A 4.7 GB database that cannot open in twenty minutes is an availability
  problem, not a benchmark inconvenience. This is the shape of a restart during
  an incident.
- It plausibly blocks END-009's 72-hour endurance run and any alpha evidence
  path that reopens a post-soak database, since a soak accumulates far more than
  a million records. Confirm or rule that out.
- The cost is CPU-bound on SHA-256, and on hosts without SHA-NI that is 10-15x
  more expensive than on hosts with it (measured: Intel Xeon @2.30GHz 476k
  hashes/s versus AMD EPYC 7B12 4.78M and Ryzen 9 7950X 7.41M, same crate and
  features). So the same defect is far worse on Intel GCP machine types than on
  AMD ones, and the workstation is the least representative host available.

## Governance

Changing what admits the fast path is a fail-closed boundary governed by
ADR-0156 / ADR-0157, which amended ADR-0019, ADR-0073 and ADR-0085 and were
accepted with exact maintainer text. Adding a *reason code* is an observability
change and should not need an amendment. Changing an admission *condition* is a
semantics change and does. Do not widen what startup accepts in order to make a
start fast — that trades corruption detection for a green run.

## Acceptance

```bash
cargo +1.97.0 test -p riffdb-storage-redb --all-features
cargo +1.97.0 test -p riffdb-server --all-features
./scripts/recovery_full
./scripts/check-requirement-coverage
./scripts/handbook check
```

The 86-arm recovery matrix must stay green — it is the closest existing proof
that startup validation still refuses what it should.
