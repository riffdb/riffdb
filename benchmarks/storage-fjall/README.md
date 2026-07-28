# Fjall Storage Comparison

This nested workspace is the isolated WP-075 comparison of Fjall 3.1.8
against RiffDB's accepted storage semantics. It is not a production storage
adapter and is not linked into `riffdbd`.

## Status

The comparison **explicitly fails** the unchanged semantic conformance suite.
The physical Fjall substrate passes limited probes for cross-keyspace atomic
transactions, owned snapshots, bounded physical scans, reopen after a clean
synchronized close, and the accepted dual-ledger migration page bounds. Those
probes do not implement or substitute for RiffDB semantic storage ports.

In particular, the comparison does not claim:

- command-candidate type state, sequence allocation, or atomic semantic graphs;
- snapshot observations or dependency evidence;
- reciprocal idempotency, outcome, commit, event, outbox, and provenance data;
- catalog, capability, audit, bootstrap, or outbox transition semantics;
- projection identity, generation, lifecycle, marker, hash, or frontier semantics;
- structural/historical exact-end startup sessions or the sealed catalog migration backend;
- V1-to-V2 rewrite, restart recovery, or the semantic crash/failpoint matrix.

The frozen result is in
[`reports/conformance-v1.tsv`](reports/conformance-v1.tsv). Passing substrate
cases never compensate for a failed semantic case. Benchmark timings are
therefore marked `not_publishable_conformance_failure` and must not be used as
redb-versus-Fjall decision evidence.

## Scope And Authority

WP-075 evaluates `STO-001`, `POC-010`, and `PRJ-001` through `PRJ-004`. The
explicit failure is the permitted WP exit path; it does not mark those semantic
requirements satisfied by Fjall.

The implementation was checked against accepted ADR-0004, ADR-0006, ADR-0010,
ADR-0017, ADR-0039, and ADR-0042. The comparison:

- uses the exact accepted 19-table physical inventory;
- probes the current 27-readable/26-writable registry, with V1 decode-only and V2 writable;
- exposes only the identity methods of `StartupIndexMigrationPort`;
- does not implement the sealed, catalog-owned migration backend;
- applies independent 500-row/4-MiB migration evidence and instruction ledgers.

## Commands

Run the exact WP-075 acceptance commands from the repository root:

```bash
cargo test --manifest-path benchmarks/storage-fjall/Cargo.toml --workspace
cargo bench --manifest-path benchmarks/storage-fjall/Cargo.toml --no-run
```

Additional local quality checks:

```bash
cargo fmt --manifest-path benchmarks/storage-fjall/Cargo.toml --all -- --check
cargo clippy --manifest-path benchmarks/storage-fjall/Cargo.toml \
  --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc \
  --manifest-path benchmarks/storage-fjall/Cargo.toml --workspace --no-deps
```

Run a bounded smoke benchmark:

```bash
cargo bench --manifest-path benchmarks/storage-fjall/Cargo.toml \
  --bench storage_comparison -- \
  --iterations 2 --warmup 1 --history-records 10
```

The harness emits `riffdb_storage_benchmark_format=1` and the same per-workload
distribution fields as the redb baseline. Its workload names are deliberately
substrate-specific because the semantic workloads are not implemented.

## Dependency Boundary

`Cargo.lock` belongs to this nested workspace. Fjall is pinned exactly to
3.1.8 with default features disabled, so its default LZ4 feature is not in the
configured graph. The human and machine-readable audit is in
[`DEPENDENCIES.md`](DEPENDENCIES.md) and
[`reports/dependency-inventory-v1.tsv`](reports/dependency-inventory-v1.tsv).

First-party code in this workspace forbids unsafe Rust. Fjall and several
transitive dependencies contain unsafe code; this is an accepted boundary only
for the isolated comparison. Selecting Fjall for production would require a
new critical-dependency and unsafe-code human review.
