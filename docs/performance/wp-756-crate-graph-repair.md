# WP-756 crate-graph repair receipt

This is non-evidentiary build feedback, not a performance or release gate. It
compares commit `8c9b4343` (immediately before WP-754) with the WP-756 candidate
on the same host on 2026-09-04.

## Host and method

- Host: `kevinamd`, Linux 7.2.0, AMD Ryzen 9 7950X, 32 logical CPUs.
- Toolchain: `rustc 1.97.1`, `cargo 1.97.1`.
- Each side used a fresh isolated `CARGO_TARGET_DIR` with `RUSTC_WRAPPER=`.
- Incremental timing first warmed
  `cargo check --workspace --all-targets --all-features`, added the same one-line
  comment beneath `riffdb-storage-api/src/lib.rs`'s crate attribute, timed the
  same command with `/usr/bin/time`, and removed the comment.
- Link timings first warmed the three selected packages together. Before each
  timed command, `cargo clean -p <package>` removed only that package's outputs;
  dependencies remained warm. The measured command was
  `cargo test -p <package> --all-features --no-run --quiet`.

## Results

| Measurement | Before WP-754 | After WP-756 | Change |
|---|---:|---:|---:|
| storage-api one-line workspace rebuild | 7.59 s | 7.77 s | +2.4% |
| `riffdb-policy` test no-run | 24.18 s | 1.99 s | -91.8% |
| `riffdb-commit` test no-run | 14.61 s | 7.79 s | -46.7% |
| `riffdb-service` test no-run | 20.83 s | 15.98 s | -23.3% |

The incremental workspace wall time is effectively flat in this single-run
receipt, while each selected test link is materially shorter. Most importantly,
the repaired manifests make the dependency claim structural: leaf crates use
the core testkit, and daemon/process harnesses remain in `riffdb-testkit-server`.
`scripts/check-crate-graph` and `scripts/check-workspace-policy` enforce that
shape independently of these timings.

The post-removal storage-API inventory found no orphan public trait: every
trait still has a production, simulator, or executor reader-fake consumer.
Accordingly WP-756 removes no storage-API record, codec, bound, or fixture. The
ADR-0124 classification is `no_format_change`; durable, wire, IR, and frozen
compatibility identities are unchanged.

Maximum resident set sizes were 575,812 KiB before and 614,012 KiB after for the
incremental check; policy 972,684/427,788 KiB, commit 967,808/719,820 KiB, and
service 1,131,304/1,122,064 KiB respectively. User/system times are retained in
the session log; this receipt deliberately makes no statistical claim from one
observation.
