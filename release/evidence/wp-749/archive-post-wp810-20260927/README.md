# WP-749 archive comparison after WP-810

The prescribed isolated pair completed on source `10c312d74`, with unmodified
production daemon source, one daemon binary and one common runner binary.
Both cells pass host, correctness and raw-sample checks. The archive gate
remains unmet: P50 **+5.52%**, P95 **+10.58%**, P99 **+40.26%**. Throughput
**-11.62%** is disclosed separately. This is retained failed-gate evidence,
not a qualified performance claim or a latency waiver.

| Metric | Disabled | Enabled |
| --- | ---: | ---: |
| Raw acknowledgement P50 (ms) | 7.953330 | 8.392720 |
| Raw acknowledgement P95 (ms) | 10.648570 | 11.774710 |
| Raw acknowledgement P99 (ms) | 92.710010 | 130.031871 |
| Throughput (ops/s) | 3,209 | 2,836 |
| Successful measured requests | 288,836 | 255,258 |

## Scope and limitations

Host: 8 vCPU AMD EPYC 9B14, hardware SHA-NI/AES/PCLMULQDQ/SSE4.2.
Rust 1.98.1, generic x86-64, thin LTO, one codegen unit, production allocator.
Full 19,220-row seed, 32 write-only clients, Standard durability, 15-second
warmup and 90-second measurement. Fixed disabled/enabled order; no extra cells
or retry. The stopped full-backup preparation is identical in both arms.

The runner-only patch retains each request duration in a preallocated bounded
array. The production daemon is built before applying this patch; no daemon
observer is enabled. All 544,094 samples are retained with zero drops; the
two arrays differ and reproduce their reported histogram buckets exactly.
Both arms have zero conflicts, errors and unavailable outcomes.

Archive collection confirms progress through application sequence 323,058 and
19,460 history transactions after the backup. Inferred terminal application
lag is zero; its method and limitations are retained in the cell analysis.
The archive's selected manifest digest is retained without the archive data.

Mean positional journal-write time is 35.527 versus 311.179 microseconds per
flush; maxima are 5.615 versus 123.636 ms. This locates additional blocking
before journal sync, but the aggregate counters do not associate it with
individual acknowledgements or checkpoints. The shared writeback/flush
mechanism previously identified by WP-810 does not attribute this new
archive-specific movement. No percentile subtraction, shared-device cause,
latency noise band or repeatability claim is made.

## Verification and custody

`SHA256SUMS` covers these exported bytes. Run:

```sh
python3 verify.py
```

This verifies identical executable identities, host receipts, raw-input
bindings and paired arithmetic. It does not manufacture a performance pass.

The complete raw reports, request samples, daemon logs, exact binaries and
source bundle remain private. Their 52-file export passed independent size
and digest checks. Running the retained analyzer on those inputs reproduced
both analyses and the full comparison byte-for-byte:

```sh
python3 analyze.py <retained-root>/results
```

The build receipt here omits the private output-directory path. Its checksum
is distinct from the original, whose digest is recorded. The raw sample
schema retains its historical private WP-810 name; the exact observer patch
was reused without changing its meaning. No private paths or machine addresses,
raw logs or compiled artifacts are committed.

`existing-proof-audit.json` binds 84 semantic test names and 20 current source
files to the successful full-CI run at `ccadaf50`; runtime and test files are
unchanged at the measured source. Source/sink isolation, exact stops,
authorization, compatibility, durable retry and process-crash proofs are green.
The two raw-sample tests pass locally and on the measurement host; analyzer
checks refuse changed samples, mismatched counts, overflow and invalid hosts.

The four runner-build warnings are existing missing field documentation on
`BackgroundScrubObservation`, not suppressed warnings or failed tests.
Host isolation was restored. This packet changes no runtime, durable bytes,
public interface, accepted latency clause or completion status.

Package: WP-749. Tier: surface evidence under existing accepted measurement
records. The detailed disposition is in
`docs/performance/wp749-archive-post-wp810-2026-09.md`.
