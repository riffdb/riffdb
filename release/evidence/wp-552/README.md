# WP-552 performance evidence

Status: complete; interactive and write-only evidence banked.

The interactive corpus passed on an idle host on 2026-08-09. It contains three
counterbalanced, isolated 90-second repetitions at 1, 8, 32, and 128 clients
for both RiffDB public gRPC and the frozen safe-app PostgreSQL comparator. The
merged report is stable, correctness-clean, same-device comparable, and marks
itself eligible.

| Evidence | SHA-256 |
|---|---|
| `interactive-90s-reps3.json` | `8865cdac9737e030572df0c3a32dbbcc9d97e928a23bf368507f0915a20ab3e2` |
| `write-only-90s-reps3.json` | `6ce3b9ca056661fd18fe489a666e13c4aafda5c9e2c9ada774719f6d78762dda` |

Both corpora contain three counterbalanced repetitions and are stable,
correctness-clean, same-device comparable, host-idle, and eligible under the
frozen safe-app comparator contract.

`manifest-v1.json` is the machine-readable release inventory for these two
reports. Verify the retained bytes and their eligibility, duration, matrix,
durability, host-validity, and correctness semantics without rerunning the
benchmark:

```bash
./scripts/check-alpha-performance-evidence --verify
```

## Public redaction and retained digests

On 2026-09-19 the manifest and table digests were refreshed to bind the public
reports after ADR-0243's filesystem-path redaction. The original reports matched
the previously frozen digests. Each public report differs only in six path
fields: the environment database root and mount, RiffDB database root and mount,
and PostgreSQL data path and mount. Measurements, eligibility, host validity,
durability, comparator and repetition data are unchanged, and the existing
semantic verifier passes. This is a digest repair for historical evidence, not
a new measurement or current release qualification.

## Earlier fail-closed receipts

The required interactive and write-only 90-second concurrency sweeps were
attempted on 2026-08-09. Both stopped before database startup because bounded
host sampling found non-harness CPU activity above the frozen threshold. This
is the required fail-closed result for an interfered host, not a substitute for
the retained three-repetition corpus.

| Receipt | SHA-256 |
|---|---|
| `interactive-host-interference.json` | `1634a281ce45451b4e902286dd3459912bd77b1382a8abc00b0281776e66a55c` |
| `write-only-host-interference.json` | `0504caf23ee58efd0a75d8c4587dbd1c1b6a4e5bd601a1ed328b101d104aa6f4` |

Each receipt contains only bounded process IDs, command names, CPU/I/O rates,
RSS, load, and memory availability. Process arguments and application values
are absent.

Reproduce the idle-host evidence:

```bash
./benchmarks/run-app-baseline --full --load interactive \
  --load-concurrency-sweep --load-duration-secs 90 \
  --postgres-comparator safe-app --reps 3 --require-stable

./benchmarks/run-app-baseline --full --load write_only \
  --load-concurrency-sweep --load-duration-secs 90 \
  --postgres-comparator safe-app --reps 3 --require-stable
```
