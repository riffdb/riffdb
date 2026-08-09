# WP-552 host-validity receipts

Status: non-evidentiary; no performance claim.

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

Rerun on an idle host:

```bash
./benchmarks/run-app-baseline --full --load interactive \
  --load-concurrency-sweep --load-duration-secs 90 \
  --postgres-comparator safe-app --reps 3 --require-stable

./benchmarks/run-app-baseline --full --load write_only \
  --load-concurrency-sweep --load-duration-secs 90 \
  --postgres-comparator safe-app --reps 3 --require-stable
```
