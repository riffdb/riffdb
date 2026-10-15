# PERF-008 Emitter-Enabled vs Emitter-Disabled Disclosure (WP-750)

This disclosure accompanies `release/evidence/replication/replication-acceptance-v1.json` (`riffdb.replication-acceptance/v1`) and records the `REP-008` / `PERF-008` comparison cells across both `PERF-018` cloud profiles under the `ADR-0171` five-repetition stability rule (`central_three_max / central_three_min <= 1.20`), together with the post-`WP-810` `8 vCPU, AMD EPYC 9B14` archive-collector steady-state write-only qualification (`release/evidence/wp-749/archive-qualification-20261015/`).

## Bound Source and Binary Identity

- **Source revision:** `f237964998a4fd47141b7dc3f72bb5e653f72bd3`
- **`Cargo.lock` SHA-256:** `f77c8d916167f97fa8f8a2e35b8790e4b6f1ffb8668fbd076dd5067851a8a32a`
- **`riffdbd` SHA-256:** `92ff6aa48c71052d586a31ac7b0febf633162a57de2493dca34c5926cf28a4e4`
- **`riffdb` SHA-256:** `e9fc075bfb80e261f5ad9c6d7fc55f8def2a57a919327a88ec7bbce67acf0c99`

## PERF-018 Profile Comparison Cells (ADR-0171 Stability Rule)

| Profile | Hardware Specification | Cell | Disabled Median Throughput (ops/s) | Enabled Median Throughput (ops/s) | Throughput Ratio (Enabled/Disabled) | Central-3 Spread (Dis / En) | Disabled Median p95 (µs) | Enabled Median p95 (µs) | p95 Ratio (Enabled/Disabled) | Central-3 p95 Spread (Dis / En) | Within ADR-0171 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `e2_standard_8_pd_ssd` | `8 vCPU, AMD EPYC 7B12, pd-ssd` | `interactive_c32` | `588.5` | `585.9` | `0.9956` | `1.0071 / 1.0060` | `41,100` | `41,400` | `1.0073` | `1.0098 / 1.0122` | `true` |
| `e2_standard_8_pd_ssd` | `8 vCPU, AMD EPYC 7B12, pd-ssd` | `write_only_c32` | `495.1` | `493.2` | `0.9962` | `1.0077 / 1.0083` | `63,900` | `64,400` | `1.0078` | `1.0110 / 1.0109` | `true` |
| `n1_standard_8_local_ssd` | `8 vCPU, AMD EPYC 7B12, local-nvme-ssd` | `interactive_c32` | `1,431.0` | `1,425.4` | `0.9961` | `1.0068 / 1.0063` | `16,300` | `16,500` | `1.0123` | `1.0186 / 1.0184` | `true` |
| `n1_standard_8_local_ssd` | `8 vCPU, AMD EPYC 7B12, local-nvme-ssd` | `write_only_c32` | `1,192.6` | `1,187.5` | `0.9957` | `1.0077 / 1.0065` | `24,600` | `24,800` | `1.0081` | `1.0164 / 1.0163` | `true` |

## Post-WP-810 Archive Collector Qualification (`8 vCPU, AMD EPYC 9B14`)

Full artifacts and SHA-256 digests are retained under `release/evidence/wp-749/archive-qualification-20261015/`:

- **Bucketed `aggregate_p50_ns`:** `5,767,167 ns` (`disabled`) vs `5,767,167 ns` (`enabled`) (`+0.00%`)
- **Bucketed `aggregate_p95_ns`:** `8,912,895 ns` (`disabled`) vs `8,912,895 ns` (`enabled`) (`+0.00%`)
- **Raw unbucketed `p50_ns` / `p95_ns` / `p99_ns`:** `5,658,270 ns` vs `5,740,610 ns` (`+1.46%`), `8,453,920 ns` vs `8,533,410 ns` (`+0.94%`), `14,131,330 ns` vs `14,180,380 ns` (`+0.35%`)
- **Steady-state throughput:** `5,056 ops/s` (`disabled`) vs `4,997 ops/s` (`enabled`) (`-1.17%`)
- **Shutdown archive lag:** `0` (`543,999 / 543,999` application sequence archived across `36,063` history transactions)
