# WP-726 unary transport floor and honest service levels

`WP-726` addresses the component `ADR-0142` routed around rather than removed:
on both inventoried cloud profiles (`n1` and `e2`), the outside-service
residual and mandatory safety boundary (`TLS` + `HTTP/2` framing + `tonic`/`prost`
codec + per-request authentication + fresh pre/post policy evaluation + Tokio
task handoff) account for the majority of single-stream unary point-read latency.

All measurements below are extracted from the banked release-build `WP-674`
five-generation (`5 x (20 warmup + 1,000 measured)`) unary campaign receipts
under `release/evidence/wp-674/{n1,e2}/safe-app-*.json` and frozen in
`release/evidence/wp-674/baseline-v1.json` (source revision
`48bcb5ced552b7a19d6ac431f05432ae70deeaa2`, `riffdbd` SHA-256
`52de06848ed2c243ceafefb29de1c7be4fd6b69dd0f8f84938d6238b40e7f783`).

## 1. Stage decomposition of the unary residual on both cloud profiles

For each named-query operation, `riffdbd` records per-stage microsecond sums
across all `1,020` operations per generation (`20` warmups + `1,000` measured
operations) inside `read_pipeline_stages`, and the client harness records
end-to-end client wall time. For compiled commands, `riffdbd` records
`write_service_stages` and `command_pipeline_stages`.

### 1.1 Named-read stage decomposition (`n1`, 8 vCPU Intel N1)

Median per-operation stage means (`µs`) across the five process generations on
`n1`, alongside the client-observed mean (`µs`) and outside-server residual
(`client_mean - server_handler`):

| Scenario | Client mean (µs) | Outside server (µs) | `server_handler` (µs) | `transport_adapt` + `encode_convert` (µs) | `authn` + `authorize_*` (µs) | `admission` + `spawn_dispatch` + `plan_lookup` + `param_materialize` (µs) | `execute` (µs) | Non-`execute` share |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `point_get_user` | 523.9 | 385.2 | 138.6 | 11.74 | 25.02 | 42.95 | 28.31 | 94.6% |
| `point_get_ticket` | 538.3 | 395.1 | 143.2 | 13.40 | 24.96 | 45.42 | 29.78 | 94.5% |
| `list_project_members` | 609.0 | 415.4 | 193.6 | 15.11 | 26.31 | 44.47 | 73.97 | 87.9% |
| `list_comments_for_ticket` | 608.7 | 414.0 | 194.7 | 16.21 | 25.59 | 48.11 | 71.48 | 88.3% |
| `list_tickets_by_project_status` | 721.2 | 470.5 | 250.7 | 27.90 | 26.46 | 50.29 | 106.91 | 85.2% |
| `ticket_detail_page` | 750.5 | 449.9 | 300.7 | 25.23 | 31.05 | 50.87 | 155.27 | 79.3% |
| `list_open_tickets_for_assignee` | 957.2 | 573.0 | 384.1 | 51.76 | 26.52 | 50.97 | 215.47 | 77.5% |
| `board_page_50` | 2,053.2 | 625.8 | 1,427.4 | 51.15 | 29.50 | 51.26 | 1,249.61 | 39.1% |
| `board_page_200` | 2,951.4 | 1,171.3 | 1,780.1 | 156.15 | 31.45 | 54.27 | 1,492.11 | 49.4% |
| `board_page_450` | 4,397.8 | 2,042.3 | 2,355.4 | 329.88 | 35.13 | 59.05 | 1,874.86 | 57.4% |

On `n1` `point_get_ticket`, the `postgres_safe_app` median is `250.1 µs` (making
the retired `1.10x` target `275.1 µs`), whereas the outside-server client/TLS/HTTP2
residual alone is `395.1 µs` (`415.4 µs` on `list_project_members`) and inside-server
non-`execute` work adds another `113.4 µs`. Eliminating `100%` of storage and query
execution (`execute = 29.78 µs`) leaves `508.5 µs`, which still exceeds the retired
`1.10x` PostgreSQL budget.

### 1.2 Named-read stage decomposition (`e2`, 8 vCPU AMD EPYC 7B12)

Median per-operation stage means (`µs`) across the five process generations on
`e2`:

| Scenario | Client mean (µs) | Outside server (µs) | `server_handler` (µs) | `transport_adapt` + `encode_convert` (µs) | `authn` + `authorize_*` (µs) | `admission` + `spawn_dispatch` + `plan_lookup` + `param_materialize` (µs) | `execute` (µs) | Non-`execute` share |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `point_get_user` | 1,394.8 | 1,057.7 | 337.1 | 34.03 | 59.44 | 95.26 | 86.49 | 93.8% |
| `point_get_ticket` | 1,573.2 | 1,183.8 | 389.4 | 42.03 | 67.26 | 105.53 | 101.78 | 93.5% |
| `list_project_members` | 1,571.1 | 1,114.9 | 456.2 | 40.72 | 63.69 | 104.65 | 180.26 | 88.5% |
| `list_tickets_by_project_status` | 1,672.2 | 1,159.7 | 512.5 | 55.96 | 62.24 | 113.39 | 210.89 | 87.4% |
| `list_comments_for_ticket` | 1,709.3 | 1,214.1 | 495.2 | 46.93 | 70.26 | 111.17 | 190.95 | 88.8% |
| `ticket_detail_page` | 1,813.8 | 1,205.8 | 608.1 | 54.04 | 70.53 | 106.06 | 300.02 | 83.5% |
| `list_open_tickets_for_assignee` | 2,182.7 | 1,451.8 | 730.9 | 83.14 | 68.50 | 123.72 | 377.53 | 82.7% |
| `board_page_50` | 3,560.6 | 1,716.2 | 1,844.4 | 74.47 | 74.10 | 112.80 | 1,483.61 | 58.3% |
| `board_page_200` | 4,625.1 | 2,397.6 | 2,227.5 | 155.67 | 83.90 | 122.91 | 1,743.38 | 62.3% |
| `board_page_450` | 6,386.8 | 3,603.1 | 2,783.7 | 280.16 | 93.14 | 135.61 | 2,134.53 | 66.6% |

### 1.3 Compiled-command stage decomposition (`n1` and `e2`)

Median per-operation write service and command pipeline stage means (`µs`)
across the five process generations:

| Profile / Scenario | Client p50 (µs) | `transport_adapt` (µs) | `service_prepare` (µs) | `coordinator_await` (µs) | `service_finish` + `encode_convert` (µs) | `admission` + `compatibility` (µs) | `evaluation` (µs) | `validation_encoding_staging` (µs) | `publication` (µs) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `n1` / `create_comment` | 3,532.4 | 21.60 | 224.98 | 4,730.67 | 22.63 | 47.23 | 294.42 | 337.58 | 1,934.95 |
| `n1` / `swap_member_roles` | 3,590.4 | 21.80 | 209.28 | 4,778.26 | 25.38 | 42.76 | 305.85 | 324.86 | 1,960.12 |
| `n1` / `close_ticket_with_comment` | 3,745.6 | 22.05 | 211.04 | 4,905.47 | 30.01 | 43.39 | 346.11 | 384.92 | 1,948.89 |
| `n1` / `open_ticket_with_labels` | 4,345.7 | 23.77 | 242.47 | 5,406.01 | 33.97 | 48.55 | 444.56 | 499.93 | 1,956.94 |
| `e2` / `swap_member_roles` | 7,831.3 | 63.67 | 593.66 | 6,780.41 | 73.77 | 151.68 | 791.64 | 729.69 | 1,451.80 |
| `e2` / `create_comment` | 8,357.7 | 67.13 | 675.77 | 7,102.26 | 74.43 | 173.59 | 809.11 | 864.05 | 1,504.03 |
| `e2` / `close_ticket_with_comment` | 8,613.5 | 65.51 | 642.00 | 7,467.78 | 88.74 | 166.47 | 870.35 | 964.78 | 1,446.68 |
| `e2` / `open_ticket_with_labels` | 9,000.9 | 64.78 | 654.74 | 7,738.04 | 96.52 | 184.58 | 1,027.87 | 1,250.55 | 1,474.55 |

### 1.4 Driver-host boundary (`DRV-004` / `ADR-0148`)

The Rust benchmark harness (`riffdb-client-rust`) connects directly to `riffdbd`
over TLS/HTTP2 without `riffdb-driverd`, so the `385–1,184 µs` outside-server
point-read residual above contains zero Unix-socket driver-host hops. Under
`ADR-0148` and `DRV-004`, `riffdb-driver-host` owns a single closed, versioned,
length-bounded, schema-hashed, identity-pinned protocol core (`DriverRequest` /
`DriverResponse`): Python calls that core in-process (`parse_in_process_query`,
`parse_in_process_command`), while Go and TypeScript reach the same core over
one local Unix socket in `riffdb-driverd` (`benchmarks/run-app-baseline-golang`
and `benchmarks/run-app-baseline-typescript`, reported as non-evidentiary
language-surface comparisons).

## 2. Decision: the unary floor is RiffDB's honest alpha cost

We decide on the record that this unary floor (`~0.52–0.54 ms` on `n1`,
`~1.40–1.60 ms` on `e2` for single-stream point reads) is **RiffDB's honest
unary cost for alpha** and is not further reduced by weakening transport,
framing, authentication, or policy invariants:

1. **Every component in the floor is a normative safety requirement (`PERF-008`, `AGENTS.md` boundary 11)**:
   - Public application traffic runs over authenticated `TLS` and `HTTP/2`
     (`tonic`/`h2`) with typed Protobuf envelopes (`ADR-0133` compact schema-bound
     named-query responses are already enabled in `WP-674` receipts).
   - Every unary request authenticates the bearer capability (`authn`), binds
     bounded admission (`admission_context`), resolves the compiled plan
     (`plan_lookup`), materializes typed parameters (`param_materialize`),
     evaluates compiled row/organization policies before and after execution
     (`authorize_begin`, `authorize_pre`, `authorize_post`), and records audit
     completion (`audit_finish`).
   - By contrast, the PostgreSQL comparator (`tokio-postgres`) sends a prepared
     `Bind`/`Execute` binary frame over a persistent TCP socket without HTTP/2
     stream framing or per-RPC TLS record framing in the same shape, and without
     an out-of-engine gRPC/Protobuf serialization boundary.
2. **No unsafe opt-out is permitted**: Introducing an unauthenticated raw-socket
   fast path, skipping per-request policy verification, or bypassing task-isolated
   admission to chase a `250 µs` single-stream TCP microbenchmark would violate
   `PERF-008` and `AGENTS.md` boundary 11. Under concurrent load (`32` and `128`
   clients), HTTP/2 multiplexing and group commit amortize both the transport
   wakeup cost and the durable flush cost.

## 3. Operative gate: `ADR-0142` absolute ceilings are outer floors; the `WP-674` low-water bank is the regression gate

As required by `WP-726` deliverable 3:

- The `ADR-0142` absolute cloud service levels (`3 ms / 6 ms` for ordinary named
  reads, `6 ms / 12 ms` for wide bounded named reads, `10 ms / 15 ms` for
  compiled commands) are **outer admission floors/ceilings** for cloud hardware
  qualification. Because ordinary point reads run at `0.52–0.53 ms` on `n1` and
  `1.40–1.60 ms` on `e2`, those absolute ceilings alone have `1.9x–5.8x` headroom
  and would not by themselves catch a `50–100%` unary regression.
- The **operative unary non-regression gate** is the frozen `WP-674` low-water
  bank (`release/evidence/wp-674/baseline-v1.json`), enforced by
  `./scripts/check-wp674-unary-baseline`: every scenario's qualified `p50` and
  `p95` on every profile (`workstation`, `n1`, and `e2`) must be **`<= 1.10x`**
  its banked RiffDB low-water mark **in addition to** clearing the `ADR-0142`
  absolute ceilings.
- Consequently, **no published service level can be met by an artifact that has
  regressed by more than `10%` against the frozen low-water bank on any unary
  scenario, percentile (`p50` or `p95`), or profile**.

## 4. Complete PostgreSQL unary disclosure (`n1` and `e2`)

In accordance with `ADR-0142` Decision 4, `PERF-018`, and `ADR-0249` (which
requires `p99` and `max` beside every `p95` latency table), the complete
five-generation unary latencies and PostgreSQL ratios (`RiffDB / PostgreSQL`)
from `release/evidence/wp-674/baseline-v1.json` and
`release/evidence/wp-674/{n1,e2}/safe-app-*.json` are disclosed below.

### 4.1 `n1` unary latency and PostgreSQL ratio disclosure

| Scenario | Class | RiffDB p50 (ms) | RiffDB p95 (ms) | RiffDB p99 (ms) | RiffDB max (ms) | PG safe-app p50 (ms) | PG safe-app p95 (ms) | PG safe-app p99 (ms) | PG safe-app max (ms) | Safe-app ratio (p50 / p95) | Minimal PG ratio (p50 / p95) | Low-water gate (1.10x p50 / p95 ms) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `point_get_user` | ordinary read | 0.517 | 0.617 | 0.688 | 1.110 | 0.241 | 0.276 | 0.289 | 0.495 | 2.15x / 2.24x | 7.55x / 5.26x | 0.569 / 0.678 |
| `point_get_ticket` | ordinary read | 0.534 | 0.634 | 0.684 | 0.729 | 0.250 | 0.298 | 0.476 | 0.886 | 2.14x / 2.13x | 5.33x / 4.12x | 0.588 / 0.697 |
| `list_project_members` | ordinary read | 0.602 | 0.703 | 0.761 | 0.858 | 0.268 | 0.308 | 0.560 | 0.785 | 2.25x / 2.28x | 6.14x / 5.22x | 0.662 / 0.773 |
| `list_comments_for_ticket` | ordinary read | 0.605 | 0.699 | 0.732 | 1.062 | 0.264 | 0.299 | 0.346 | 0.581 | 2.29x / 2.34x | 5.53x / 3.69x | 0.665 / 0.769 |
| `list_tickets_by_project_status` | ordinary read | 0.715 | 0.821 | 0.882 | 1.059 | 0.296 | 0.330 | 0.379 | 0.506 | 2.42x / 2.49x | 5.71x / 4.70x | 0.787 / 0.903 |
| `ticket_detail_page` | ordinary read | 0.750 | 0.851 | 0.885 | 0.990 | 0.479 | 0.524 | 0.585 | 1.120 | 1.56x / 1.63x | 2.53x / 2.37x | 0.825 / 0.936 |
| `list_open_tickets_for_assignee` | ordinary read | 0.953 | 1.060 | 1.123 | 1.257 | 0.392 | 0.459 | 0.600 | 0.798 | 2.43x / 2.31x | 4.61x / 4.06x | 1.048 / 1.166 |
| `board_page_50` | wide read | 2.043 | 2.180 | 2.279 | 2.611 | 1.918 | 2.128 | 2.230 | 2.467 | 1.07x / 1.02x | 1.59x / 2.21x | 2.248 / 2.398 |
| `board_page_200` | wide read | 2.940 | 3.108 | 3.208 | 3.360 | 4.666 | 4.903 | 5.001 | 5.076 | 0.63x / 0.63x | 0.82x / 1.13x | 3.234 / 3.418 |
| `board_page_450` | wide read | 4.381 | 4.628 | 4.757 | 4.986 | 2.931 | 3.101 | 3.184 | 3.542 | 1.49x / 1.49x | 1.91x / 2.55x | 4.819 / 5.091 |
| `create_comment` | command | 3.532 | 3.924 | 4.222 | 2,093.219 | 2.286 | 2.618 | 3.291 | 4.602 | 1.55x / 1.50x | 4.10x / 31.55x | 3.886 / 4.317 |
| `swap_member_roles` | command | 3.590 | 4.001 | 4.155 | 2,054.815 | 2.329 | 2.679 | 2.844 | 5.008 | 1.54x / 1.49x | 3.84x / 3.61x | 3.949 / 4.401 |
| `close_ticket_with_comment` | command | 3.746 | 4.163 | 4.391 | 2,089.528 | 2.574 | 2.917 | 4.019 | 6.597 | 1.46x / 1.43x | 2.98x / 2.72x | 4.120 / 4.579 |
| `open_ticket_with_labels` | command | 4.346 | 4.797 | 5.056 | 2,070.566 | 2.546 | 2.897 | 3.197 | 6.494 | 1.71x / 1.66x | 3.85x / 3.53x | 4.780 / 5.276 |

### 4.2 `e2` unary latency and PostgreSQL ratio disclosure

| Scenario | Class | RiffDB p50 (ms) | RiffDB p95 (ms) | RiffDB p99 (ms) | RiffDB max (ms) | PG safe-app p50 (ms) | PG safe-app p95 (ms) | PG safe-app p99 (ms) | PG safe-app max (ms) | Safe-app ratio (p50 / p95) | Minimal PG ratio (p50 / p95) | Low-water gate (1.10x p50 / p95 ms) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `point_get_user` | ordinary read | 1.399 | 2.038 | 2.299 | 2.596 | 0.335 | 1.365 | 2.067 | 2.726 | 4.18x / 1.49x | 4.62x / 2.83x | 1.539 / 2.242 |
| `point_get_ticket` | ordinary read | 1.598 | 2.166 | 2.373 | 2.622 | 0.349 | 1.641 | 1.971 | 2.600 | 4.58x / 1.32x | 3.72x / 2.83x | 1.758 / 2.382 |
| `list_project_members` | ordinary read | 1.589 | 2.215 | 2.501 | 2.822 | 0.365 | 1.603 | 2.158 | 2.702 | 4.36x / 1.38x | 4.53x / 2.84x | 1.748 / 2.436 |
| `list_tickets_by_project_status` | ordinary read | 1.732 | 2.404 | 2.674 | 3.334 | 0.432 | 1.764 | 2.459 | 3.088 | 4.01x / 1.36x | 4.56x / 3.04x | 1.905 / 2.644 |
| `list_comments_for_ticket` | ordinary read | 1.736 | 2.309 | 2.562 | 2.770 | 0.368 | 1.526 | 2.165 | 2.649 | 4.72x / 1.51x | 4.32x / 2.91x | 1.910 / 2.540 |
| `ticket_detail_page` | ordinary read | 1.855 | 2.540 | 2.766 | 3.164 | 0.801 | 3.081 | 3.665 | 4.116 | 2.32x / 0.82x | 1.79x / 1.28x | 2.040 / 2.794 |
| `list_open_tickets_for_assignee` | ordinary read | 2.246 | 2.997 | 3.328 | 4.105 | 0.583 | 2.232 | 2.691 | 3.144 | 3.85x / 1.34x | 4.36x / 3.09x | 2.470 / 3.297 |
| `board_page_50` | wide read | 3.656 | 4.344 | 4.617 | 5.255 | 2.651 | 3.723 | 4.098 | 5.255 | 1.38x / 1.17x | 2.40x / 2.34x | 4.021 / 4.779 |
| `board_page_200` | wide read | 4.681 | 5.434 | 5.858 | 6.560 | 5.185 | 6.112 | 6.573 | 7.745 | 0.90x / 0.89x | 1.48x / 1.53x | 5.149 / 5.978 |
| `board_page_450` | wide read | 6.282 | 7.510 | 8.897 | 11.834 | 4.465 | 5.516 | 5.984 | 6.820 | 1.41x / 1.36x | 2.43x / 2.56x | 6.911 / 8.261 |
| `swap_member_roles` | command | 7.831 | 9.317 | 10.074 | 1,598.790 | 7.426 | 12.448 | 14.381 | 19.344 | 1.05x / 0.75x | 2.66x / 2.91x | 8.614 / 10.249 |
| `create_comment` | command | 8.358 | 10.203 | 10.955 | 1,525.425 | 8.652 | 13.259 | 14.444 | 17.361 | 0.97x / 0.77x | 3.68x / 6.23x | 9.193 / 11.223 |
| `close_ticket_with_comment` | command | 8.613 | 10.511 | 12.184 | 1,517.705 | 9.203 | 14.677 | 16.748 | 18.742 | 0.94x / 0.72x | 1.98x / 3.14x | 9.475 / 11.563 |
| `open_ticket_with_labels` | command | 9.001 | 10.818 | 11.862 | 1,500.383 | 8.650 | 14.055 | 15.690 | 18.993 | 1.04x / 0.77x | 2.53x / 4.32x | 9.901 / 11.900 |

## 5. Verification

```bash
./scripts/check-alpha-performance-evidence
./scripts/check-wp674-unary-baseline --self-test
```
