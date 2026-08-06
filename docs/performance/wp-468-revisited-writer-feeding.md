# WP-468: Revisited bounded writer feeding

WP-457 correctly retained generated-command concurrency 128 when its 192 and
256 candidates increased serialized validation, encoding, and staging work
more than they saved in durable commits. WP-458 through WP-464 subsequently
removed shared-graph clones, consumed staged graphs earlier, eliminated
temporary reciprocal-validation allocations, made events single-owner,
removed wire-reservation vectors, compared read dependencies in place, and
coalesced authenticated transport ingress. WP-468 therefore reruns the
decision against the current pipeline instead of treating the old result as a
permanent limit.

## Retained public bound

Rust, TypeScript, and Python generated command batches accept concurrency from
1 through 384 and reject zero or 385 before submitting work. The input
collection remains capped at 4,096 items. This is client-side backpressure,
not a larger transaction:

- each public `ExecuteBatch` request still contains at most 16 ordinary
  commands;
- concurrency 384 creates at most 24 bounded transport exchanges;
- each item keeps its own authorization, idempotency identity, outcome,
  retry, cancellation, progress, checkpoint, and uncertainty behavior;
- the coordinator queue remains independently capped at 512 messages and
  32 MiB; and
- a physical command group remains dynamically selected under the existing
  256-command and 16 MiB storage ceilings.

The CLI command-batch surface retains its separate operator-facing
concurrency bound. Raising the generated SDK ceiling does not silently widen
operator CLI resource use.

## Retain-or-revert evidence

The full TicketDesk seed contains 19,220 ordinary commands and runs through
the public generated Rust client and gRPC application service. On the retained
ext4/NVMe host, the adjacent diagnostics were:

| Generated concurrency | Seed | Physical groups | Mean commands/group | Durable commit time | Validation/encoding/staging |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 128 | 3.708 s | 302 | 63.6 | 2.308 s | 0.925 s |
| 256 | 3.054 s | 161 | 119.4 | 1.850 s | 0.823 s |
| 384 | 2.478 s median | 116 | 165.7 | 1.486 s | 0.813 s |

Three adjacent 384 runs completed in 2.688, 2.411, and 2.478 seconds, for a
2.478-second median. None had overload, conflict, idempotency, or semantic
failure. Representative post-seed unary commands remained approximately
1.7--3.3 ms, within the same run-to-run range as the smaller candidates. The
group, durable-commit, and staging columns above are from the 2.688-second
trace. The larger bound is retained because the intervening CPU work reversed
WP-457's result and the public seed now clears the three-second engineering
target without weakening durability or command isolation.

These are short same-host retain-or-revert measurements, not a published
cross-database performance claim. The ordinary mixed-load concurrency curve
continues to use its independent 1/8/32/128 client points.
