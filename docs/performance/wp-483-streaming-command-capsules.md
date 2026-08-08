# WP-483 streaming command capsules

The authoritative successful-command seal now writes the complete V1 command
capsule in canonical field order without constructing its generated-Protobuf
object graph. Fixed identifiers, hashes, partition keys, event payloads, and
manifest keys are borrowed from the checked semantic records. Repeated entity
references, event references, outbox identities, audit targets, durable events,
and index-generation transitions are handled one bounded member at a time.

The V2 capsule and segment encoders similarly retain only the current encoded
capsule or nested member while building the final segment body. The existing
generated-Protobuf encoder remains independent and unchanged. It is still used
to validate already sealed values and is the byte-for-byte oracle for the
streaming write path.

This changes temporary ownership only. It does not change command grouping,
transaction order, validation, hashes, durable bytes, acknowledgement,
recovery, or any public interface. Exact-equality tests cover two contiguous
commands, a predecessor segment, non-default scalar fields, causal metadata,
events, a nonzero prior index generation, audit optionals, and a 59,000-entry
manifest. The complete storage codec suite continues to decode and validate the
streamed representation.

## Public-path checkpoint

Fresh full-seed, per-session HTTP/2 diagnostics at 32 clients produced:

| measurement | WP-482 | WP-483 | change |
|---|---:|---:|---:|
| write-only throughput, three-repetition mean | 3,945 ops/s | 4,650 ops/s | +17.9% |
| write-only p50 | 8.126 ms | 6.816 ms | -16.1% |
| validation/encoding/staging per selected command | 55.3 us | 45.5 us | -17.7% |
| full 19,220-command seed, three-repetition mean | 2.614 s | 2.139 s | -18.2% |
| write-only mean peak sampled RSS | 889 MB | 895 MB | +0.6% |
| interactive throughput | 21,611 ops/s | 24,742 ops/s | +14.5% |
| interactive aggregate/write p50 | 0.344 / 7.864 ms | 0.246 / 7.340 ms | improved |

The WP-483 write-only repetitions were 4,690, 4,674, and 4,586 ops/s. All
three write-only runs and the interactive run completed without an application
failure, conflict, idempotency mismatch, unavailable result, or overload.

The interactive run retained more live database state because it completed
14.5% more operations in the same ten-second window; its sampled RSS was 826 MB
versus WP-482's 746 MB. The controlled write-only mean, where the workload shape
is directly comparable across three repetitions, remained effectively flat.
Durable-file growth remains diagnostic because recyclable journal extents and
redb allocation make process-level file deltas discontinuous. Exact byte
equality, rather than filesystem allocation, proves the compatibility claim.

This result removes a material CPU and allocation layer but does not claim
PostgreSQL write parity. The remaining validation/staging time includes semantic
graph validation, canonical digest and CRC work, redb application, and bounded
member-message construction.

Artifacts:

- `target/app-baseline/wp483-write32.json`
- `target/app-baseline/wp483-interactive32.json`

These are local diagnostic artifacts rather than published benchmark claims.
