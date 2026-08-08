# WP-484 proven command-segment framing

The checked command-segment seal now frames the canonical payload bytes it has
just constructed without walking the complete nested Protobuf document a
second time. This uses the narrow ADR-0060 exception for sealed typed records:
the semantic constructors and structural encoder have already established the
field shapes, canonical order, cardinalities, and nested bounds.

The shortcut does not accept external or recovered bytes. It is selected only
by the authoritative command-segment seal and only for the sealed current
`StoredCommandSegmentV1` record identity. Compact tag and schema-revision
selection, the exact registered payload ceiling, and CRC-32C framing remain in
the shared durable-envelope implementation. General current-record encoding,
the independent generated-Protobuf segment encoder, compatibility decoding,
startup recovery, migration, and corruption checks retain their complete
preflight and semantic validation paths.

Exact-equality tests compare the proven framing with ordinary checked framing
and the independent generated encoder for rich multi-command segments and a
59,000-entry bounded manifest. The proven framing boundary also rejects a
payload one byte over its registered maximum. Storage and recovery suites keep
their torn-tail, checksum, hash-chain, migration, and malformed-record tests.

This changes validation work only. It does not change durable bytes, command
order, transaction validation, acknowledgement, visibility, recovery, or a
public application interface.

## Public-path checkpoint

Fresh full-seed, per-session HTTP/2 diagnostics at 32 clients produced:

| measurement | WP-483 | WP-484 | change |
|---|---:|---:|---:|
| write-only throughput, three-repetition mean | 4,649 ops/s | 4,805 ops/s | +3.3% |
| write-only p50 | 6.816 ms | 6.554 ms | -3.8% |
| validation/encoding/staging per selected command | 45.5 us | 44.3 us | -2.7% |
| full 19,220-command seed, three-repetition mean | 2.139 s | 2.025 s | -5.3% |
| write-only mean peak sampled RSS | 853 MiB | 859 MiB | +0.6% |
| durable growth per successful write-only mutation | 9,350 bytes | 9,055 bytes | -3.2% |
| interactive throughput, three-repetition mean | 24,741 ops/s | 25,645 ops/s | +3.7% |
| interactive aggregate/write p50 | 0.246 / 7.340 ms | 0.238 / 6.991 ms | improved |

The WP-484 write-only repetitions were 4,774, 4,821, and 4,819 ops/s. The
interactive repetitions were 25,749, 25,836, and 25,349 ops/s. Every run had
zero failures, conflicts, idempotency mismatches, unavailable results,
overloads, or replays.

Interactive sampled RSS rose from 788 MiB to 810 MiB while the same fixed
window completed 3.7% more operations and retained more live state. The
controlled write-only mean stayed within 0.6%. Durable bytes are proven
compatible by exact encoded equality; process-level file growth remains a
diagnostic affected by redb allocation and recyclable journal extents.

The result confirms that redundant structural preflight was a measurable but
secondary cost. It does not close the PostgreSQL write gap; the next material
campaign must target the remaining transaction-local storage application and
physical write work rather than another isolated encoding pass.

Artifacts:

- `target/app-baseline/wp484-write32.json`
- `target/app-baseline/wp484-interactive32.json`

These are local diagnostic artifacts rather than published benchmark claims.
