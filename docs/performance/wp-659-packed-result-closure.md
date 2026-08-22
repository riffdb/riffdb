# WP-659 packed named-result carriage

Status: closed honestly without production activation.

WP-659 adds an additive `PACKED_V1` named-result arm that reuses the projected
query canonical-column codec. The server validates one compiler-owned covered
result, packs bounded columns with canonical cells and exact offsets, and
returns exactly one legacy, compact, or packed response arm. The candidate
included direct Rust, Go, TypeScript, and Python generated decoders while it
was measured. V1 legacy and V2 compact driver peers remain accepted by the V3
driver host.

## Reject-first decision

ADR-0135 requires `BoardPage450` to improve at least 30 percent from
`COMPACT_V1` on every profile and to reach at most 1.10x same-run safe
PostgreSQL on both cloud profiles. The first paired workstation cell failed
both candidate thresholds:

| Workstation path | 50 rows | 200 rows | 450 rows |
|---|---:|---:|---:|
| Compact control | 0.488 ms | 0.873 ms | 1.577 ms |
| Packed candidate | 0.446 ms | 0.768 ms | 1.359 ms |
| Improvement | 8.6% | 12.1% | 13.9% |
| Same-run safe PostgreSQL (packed cell) | 0.518 ms | 1.307 ms | 0.821 ms |

The 450-row candidate is 1.65x same-run safe PostgreSQL and improves only
13.9 percent from compact. Because the activation rule is conjunctive, this
workstation miss rejects activation before consuming N1/E2 time or claiming
the larger no-regression matrix. It is not promoted into a cloud or release
performance result.

Production generated clients therefore continue to negotiate `COMPACT_V1`.
They do not advertise `PACKED_V1`, and callers receive no option that selects
an encoding. The additive protocol arm, strict preflight, server adapter, and
driver V3 compatibility remain as non-default compatibility code. The unused
generated decoder prototypes increased representative generated clients by
5.5--8.7 percent, independently failing ADR-0135's generated-size/no-
measurable-regression condition, so they were removed with production
selection. An architecture test pins that generated clients contain neither
packed selection nor dormant packed decoder bulk.

## Semantic and compatibility evidence

- Rust, Go, TypeScript, and Python passed the shared remote driver corpus while
  the candidate and direct generated decoders were selected. The corpus passed
  again after production selection and generated decoder bulk were removed.
- Driver V1 legacy and V2 compact handshakes and invokes remain readable; V3
  rejects packed-without-compact negotiation.
- Packed parent bounds, field uniqueness, row/column/offset agreement,
  canonical cell decoding, exclusive result arms, truncation, and malformed
  cells fail closed.
- The paired application run returned the same 50/200/450 ticket identities
  from PostgreSQL and RiffDB and reported no correctness error.

Raw value-free receipts remain outside the repository:

| Receipt | SHA-256 |
|---|---|
| Workstation packed candidate | `acfd2c9150c8fe93551573aef5c1b5be78711ffa4287ad921861a1070504b503` |
| Workstation compact control | `356815bb337fc065e732a23c163cc077839cca884f11ac0d8e3186d19d7d7d73` |

This closure claims no PERF-018 activation and changes no existing performance
gate. A future packed candidate must begin from a new accepted decision and
must clear the complete ADR-0135 gate rather than reusing this failed receipt.
