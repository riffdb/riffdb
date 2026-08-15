# ADR-0125: Command-Segment Raw Fallback Compatibility

- **Status:** Accepted
- **Date:** 2026-08-15
- **Direction approved:** Yes
- **Exact text accepted:** Yes — 2026-08-15
- **Amends:** ADR-0123 section 3 only
- **Defines or blocks:** WP-621

## Context

ADR-0123 accepted a successor command-segment envelope with closed `Raw` and
`Lz4Block` encodings. The implementation preflight found a maximum-boundary
compatibility problem before any durable bytes changed. A successor `Raw`
Protobuf message needs fields for codec, uncompressed length, and digest in
addition to the current canonical body. Those fields consume several dozen
bytes inside the unchanged 16-MiB stored-envelope ceiling. An incompressible
segment accepted by the current record at that boundary could therefore fail
solely because it was wrapped in the successor.

Narrowing an accepted command or frame bound is not an optimization. Raising
the stored-envelope ceiling would instead widen recovery, allocation, suffix,
and checkpoint bounds. Neither outcome is permitted by ADR-0123.

## Proposed Decision

The existing current command-segment record is the `Raw` representation. The
successor record represents `Lz4Block` only and carries the independently
checked uncompressed length and SHA-256 digest required by ADR-0123, the
compressed canonical current-body bytes, and the existing semantic segment
digest.

The writer constructs the current canonical segment bytes once, attempts the
bounded compression, and selects the successor only when its complete stored
envelope is at least 12.5 percent smaller than the complete current stored
envelope. Otherwise it persists the current record byte-for-byte. The current
and successor record identities are both writable under a `least_sufficient`
policy; this is an intentional per-segment choice, not a deployment mode or an
operator option.

Readers continue to accept both records. Both decode to the same
`StoredCommandSegmentV1` semantic type and participate in one unchanged segment
digest chain. Journal frames, redb checkpoints, backups, and recovery may
contain either record in any order. The durable-format manifest and version
topology register both writable identities and the deterministic selection
rule. Unknown records and codecs remain fail-closed.

This amendment changes no codec, threshold, digest, validation, dependency,
activation gate, or recovery rule accepted by ADR-0123. It only uses the
already-readable current record for the raw branch so the optimization cannot
narrow an existing maximum.

## Consequences

- Incompressible and very small segments remain byte-identical current records.
- Compressible segments use the successor and pay its extra validation fields
  only when the total stored result is materially smaller.
- The selected writer intentionally emits two registered record identities.
- A future retirement of the current decoder requires a separate ceremony and
  cannot occur while it is the raw representation.

## Compatibility

The complete previously accepted current-record input space remains writable.
No envelope, command, frame, suffix, allocation, or checkpoint bound changes.
Old binaries fail closed when they encounter the compact successor, as already
required by ADR-0123; they continue to read raw current records.

## Security

The fallback is based only on public-format byte lengths after complete
canonical construction. It is not application-visible and does not cross an
authorization boundary. Compression remains safe/checked, bounded before
allocation, independently digested, and followed by the full current semantic
decoder.

## Standing Design Tests

- **Interface safety:** applications cannot select, observe, or weaken the
  record choice. Every command retains identical authorization, idempotency,
  atomicity, durability, and recovery behavior.
- **Scale:** the current 16-MiB ceiling remains exact. Compression and both
  candidate envelopes use bounded owned buffers, and only one selected stored
  value survives staging.

## Testing

- A maximum accepted incompressible current segment selects the current record
  and remains writable without changing one byte or bound.
- Inputs immediately below, at, and above the 12.5-percent complete-envelope
  threshold select deterministically.
- Both record identities mix across journal frames, checkpoints,
  backup/restore, crash recovery, and segment-chain validation.
- Property tests decode both representations to equal semantic segments and
  reject length, digest, codec, and trailing-byte corruption.

## Decision Deadline

Exact maintainer acceptance is required before WP-621 adds the successor record
or the `lz4_flex` dependency.
