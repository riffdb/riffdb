# ADR-0103: Preallocated Recyclable Durability Journal

- **Status:** Accepted
- **Date:** 2026-08-07
- **Accepted:** 2026-08-07
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `REC-001`, `REC-002`, `TXN-041`, `TXN-042`,
  `TXN-043`, `TXN-044`, `PERF-004`, `PERF-005`, `PERF-009`, `PERF-015`,
  `PERF-017`
- **Related work package:** `WP-480`
- **Amends:** ADR-0006, ADR-0061, ADR-0098, ADR-0101

## Context and mechanics evidence

ADR-0101 removed a redb durability fence from every command group, but its
extending append journal still pays filesystem metadata cost. WP-480 measured
the physical alternatives on the benchmark filesystem. Each scenario used 96
measured fences after 8 warmups, 3,000-byte and 64-KiB frames, with and without
4-KiB padding. Creation and pre-zeroing were outside the fence interval.

| mechanism | best p50 | representative p99 |
|---|---:|---:|
| extending append + `fdatasync` | 4,479 us | 5,325 us |
| sparse `set_len` + positional + `fdatasync` | 4,452 us | 5,343 us |
| fully zero-filled + positional + `fdatasync` | 880 us | 1,894 us |
| fully zero-filled + positional + `O_DSYNC` | 880 us | 1,990 us |

The selected mechanism is 5.09 times faster. Sparse allocation provides no
material improvement, proving that unwritten-to-written extent conversion is
the expensive metadata operation. `O_DSYNC` provides no advantage, so the
first implementation retains explicit `fdatasync` fence placement.

The evidence schema is `riffdb.journal-mechanics/v1`; reproduce it with:

```text
cargo +1.97.0 run --release \
  --manifest-path benchmarks/journal-mechanics/Cargo.toml
```

## Decision

### Fixed, fully materialized extents

Before readiness, RiffDB creates the complete bounded journal extent, writes
zeros through every data block, calls `fdatasync`, and syncs the containing
directory. `set_len` or unwritten `fallocate` alone is insufficient. Hot-path
publication uses positional writes inside the existing extent and one
`fdatasync`; it never extends, truncates, renames, allocates, or converts an
unwritten extent.

Capacity covers the existing recovery-suffix bound plus closed framing and
alignment overhead. It is checked before readiness and command acceptance.
Creation fails on insufficient capacity. ENOSPC therefore moves to preflight
rather than first appearing because an accepted frame needs file allocation;
ordinary I/O uncertainty retains ADR-0101's fencing behavior.

### Dual generation headers and position-bound frames

Two independently checksummed block-sized header slots bind format version,
database incarnation, monotonically increasing extent generation, checkpoint
application and administration frontiers, checkpoint frame hash, data bounds,
and slot identity. Recovery chooses the highest valid compatible generation.
Recycle writes and fences the inactive slot before the generation is usable;
the old slot remains a complete fallback across a torn header write.

Every physical frame wrapper binds extent generation, exact aligned physical
position, encoded and padded lengths, logical-frame hash, and a checksum over
wrapper and payload. The enclosed ADR-0101 frame retains its database,
predecessor, frontier, count, hash-chain, and mutation proofs.

Recovery scans only the expected next position. A valid older-generation
wrapper or zero residue ends the current log. A current-generation position
mismatch, gap, malformed length, checksum failure, inner/outer hash mismatch,
or noncanonical padding is corruption. A torn current frame may be ignored
only at the terminal position; it can never join valid stale residue from a
prior generation.

### Durable states and crash arms

The durable states are: old generation plus suffix; durable redb checkpoint
plus that suffix; checkpoint plus newly fenced header `g+1` with only stale
generation-`g` data; `g+1` plus a complete frame prefix; or `g+1` plus one torn
terminal frame. No state permits mixed generations.

Process tests crash during header-slot overwrite, after header fence, while a
new frame overwrites stale residue, during frame fence, after fence before
publication, and during checkpoint/recycle. They include torn-current-prefix
over valid stale tail and explicitly mixed-generation extents.

### Activation, backup, and replication

This pre-alpha incompatible format activates only when the legacy journal has
an empty suffix at a clean redb checkpoint. A nonempty old-format journal is
never guessed or discarded; startup returns a typed upgrade-required error
whose operator rendering names the journal path and requests a clean checkpoint
with the prior binary.

The full extent inventory and selected generation join the redb checkpoint in
backup manifests, capacity checks, path-disjointness checks, copy verification,
restore staging, and corruption validation. Checkpoint plus suffix remains one
verified backup unit.

This changes no replication, changelog, event, query, or public wire format.
ADR-0100 derives changelog frames from published frontiers rather than local
journal bytes, and ADR-0101 excludes those bytes from replication. That
layering deliberately permits this physical format change.

## Consequences

- The measured fence floor falls from roughly 4.5 ms to 0.9 ms without
  weakening acknowledgement durability.
- Allocation and zero-fill move to bounded readiness/recycle work.
- Recovery gains generation, slot, position, padding, and stale-residue proof.
- O_DSYNC is not added because it measured no better than explicit fdatasync.
- Remaining deficits after c8/c32/c128 reruns belong to command CPU, redb apply,
  or transport rather than extending-file durability mechanics.

## Rejected alternatives

- **Sparse preallocation:** measured effectively unchanged.
- **O_DSYNC:** equal p50 and slightly worse representative p99.
- **Extent-header-only generations:** torn overwrite can expose valid stale
  frame residue; every frame must bind generation and position.
- **Permanent dual-format recovery:** unjustified ambiguity before alpha.
- **Replication from journal bytes:** would freeze a local optimization into a
  distributed contract.

## Acceptance

The maintainer required a mechanics gate of at least 2x and accepted the format
after the probe exceeded it at 5.09x, explicitly requiring zero-fill,
per-frame generation/position binding, crash-state enumeration, empty-journal
activation, backup inventory, ENOSPC preflight, unchanged replication layering,
and c8/c32/c128 evidence.

