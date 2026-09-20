---
adr: "0247"
title: Hardware CRC32C And Sub Variance Wins
status: accepted
tier: surface
date: 2026-09-19
accepted: 2026-09-19
acceptance: 'maintainer, in session, 2026-09-19: "I accept"; the principle stated
  as "in general we should not leave anything on the table. in a database, at
  scale, these things can truly add up" (ADR-0247 as written)'
requires: [ADR-0239, ADR-0245]
amends: []
supersedes: []
requirements: []
packages: [WP-798]
obligations:
  - id: OBL-0247-1
    package: WP-798
    proof: crc_matches_the_software_table_across_sizes_and_alignments
    says: The envelope checksum agrees with the software CRC-32C table it
      replaced on every payload, so a dependency computing a different
      polynomial fails rather than silently rewriting every checksum.
review_triggers:
  - A checksum, digest or hash implementation would be substituted without a
    test holding it against the implementation it replaces and a published
    check vector.
  - A register entry would be quoted as a system-level result before the batch
    is measured on the bench host.
---
# ADR-0247: Hardware CRC32C And Sub Variance Wins

## Context

RiffDB computes CRC-32C over every envelope payload, on encode and on decode.
It used `crc`'s slice-by-16 software table, measured at 5.54 GB/s at the
6,393-byte frame size. The `crc32c` crate's SSE4.2 path measures 9.52 GB/s for
byte-identical output: **1.72x**, saving 482 ns per call, about **0.34%** of
writer busy time.

That is far below the bench host's 2 to 4 percent run-to-run variance, and
ADR-0239 decision 3 refuses a delta smaller than that as a result. Applied to
each change alone, that rule would refuse every improvement under roughly 3
percent permanently. In a database, at scale, that is most of them.

The substitution also surfaced a hazard worth recording. The obvious candidate,
`crc32fast`, is the most widely used CRC crate and computes **CRC-32/IEEE, not
CRC-32C**. Swapping it in would have changed every envelope checksum in every
database. No performance test would have caught it, and neither would the
envelope's own round-trip tests, because both sides of an encode and decode
would have agreed with each other. An equivalence assertion against the
outgoing implementation caught it on the first run.

## Decision

1. Use hardware CRC-32C for envelope checksums. The software table is retained
   as a dev-dependency oracle, not as a fallback.
2. A checksum, digest or hash substitution is accepted only with a test holding
   the new implementation against the one it replaces, across sizes and
   unaligned offsets, **and** against a published check vector. Agreement with
   another implementation in this repository is not sufficient on its own;
   `payload_crc32c(b"123456789") == 0xE3069283` anchors the polynomial
   externally.
3. Improvements too small to measure alone are recorded in
   `docs/performance/sub-variance-wins-register.md` and merged on evidence at
   the level the win occurs — a primitive benchmark, a complexity change, a
   removed allocation. They carry **no system-level claim** until the batch is
   measured together on the bench host against the previous banked revision,
   under ADR-0239's discipline.
4. The batch is measured when its estimated aggregate approaches 3 percent. If
   the aggregate does not appear, that is the result and is recorded as such.

## Consequences

ADR-0239's discipline is preserved exactly: nothing here is banked or quoted
before it is seen. What changes is that a real improvement is no longer
discarded for being individually invisible.

The register carries a risk of its own, and decision 4 names it. If a batch of
per-primitive wins does not show up in aggregate, the per-primitive numbers were
not reaching the system. That is the same failure mode the 2026-09 capture
attribution nearly shipped a durable-format change on, and finding it in a batch
of cheap reversible changes is a better place to find it than in an expensive
irreversible one.

The dependency swap is close to neutral in count: `crc` and `crc-catalog` leave,
`crc32c` and its build-time `rustc_version` and `semver` arrive. All clear the
existing licence and source policy.
