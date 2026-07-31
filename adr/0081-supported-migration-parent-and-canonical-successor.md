# ADR-0081: Supported Migration Parent and Canonical Successor

- **Status:** Accepted
- **Date:** 2026-07-31
- **Decision owners:** RiffDB maintainers
- **Acceptance reference:** Human maintainer approval in the WP-406 implementation session on 2026-07-31
- **Depends on:** ADR-0075, ADR-0076, ADR-0077
- **Amends:** ADR-0077

## Context

`ContractBundle` records one exact compilation parent and includes that parent
identity in its canonical hash. Application lock V4 must nevertheless support
direct migration from as many as 32 retained older bundles to one canonical
successor bundle. Requiring every supported migration parent to equal the
successor bundle's immediate compilation parent would make that impossible.
Compiling a separate successor against every retained parent would instead
create divergent bundle hashes and potentially divergent stable-ID ledgers for
the same public contract version.

## Decision

The successor bundle in one application release remains singular and is
compiled through the canonical lineage path required by ADR-0075. A supported
migration parent is an exact lower-version bundle in the same lineage; it does
not need to equal the successor bundle's immediate compilation parent.

For every `.riffm` entry, the migration compiler compares that exact retained
parent directly with the canonical successor schema, plans, identities, and
generated registries. It derives proof obligations from this direct comparison,
not from the successor bundle's stored immediate-parent compatibility report.
The migration artifact binds the exact retained-parent and canonical-successor
bundle hashes. Any incompatible direct comparison, wrong lineage, non-lower
source version, incomplete proof, or stale hash fails closed.

The server still performs no migration chaining. At application time it selects
the one V4 entry whose exact parent version and hash match the active database,
then publishes the single canonical successor bundle named by the lock.

## Consequences

- One release has one successor bundle and may have up to 32 direct migration
  proofs into it.
- The contract bundle's immediate parent continues to describe canonical
  compilation ancestry; a migration entry describes a supported operational
  starting point.
- Migration compilation must run the IR-owned comparator for each retained
  parent instead of trusting the candidate's stored compatibility report.
- Parent-specific successor bundles with the same public version are rejected.

## Compatibility

No existing bundle, lock, or durable encoding changes meaning. Application lock
V4 and `MigrationBundleV1` are not yet released formats. V1 through V3 locks and
all existing contract bundles retain their exact bytes and semantics.

## Security

The relaxation is not a hash relaxation. Both endpoint hashes remain mandatory,
the lineage and version order are checked, and compatibility/proof coverage is
recomputed from strictly decoded artifacts before a migration is admitted.

## Testing

- Compile two retained lower-version parents into migration bundles targeting
  one byte-identical successor bundle.
- Reject a different lineage, equal or newer source version, changed parent
  bytes, changed successor bytes, incompatible direct diff, and stale proof.
- Verify lock V4 selects by both parent version and bundle hash without chaining.

## Requirements and Work Packages

- **Requirements:** `MIG-001`, `MIG-002`, `MIG-003`, `MIG-017`, `MIG-019`
- **Defines or blocks:** `WP-406`, `WP-409`, `WP-411`
- **Final evidence:** `WP-413`
