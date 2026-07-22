# ADR-0032: Same-Session Retained Metadata Handoff

- **Status:** Accepted
- **Direction approved:** 2026-07-21
- **Exact text accepted:** 2026-07-21
- **Accepted:** 2026-07-21
- **Requires:** ADR-0004, ADR-0007, ADR-0009, ADR-0019, ADR-0025, and
  ADR-0030
- **Amends:** The contents of `StructurallyOpened` and WP-130's readiness join
- **Decision deadline:** Before WP-130 composes lifecycle and readiness

The human maintainer approved this decision and its authoritative-file
amendments on 2026-07-21. This record exposes an already accepted semantic
metadata value only inside the completed same-session startup handoff. It adds
no durable category, storage key, record, migration, operational scan, or
transport field.

## Context

The accepted P1 lifecycle requires WP-130 to distinguish three facts after the
exclusive startup validation:

- whether the durable bootstrap marker is absent or present;
- whether both independent authoritative sequence allocators can progress; and
- whether retained identity and active-catalog metadata agree with the separate
  catalog-owned same-session proof.

Those facts determine whether restricted principal-less Health and one-time
loopback bootstrap remain legal, whether authenticated deployment is required,
and whether authoritative readiness is possible. Active-catalog absence cannot
distinguish a fresh unbootstrapped database from a bootstrapped database awaiting
its first deployment.

`RetainedMetadataV1` already owns exactly the six accepted POC categories and
their checked values. Storage validates them from the exclusive immutable
startup snapshot. The first `StructurallyOpened` handoff, however, retained only
database/session identity and dormant ports. No legal post-open reader exposes
the singleton bootstrap marker or allocator state, and adding an enumeration
port or probing bootstrap would expand authority or mutate durable state.

## Decision

### Handoff contents

`StructurallyOpened<P>` also contains the exact `RetainedMetadataV1` decoded
from the same immutable startup snapshot. Its backend-authorized constructor
takes that value, and its consuming decomposition returns database ID, open
session ID, retained metadata, and dormant ports. A read-only metadata getter
may also be provided.

This amends ADR-0030's descriptive statement that the value contains only
session identity and dormant ports. The retained metadata is bounded semantic
observation, not a storage handle, readiness proof, catalog proof, mutation
authority, cache, or new operational port.

The memory and redb startup sessions capture the complete checked value from
their already exclusive snapshots. Redb decodes the existing five `meta` rows
plus the existing `catalog_active/0x01` category; memory clones its existing
retained value under the exclusive store access. Each backend releases the value
only when both structural and historical streams reached their exact end, the
supplied exact-end tokens match the session, and no authoritative finding was
observed. Failure drops the session and publishes neither metadata nor ports.

### WP-130 readiness join

Only server composition mechanically joins:

1. matching database and open-session identities from `StructurallyOpened` and
   `ValidatedCatalogHistory`;
2. retained metadata whose database ID equals that structural identity;
3. an absent active pointer with absent validated active bundle, or an active
   pointer whose lineage, version, and bundle hash exactly equal the catalog
   proof; and
4. configured readable digest inventories and all other accepted startup
   checks.

An inconsistency fails closed before activating dormant ports. Neither storage
nor catalog receives the other's proof.

The retained bootstrap marker controls the lifecycle branch:

- marker absent: restricted principal-less Health and the exact loopback
  bootstrap operation remain available after validation;
- marker present with no active contract: Health is authenticated and ordinary
  authorized deployment is available, but command/general-read readiness is
  false; and
- marker present with a valid active contract: authenticated operation may
  become ready only when all other checks pass.

Both application and administration allocators must be `Next(_)` before
authoritative readiness becomes true. A matching `Exhausted` allocator remains
structurally canonical but produces authenticated not-ready health and no new
authoritative admission.

The server closes and drops its `PreBootstrapHealthContextIssuer` before
submitting a structurally valid bootstrap attempt to the coordinator. If the
bootstrap result becomes uncertain after possible durable marker creation, the
server stops routing and relies on full restart validation; it does not reopen
principal-less Health. Exact retained-token bootstrap replay remains available
after restart, but Health after a present marker is authenticated.

## Options Considered

1. **Carry existing retained metadata in `StructurallyOpened`:** Accepted. It
   preserves same-snapshot evidence and introduces no duplicate semantic type.
2. **Add post-open metadata or marker readers:** Rejected. It expands operational
   storage authority and creates a second read after the validated snapshot.
3. **Infer bootstrap from active catalog state:** Rejected. Bootstrap validly
   precedes first deployment.
4. **Probe or replay bootstrap to discover marker state:** Rejected. A readiness
   observation must not perform an authoritative mutation or consume audit
   sequence space.
5. **Return only booleans for marker/allocator state:** Rejected. It duplicates
   existing semantic metadata and prevents exact identity/active-pointer
   agreement checks.

## Consequences

- WP-070/storage API and memory conformance receive a focused startup-handoff
  correction before WP-130.
- WP-130 can construct the accepted lifecycle without direct storage access or
  a race after startup validation.
- Exhaustion, bootstrap, and active-catalog decisions are made from one
  immutable validated snapshot.
- WP-185 reuses the activated P1 graph and does not add another metadata path.

## Compatibility

This changes an internal Rust type-state handoff only. It changes no retained
metadata category, redb table or key, Protobuf field, stored envelope, durable
bytes, backup format, contract grammar, hash, public API, or migration rule.
Existing POC databases reopen without modification.

## Security

Principal-less Health authority closes before a bootstrap transition can become
durable and cannot be reconstructed when the retained marker is present. The
handoff contains no credential, digest key material, raw token, policy proof,
transaction, iterator, or write handle. Mismatched metadata/catalog evidence,
an exhausted allocator, and uncertain bootstrap all fail closed.

## Testing

Storage API tests freeze construction authority, retained metadata access, and
the consuming tuple. Memory and redb tests cover initial metadata, present
bootstrap marker, absent/present active pointer, both allocator exhaustion
states, same-session retention, exact-end requirements, authoritative findings,
and malformed or inconsistent metadata refusal without port publication.

WP-130 lifecycle tests cover fresh bootstrap, post-bootstrap/pre-deployment,
ready active state, both exhausted allocators, active-pointer/catalog mismatch,
pre-bootstrap issuer closure before submission, uncertain bootstrap process
stop, restart-driven marker selection, and authenticated retained-token replay.

## Requirements and Work Packages

- **Requirements:** `STO-012`, `REC-001`, `REC-002`, `API-001`, `POC-009`
- **Corrects:** `WP-070` and its shared storage startup API
- **Blocks:** `WP-130`
- **Consumed by:** `WP-130`, `WP-185`, and `WP-200`
- **Final evidence:** `WP-200`
