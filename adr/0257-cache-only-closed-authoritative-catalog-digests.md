---
adr: "0257"
title: Cache Only Closed Authoritative Catalog Digests
status: accepted
tier: surface
date: 2026-09-22
accepted: "2026-09-22"
acceptance: >-
  Exact-text agent review and acceptance under the maintainer's explicit
  in-session delegation, "you have permission to approve adrs without a human
  to accomplish this work", for WP-806. This includes the narrow package scope
  extension in Decision 4. It authorizes a measured candidate, not a performance
  claim or a relaxation of the C3D retention criterion.
requires: [ADR-0186, ADR-0239]
amends: []
supersedes: []
requirements: [REP-003, REP-005, STO-012]
packages: [WP-806]
obligations:
  - id: OBL-0257-1
    package: WP-806
    proof: generated_authority_inventory_matches_its_single_owner
    says: The frozen V1 declaration bytes remain identical.
  - id: OBL-0257-2
    package: WP-806
    proof: primary_admission_is_exact_source_only_domain_and_never_a_v1_mutation
    says: The V2 inventory remains distinct from V1 and does not reinterpret its domains.
  - id: OBL-0257-3
    package: WP-806
    proof: competing_first_catalog_digests_preserve_fixture_hashes_and_compute_once
    says: Competing cold callers receive the independent frozen fixture hashes with one computation per version.
review_triggers:
  - The cached value depends on a request, principal, database, stored bytes, or active runtime state.
  - An existing verification, authority comparison, audit transition, or durable byte changes.
  - A cache grows beyond the two fixed declaration digests or becomes caller configurable.
  - The candidate would be retained without the ADR-0239 C3D comparison discipline.
---
# ADR-0257: Cache Only Closed Authoritative Catalog Digests

## Context

WP-806's current-revision CPU sample resolves repeated string formatting,
reallocation, catalog fixture construction and hashing during ordinary writes.
Both `AuthoritativeStateCatalogV1::digest` and its V2 counterpart rebuild and
hash a closed compile-time declaration on every invocation. Neither accepts
request or database input. The existing record-registry digest already uses a
fixed process cache for the same kind of immutable declaration.

The package currently permits measurement files but not this storage API
implementation. This record reviews that exact scope extension under delegated
ADR authority; it does not silently broaden the package or infer that hashing
persisted records can be skipped.

## Decision

1. Each catalog's `digest` may use its own `std::sync::OnceLock<[u8; 32]>`.
   Initialization computes the existing SHA-256 over the existing canonical
   fixture. Return type and every digest byte remain unchanged. V1 and V2
   cannot share a cache cell. The fixture generator and namespace definitions
   remain the sole source of these bytes; no hand-maintained digest is added.
2. Only these two fixed declaration digests are cached. Stored bytes, active
   catalog selection, lineage, admission, authority checks, record hashes,
   audit, provenance and command results are never cached by this change.
   Existing callers still compare the expected digest to freshly obtained
   evidence at their existing boundaries.
3. Initialization has no new I/O, dependency, unsafe code or application knob.
   These storage-owner helpers are outside the deterministic command runtime.
   Failed initialization does not publish a partial digest; synchronization is
   owned by the standard-library primitive.
4. WP-806 may additionally touch the two catalog implementation files,
   `crates/riffdb-storage-api/src/authoritative_catalog/digest_tests.rs`, this
   record and the generated ADR index. A test-only computation counter may
   prove bounded initialization without measuring wall-clock time or replacing
   the allocator. No other runtime path is added to scope.
5. Before retention, compare the candidate with the pre-change and banked
   revisions on C3D under the same loads and client counts, preserve all runs,
   and apply ADR-0239's four-percent noise discipline. If no qualifying benefit
   is demonstrated, remove the candidate and retain the negative finding.

## Standing design tests

- **Interface safety:** no signature, command, protocol, durable format or
  application choice changes. There is no means to disable a check or select
  another digest. Independent hashes of the frozen fixtures prove equivalence.
- **Scale:** retained cache payload is exactly two 32-byte digests, plus two
  fixed synchronization cells. Initialization work is bounded by the fixed
  inventories and independent of data size, request count and concurrency.

## Checks

- Existing V1/V2 fixture, namespace, unknown-domain and handshake tests remain
  unchanged and pass; generated artifacts have no byte differences.
- The package's new regression test first demonstrates repeated computation,
  then uses a fresh child process and an explicit barrier to exercise competing
  first callers, exact independent fixture hashes, and one computation per
  version. No sleeps synchronize the correctness test.
- Scoped acceptance, full `ci-all`, and the recorded C3D comparison qualify the
  retained implementation. Profiling-only results never replace uninstrumented
  comparator results or the N1/E2 release gates.
