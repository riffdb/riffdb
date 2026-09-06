---
adr: "0207"
title: Frozen Changelog Compatibility Exception During the Pre-External Window
status: proposed
tier: guarantee
date: 2026-09-06
accepted: null
requires: [ADR-0100, ADR-0112, ADR-0124, ADR-0178, ADR-0181, ADR-0186, ADR-0204]
amends: [ADR-0181, ADR-0204]
supersedes: []
requirements: [GOV-001, GOV-002, GOV-003, REP-003, VER-005]
packages: [WP-757, WP-772]
obligations:
  - id: OBL-0207-1
    package: WP-757
    proof: topology_allows_only_exact_changelog_compatibility_exception
    says: While the pre-external window is open, the topology accepts exactly the frozen
      authoritative-changelog V1/V2/V3 reader set with V3 as its sole writer and current identity,
      and continues to reject every other multiple-identity domain or variation of that set.
  - id: OBL-0207-2
    package: WP-772
    proof: changelog_v3_is_only_production_replication_identity
    says: Existing V1 and V2 decoders and tests remain read-only evidence, frozen compatibility
      fixtures are added without changing their bytes, and every production negotiation, emission,
      apply, bootstrap tail, and archive path selects only exact V3.
review_triggers:
  - Any pre-external topology domain other than the one exact authoritative-changelog domain would
    carry more than one readable, writable, current, or active identity.
  - The authoritative-changelog identity sets, order, lifecycle, or writer policy would differ
    from the closed values in this record.
  - V1 or V2 would become writable, current, active, negotiated, emitted, applied, bootstrapped,
    archived, translated, or selectable by a caller, application, operator, or configuration.
  - V3 would cease to be the sole production changelog identity, or production would fall back
    after a V3 format, catalog, lineage, epoch, bound, or position refusal.
  - An exception would become configurable, generic, inferred, or extensible without another
    accepted guarantee ADR.
  - This proposal would change runtime code, durable bytes, fixtures, topology data, package text,
    or the external-database registry before exact acceptance and separate reviewed changes.
---
# ADR-0207: Frozen Changelog Compatibility Exception During the Pre-External Window

## Context

ADR-0181 and ADR-0204 require every topology domain to have one equal readable, writable, and
current active identity while `external_databases` is empty. ADR-0186, accepted later, requires the
successor authoritative changelog to retain V1 and V2 decoders and fixtures as compatibility
evidence while production writes and applies only V3. WP-772 repeats that requirement as
V1/V2-readable plus V3-writable topology registration. Neither implementation package may choose
which accepted guarantee to weaken.

The existing V1/V2 Rust decoders, source, unit tests, and recovery tests prove that V1 remains
put-only, V2 preserves delete-aware transition and rotation semantics, neither old identity is
reinterpreted as V3, and V3 refuses downgrade rather than claiming incomplete replication. No
frozen standalone V1/V2 frame fixtures exist yet; WP-772 must add them beside V3 rather than
misrepresenting tests as fixtures. The old formats do not serve an external database, authorize a
legacy production path, or justify closing the pre-external window. A closed topology exception
preserves that negative evidence without reopening ADR-0181's general multi-reader carrying cost.

## Decision

1. While `release/version-topology-v1.json.external_databases` is empty, exactly one domain is
   exempt from ADR-0181's universal single-readable-identity rule. Its domain ID is
   `durable.authoritative_changelog`, its ordered `readable_identities` are exactly
   `riffdb.changelog-frame/v1`, `riffdb.changelog-frame/v2`, and
   `riffdb.changelog-frame/v3`, and its `writable_identities` and `current_identities` are each
   exactly `riffdb.changelog-frame/v3`. Its writer policy is exactly `single_current`.

   The identity bindings are also exact. `riffdb.changelog-frame/v1` names the existing Rust type
   `ChangelogFrameV1`, numeric format version `1`, frame magic `RDBCLF01`, and footer magic
   `RDBCLE01`. `riffdb.changelog-frame/v2` names the existing Rust type `ChangelogFrameV2`, numeric
   format version `2`, frame magic `RDBCLF02`, and footer magic `RDBCLE02`.
   `riffdb.changelog-frame/v3` names ADR-0186's accepted V3 identity, numeric format version `3`,
   frame magic `RDBCLF03`, and footer magic `RDBCLE03`. The V1/V2 symbolic labels are frozen here;
   they name those existing bytes and create no new encoding or interpretation.

2. The domain's lifecycle is exact: V3 is its sole `active` identity; V1 and V2 are exactly
   `read_only`; `retirement_candidate` and `retired` are empty. The existing V1/V2 source decoders,
   unit tests, and recovery tests remain only for ADR-0100 and ADR-0186 compatibility evidence.
   WP-772 adds frozen standalone V1 and V2 frame fixtures alongside its V3 fixtures and proves they
   decode under the exact bindings in Decision 1. Adding those fixtures records existing canonical
   output; it changes no V1/V2 byte, magic, numeric version, tag, chain, rotation receipt,
   checksum, decoder, or interpretation.

3. Topology readability grants no production selection. Every production replication handshake
   selects only exact V3 under ADR-0186. Emission, follower apply, bootstrap tail, and WP-749
   archive carry, accept, and persist only exact V3. V1/V2 never provide fallback, translation,
   bootstrap, restore, archive, downgrade, or a partial-authority mode. An unsupported or
   mismatched V3 format, catalog, lineage, epoch, bound, or position refuses before transfer or
   mutation.

4. Every other topology domain remains subject to ADR-0181 and ADR-0204 without exception while
   the pre-external window is open: exactly one readable, writable, and current identity; all three
   equal; exactly that identity active; and no other lifecycle member. The changelog exception is
   a hard-coded equality check over the domain ID and complete sets above. It is not a topology
   flag, allow-list entry, feature, environment variable, configuration, capability, CLI option,
   API field, or operator ceremony. Another exception requires another accepted guarantee ADR.

5. This record amends ADR-0181 Decisions 1 through 3, OBL-0181-1, and their consequence, review,
   compatibility, and test language only by replacing universal pre-external wording with “every
   domain except the exact ADR-0207 authoritative-changelog domain.” The existing proof
   `topology_refuses_second_readable_identity_before_external_database` continues to reject a
   second reader everywhere else. No epoch-1 database reader, fixture, or in-place path is restored;
   the ADR-0112 export/reimport ceremony and the truthful external-database registry are unchanged.

6. This record amends ADR-0204 Decisions 1 and 8 and their review and check language only to carry
   the same exact exception. ADR-0204's 32 retired registry identities, 26 deleted declarations,
   five historical-row removals, 27 retained descriptor rotations, tag-65 removal, tag-67
   preservation, epoch-first refusal, and two authorized storage-redb guarantee paths remain
   byte-for-byte and semantically unchanged. Changelog V1/V2 add nothing to those closed sets.

7. ADR-0100, ADR-0124, ADR-0178 as amended by ADR-0186, and ADR-0186 otherwise remain unchanged.
   If a genuine external database is later registered, ADR-0181's special pre-external rule ends;
   this domain then remains governed by ADR-0124's ordinary explicit lifecycle until a separately
   accepted retirement. Registration does not automatically delete, promote, or reinterpret an
   identity, and this record neither creates nor fabricates an external-database entry.

8. After exact acceptance, one separate governance commit updates WP-757: add ADR-0207 to
   `required_adrs`; narrow the objective and global reader-removal deliverable; replace the
   topology deliverable and exit gate with the exact exception; and narrow the corresponding human
   review trigger. The same commit updates WP-772: add ADR-0207 to `required_adrs`; bind its
   topology deliverable to the exact domain, identity bindings, sets, lifecycle, V1/V2 fixture
   addition, and V3-only production rule; and add exception drift to its human review triggers.
   Dependencies, allowed paths, requirements, other deliverables, and acceptance commands remain
   unchanged.

9. WP-757 implements `topology_allows_only_exact_changelog_compatibility_exception` in
   `scripts/check-version-topology`. Its self-tests accept the exact open-window domain and reject:
   another exceptional domain; any missing, extra, duplicate, or reordered identity; any symbolic,
   Rust-type, numeric-version, or magic mismatch; V1/V2 in writable, current, or active; V3 absent
   from any required set or lifecycle; a nonempty candidate or retired set; a different writer
   policy; and any configurable exception field. The existing paired
   nonempty-`external_databases` case continues to prove ordinary ADR-0124 behavior after the window
   closes. WP-772 separately freezes V1/V2 fixtures and proves no production path selects them.

10. This proposal changes only this decision record and the generated ADR index. It changes no
    runtime, durable or wire byte, fixture, topology value, package, public interface, authority,
    or external state. Acceptance authorizes only the separate governance edits in Decision 8;
    WP-757 and WP-772 retain their own implementation scopes, commits, reviews, and gates.

## Options considered

1. **Retire V1/V2 and make V3 the only reader:** rejected because it reopens ADR-0100, ADR-0178,
   and ADR-0186, removes the existing decoders and unit/recovery evidence before WP-772 freezes
   their compatibility bytes, and requires broad runtime and recovery-test surgery merely to
   satisfy a repository rule.
2. **One frozen changelog-only exception:** chosen because it reconciles both accepted decisions in
   the topology checker without changing runtime behavior, durable bytes, or production selection.
3. **Register an external database:** rejected as an unblock mechanism because no qualifying
   operator evidence was supplied and registration permanently closes the window for every domain.

## Consequences

- Existing V1/V2 decoders and unit/recovery tests remain, and WP-772 adds their first frozen
  standalone frame fixtures, while V3 is the sole production replication and archive identity.
- The pre-external checker gains one permanent closed special case and its negative-test burden;
  every other domain retains the simpler one-identity rule.
- Retirement of V1/V2, another compatibility reader, translation, production fallback, and any
  caller-selectable format remain deferred to a separate accepted guarantee ADR.

## Standing design tests

- **Interface safety:** The topology is descriptive and the exception has no runtime control. An
  application, agent, operator, configuration, or peer cannot select V1/V2, request fallback, or
  weaken V3 validation, transfer, apply, bootstrap, archive, or refusal behavior.
- **Scale:** The checker compares one fixed three-reader domain and otherwise retains constant-size
  per-domain validation. No database scan, data-dependent memory, co-location assumption, or
  production work is added.

## Checks

- `topology_allows_only_exact_changelog_compatibility_exception` and
  `topology_refuses_second_readable_identity_before_external_database` prove the exact exception,
  every rejected near miss, and the unchanged rule for all other domains.
- `changelog_v3_is_only_production_replication_identity` proves V1/V2 are compatibility-only and
  frozen without byte or interpretation drift, and unreachable from production negotiation,
  emission, apply, bootstrap tail, and archive paths.
- `./scripts/check-version-topology`, `./scripts/check-generated`, requirement coverage, ADR form,
  obligation, index, and allowed-path checks remain required in the owning package gates.
