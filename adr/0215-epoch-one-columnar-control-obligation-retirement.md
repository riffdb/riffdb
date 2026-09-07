---
adr: "0215"
title: Epoch-One Columnar Control Obligation Retirement
status: proposed
tier: guarantee
date: 2026-09-07
accepted: null
requires: [ADR-0181, ADR-0192, ADR-0204, ADR-0209]
amends: [ADR-0192]
supersedes: []
requirements: [GOV-001, GOV-002, GOV-003, PRJ-007]
packages: [WP-757]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - OBL-0192-3 or OBL-0192-4 would be renamed, assigned a semantically different proof, or erased from the historical record.
  - A retired epoch-one V1 advancement, fresh-V1 rebuild, selector, artifact, transition, or fallback would remain reachable in epoch two.
  - An OBL-0192-3 or OBL-0192-4 clause would be mapped beyond the exact says text of OBL-0209-1 through OBL-0209-5.
  - OBL-0192-10, its exact says text, package, or proof would change.
  - This proposal would edit an accepted ADR, work package, governance rule, requirement, proof tag, or implementation before exact acceptance.
---
# ADR-0215: Epoch-One Columnar Control Obligation Retirement

## Context

ADR-0192 assigned two WP-776 obligations to the epoch-one columnar-control
bridge. WP-776 proved them exactly. ADR-0209 later made epoch two V2-only and
deleted the V1 advancement and fresh-V1 runtime behavior those obligations
name. Keeping the two obligations live would make WP-757 restore forbidden V1
behavior or leave accepted obligations pointing at deliberately retired proof
names.

The historical evidence must not be rewritten as a different test. The live
epoch-two guarantees are the five obligations ADR-0209 created for WP-757, and
tag-65 retirement remains the independent OBL-0192-10. This record resolves
only that obligation-lifecycle conflict; it does not supersede ADR-0192 or
authorize an implementation change.

## Decision

1. ADR-0192 remains accepted and is amended only in the live status of
   OBL-0192-3 and OBL-0192-4 at the epoch-two boundary. Both obligations remain
   exact historical WP-776 evidence:

   | ID | Package and proof | Exact accepted claim |
   | --- | --- | --- |
   | OBL-0192-3 | WP-776; `columnar_control_lifecycle_shapes_and_retention_inputs_are_closed` | Every Unprepared/Prepared Candidate state, pointer role, lifecycle, Candidate/Published-failure transition, predecessor class, servability result, retention input, initial publication, and V1 advancement is exact; advancing V1 preserves an independent V2 candidate byte-exact. |
   | OBL-0192-4 | WP-776; `columnar_control_fresh_rebuild_ignores_every_legacy_selector` | Startup atomically establishes each common control at a BeforeFirst retention fence, advances that fence only after the captured authoritative snapshot is durably validated, and rebuilds fresh V1 without reading, translating, copying, or selecting legacy scalar directories or tag-65 values. |

2. Epoch two retires, without replacement or proof relabelling, the
   OBL-0192-3 clauses requiring V1 advancement and preservation of an
   independent V2 candidate across that advancement. It also retires, without
   replacement or proof relabelling, the OBL-0192-4 fresh-V1 rebuild and its V1
   fence-advancement ceremony. Those clauses are neither requirements of
   WP-757 nor claims made by an epoch-two test.

3. The following clause map identifies only live epoch-two successor
   ownership. Each destination remains limited by its own exact ADR-0209
   `says` text; this map does not make an old proof prove a new claim or expand
   a new obligation:

   | Historical concern that remains live in epoch two | Sole live owner |
   | --- | --- |
   | Closed control shapes, pointer roles, lifecycle, failure, predecessor, and servability semantics, now V2-only | OBL-0209-1 |
   | Fresh control creation and reset/retarget/failed-initial allocation, now as deterministic checked V2 candidates | OBL-0209-2 |
   | Initial capture, durable validation, publication, and installation without a V1 artifact, selector, transition, dispatch, or fallback | OBL-0209-3 |
   | Candidate construction and restart path retirement under exact bounded collision handling | OBL-0209-4 |
   | Transaction-current-head handling for a prepared initial V2 candidate, by checked retirement and replacement rather than V1 advancement | OBL-0209-5 |

4. OBL-0192-10 remains live, byte-for-byte and ownership-for-ownership:
   package WP-757; proof `epoch_two_removes_legacy_vector_control_bridge`;
   claim, "Epoch 2 removes tag-65 source, decoder, table, fixture, manifest
   entry, and topology exception while permanently reserving every retired
   numeric identity." It is not folded into an ADR-0209 obligation. Every
   other ADR-0192 obligation also remains unchanged.

5. After exact acceptance, a separate governance change removes only
   OBL-0192-3 and OBL-0192-4 from ADR-0192's live front-matter `obligations`
   array and makes WP-757 cite this accepted amendment. That change preserves
   this record's exact historical entries and does not rename, retag, or reuse
   either proof. This record creates no obligation.

6. This proposal changes only this ADR and the generated ADR index. It changes
   no accepted ADR, work package, requirement, governance rule, proof tag,
   implementation, runtime behavior, durable byte, public interface, or
   external state before exact human acceptance and the separate governance
   change in Decision 5.

## Options considered

1. **Narrow retirement with a truthful successor map:** chosen. It preserves
   epoch-one evidence and leaves the V2-only obligations as the live contract.
2. **Keep both obligations live:** rejected because WP-757 would be required
   both to delete and to prove runtime V1 behavior.
3. **Rename an ADR-0209 test as an ADR-0192 proof:** rejected because the
   semantics differ and the historical evidence would become false.
4. **Supersede all of ADR-0192:** rejected because its identities, hashes,
   collision rules, remaining obligations, and OBL-0192-10 stay authoritative.

## Consequences

- WP-757 can enforce V2-only control without carrying impossible V1 proof
  requirements, while the exact WP-776 evidence remains reviewable.
- The obligation checker becomes truthful only after the separately reviewed
  post-acceptance governance change; this proposal intentionally does not make
  that live edit.
- No implementation, compatibility, storage, wire, generated artifact, or
  operator behavior changes here. Acceptance of this record does not itself
  discharge OBL-0209-1 through OBL-0209-5 or OBL-0192-10.
- Changes to epoch-two control semantics remain deferred to a separately
  accepted guarantee ADR rather than an implementation-package deviation.

## Standing design tests

- **Interface safety:** no application, agent, operator, SDK, or transport
  gains a selector, V1 fallback, advancement request, identity, layout,
  generation, fingerprint, validation mode, or bound. The V2-only public
  safety boundary remains exactly ADR-0209's.
- **Scale:** this is ledger-only. It adds no scan, read, write, collection,
  retry, diagnostic, or retention input and changes none of ADR-0192 or
  ADR-0209's bounds.

## Checks

- `scripts/adr-index --check` proves the generated index matches this proposal.
- `scripts/check-adr-obligations` continues to validate the current accepted
  obligation graph before acceptance and must validate the narrowed live graph
  after Decision 5 is applied.
- OBL-0209-1 through OBL-0209-5 retain exactly their five named WP-757 proof
  functions; OBL-0192-10 retains
  `epoch_two_removes_legacy_vector_control_bridge`.
- WP-757 acceptance must prove the V2-only obligations and tag-65 retirement;
  no passing gate may depend on either retired WP-776 proof being renamed or
  recreated.
- `scripts/check-version-topology` continues to enforce the epoch-two identity
  topology; this record grants no exception.
