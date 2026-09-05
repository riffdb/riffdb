---
adr: "0201"
title: Exact Adapter Evidence Classifications
status: proposed
tier: surface
date: 2026-09-05
accepted: null
requires: [ADR-0110, ADR-0117, ADR-0148]
amends: [ADR-0110, ADR-0117]
supersedes: []
requirements: [SAFE-001, DRV-014, APE-001, APE-013, EXP-014, END-001]
packages: [WP-724, WP-782]
obligations:
  - id: OBL-0201-1
    package: WP-724
    proof: scripts/check-adapter-evidence-claims
    says: One canonical inventory and every SPEC, handbook, and release-gate claim agree that only OpenFGA has upstream-suite evidence, Better Auth has exact materialized-profile integration evidence, and MLflow and Woodpecker have only RiffDB-owned language-expressiveness fixture evidence.
  - id: OBL-0201-2
    package: WP-724
    proof: adapter_evidence_claim_negative_self_test
    says: The claim guard rejects missing, duplicate, or widened classifications, any MLflow or Woodpecker upstream promotion, the legacy four-adapter wording, and any Better Auth claim wider than its materialized profile.
review_triggers:
  - Any named evidence class, owner, upstream-suite flag, source claim, adapter set, or release-gate wording would change without exact human review of the inventory and every derived claim.
  - Better Auth would gain dynamic schema, plugin, CRUD, transaction, or runtime fallback behavior, or be described beyond an exactly generated materialized profile.
  - A language-expressiveness fixture would be presented as upstream framework conformance, or repository-local evidence would be presented as an external execution receipt.
  - Any public interface, runtime behavior, durable byte, protocol, storage key, authorization, transaction, evidence artifact, acceptance threshold, or 72-hour evidence result would change.
---
# ADR-0201: Exact Adapter Evidence Classifications

## Context

ADR-0110 Amendment 1 says the alpha conformance set is OpenFGA, MLflow,
Better Auth, and Woodpecker and says Better Auth uses its published upstream
suite. The current evidence does not support those equal claims: OpenFGA has
an upstream-suite adapter, MLflow and Woodpecker are repository-authored domain
fixtures, and Better Auth proves an exact generated profile boundary but no
current upstream-suite execution. WP-724 cannot resolve this accepted-authority
conflict because its paths exclude `adr/**`.

ADR-0117's safety boundary remains right: an external framework-support release
must live in its owning repository, use a compiled profile, fail unsupported
configuration at generation, and run the framework's suite where one exists.
That prerequisite must not be mistaken for evidence the current alpha gate has
already banked. ADR-0148's driver-core equivalence proves no adapter class.

## Decision

1. The canonical inventory has exactly four entries. OpenFGA is
   `upstream_suite_adapter`, externally owned, with `upstream_suite: true`.
   Better Auth is `materialized_profile_integration`, externally owned, with
   `upstream_suite: false`. MLflow and Woodpecker are each
   `language_expressiveness_fixture`, RiffDB-owned, with
   `upstream_suite: false`. Payload remains a named post-alpha RiffDB-owned
   language-expressiveness fixture and is not a fifth alpha entry.

2. Only OpenFGA may satisfy a named upstream-suite release claim. MLflow and
   Woodpecker installation/evolution, generated clients, export/reimport,
   restore, and endurance observations prove only that exact RiffDB-authored
   domain shape. They do not prove upstream framework compatibility or complete
   adapter conformance.

3. Better Auth evidence covers only the exactly generated materialized profile:
   its selected configuration, fields, plugins, schema, named compiled
   commands, queries, roles, and policies. Additional fields or plugins require
   generation and review of a new profile. Unsupported configurations fail at
   generation or installation; no runtime field omission, generic CRUD,
   sequential transaction emulation, or fallback is allowed. A future Better
   Auth support claim still requires ADR-0117's owning-repository upstream-suite
   evidence; current alpha text must not imply that receipt exists.

4. `APE-013`, `EXP-014`, and `END-001` retain their installation, evolution,
   export/reimport, and restore semantics, but “adapter” in those release claims
   is qualified by this inventory. Each entry must complete its required
   RiffDB public-surface drills; only its declared evidence class determines the
   compatibility claim those drills support. WP-578 remains the separate owner
   of the actual 72-hour receipt and gains no substitute or early closure.

5. WP-782 performs governance reconciliation only after exact acceptance: it
   aligns ADR-0110/ADR-0117, the named SPEC requirements and milestone, and the
   WP-724/WP-578/WP-579 package claims. WP-724 then owns the canonical inventory,
   handbook/release wording, requirement-tagged positive guard, and adversarial
   negative self-test named by `OBL-0201-1` and `OBL-0201-2`.

6. Human fixture review covers every one of the four inventory entries and its
   exact class, owner, `upstream_suite` value, and source claim; every changed
   SPEC, handbook, and release-gate sentence; and every future classification
   change. A green local fixture cannot silently promote its evidence class.

7. This decision changes evidence labels and claim custody only. Runtime and
   public behavior, durable formats, protocols, storage, authorization,
   transaction semantics, fixtures' evidence bytes, acceptance thresholds, and
   retained evidence results are unchanged.

## Standing design tests

- **Interface safety:** evidence metadata adds no application or operator
  surface and cannot enable generic writes, callbacks, runtime fallback, or a
  caller-selected guarantee. Better Auth remains fail-closed at generation.
- **Scale:** the inventory has four bounded alpha entries and claim validation
  scans bounded repository text; it reads no database or duration-sized state.

## Checks

- `scripts/check-adapter-evidence-claims` proves exact inventory/source parity
  and every normative and handbook claim after WP-724 implements it.
- `adapter_evidence_claim_negative_self_test` proves every widening and legacy-
  wording negative in `OBL-0201-2`; governance and allowed-path checks prove
  WP-782 changes only authority text.
