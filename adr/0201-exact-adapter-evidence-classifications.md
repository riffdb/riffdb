---
adr: "0201"
title: Exact Adapter Evidence Classifications
status: accepted
tier: surface
date: 2026-09-05
accepted: 2026-09-05
requires: [ADR-0110, ADR-0117, ADR-0148]
amends: [ADR-0110, ADR-0117]
supersedes: []
requirements: [SAFE-001, DRV-014, BLK-014, BLK-064, BLK-070, DEL-012,
  OQ-016, OQ-030, OQ-100, OQ-112, WF-014, APE-001, APE-013, RAP-016, EXP-014,
  END-001, QSO-012]
packages: [WP-724, WP-782]
obligations:
  - id: OBL-0201-1
    package: WP-724
    proof: scripts/check-adapter-evidence-claims
    says: The canonical V1 inventory has exactly the ten claim records, scopes, classes, custody values, upstream-suite booleans, and source locators fixed by this record, and every SPEC, handbook, and release-gate statement agrees with its applicable record.
  - id: OBL-0201-2
    package: WP-724
    proof: adapter_evidence_normative_claim_inventory_is_complete
    says: Every named adapter claim in BLK-014, BLK-064, BLK-070, DEL-012, OQ-016, OQ-030, OQ-100, OQ-112, WF-014, APE-013, RAP-016, EXP-014, END-001, and QSO-012 is either reconciled or explicitly preserved at its exact evidence scope.
  - id: OBL-0201-3
    package: WP-724
    proof: adapter_evidence_claim_negative_self_test
    says: The guard rejects a missing, duplicate, reordered, widened, or unknown record; scope collapse; invented source locators; fixture-to-external or partial-to-complete promotion; legacy equal-adapter wording; and weakening the compiled-profile dispatcher boundary.
review_triggers:
  - Any inventory path, schema, claim ID, subject, scope, class, custody, complete-upstream-suite value, source locator, named requirement, or release-gate wording would change without exact human review of the fixture and derived text.
  - A capability-specific external receipt would be erased by a general matrix label, or a repository fixture or partial profile would be presented as a complete upstream suite.
  - Better Auth would gain a generic RiffDB mutation surface, uncompiled transaction behavior, dynamic schema, runtime fallback, or a profile-facade claim wider than generated configuration.
  - Any runtime/public/durable/protocol/storage/authorization/transaction behavior, evidence byte or result, acceptance threshold, or 72-hour execution would change.
---
# ADR-0201: Exact Adapter Evidence Classifications

## Context

ADR-0110 Amendment 1 gives OpenFGA, MLflow, Better Auth, and Woodpecker one
undifferentiated alpha-conformance label and says Better Auth runs its complete
upstream suite. The evidence is not uniform. OpenFGA has accepted live upstream-
suite evidence; RiffDB's general MLflow and Woodpecker matrices are authored
fixtures; and Better Auth is a generated compiled profile without a complete
upstream-suite receipt. But completed WP-630, WP-648/WP-655, WP-738, WP-743,
and WP-744 also retain narrower scoped profile, external-live, and RiffDB-live
evidence. A global “fixture” label would make those truthful claims false.

ADR-0117 remains authoritative. RiffDB exposes no generic CRUD or transaction
surface. An external adapter may implement its framework's generic facade only
as a profile-bound dispatcher to exact generated commands and named queries.
Selected fields, plugins, schemas, and transaction shapes are resolved when the
profile is generated/configured; unsupported configuration fails there. An
unknown runtime call refuses rather than falling back or synthesizing behavior.

## Decision

1. WP-724 creates exactly `fixtures/adapters/adapter-evidence-v1.json`, with
   schema identity `riffdb.adapter-evidence-inventory/v1`. Its root has exactly
   `schema`,`claims`. Each claim has exactly `claim_id`,`subject`,`scope`,
   `evidence_class`,`custody`,`complete_upstream_suite`,`source_claims`.
   Claims are ordered by unique ASCII `claim_id`; source locators are ordered,
   unique nonempty strings. No defaults, aliases, extra members, or inference
   from fixture names are permitted.

2. The four `wp578_wp579_general_alpha` records are exact:
   - `alpha.better_auth.general`: subject `better_auth`, class
     `materialized_profile_matrix`, custody `mixed`,
     `complete_upstream_suite: false`; sources
     `release/evidence/alpha/conformance-better-auth-v1.json` and
     `work_packages.yaml#WP-630.closure`.
   - `alpha.mlflow.general`: subject `mlflow`, class
     `language_expressiveness_matrix`, custody `riffdb`,
     `complete_upstream_suite: false`;
     source `release/evidence/alpha/conformance-mlflow-v1.json`.
   - `alpha.openfga.general`: subject `openfga`, class
     `upstream_suite_adapter`, custody `external`,
     `complete_upstream_suite: true`; sources
     `adr/0169-optional-aggregate-root-materialization.md#testing` and
     `release/evidence/alpha/conformance-openfga-v1.json`.
   - `alpha.woodpecker.general`: subject `woodpecker`, class
     `language_expressiveness_matrix`, custody `riffdb`,
     `complete_upstream_suite: false`; source
     `release/evidence/alpha/conformance-woodpecker-v1.json`.

3. Six separately scoped records preserve narrower evidence:
   - `partial.better_auth.admin`: subject `better_auth`, scope `OQ-030`, class
     `external_materialized_profile`, custody `external`,
     `complete_upstream_suite: false`; sources
     `work_packages.yaml#WP-648.closure` and `work_packages.yaml#WP-655.closure`.
   - `partial.better_auth.lifecycle`: subject `better_auth`, scope
     `DEL-012,QSO-012`, class `materialized_profile_matrix`, custody `riffdb`,
     `complete_upstream_suite: false`; source
     `work_packages.yaml#WP-630.closure`.
   - `partial.mlflow.filtered_search`: subject `mlflow`, scope `OQ-100`, class
     `external_partial_profile_live`, custody `external`,
     `complete_upstream_suite: false`; source
     `release/evidence/long-pattern-readiness-mlflow-loopback-v1.json`.
   - `partial.mlflow.large_run`: subject `mlflow`, scope `BLK-064`, class
     `riffdb_shape_live_loopback`, custody `riffdb`,
     `complete_upstream_suite: false`; source
     `release/evidence/large-atomic-command-envelope-v1.json#/mlflow_loopback`.
   - `partial.mlflow.partition_search`: subject `mlflow`, scope `OQ-112`, class
     `external_partial_profile_live`, custody `external`,
     `complete_upstream_suite: false`; sources
     `release/evidence/partition-set-exact-prefix-repro-v1.json` and
     `release/evidence/partition-set-mlflow-loopback-v1.json`.
   - `post_alpha.payload.shape`: subject `payload`, scope `BLK-014,RAP-016`, class
     `post_alpha_language_expressiveness`, custody `riffdb`,
     `complete_upstream_suite: false`; source
     `work_packages.yaml#WP-569.closure`.

4. These records classify claims, not products. No general-alpha record may
   erase a partial record, and no partial record proves complete upstream-suite
   compatibility. Only `alpha.openfga.general` has
   `complete_upstream_suite: true`. Better Auth fields/plugins require a newly
   generated and reviewed materialized profile; ADR-0117 still requires an
   owning-repository upstream suite before a complete framework-support claim.

5. `BLK-014`, `OQ-016`, `WF-014`, and `RAP-016` remain language/domain-shape
   corpora; `BLK-064`, `DEL-012`, `OQ-030`, `OQ-100`, `OQ-112`, and `QSO-012` retain their
   exact bounded capability requirements at the evidence strengths above;
   `BLK-070` retains its generated/redb execution rule. `APE-001` and `DRV-014`
   remain unchanged. `APE-013`,
   `EXP-014`, `END-001`, and the WP-578/WP-579 general matrix are reconciled to
   the four alpha records without weakening any drill or promoting its claim.

6. WP-578 remains the separate 72-hour evidence owner and depends on completed
   WP-782 and WP-724 classification work. WP-579 remains downstream of WP-578
   and WP-724. Both require this ADR; neither may use classification metadata as
   execution evidence or as a substitute for any installation, export, restore,
   conformance, or endurance observation.

7. Human review covers the complete V1 fixture diff, every source locator and
   referenced evidence-class claim, and every changed SPEC, handbook, and gate
   sentence. This changes claim custody only: runtime, public and durable
   formats, protocols, storage, authorization, transactions, evidence bytes and
   results, thresholds, and retained receipts remain exact.

## Standing design tests

- **Interface safety:** classification adds no callable surface. ADR-0117's
  generated profile is the only map from a framework facade to safe operations.
- **Scale:** ten closed records and bounded repository text are checked; no
  database, population, or duration-sized state is read.

## Checks

- The three WP-724 obligations freeze exact records, normative coverage,
  negative promotion/refusal cases, and human-reviewed fixture/text parity.
- Governance and allowed-path checks prove WP-782 changes authority only.
