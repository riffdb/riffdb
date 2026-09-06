# Adapter evidence classes

RiffDB classifies each adapter-related claim at the strength of its retained
evidence. The canonical machine-readable inventory is
`fixtures/adapters/adapter-evidence-v1.json`; the records below are a handbook
view of that closed ADR-0201 inventory. Classification metadata is not execution
evidence and cannot replace an installation, conformance, export, restore, or
endurance observation.

Only `alpha.openfga.general` has `complete_upstream_suite: true`. General-alpha
and capability-specific records remain separate: a general matrix cannot erase
a narrower external or live receipt, and a partial receipt cannot establish
complete framework compatibility.

## General-alpha records

| Claim | Subject | Scope | Evidence class | Custody | Complete upstream suite | Sources |
|---|---|---|---|---|---|---|
| `alpha.better_auth.general` | Better Auth | `wp578_wp579_general_alpha` | `materialized_profile_matrix` | `mixed` | false | `release/evidence/alpha/conformance-better-auth-v1.json`; `work_packages.yaml#WP-630.closure` |
| `alpha.mlflow.general` | MLflow | `wp578_wp579_general_alpha` | `language_expressiveness_matrix` | `riffdb` | false | `release/evidence/alpha/conformance-mlflow-v1.json` |
| `alpha.openfga.general` | OpenFGA | `wp578_wp579_general_alpha` | `upstream_suite_adapter` | `external` | true | `adr/0169-optional-aggregate-root-materialization.md#testing`; `release/evidence/alpha/conformance-openfga-v1.json` |
| `alpha.woodpecker.general` | Woodpecker | `wp578_wp579_general_alpha` | `language_expressiveness_matrix` | `riffdb` | false | `release/evidence/alpha/conformance-woodpecker-v1.json` |

The MLflow and Woodpecker rows prove that RiffDB's bounded symbolic interfaces
can express and preserve their declared data and workflow shapes. They do not
show an upstream MLflow or Woodpecker release operating against RiffDB. Better
Auth's row covers the exact generated materialized-profile matrix, not the
framework's complete upstream suite.

## Capability-specific and post-alpha records

| Claim | Subject | Scope | Evidence class | Custody | Complete upstream suite | Sources |
|---|---|---|---|---|---|---|
| `partial.better_auth.admin` | Better Auth | `OQ-030` | `external_materialized_profile` | `external` | false | `work_packages.yaml#WP-648.closure`; `work_packages.yaml#WP-655.closure` |
| `partial.better_auth.lifecycle` | Better Auth | `DEL-012,QSO-012` | `materialized_profile_matrix` | `riffdb` | false | `work_packages.yaml#WP-630.closure` |
| `partial.mlflow.filtered_search` | MLflow | `OQ-100` | `external_partial_profile_live` | `external` | false | `release/evidence/long-pattern-readiness-mlflow-loopback-v1.json` |
| `partial.mlflow.large_run` | MLflow | `BLK-064` | `riffdb_shape_live_loopback` | `riffdb` | false | `release/evidence/large-atomic-command-envelope-v1.json#/mlflow_loopback` |
| `partial.mlflow.partition_search` | MLflow | `OQ-112` | `external_partial_profile_live` | `external` | false | `release/evidence/partition-set-exact-prefix-repro-v1.json`; `release/evidence/partition-set-mlflow-loopback-v1.json` |
| `post_alpha.payload.shape` | Payload | `BLK-014,RAP-016` | `post_alpha_language_expressiveness` | `riffdb` | false | `work_packages.yaml#WP-569.closure` |

These narrower records keep their own scope and custody. In particular, the
external MLflow search receipts are not collapsed into the RiffDB-authored
general matrix, the 100-tag MLflow Run remains a RiffDB-custodied live
loopback, and Payload remains a post-alpha language-expressiveness shape.

## Better Auth support boundary

An external Better Auth integration may present the framework's generic facade
only as a profile-bound dispatcher to the exact generated commands and named
queries selected for its materialized profile. Models, fields, plugins,
schemas, and atomic transaction shapes are fixed while that profile is
generated and configured. Additional fields or plugins require a newly
generated and reviewed profile plus its generated operations and conformance
evidence. Unsupported configuration and unknown calls refuse; there is no
generic RiffDB CRUD, dynamic schema, transaction callback, or runtime fallback.

## Repository check

Run the bounded offline guard after changing any classification or release-gate
wording:

```bash
./scripts/check-adapter-evidence-claims
./scripts/check-adapter-evidence-claims --self-test
```

The first command checks the exact ten records, source locators, normative SPEC
claims, handbook language, and gate wiring. The self-test proves that missing,
duplicate, reordered, widened, unknown, collapsed, or promoted claims and the
two unsafe legacy wordings are rejected.

## Change note

Package: WP-724

Tier: surface

Behavior added or changed: the release gate and handbook now report each of the
ten ADR-0201 claims at its exact scope, class, custody, and upstream-suite
strength; the checked JSON inventory is the repository source of truth.

Checks run: package acceptance, the exact and adversarial claim guards, the
four-subject/four-language adapter conformance gate, and local sibling compile
checks against `riffdb-openfga`, `riffdb-better-auth`, and `riffdb-mlflow` with
`./scripts/downstream-adapter-check --repo-root /home/kevin/dev`. Those local
workstation checks are not pinned CI evidence.

Compatibility: classification and wording only. Runtime behavior, public and
durable formats, protocols, storage, authorization, transactions, evidence
bytes and results, thresholds, and retained receipts are unchanged.

Hazards and follow-ups: any changed record, locator, requirement wording, or
release claim requires human review. WP-578 still owns the separate 72-hour
execution and classification metadata cannot satisfy it. WP-723 remains open:
immutable adapter revision locks and required CI enforcement are its separate
unresolved concerns.
