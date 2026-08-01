# Contract Migration Fixture Manifest

This directory is the compatibility-fixture root for P7. ADR-0076 through
ADR-0079 are Accepted, but no fixture represents an implemented product
interface until its owning work package merges it.

## Interface Owners

| Boundary | Owner | First producer | First consumer |
|---|---|---|---|
| `.riffm` syntax, spans, formatter | `riffdb-contract-syntax` | WP-406 | `riffdb-contract-compiler` |
| Migration HIR, checks, lowering | `riffdb-contract-compiler` | WP-406 | `riffdb-contract-ir`, `riffdb-catalog` |
| `MigrationBundleV1`, compatibility codes, canonical hash | `riffdb-contract-ir` | WP-406 | `riffdb-catalog`, application lock |
| Application source V3 and lock V4 | `riffdb-query-module` | WP-406 | CLI generation/deployment |
| Sealed validated migration plan | `riffdb-catalog` | WP-407 | `riffdb-commit` |
| Row transform and authoritative batch/cutover authority | `riffdb-commit` | WP-407 | memory/redb sealed ports |
| Journal/record/archive value semantics | `riffdb-storage-api` | WP-407 | memory/redb backends |
| Durable codec, table keys, receipt, stage/publication | `riffdb-storage-redb` | WP-408 | server recovery controller |
| Authentication handoff | `riffdb-auth` | WP-409 | gRPC/service composition |
| `MigrateContract` permission and proof | `riffdb-policy` | WP-409 | `riffdb-service` |
| API-neutral request/result and orchestration | `riffdb-service` | WP-409 | gRPC adapter |
| Public wire mapping | `riffdb-api-grpc` / `riffdb-proto` | WP-409 | Rust client and CLI |
| CLI grammar and local plan | `riffdb-cli` | WP-406/WP-409 | installed acceptance |
| Projection candidate rebuild | `riffdb-projection` | WP-407 | staged migration driver |

Storage and server code must not construct migration expressions, validated
plans, row transforms, or authoritative batches. Transports must not compile or
reinterpret them. MCP and application-driver packages receive negative fixtures
only.

## Frozen Fixture Groups

WP-406 creates:

- `source/valid/` and `source/invalid/` span fixtures;
- `bundle/v1/` canonical bytes, hashes, malformed encodings, and step variants;
- `compatibility/v1/` parent/candidate/proof reports for every change code; and
- `application/` source V3, lock V4, direct-parent, and prior-format fixtures.

WP-407 creates `model/gate-a/` generated boundary histories and pure expected
states. `scripts/generate-migration-model-fixtures` owns that subtree.
WP-408 creates `durable/v1/` receipt, journal, permanent record, archive, table-
key, registry, and impossible-pair fixtures. The redb-owned generator is
`scripts/generate-migration-durable-fixtures`; the process recovery matrix lives
under `tests/recovery/`. WP-409 creates `public/v1/`
descriptor, wire, SDK, CLI JSONL, authorization, redaction, and negative MCP
fixtures. WP-411 and WP-412 add Gate-B and Gate-C cases without changing an
existing V1 tag or fixture meaning. WP-413 freezes the installed upgrade matrix.

## Change-Class Gates

| Gate | Enabled classes |
|---|---|
| A / WP-410 | Required-field backfill; index, relationship, unique rule, entity invariant, and new projection on existing schema |
| B / WP-411 | Semantic rename; logical retirement; fresh-ID field/type replacement; exhaustive enum mapping; replacement projection |
| C / WP-412 | Primary-key replacement; rekey; repartition; aggregate membership; relationship target; conflict domain |

Unimplemented but decoded V1 step tags fail closed. Committed events, outcomes,
provenance, idempotency rows, commit records, historical bundles, and application
sequences never have rewrite fixtures because rewriting them is prohibited.
