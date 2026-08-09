# Deployable Application Alpha Architecture Freeze

This page records WP-550's accepted interface and dependency boundary. It is a
design freeze, not a claim that the alpha features are implemented. The
individual work packages and the final WP-579 evidence gate remain mandatory.

## Acceptance and dependency baseline

ADR-0105 through ADR-0112 were accepted exactly by the human maintainer on
2026-08-09. Their implementation baseline contains:

| Dependency | Merged revision | Evidence represented |
|---|---|---|
| WP-398 | `59c51a2` | Rust-owned Python driver evaluation |
| WP-414 | `aec343f` | Reactive architecture/format ownership |
| WP-484 | `10edda9` | Proven command-segment framing |
| WP-491 / RE1 | `c48802b` | Changelog emitter merge |
| WP-492 | `078802c` | Projected aggregate/group-by carriage merge |
| WP-496 | `ea3f8ac` | Public website/release-facing surface |

RE2 `ShipChangelog` is not present in this baseline. WP-553 therefore starts
from the RE1 inventory above. If RE2 merges before WP-553 changes transport or
Protobuf inventory, WP-553 MUST record and rebase on the merged RE2 revision
first. TLS wraps replication transport; it cannot reinterpret its framing,
authorization, ordering, resume, or backpressure semantics.

## Format and semantic owners

| Artifact or interface | Sole first-party owner | Compatibility evidence |
|---|---|---|
| Listener profiles, endpoint identity, certificate/trust snapshots | `riffdb-config`, `riffdb-server`, `riffdb-client-rust` | Versioned config fixtures and hostile remote-ingress corpus |
| Driver-host protocol and handshake | `riffdb-driver-host` | Old/current golden frames, schema/identity neuters, fuzz corpus |
| Generated Rust API | `riffdb-query-module` plus `riffdb-client-rust` facade | Application-manifest and generated-binding golden corpus |
| Generated Go API | `riffdb-query-module`; transport semantics in `riffdb-driver-host` | Cross-language application and driver conformance corpus |
| Generated TypeScript API | `riffdb-query-module`; transport semantics in `riffdb-driver-host` | Cross-language application and driver conformance corpus |
| Generated Python API and wheel ABI | `riffdb-query-module` plus `riffdb-client-python-native` | abi3/platform matrix and shared semantic corpus |
| Bulk grammar, list bounds, delete policy | Contract syntax/IR/compiler crates | Old/current syntax/IR/plan/hash fixtures and negative corpus |
| Bulk durable command graph | Runtime, commit coordinator, storage API/redb | Memory/redb conformance and crash/recovery matrix |
| Operational RiffQL grammar, plan families, cursors | Query syntax/IR/compiler/module crates | Receipted repository-wide identity rotation and cursor neuters |
| Unicode text-key profile | Query compiler/executor with checked versioned tables | Full transform golden corpus and index migration fixtures |
| Workflow plans, service values, fencing tokens | Contract compiler, runtime, service, commit coordinator | IR/plan/protocol fixtures and deterministic race/crash schedules |
| Installation plan, campaign, and receipt | `riffdb-application` and shared application service | Old/current formats and interruption-at-every-stage corpus |
| Adapter conformance manifest | `riffdb-application` and adapter-owned public tests | Schema, feature catalog, signed/content digest, observations |
| Row-policy grammar, facts, and plan | Contract/query compiler, `riffdb-auth`, `riffdb-policy` | Compiler fixtures, pure evaluator, cross-surface differential corpus |
| Durable-format manifest | Storage API/redb plus release verification | Complete release-pair fixture matrix and refusal-before-mutation tests |
| Symbolic export manifest and receipt | Shared service and operator client/CLI | Canonical JSONL goldens, kill/resume, export/reimport reconciliation |

No generated target-language package may become a second semantic owner to
avoid coordinating one of these formats.

## Critical dependency review

### TLS and cryptography

The implementation recommendation is Tonic `0.14.6` with only its `tls-ring`
feature, yielding one Rustls/Tokio-Rustls stack and the Ring provider. Native
root discovery, OpenSSL/native-tls, AWS-LC, compression, and alternate providers
remain forbidden. TLS stays default-disabled for `loopback_cleartext`.

Ring includes native C/assembly and internal unsafe code. AGENTS.md therefore
requires a separate explicit human dependency approval before WP-553 adds it to
the workspace. The replacement architecture test MUST pin the complete resolved
closure and fail if another cryptographic provider appears.

### Unicode text normalization

The operational text-key profile requires Unicode 17.0.0 normalization and full
non-Turkic case folding in a fixed order selected by WP-563. The reviewed
candidate is `unicode-normalization` `0.1.25` plus a first-party checked casefold
table generated from Unicode 17.0.0 data. `unicode-casefold` `0.2.0` is rejected:
its bundled table is Unicode 9.0.0 and cannot share the required versioned
profile. Runtime locale and platform collation remain forbidden.

`unicode-normalization` contains narrowly allowed internal unsafe code despite
being pure Rust at the packaging boundary. It also requires explicit human
dependency approval before WP-563 adds it. Its version, table version, transform
order, input/output byte ceilings, and exhaustive golden corpus become part of
the index identity and migration boundary.

### Language packaging

Go and TypeScript add no FFI or target-language remote TLS dependency: they use
the closed local protocol to the first-party Rust driver host. Python retains
the already accepted PyO3 native boundary and may only expand its exact abi3
wheel/platform matrix. A C ABI, cgo, N-API, pure-language remote transport,
runtime package download, or per-call subprocess requires another accepted ADR.

## Negative-capability matrix

| Unsafe pattern | Construction that makes it unavailable |
|---|---|
| Non-loopback cleartext or trust-all TLS | Closed listener enum and pre-bind endpoint validation |
| Proxy-asserted principal/tenant/database | Bearer authentication and current shared-service authorization on every operation |
| Target-language transport/retry fork | Generated bindings can call only the closed local Rust driver protocol |
| Generic transaction or storage batch | Only compiler-sealed ordinary or bounded bulk command plans enter runtime |
| Caller-submitted query/policy AST | Runtime accepts typed values selecting a finite compiled plan/policy family |
| Unindexed or cross-partition ACL | Row-policy compiler requires one route and bounded declared relationship index |
| Policy after limit/rank/aggregate | Shared executor enforces policy before candidate shaping and protected release |
| Unchecked workflow update or stale lease holder | Generated revision/fence parameters plus transaction-current revalidation |
| Caller clock or random workflow identity | Service observations are sealed into deterministic transaction context |
| Installer bypass or silent role widening | Content-addressed campaign, explicit authority diff, and receipted stages |
| Adapter server hook | Conformance manifest is bounded data and cannot name executable behavior |
| Command-time delete invisible to follower | Delete compilation is gated on WP-559 changelog tombstones and validation proof |
| Unsupported data opened or reset | Format manifest comparison refuses before any mutation |
| Raw export/import escape | Symbolic paged export and compiled command/migration reimport only |
| Paper-only disaster recovery | Every adapter destroys its volume, restores remotely, and reruns conformance |
| Short or incomplete soak passed as evidence | Release requires one valid retained 72-hour lifecycle receipt |

## Deferred remote abuse controls

Per-principal request-rate limits and per-tenant storage/work quotas are an
explicit alpha deferral. Global admission, connection, stream, query, command,
and storage bounds remain required, and alpha deployment is restricted to
controlled design-partner networks. The deferral must be resolved before an
untrusted shared multi-tenant service claim.
