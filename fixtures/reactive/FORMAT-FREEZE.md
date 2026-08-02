# P8 Reactive Format Freeze

This directory is the compatibility-fixture root for P8. ADR-0080 is Accepted.
A path listed here reserves an evidence boundary; it does not represent an
implemented product interface until its owning work package merges generated
bytes and tests for that path.

The event partition, route-index, and historical materialization foundations
already in the repository are WP-415 inputs. They landed after ADR-0080 was
accepted and do not make durable consumers, live queries, or contextual
subscriptions available.

## Compatibility Anchors

The following existing meanings are inputs, not extension points:

- `StoredDurableEventV1`, `EventId`, canonical event payload bytes, and event
  hashes remain the only authoritative event facts.
- Commit sequences, event ordinals, `StoredCommitRecordV*`, outcomes,
  idempotency records, and historical contract bundles are not rewritten.
- Existing projection group/generation/frontier and outbox intent/status
  records retain their meanings and continue to accept unpartitioned events.
- The kernel commit-subscription protocol remains administrative. It is not an
  application event protocol and gains no reactive compatibility promises.
- Query Module V1, Application Source V1 through V3, and Application Lock V1
  through V4 retain their exact prior bytes and identities.
- `StoredHistoryIncarnationV1` remains the restore fence for every new cursor,
  lease, checkpoint, and causation token.
- Database aliases and credentials retain ADR-0063 scoping. No reactive token,
  cursor, lease, or consumer name can select or authorize another database.
- Existing MCP underscore names, catalog filtering, shared-service routing,
  and payload redaction remain mandatory.

The existing `StoredEventRouteV1` is a derived integrity index keyed by the
canonical partition hash and `EventId`. It contains only event identity/hash
evidence, never a second payload. WP-415 owns its compatibility fixtures and
must either prove its current schema satisfies ADR-0080 or stop for human
review before changing it.

## Interface Owners

| Boundary | Semantic owner | First producer | First consumer | Stability before consumer starts |
|---|---|---|---|---|
| Event `partition_by` syntax and spans | `riffdb-contract-syntax` | WP-415 | `riffdb-contract-compiler` | Grammar shape, bounds, and diagnostics frozen |
| Event partition schema and emit-equivalence proof | `riffdb-contract-ir` / `riffdb-contract-compiler` | WP-415 | catalog and application compiler | Canonical encoding/hash and compatibility rules frozen |
| Event route value, physical key, scan fence, and backfill evidence | `riffdb-storage-api` / `riffdb-storage-redb` | WP-415 | catalog replay | Durable V1 bytes, key order, registry entry, and recovery rules frozen |
| Historical event materialization | `riffdb-catalog` | WP-415 | event service and contextual hydration | Opaque catalog result and fail-closed proof rules stable |
| Safe symbolic event envelope and replay service | `riffdb-service` | WP-415 | gRPC, CLI, SDK, MCP | API-neutral variants, budgets, redaction, and cursor identity stable |
| Stream/subscription syntax and reactive typed IR | `riffdb-query-syntax` / `riffdb-query-ir` | WP-416 | query module and policy | Grammar, closed tags, bounds, canonical codec, and hash stable |
| Reactive module identity | `riffdb-query-module` | WP-416 | policy, consumers, generated clients | Canonical module/operation/parameter identity stable |
| Application Source V4 and Lock V5 | `riffdb-query-module` | WP-416 | CLI generation/deployment | Prior formats readable; exact successor identity and migration frozen |
| Stream/watch/subscription permissions | `riffdb-types` / `riffdb-policy` | WP-416 | all reactive service operations | Exact symbolic permission facts and deny behavior stable |
| Consumer identity and operational records | `riffdb-storage-api` | WP-417 | memory/redb and consumer coordinator | Versioned keys/records, bounds, reciprocity, and incarnation fences frozen |
| Consumer transition authority | `riffdb-service` consumer coordinator | WP-417 | public adapters | Atomic transition model and cancellation/recovery contract stable |
| Consumer public protocol | `riffdb-service` / `riffdb-proto` | WP-417 | gRPC, Rust SDK, CLI, MCP | API-neutral model precedes transport schemas; all variants and errors closed |
| Live invalidation and patchability plan | `riffdb-query-ir` / `riffdb-query-executor` | WP-418 | live service | Plan identity, dependency set, public-key proof, and cost bounds stable |
| Live cursor, update union, and watch service | `riffdb-service` / `riffdb-proto` | WP-418 | generated clients and MCP | Cursor binding and Snapshot/Patch/Reset/Checkpoint/Terminal semantics stable |
| Generated reactive bindings and relay surface | `riffdb-query-module` | WP-419 | Rust, TypeScript, Python, CLI, MCP, relay templates | One generated schema inventory and cross-language observations frozen |
| Contextual plan and work-item service | `riffdb-query-ir` / `riffdb-service` | WP-420 | generated reaction helpers | Shared-snapshot hydration, limits, and authorized-command filtering stable |
| Causation provenance successor | `riffdb-storage-api` / `riffdb-commit` | WP-420 | contextual service and recovery | Additive record, atomic commit ownership, and old provenance readability frozen |
| Lease-bound causation token | `riffdb-service` | WP-420 | generated reaction helpers | Opaque self-contained binding, expiry, replay resolution, and redaction stable |

Storage and server code must not compile reactive source, materialize event
payloads, construct policy decisions, or assign application commit sequences
for consumer transitions. Transports and generated clients must not reinterpret
IR, authorize from cursor possession, or bypass the API-neutral service.

## Requirement Accountability

"Implementation owner" is the package accountable for making the normative
requirement first complete. Other packages may provide a prerequisite or an
adapter, but must not define a competing meaning. WP-421 supplies final P8
evidence for every requirement.

| Requirement | Implementation owner | Earliest frozen evidence | Final evidence |
|---|---|---|---|
| EVT-001 | WP-415 | authoritative-event identity and payload/hash compatibility vectors | WP-421 immutable-history acceptance |
| EVT-002 | WP-415 | partition grammar, IR, and emit-equivalence diagnostics | WP-421 TicketDesk contract fixture |
| EVT-003 | WP-415 | optional-field evolution and new-event-type compatibility cases | WP-421 evolution acceptance |
| EVT-004 | WP-415 | route key/value goldens, backfill, corruption, and recovery matrix | WP-421 restart/restore acceptance |
| EVT-005 | WP-416 | stream IR, one-partition proof, predicate-bound negatives | WP-421 generated application acceptance |
| EVT-006 | WP-415 | safe-envelope schema and forbidden-field/redaction vectors | WP-421 disclosure audit |
| EVT-007 | WP-415 | shared-service authorization and transport-conformance matrix | WP-421 installed surface parity |
| EVT-008 | WP-415 | explicit negative inventory for raw CDC/global order/retention | WP-421 limitation review |
| CON-001 | WP-417 | consumer identity/hash/key vectors and definition-drift cases | WP-421 evolution/restart acceptance |
| CON-002 | WP-417 | ordered duplicate-delivery histories | WP-421 kill/reaction acceptance |
| CON-003 | WP-417 | contiguous checkpoint, bounded sparse-ack, and lease schedules | WP-421 recovery acceptance |
| CON-004 | WP-417 | atomic transition model and no-application-sequence assertions | WP-421 operational audit |
| CON-005 | WP-417 | item/byte/wait/lease/in-flight/retry/delay boundary vectors | WP-421 saturation acceptance |
| CON-006 | WP-417 | startup reciprocity, expiry, corruption, and incarnation tests | WP-421 restore/restart acceptance |
| CON-007 | WP-417 | operation-by-operation authorization and stale-token negatives | WP-421 revocation acceptance |
| CON-008 | WP-417 | API-neutral/public-adapter parity and payload-free wakeup tests | WP-421 generated surface acceptance |
| LIVE-001 | WP-418 | barrier-controlled snapshot/catch-up interleavings | WP-421 two-browser race acceptance |
| LIVE-002 | WP-418 | compiler dependency plans and conservative invalidation oracle | WP-421 fresh-query equivalence |
| LIVE-003 | WP-418 | closed update union and keyed-patch/reset vectors | WP-421 client-state equivalence |
| LIVE-004 | WP-418 | cursor identity, drift, restore, expiry, and mismatch vectors | WP-421 reconnect/evolution acceptance |
| LIVE-005 | WP-418 | reauthorization schedules and protected-state clearing tests | WP-421 revocation acceptance |
| LIVE-006 | WP-418 | watch-count/buffer/lifetime limits and coalescing oracle | WP-421 saturation/convergence acceptance |
| LIVE-007 | WP-418 | outcome/diff/pressure/module-change reset and terminal vectors | WP-421 failure-shaping acceptance |
| LIVE-008 | WP-419 | Rust/TypeScript/Python/CLI/MCP observations and relay boundary lint | WP-421 installed client parity |
| CTX-001 | WP-416 | subscription IR, field-unification, locality, and bound diagnostics | WP-421 TicketDesk subscription fixture |
| CTX-002 | WP-420 | shared-snapshot frontier and fresh-redelivery context histories | WP-421 contextual consistency acceptance |
| CTX-003 | WP-420 | closed work-item schema and authorized-command filtering matrix | WP-421 agent work acceptance |
| CTX-004 | WP-420 | deterministic reaction-idempotency vectors and unsupported shapes | WP-421 generated reaction acceptance |
| CTX-005 | WP-420 | causation-token validation and expired prior-outcome schedules | WP-421 kill/retry acceptance |
| CTX-006 | WP-420 | provenance successor bytes and atomic causation/correlation assertions | WP-421 provenance audit |
| CTX-007 | WP-420 | commit-before-ack crash schedules and reference-model comparison | WP-421 process kill matrix |
| CTX-008 | WP-420 | negative boundary inventory and payload-free MCP notification tests | WP-421 architecture audit |

## Frozen Fixture Groups

Fixture contents are generated by production registries where applicable.
Handwritten expected bytes are prohibited.

WP-415 owns:

- `contract/event-partition/` source, spans, compatibility, canonical IR, and
  hash fixtures;
- `durable/event-route-v1/` route value/key/registry/backfill/corruption
  fixtures, cross-referenced to the existing `fixtures/proto/` durable vectors;
- `catalog/event-replay-v1/` historical normalization, writer-plan resolution,
  missing evidence, and page-order observations; and
- `public/event-replay-v1/` API-neutral and wire envelopes, cursors, bounds,
  authorization, redaction, database isolation, and transport observations.

WP-416 owns:

- `module/reactive-v1/` canonical source, typed IR, plan identity, closed tags,
  hashes, diagnostics, and decoder negatives;
- `application/source-v4/` and `application/lock-v5/` exact successor and
  prior-format compatibility fixtures; and
- `policy/reactive-role-v1/` symbolic permission derivation, least-authority,
  and forbidden-field cases.

WP-417 owns:

- `durable/consumer-v1/` identities, keys, checkpoints, leases, sparse acks,
  retries, dead letters, registry entries, corruption, and transition vectors;
- `public/consumer-v1/` request/result/error/stream descriptor and wire vectors;
  and
- `recovery/consumer-v1/` deterministic schedules and process-crash
  observations.

WP-418 owns:

- `ir/live-plan-v1/` invalidation dependencies, patchability proofs, identities,
  costs, and compiler negatives;
- `public/live-v1/` update unions, cursor bytes, reset/terminal reasons,
  authorization, and response-budget vectors; and
- `recovery/live-v1/` snapshot/catch-up, reconnect, restore, revocation,
  pressure, and coalescing observations.

WP-419 owns `generated/reactive-v1/`: one operation/schema inventory plus exact
Rust, TypeScript, Python, CLI, MCP, and SSE-relay observations. MCP notification
fixtures contain only operation identity and a wakeup hint; any event or context
payload is a failing fixture.

WP-420 owns:

- `ir/contextual-plan-v1/` stream/hydration/command closure, bounds, hashes, and
  unsupported reaction shapes;
- `durable/causation-v1/` additive provenance, relationship, corruption, and
  old-record readability vectors;
- `public/contextual-v1/` work items, causation tokens, authorization, redaction,
  expiry, and prior-outcome observations; and
- `recovery/contextual-v1/` commit/ack crash schedules and idempotent reaction
  histories.

WP-421 owns `acceptance/ticketdesk-v1/`: installed multi-database browser,
worker, agent, evolution, restore, revocation, saturation, generated-client,
and no-internal-access evidence. It may aggregate digests from earlier groups;
it must not silently replace their compatibility meaning.

## Explicit Deferrals

P8 fixtures must not imply physical time-based retention, raw CDC, global or
cross-partition ordering, exactly-once delivery/external effects, event-sourced
reconstruction, persisted hydrated context, direct browser credentials,
connectors, webhooks, arbitrary callbacks, declarative reactions, or in-process
agent inference. Adding one of these requires a later requirement and reviewed
ADR rather than an extra case under a P8 V1 fixture group.
