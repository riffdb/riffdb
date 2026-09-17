# WP-748 public registration and retirement fixture review

Package: WP-748. Tier: guarantee. Base: `6510d052`.
Status: implementation and focused checks complete; full validation and human
fixture review pending. This implements the already accepted registration-audit
amendment; it does not propose another guarantee amendment or close WP-748.

## Behavior

`AdminService.RegisterFollower` and `AdminService.RetireFollower` use the shared
service and sole coordinator. They require current global administrative
authority, retain an exact lineage-scoped target through service audit, and
link success to the exact authoritative lifecycle action and target. A retry
uses a fresh transport request ID and the original immutable selection. Retrying
registration after retirement returns historical evidence without restoring
custody. Both calls are available before contract deployment and refuse on a
follower. They are exposed through gRPC, the Rust operator client and CLI.

## Fixture diff to review

- `proto/riffdb/v1/replication.proto`: six additive
  messages and one closed refusal enum. Selection carries database ID,
  incarnation, epoch and opaque hold ID; registration adds a positive budget
  and optional positive application expiry; retirement selects the original
  generation. Results contain a receipt or one of five bounded refusals.
- `proto/riffdb/v1/services.proto`: append two unary
  methods to `AdminService`, preserving prior method order.
- `fixtures/proto/public-schema-hashes.txt`: 36 added
  entries. Existing changes are confined to the two containing file hashes,
  `AdminService` and the overall public schema. No existing field, message,
  enum or RPC identity is changed or removed.
- `fixtures/proto/public-client-vectors.txt`: 17 new
  request/response vectors cover both expiry shapes, exact generation, new and
  replayed receipts, and every refusal for both operations. The registry adds
  the six enum values and optional expiry coverage.
- `fixtures/compatibility/public-adapter-freeze-v1.tsv`:
  regenerated from the canonical validation, gRPC, Rust-client and CLI
  templates. The registry now has 59 semantic operations. New tags are 0x3a and
  0x3b; prior tags retain their values.
- [CLI reference](../reference/CLI.md): `follower register` and `follower retire`
  require explicit lineage and positive policy/generation. Hold IDs are exactly
  32 lowercase hex characters and nonzero. Receipt sequences are decimal strings.

The durable-format manifest, frozen durable schemas and old audit encodings are
unchanged. New lifecycle audit operations use the already accepted V3 envelope.
Success without its exact follower target and control-plane link is refused on
append and reconstruction. The Rust client's retirement exchange additionally
checks that the returned generation equals the requested generation.

## Checks and compatibility

Focused storage audit-link, service authorization/cancellation, daemon restart,
wire-validation and CLI tests pass, including retirement-generation substitution
and duplicate-request fail-closed recovery. Static acceptance passed all 14
checks. Full acceptance of this public increment remains pending. Prerequisite
revisions `88c1a275` and `6510d052` each passed all 12 full acceptance checks;
the final prerequisite CI run took 3,278.8 seconds on 2026-09-17.

Existing clients retain their prior message shapes. Calling these additive RPCs
requires updated operator bindings. Existing request IDs still name one
submission; reusing a completed ID fails closed. No MCP tool, application-role
permission, automatic migration, unfence operation or promotion is introduced.
Benchmark qualification remains paused.

Handbook updates: [Follower registration and retirement](../operations/FOLLOWER-ADMINISTRATION.md),
[Compatibility](../compatibility.md), [Remote ingress](../operations/REMOTE-INGRESS.md),
[Backup and restore](../backup-restore.md), [Known limitations](../known-limitations.md),
and the generated CLI reference. Fencing, authenticated fence proof, promotion,
incarnation/epoch advancement and exact RPO remain WP-748 follow-up work.
