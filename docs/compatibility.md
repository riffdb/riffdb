# Compatibility

RiffDB 0.1.0 is a pre-release POC. Compatibility is checked at explicit
boundaries; it is not a blanket promise that arbitrary commits can be mixed.

## Boundaries

| Boundary | POC policy |
|---|---|
| Public gRPC | Versioned `riffdb.v1` Protobuf, reserved-field policy, descriptor and wire fixtures |
| Durable records | Versioned Protobuf payloads inside checked stored envelopes |
| Storage | redb baseline with startup format/integrity validation |
| Contract IR | Versioned deterministic IR and plan-hash domains |
| Migration IR | Exact-parent `MigrationBundleV1`, domain-separated source and bundle hashes, canonical binary goldens |
| MCP | Fixed protocol baseline, generated schemas, stable compiler-owned command names |
| CLI machine output | Closed `riffdb.cli.output/v1` JSON envelope |
| Backup | Immutable manifest v1 plus checksums and exact database identity |
| Maintenance receipt | Separate versioned external format beneath `.maintenance` |

Generated source, descriptors, schema inventories, wire vectors, contract
fixtures, and interface checkpoints are compatibility evidence. Run
`./scripts/check-generated` before release.

The production storage engine in `riffdbd` is redb. Any Fjall comparison lives
in an isolated nested workspace and cannot be substituted into the server.

## Upgrade Rules

- Start a database only with a binary that recognizes its storage and durable
  format versions.
- Never edit stored envelopes, maintenance receipts, manifests, generated
  descriptors, or contract IR by hand.
- Do not deploy two independently changed public/durable schemas under the same
  version.
- A downgrade is unsupported.
- A backup should be restored with the release family that created and verifies
  its manifest before any later migration is attempted.
- Digest-key rotation is compatible only while every key needed to read
  retained records remains present with its original ID and material.
- A migration targets one exact parent bundle and one exact canonical
  successor. It is never inferred from version numbers or chained through an
  intermediate contract.
- Application Source V3 and Lock V4 are additive formats. V1/V2 source and
  V1/V2/V3 lock decoders remain strict compatibility boundaries.

Restore preserves the backed-up `DatabaseId` but does not preserve observations
from the destroyed suffix. It is therefore not a mechanism for maintaining
globally stable post-backup locators. ADR-0072's durable `history_incarnation`
makes restore rewinds detectable for clients that send optional
`observed_history_incarnation`; non-participating clients remain unvalidated.
