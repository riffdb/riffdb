# ADR-0024: Canonical Provenance Resource Locator

- **Status:** Accepted
- **Direction approved:** 2026-07-21
- **Exact text accepted:** 2026-07-21
- **Accepted:** 2026-07-21
- **Requires:** ADR-0006, ADR-0007, ADR-0011, and ADR-0018
- **Amends:** ADR-0008 only for the exact provenance resource-locator spelling;
  ADR-0008 remains Proposed for every other resource URI, HTTP audience,
  transport, cursor-presentation, and remaining protocol-fixture decision
- **Decision deadline:** Before WP-127 freezes completed public response fields

The human maintainer accepted this exact narrow locator rule on 2026-07-21. No
other proposed part of ADR-0008 is accepted by this record.

## Context

The phase-zero Execute response and the specification example already carry a
provenance resource locator. The application-service boundary correctly returns
a typed `ProvenanceId`, not transport text, but WP-127 cannot freeze the public
response fixture while the exact mapping remains only a Proposed MCP decision.

The mapping must have one spelling. Alternate case, escaping, or URI components
would create multiple public identities for the same provenance record and make
generated fixtures, clients, and MCP resource dispatch disagree.

## Decision

The canonical version-one provenance resource locator is exactly:

```text
riffdb://provenance/<provenance-id>
```

`<provenance-id>` is the canonical 36-byte UUID text for the checked
`ProvenanceId`: lowercase ASCII hexadecimal, with hyphens at byte positions 8,
13, 18, and 23, and with valid UUIDv7 version and RFC variant bits. The prefix
is the literal 20-byte lowercase ASCII string `riffdb://provenance/`. The
complete locator is therefore exactly 56 bytes.

There is no percent encoding, alternate case, omitted or additional hyphen,
authority variation, port, user information, path suffix, trailing slash,
query, fragment, or alternate spelling. Producers emit only the canonical
form. Parsers reject every noncanonical form rather than normalizing it.

The locator is a public reference to a typed provenance record only. It grants
no authorization, conveys no UUID or commit ordering, is not an idempotency
identity, and does not prove that the referenced record is visible to the
caller. Every dereference reauthorizes through the shared API-neutral service
and applies current redaction obligations. No adapter may use the locator to
access storage directly.

### Ownership and layering

- `riffdb-types` owns checked `ProvenanceId` bytes and UUIDv7 structure.
- `riffdb-service` carries typed `ProvenanceId` values and never constructs or
  parses this transport locator.
- `riffdb-proto` and `riffdb-api-grpc` own the public response conversion and
  reject structurally invalid or noncanonical locator text.
- `riffdb-api-mcp` uses the same exact locator for provenance links and resource
  dispatch, then invokes the authorized service operation.
- SDKs may expose a convenience formatter or parser only when it is byte-for-
  byte equivalent to this rule.

## Options Considered

1. **One fixed locator plus canonical UUIDv7 text:** accepted; it matches the
   existing specification and phase-zero fixture and has one public identity.
2. **General URI parsing with normalization:** rejected; equivalent spellings
   would cross public and cache boundaries differently.
3. **Opaque or percent-encoded identifier text:** rejected for version one; the
   identifier is already a fixed checked UUIDv7 and needs no escape syntax.
4. **Keep the rule inside MCP only:** rejected; the public Execute response and
   gRPC clients already consume the same reference before the MCP package.

## Consequences

- Public Execute responses and MCP provenance links share one formatter.
- A canonical locator is always 56 bytes; broader generic URI bounds do not
  permit a longer provenance locator.
- Other RiffDB resource URI templates remain unresolved under Proposed
  ADR-0008.
- A future incompatible spelling requires a new compatibility decision and a
  versioned public boundary; version-one bytes are never reinterpreted.

## Compatibility

The accepted golden is:

```text
riffdb://provenance/019bf6aa-a640-7de6-89c9-8a7f70bbbd23
```

This record freezes public locator text only. It changes no durable record,
storage key, contract IR, command plan, canonical hash, or idempotency identity.
Durable records continue to store the typed 16-byte `ProvenanceId` according to
their accepted schema.

## Security

Formatting a locator never establishes visibility. Resource listing, lookup,
and dereference remain capability-filtered and redacted. Invalid locator text
fails closed with a bounded public error and must not be echoed into diagnostics
without the normal redaction and size controls.

## Testing

WP-127 freezes the accepted golden plus wrong prefix, uppercase, malformed
hyphen, wrong length, wrong UUID version, wrong UUID variant, suffix, query,
fragment, percent-encoding, and trailing-slash rejection fixtures. Generated
Protobuf artifacts must reproduce exactly. WP-130 adds service-to-gRPC
round-trip tests. WP-140 proves MCP emits and parses the same bytes, reauthorizes
every dereference, and has no storage dependency. WP-200 supplies final
cross-transport evidence.

## Requirements and Work Packages

- **Requirements:** `ID-001`, `API-001`, `MCP-030`, `MCP-033`
- **Defines or blocks:** `WP-127`, `WP-130`, and the provenance-resource portion
  of `WP-140`
- **Final evidence:** `WP-200`

## Decision Deadline

This exact text is accepted before WP-127 freezes completed public message
fixtures. Every other resource URI and MCP transport decision remains Proposed
until separately reviewed.
