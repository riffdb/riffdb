# ADR-0027: Service Response Budget and Oversize Disposition

- **Status:** Accepted
- **Direction approved:** 2026-07-21
- **Exact text accepted:** 2026-07-21
- **Accepted:** 2026-07-21
- **Requires:** ADR-0006, ADR-0007, ADR-0017, ADR-0021, and ADR-0026
- **Clarifies:** API-neutral response accounting, whole-item pagination, and the
  failure used when one valid result cannot fit the service response bound
- **Decision deadline:** Before WP-120 publishes bounded service results and
  before WP-127 freezes their public wire mapping

The human maintainer approved this narrow decision on 2026-07-21. It does not
lower an accepted durable-record bound, add a retrieval operation, or make a
transport adapter responsible for semantic response limiting.

## Context

ADR-0007 and SPEC require every API-neutral unary response to fit a 4 MiB hard
bound before adapter encoding. They do not define a Protobuf-independent charge
for service DTOs. Some legal authoritative objects, including a commit record or
provenance result, may be larger than 4 MiB under the accepted 16 MiB durable
limits. Item-count bounds alone therefore cannot establish the service bound.

WP-120 must enforce the bound before releasing an authorized result and before
WP-127 introduces Protobuf as a downstream representation. It also needs one
closed result for a valid indivisible object that cannot fit. Relabeling that
case as invalid input, missing state, or storage unavailability would make a
false claim; silently truncating an object would weaken its semantics.

## Decision

### API-neutral response charge

`riffdb-service` owns a versioned, Protobuf-independent conservative response
charge for every service result that can contain variable-size caller-visible
data. The POC charge ceiling is exactly 4,194,304 bytes. Configuration may lower
but may not raise it.

The charge is deterministic, uses checked arithmetic, and includes every
caller-visible variable-length byte plus fixed conservative charges for tags,
presence, lengths, scalar fields, and collection entries. It must never be less
than the encoded size of the corresponding supported WP-127 Protobuf response.
WP-127 freezes golden and boundary fixtures that compare the service charge with
actual wire encoding. If a future wire change can exceed the accepted charge,
that change requires an amended response-charge version before merge.

The service applies authorization, field redaction, and other narrowing
obligations before charging the releasable result. It never charges or exposes
redacted bytes. Charge overflow fails closed.

### Whole-item pagination

For every list or scan operation, including discovery, the service accumulates
only complete items while both the effective item limit and response budget
permit them. It never splits, truncates, or partially redacts one semantic item
to fit the page. When another available item would exceed the budget, the page
ends at the last released item and receives an opaque cursor whose lower
continuation and accepted consistency fence resume at the withheld item. An
empty page may not carry a cursor.

Cursor creation remains subject to ADR-0007 capacity, lifetime, policy binding,
and failure rules. A page does not silently report completion merely because its
byte budget was reached. Discovery lists use the same page and opaque-cursor
semantics as other list operations; adapters neither decode cursor state nor
invent transport-specific pagination.

### Indivisible oversize result

If a single complete item or non-page result cannot fit the effective response
budget, the service withholds it and returns the closed
`ServiceFailure::ResponseTooLarge` disposition. This disposition contains no
object data, size value, internal diagnostic, or caller-controlled text. It does
not assert invalid input, absence, storage unavailability, or an internal defect.

The service selects and durably attempts the operation's normal terminal audit
before releasing this disposition. An audit failure remains fail closed under
ADR-0007 and ADR-0026. For a stream, each visible item must independently fit
the same service response budget; an oversize next item terminates the stream
with `ResponseTooLarge` and is not partially emitted.

WP-130 maps `ResponseTooLarge` to gRPC `RESOURCE_EXHAUSTED` with a bounded static
message and no structured `PublicError`. MCP, CLI, SDK, and in-process callers
consume the same API-neutral disposition. A transport may impose a lower
configured message limit but may not reinterpret or bypass the service result.

### Explicit deferral

The POC does not add chunked retrieval, byte-range reads, partial commit or
provenance records, or another RPC. Legal durable administrative objects larger
than the service response bound remain authoritative and writable but cannot be
retrieved whole through the POC public unary/stream operations. A post-POC
chunked retrieval design must preserve authorization, redaction, identity,
integrity, cursor, and audit semantics and requires a separate ADR.

## Options Considered

1. **Conservative service-owned charge and closed oversize disposition:**
   accepted; it enforces the existing boundary without coupling the service to
   Protobuf or making a false error claim.
2. **Measure encoded Protobuf in `riffdb-service`:** rejected; it reverses the
   accepted service/protocol dependency and makes transport encoding semantic.
3. **Lower all durable records to 4 MiB:** rejected; it silently weakens accepted
   durable-format and transaction limits.
4. **Return the first oversize item despite the bound:** rejected; it makes the
   normative 4 MiB service ceiling unenforceable.
5. **Truncate a semantic item:** rejected; partial records and provenance can be
   misleading and cannot satisfy their existing typed contracts.
6. **Add chunking or new retrieval RPCs now:** rejected from the POC critical
   path; their integrity and authorization contract needs independent design.

## Consequences

- WP-120 has one owner and deterministic rule for its 4 MiB response guarantee.
- Page boundaries may be selected by bytes before the caller's item limit.
- WP-127 must prove the conservative charge covers exact supported wire bytes.
- A legal large durable object may be unavailable through the POC public API;
  this is an explicit implementation limitation, not data loss.
- Adding a field to a variable-size public response requires reviewing both its
  wire bound and the response-charge version.

## Compatibility

This decision adds one API-neutral Rust failure disposition and pagination to
list/discovery DTOs that were not yet published. WP-127 adds the corresponding
closed transport mapping without changing the accepted service or RPC names.
The gRPC status mapping is additive behavior for a previously unspecified
oversize case.

There is no contract grammar, typed IR, plan hash, canonical input hash,
idempotency identity, durable Protobuf, storage key, commit record, commit order,
or atomicity change. Existing 16 MiB durable ceilings remain unchanged.

## Security

Charging occurs only after current authorization and obligations have selected
the releasable representation, so hidden fields do not influence visible output
or escape through partial data. The oversize failure has a static bounded shape
and reveals neither exact object size nor protected content. Cursors remain
server-side, policy-bound, capacity-limited, and non-authoritative.

All adapters consume the same service decision. MCP and gRPC cannot fetch an
oversize object through direct storage access, substitute their own page split,
or use a larger transport limit to bypass the service ceiling.

## Testing

WP-120 unit and integration tests freeze exact/equal-plus-one response charges,
checked-arithmetic failure, redaction-before-charge, item-limit versus byte-limit
page boundaries, continuation at the withheld item, empty-page rejection,
cursor-capacity failure, indivisible oversize withholding, stream termination,
and terminal-audit-before-release behavior.

WP-127 golden tests encode maximum charged responses and prove actual supported
Protobuf bytes never exceed their service charge. Equal-limit and one-byte-over
fixtures cover each variable-size response family and the closed oversize
mapping. WP-130 gRPC conformance tests freeze `RESOURCE_EXHAUSTED`, bounded static
text, absent `PublicError` details, and unchanged behavior after restart. WP-140
and WP-200 prove MCP uses the same service disposition and cannot bypass it.

## Requirements and Work Packages

- **Requirements:** `API-001`, `VAL-003`, `MCP-030`, `MCP-033`, `SEC-001`
- **Defines or blocks:** `WP-120`, `WP-127`, `WP-130`, and the corresponding
  `WP-140` MCP mapping
- **Final evidence:** `WP-200`

## Decision Deadline

This exact text is accepted before WP-120 publishes response-bounded service
DTOs and failures. The charge and paging behavior land in WP-120; WP-127 freezes
wire coverage before WP-130 exposes the first public server.
