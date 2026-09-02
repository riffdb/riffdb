# Named-result carriage

Generated named queries have one semantic result shape fixed by the compiled
query plan. RiffDB may carry that shape through a negotiated wire encoding,
but application code and agents cannot select the physical encoding or provide
field ordinals, layouts, or schemas.

The closed encoding order is:

1. `LEGACY_RECORDS`
2. `COMPACT_V1`
3. `PACKED_V1`

Each request advertises a contiguous prefix. Servers return exactly one arm,
and clients reject an unadvertised, mixed, malformed, over-bound, or
identity-mismatched arm before releasing a partial result.

Current generated clients advertise through `COMPACT_V1` for eligible
compiler-covered queries. `PACKED_V1` exists in the additive protocol and
strict driver compatibility codec, but generated clients neither select it nor
carry dormant direct decoders because its WP-659 candidate missed both the
performance and generated-size gates. There is no application configuration
switch for it. Queries without a complete safe cover continue to use legacy
records.

One-level bounded expansion also uses the existing name-addressed legacy value
carrier. Each driver record owns one compiler-named list of target records;
the executor retains the driver key while assembling the flat authorized
target observations, then releases only the nested typed result. Generated
Rust, Go, TypeScript, and Python decoders accept exactly that compiled record
shape and reject missing, extra, malformed, or over-bound nested values. MCP
and CLI render the same named list and cannot request a flat mode, index,
driver, fan-out, or per-driver cursor.

The local driver protocol is V3. The V3 host remains read-compatible with V1
legacy and V2 compact clients. Remote trust, authorization, freshness,
cancellation, retry, and uncertainty semantics do not vary with carriage.
