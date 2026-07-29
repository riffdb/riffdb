# Symbolic catalog and query surface IR v1

WP-220 resolves RiffQL against one exact validated contract bundle. The
symbolic catalog indexes entities, fields, enums, variants, and indexes by
their exact source names. Internal stable IDs remain compiler data and never
appear in parameter schemas, result schemas, source maps, or diagnostics.

The resolved surface artifact begins with `RIFFDB-QUERY-SURFACE\0`, query-IR
version `1`, the exact bundle hash, lineage and contract version, canonical
RiffQL source, resolved binding identities, and complete name-addressed
parameter/result schemas. Lengths and counts are big-endian `u32`; the complete
artifact is bounded to 4 MiB. Source maps are kept separately so whitespace and
source offsets do not change canonical semantic bytes.

Parameter types are contract enums, `Entity.field` references, optional
wrappers, bounded query sets, `Cursor`, or `Limit`. Result schemas contain only
source names and closed scalar/optional/record/list shapes. A list repeats its
literal bound or typed `Limit` parameter name.

Resolution diagnostics are bounded and value-free:

| Code | Meaning |
|---|---|
| `RDB-QR001` | unknown symbol |
| `RDB-QR002` | ambiguous symbolic path |
| `RDB-QR003` | duplicate query-local/result name |
| `RDB-QR004` | invalid query type |
| `RDB-QR005` | invalid symbolic path |
| `RDB-QR006` | schema, source-map, or artifact limit |

Each diagnostic carries the closed compiler stage, primary UTF-8 byte span, a
safe source-name path, static summary, and optional static help. It contains no
query parameter, business value, raw source, storage key, or internal error.
