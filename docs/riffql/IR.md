# Symbolic catalog and query surface IR

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

Finite operational plan families use additive query-IR version 2. They seal
every compiler-enumerated presence member, the whole-family authorization
union, maximum cost, and stable cursor identity. A family containing bounded
exact aggregate descriptors uses additive query-IR version 3 for both its
resolved surface and enclosing family. Version 3 adds
the symbolic source binding/entity, ordered group keys and result types,
ordered measures and result types, and the compiler-proven maximum group
count. Ordinary version-1 and non-aggregate version-2 bytes do not rotate.

The additive exact aggregate core uses query-IR version 11. It adds closed
function tags for present count, both exact distinct forms, exact mean state,
Boolean any, and Boolean all, plus compiler-sealed distinct-value,
partial-state-byte, and arithmetic-operation budgets. `ExactMeanV1` is a
structural result record containing a widened exact decimal `total` and `u64`
`count`; the IR never carries a quotient, rounding mode, or floating-point
value. Earlier aggregate surface and family bytes remain version 3.

An exact secret-output declaration selects additive query-IR version 4. The
surface records the immutable contract identity plus the query-local binding,
symbolic and stable entity/field identities, result branch and nested slot,
and declaration source span for every returned secret leaf. These ordered,
bounded requirements participate in the surface, plan, module, and role
hashes. A declaration-free query retains its earlier exact bytes.

Parameter types are contract enums, `Entity.field` references, optional
wrappers, bounded query sets, `Cursor`, `Limit`, or the closed query-only
`Limit<MAX>` refinement. Result schemas contain only source names and closed
scalar/optional/record/list shapes. A list repeats its literal bound, plain
`Limit` parameter name, or bounded-limit parameter name and inclusive maximum.

RiffQL V9 selects additive query IR V12. V12 encodes `Limit<MAX>` with its
positive maximum before its optional default in the parameter schema and row
limit, and uses the same maximum for result-list bounds and whole-plan cost.
Existing V1 through V11 bytes remain unchanged. Changing only `MAX` changes
the surface, plan, module, role, lock, generated schema, and operation hashes.

ADR-0159 adds no IR field or version. Cursor-page-cardinality eligibility is
derived from the existing V12 row-limit, cursor, cardinality, and physical
access structure: a parameter qualifies only when every use is an ordinary
ordered cursor-page limit. The declared `MAX` remains identity-bearing in the
IR and plan; only the validated submitted value is omitted from the separate
process-local cursor lookup hash.

Resolution diagnostics are bounded and value-free:

| Code | Meaning |
|---|---|
| `RDB-QR001` | unknown symbol |
| `RDB-QR002` | ambiguous symbolic path |
| `RDB-QR003` | duplicate query-local/result name |
| `RDB-QR004` | invalid query type |
| `RDB-QR005` | invalid symbolic path |
| `RDB-QR006` | schema, source-map, or artifact limit |
| `RDB-QR007` | missing, duplicate, or invalid exact secret-output declaration |

Each diagnostic carries the closed compiler stage, primary UTF-8 byte span, a
safe source-name path, static summary, and optional static help. It contains no
query parameter, business value, raw source, storage key, or internal error.
