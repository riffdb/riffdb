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

Candidate syntax selects additive query IR V14 and query-module V14. The
resolved surface canonically records each non-output binding's name, root
entity/key, closed set operator, ordered source entity/key/access triples,
positive-source count, distinct-key maximum, and refusal outcome. The access
program adds complete candidate-source row limits, candidate predicate
references, and one root-hydration access with its maximum, complete key, and
finite total-order terms. Source-map tags 11 and 12 identify the declaration
and each source access. These fields are plan and role identity only; public
parameter and result schemas remain ordinary business values and never expose
candidate structure.

Bounded `Set<T, MAX>` syntax selects RiffQL V13. A query using that value as
the exact partition-field membership route selects query IR V16 and
query-module V16. The program encodes the parameter name and declared partition
maximum, one partition-set index access with its physical direction, complete
global order, per-partition scan ceiling, and hidden entity-key tie fields,
plus multiplied cost and authority. Canonical cursor parameters retain the
normalized complete set. Existing V1 through V15 query plans and modules are
unchanged.

When the partition component is followed by one or more invariant exact
predicate components before the order suffix, the same RiffQL V13 source
selects additive query IR V17 and query-module V17. V17 additionally encodes
the fixed prefix width so continuation reconstructs the partition, every exact
filter value, and the global order key without placing invariant filter values
in the public cursor marker. Exact-filter parameters remain cursor-identity
bearing. A partition-set plan without this prefix retains byte-exact V16 plan
and module artifacts.

Bounded expansion syntax selects RiffQL V14, query IR V18, and query-module
V18. The expansion access encodes the selected declared index, physical order,
driver binding and singular item identity, per-driver maximum, and proven
whole-expansion product maximum. Its predicate retains an ordinary binding-field
dependency on the driver, and its result schema nests the bounded target list
under the driver record. Queries without an expansion keep their predecessor
surface, plan, module, hash, and cursor bytes. V18 adds no provider descriptor,
provider epoch, provider state, service port, generated input, or durable
storage format.

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
