# ADR-0013: Stable IDs, IR Encoding, and Plan-Hash Framing

- **Status:** Accepted
- **Direction approved:** 2026-07-13
- **Exact text accepted:** 2026-07-13, clarified 2026-07-13 and 2026-07-20
- **Clarified by:** ADR-0004 (complete executable plan reference), ADR-0007
  (unjournaled read-only service boundary), ADR-0012 (runtime result/fault
  boundary), ADR-0016 (canonical root-derivation equality), and ADR-0017 (bound
  projection group schema)
- **Decision deadline:** Before WP-040 public interfaces or fixtures merge

The human maintainer accepted this exact text, the canonical root-derivation
equality clarification, and the companion clarifications below on 2026-07-13.
On 2026-07-20 the maintainer accepted the format-neutral conservative
derived-index admission bounds below. They tighten checked IR-v1 acceptance but
add no encoded field, tag, or plan-hash input.
On 2026-07-20 the maintainer also accepted the bounded active-lineage
materialization clarification below. It gives optional-null evolution exact
ancestry proof, omission, activation, and startup semantics without adding a
bundle field, IR node, durable value, public field, or hash input.
On 2026-07-20 the maintainer accepted the projection application clarification:
catalog owns the opaque process-local event-materialization view, projection
uses it for both catch-up and rebuild, and durable event bytes/hashes and the
IR-blind storage boundary remain unchanged.
The accepted initial generated review artifacts are identified outside their own
bytes by these exact SHA-256 digests:

- `crates/riffdb-contract-ir/FORMAT.md`:
  `5e99e84900cdf795747711e0cab09140eebeac2fae257b4ad67ab25c086454e4`
- `crates/riffdb-contract-ir/JSON_SCHEMA_FORMAT.md`:
  `6d5a95b81baf97d417372c0a4f2f01a392402b66105ed625a3a7bc2f856c9b64`

Any byte change produces a different artifact and requires the compatibility and
human-review process below; regenerating the source and document together does
not preserve this initial acceptance identity.

## Context

ADR-0002 freezes grammar version 1 and the AST/HIR/IR ownership boundary without
guessing the executable representation. WP-040 must now make equal compilation
inputs produce equal IDs, schemas, executable plans, and bundle bytes while
preserving stable identities across compatible versions in one contract lineage.

The bundle is an executable and compatibility artifact, but WP-040 does not
depend on `riffdb-proto`. A catalog record added later through the ADR-0006
proto-owner process will carry the exact versioned bundle bytes. The semantic IR
must therefore own a checked canonical encoding without coupling runtime crates
to generated Protobuf types.

## Decision

### Ownership and construction boundary

`riffdb-contract-ir` owns span-free typed HIR structs, executable plans, schema
IR, stable-ID lineage data, compatibility types and comparison, the canonical
bundle codec, and checked validators. HIR is not canonical, durable, hash input,
or executable. Compiler-private side tables associate HIR nodes and diagnostics
with `riffdb-contract-syntax::Span`; source spans never enter bundle bytes.

Executable plan and bundle fields are private outside their owning modules.
Public construction uses checked builders or checked decoding. A decoded bundle
is returned only after bounds, canonical order, references, expression types,
forward execution, stored hashes, and supported versions have all been
validated. Runtime code cannot opt out of validation or execute syntax AST/HIR.

`riffdb-contract-compiler` owns parsing orchestration, source diagnostics, name
resolution, type checking, stable-ID allocation, semantic analysis, lowering,
and explain generation. It depends on syntax, IR, and `riffdb-types`; it has no
storage, service, transport, clock, filesystem, environment, or randomness
dependency in the compilation path.

Compatibility report types and the pure comparison algorithm live in
`riffdb-contract-ir`. The compiler and catalog call the same implementation. A
stored report is display evidence and is always reproducible from the parent and
candidate bundles; the catalog does not trust a separately supplied verdict.

### Stable semantic IDs

All stable semantic IDs are one-based `u32` values. Zero is invalid and reserved.
The first allocated ID is 1. An ID is never derived from a truncated hash and is
never reused.

The v1 namespace registry is:

| ID type | Namespace and identity key |
|---|---|
| `EntityTypeId` | lineage-global entity name |
| `EventTypeId` | lineage-global event name |
| `EnumTypeId` | lineage-global enum name |
| `AggregateTypeId` | lineage-global aggregate name |
| `CommandId` | lineage-global command name |
| `ProjectionId` | lineage-global projection name |
| `IndexId` | lineage-global `(EntityTypeId, index name)` path |
| `InvariantId` | lineage-global `(entity or aggregate owner kind, owner stable ID, invariant name)` path |
| `FieldId` | scoped to one record schema and exact field name |
| `OutcomeId` | scoped to one `CommandId` and outcome name |
| `EnumVariantId` | scoped to one `EnumTypeId` and variant name |

The immutable stable-ID namespace tags are entity `0x01`, event `0x02`, enum
`0x03`, aggregate `0x04`, command `0x05`, projection `0x06`, index `0x07`,
invariant `0x08`, field `0x09`, outcome `0x0a`, and enum variant `0x0b`.
Global declarations use owner-kind `0x00` and zero owner components. Indexes use
owner-kind entity `0x01` and one `EntityTypeId`. Invariant owner tags are entity
`0x01` and aggregate `0x02`, each with its one stable owner ID. Outcomes use
owner-kind command `0x01` and one `CommandId`; enum variants use owner-kind enum
`0x01` and one `EnumTypeId`. Command `require` statements use dense
command-local requirement indices, not `InvariantId`.

Entity key fields and stored fields share the entity record's `FieldId`
namespace. Record-owner tags and paths are entity `0x01` plus `EntityTypeId`,
event payload `0x02` plus `EventTypeId`, command input `0x03` plus `CommandId`,
command outcome `0x04` plus `CommandId` and `OutcomeId`, and projection result
`0x05` plus `ProjectionId`. Projection key components remain an ordered tuple;
declared projection measures use fields in the projection-result namespace.
Equal rejection outcome names used at multiple sites in one command must resolve
to one outcome ID and one normalized field/type shape. The compiler forms the
union of their named payload fields; every present occurrence of a field must
infer the same exact type, a required field must occur at every site, and a field
may be absent at a site only when its resolved type is optional, in which case
canonical null is inserted. The terminal success outcome name must be distinct
from every rejection outcome name. Violations fail compilation.

`ExprId`, `BindingId`, instruction positions, and other plan-local indices are
zero-based dense `u32` positions scoped to one plan. They are not semantic
identities and never appear without their containing plan hash. The compiler
allocates them in deterministic lowering order; changing them changes the plan
hash.

For deterministic allocation, an identity key is encoded as the one-byte
namespace tag, a one-byte owner-kind tag (`0x00` when unowned), a one-byte
owner-component count, each owner ID as `u32` big endian, a `u16` big-endian name
length, and the exact ASCII identifier bytes. Genesis allocation sorts these
encodings bytewise inside each exact allocation namespace and assigns ascending
IDs from 1. Parent identities required to form child paths are allocated before
their child namespace.

Every bundle contains lineage ledger version 1 with active entries, tombstones,
and one `max_allocated: u32` state for every global or dynamic owner-scoped
namespace. Entity, event, enum, aggregate, command, projection, index, and
invariant IDs each have one lineage-global allocation state. Fields have one
state per exact record-owner path, outcomes one per `CommandId`, and enum
variants one per `EnumTypeId`. An empty namespace has maximum zero.

For every nonempty allocation state, the ledger contains exactly one active or
tombstoned entry for every numeric ID in `1..=max_allocated`, with no gap,
duplicate ID, duplicate identity key, or active/tombstone overlap; its highest
ID equals `max_allocated`. Entries are ordered by numeric ID inside their exact
allocation namespace. Allocation states are ordered first by namespace tag and
then, for scoped states, by the exact encoded owner-kind/count/ID path. The v1
ledger-entry and bundle bounds are reached long
before `u32` exhaustion, so allocation that would exceed either bound fails with
a stable diagnostic and no partial bundle. There is no sparse or caller-supplied
high-water mark from which a removed ID could be reused.

Successor compilation requires the exact validated parent bundle, not a
caller-authored ID map. A surviving identity retains its ID. Removed identities
become permanent tombstones. New identities are sorted by their encoded keys and
allocated monotonically above `max_allocated`. Moving or renaming a declaration
is removal plus addition. Reintroducing an exact tombstoned identity path is
rejected in v1; future explicit rename or resurrection semantics require another
ADR.

Genesis compilation has no parent, requires a nonzero application contract
version, and creates a new lineage whose exact name is the parsed contract name.
A successor must have the same lineage name and an application version strictly
greater than its parent; versions need not be contiguous. The bundle records the
parent application version and `ContractBundleHash`. Compilation determinism is
defined over the exact source bytes, compiler version, and the same
genesis/successor lineage input. IR v1 has no caller-selectable option that can
change semantic output. Supplying different lineage history is a different
compilation input even when source text is equal.

### Canonical bundle encoding v1

`riffdb-contract-ir` uses a small checked positional binary codec. It does not use
Rust layout, Serde defaults, map iteration, or Protobuf serialization. A later
catalog Protobuf record carries these bytes under the accepted durable envelope;
it does not reinterpret or duplicate the IR.

The exact top-level byte sequence is:

1. ASCII `RIFFDB-BUNDLE` followed by `0x00`.
2. Bundle format version as `u32` big endian; v1 is `1`.
3. Grammar version as `u32` big endian; grammar v1 is `1`.
4. Executable IR version as `u32` big endian; IR v1 is `1`.
5. Compiler version string.
6. Contract lineage string and application `ContractVersion`.
7. Optional parent application version and parent `ContractBundleHash`.
8. `SourceHash` and `ContractPlanRootHash`.
9. Stable-ID lineage ledger.
10. Contract schema IR.
11. Command plans sorted by `CommandId`.
12. Projection plans sorted by `ProjectionId`.
13. Generated schema artifacts sorted by their closed artifact key.
14. Compatibility report in stable code then stable-ID order.

The crate must check in `FORMAT.md` containing the complete nested v1 field
sequence and tag registry generated from the codec's single declarative format
registry. The registry, encoder, decoder, and document must byte-compare in CI.
The initial `FORMAT.md` is a public/durable interface artifact and requires
maintainer review in the WP-040 IR-interface PR; implementation struct order is
never allowed to define the format implicitly.

The bundle does not contain source text, source paths, spans, wall/deployment
time, principal, request ID, Git revision, host information, or its own
`ContractBundleHash`.

Codec primitives are fixed: unsigned integers are big endian; signed integers
are two's-complement big endian; Boolean is exactly `0x00` or `0x01`; option is a
one-byte `0x00`/`0x01` marker; byte strings and UTF-8 strings use a `u32`
big-endian byte length; lists use a `u32` big-endian count; digests are exactly 32
bytes. `CanonicalValue` payloads use the complete ADR-0011 canonical document,
prefixed by a `u32` byte length. Names retain exact case-sensitive ASCII spelling.

Semantic sets are sorted by stable numeric ID, and duplicate or decreasing IDs
are rejected. Record fields are sorted by `FieldId`; enum variants by
`EnumVariantId`; outcomes by `OutcomeId`; schema artifacts by their encoded key;
and compatibility entries by stable code then affected stable path. Tuple
components, instruction streams, expression operands, object construction, and
event occurrence preserve semantic order. No unordered map or set is encoded.

The closed schema-artifact key tags are entity record `0x01` plus
`EntityTypeId`, event payload `0x02` plus `EventTypeId`, command input `0x03`
plus `CommandId`, command outcome union `0x04` plus `CommandId`, and projection
result `0x05` plus `ProjectionId`. An artifact key is exactly its tag followed by
the listed stable ID as `u32` big endian. Unknown tags and duplicate keys reject.

The immutable v1 type-tag registry is:

| Tag | Type |
|---:|---|
| `0x01` | Boolean |
| `0x02` | signed 64-bit integer |
| `0x03` | unsigned 64-bit integer |
| `0x04` | fixed precision/scale decimal |
| `0x05` | fixed-currency money |
| `0x06` | bounded UTF-8 string |
| `0x07` | bounded bytes |
| `0x08` | timestamp |
| `0x09` | date |
| `0x0a` | UUID |
| `0x0b` | enum reference |
| `0x0c` | optional |
| `0x0d` | bounded list |
| `0x0e` | typed record reference |

Decimal precision and scale use the accepted ADR-0011 bounds. Grammar-v1
`money<CURRENCY>` has precision 38 and scale 2 for every exact three-letter
currency label; v1 does not consult an operating-system or mutable ISO currency
registry. A different currency scale requires a later grammar/type version.
`string<N>` and `bytes<N>` require `1 <= N <= 1,048,576`; `list<T,N>` requires
`1 <= N <= 65,535`; recursive type nesting is at most 32. Nested optionals are
invalid because canonical null cannot distinguish their layers. A named source
type resolves only to one declared enum in grammar v1.

Expressions are an immutable topologically ordered arena. Every referenced
operand index must be lower than the referencing node index. Each node stores its
validated result type. The v1 expression tags are constant `0x01`, input field
`0x02`, complete bound record `0x03`, bound-record field `0x04`, schema field
`0x05`, source-event field `0x06`, `tx.time` `0x07`, `tx.date` `0x08`, unary
`0x09`, binary `0x0a`, and root-validation field `0x0b`. A schema-field node
contains its `EntityTypeId` and `FieldId` and occurs only in an entity/aggregate
invariant or aggregate key template before command-specific instantiation. A
root-validation-field node contains `RootValidationReadId` and `FieldId` and is
valid only in an instantiated aggregate commit check. Unary tags are NOT `0x01`
and checked negation `0x02`. Binary tags in
source precedence order are multiply `0x01`, divide `0x02`, add `0x03`, subtract
`0x04`, equal `0x05`, not-equal `0x06`, less `0x07`, less-equal `0x08`, greater
`0x09`, greater-equal `0x0a`, and `0x0b`, or `0x0c`.

### Expression typing and evaluation

The v1 operator matrix is closed:

| Operator | Operand types | Result and rule |
|---|---|---|
| `!` | `bool` | `bool` |
| unary `-` | `i64`, decimal, or money | same exact type, checked |
| `+`, `-` | equal `i64` or equal `u64` | same type, checked |
| `+`, `-` | equal decimal precision/scale | same decimal type, checked |
| `+`, `-` | equal money currency/precision/scale | same money type, checked |
| `*`, `/` | equal `i64` or equal `u64` | same type, checked |
| `==`, `!=` | equal Boolean, integer, decimal, money, string, bytes, timestamp, date, UUID, or enum types | `bool` |
| `==`, `!=` | equal `optional<T>` where `T` supports equality, or one `optional<T>` and `null` | `bool` |
| `<`, `<=`, `>`, `>=` | equal integer, decimal, money, timestamp, or date types | `bool` |
| `&&`, `||` | `bool`, `bool` | `bool`, left-to-right short-circuit |

"Equal" means the complete static type is identical; there is no integer
signedness conversion, decimal precision/scale widening, currency conversion,
string-bound conversion, enum-name comparison, or optional lifting inside an
operator. Decimal/money multiplication and division, string concatenation,
Boolean/UUID/enum ordering, and list/record operators are invalid v1 plans.
Contextual assignment, binding arguments, event fields, and declared entity
fields may inject a nonoptional `T` value into an expected `optional<T>` or use
`null`; there is no implicit optional unwrapping.

Arithmetic is deterministic and checked. Signed division truncates toward zero;
unsigned division uses integer quotient. Division by zero, signed minimum divided
by `-1`, integer overflow/underflow, decimal precision overflow, money amount
overflow, and unary-negation overflow produce the same typed runtime arithmetic
fault on every platform. They never wrap, saturate, panic, or become a business
outcome. Evaluation is left to right, with only `&&` and `||` short-circuiting.
If a runtime arithmetic fault occurs, the interpreter discards its in-memory
working mutations, captured events, and outcome and returns no `CommitIntent`.
The durable reservation and public retry mapping for this execution-failure
class must be accepted before WP-080; it is not caller-selectable plan policy.

Boolean literals have type `bool`. A numeric, string, or null literal first
receives an exact expected type from its enclosing field, binding argument,
operator constraint, or assignment when one exists. An unsigned-integer lexeme
may inhabit expected `i64`, `u64`, decimal, or money only when its exact value
fits; decimal/money interpretation appends the declared number of fractional
zeroes. A fixed-decimal lexeme may inhabit expected decimal or money only when
its fractional digit count exactly equals the declared scale and its coefficient
fits. A string lexeme is decoded as JSON Unicode scalar values and may inhabit
expected `string<N>` only when its exact UTF-8 bytes fit. `null` requires an
expected optional type. Bytes, UUID, date, timestamp, and money have no
standalone source-literal syntax; money literals arise only by contextualizing a
fixed decimal under a known currency.

When no expectation exists, an integer literal has `i64` type if it fits and
otherwise `u64` if it fits. An immediately negated integer literal may represent
the exact `i64::MIN` value. An uncontextualized fixed-decimal literal receives
scale equal to its fractional digit count and the smallest valid precision that
contains its normalized coefficient and is at least its scale. An
uncontextualized string literal receives `string<N>` where `N` is its exact
UTF-8 byte length, with `N=1` for an empty string. Any numeric magnitude that
cannot inhabit its context or these inference rules fails
compilation; valid unary-negated `i64`, decimal, and money literals remain
allowed by the operator matrix.

Constraint solving is deterministic rather than traversal-dependent. A typed
nonliteral operand constrains a literal peer to its exact type. Two
uncontextualized integer literals use the smallest common default above; two
fixed-decimal literals must have equal scale and use the smallest common
precision; two string literals use the larger inferred bound. Mixed integer and
fixed-decimal literal categories without an enclosing exact decimal/money type,
or any other underconstrained/ambiguous literal, fail compilation. There are no
additional coercions.

An enum constant is exactly a two-segment `EnumName.VariantName` path resolved
to stable enum and variant IDs. In command value scope, one-segment paths resolve
to inputs or complete bindings and two-segment binding paths resolve fields; the
compiler rejects input/binding name collisions and any two-segment ambiguity
between a binding name and an enum type name. Entity invariants resolve
unqualified fields of their entity. Aggregate invariants and aggregate key
templates resolve unqualified root primary-key or root-field paths as allowed by
their context; grammar v1 aggregate invariants are root-record-only because no
bounded child instance is named by the source. Projection paths resolve source
event fields. The exact lowercase name `tx` is reserved from declarations;
`tx.time` is command-only and `tx.date` projection-only. Any other path shape is
invalid.

All command requirements are evaluated in source order against the initial
records, and the first false predicate constructs its declared rejection.
Entity and aggregate invariant templates are instantiated into explicit
binding/read and commit-validation expressions; a child mutation that requires
the aggregate root adds a declared root read template derived from the shared
key prefix. No invariant evaluation performs an undeclared runtime lookup.

`RootValidationReadId` is a dense zero-based `u32` plan-local identifier. A
command plan encodes its root-validation-read table immediately after source
bindings and before locality. Each entry contains its ID, the lowest
source-declared child `BindingId` that requires it, the root `EntityTypeId`, the
exact root `KeySchema`, an ordered tuple of input/constant-computable key
`ExprId`s, and an ordered duplicate-free set of root `FieldId`s accessed by its
aggregate commit checks. The accessed-field set may be empty for a constant
aggregate invariant because root presence and invariant application remain
required.

Checked root-key derivations use ADR-0016 canonical structural expression
equality after name resolution and lowering, not encoded `ExprId` identity or
runtime value equality. Tuples compare component by component. Trees compare
result type, node kind, exact stable resolved references, canonical constants,
unary operator/operand, and binary operator with ordered left/right operands.
Spans, aliases and source spelling, plan-local IDs, arena insertion order, and
shared-versus-duplicated DAG representation are ignored. No constant folding,
algebraic equivalence, or commutative reordering is performed.

Mutable children with equal derivations form one group whose representative is
its lowest source `BindingId`; groups and dense `RootValidationReadId` values are
ordered by representative. A plan uses no entry when an exact source-declared
root binding has the same structural derivation and supplies that root record;
the lowest matching root `BindingId` is canonical when more than one matches.

Each commit check encodes separately its ordered source `BindingId` application
subjects and ordered `RootValidationReadId` application subjects. These subjects
are not inferred from expression field dependencies: a constant invariant still
has an application subject. The aggregate-root read and the `0x0b` expression
tag are part of executable IR version 1 because its first public/durable freeze
has not occurred. They are included in command plan encoding, validation,
`PlanHash`, bundle compatibility, explain output, and generated `FORMAT.md`.

Expression contexts are validated, not inferred from the presence of a tag.
Command plans permit input, binding, constant, and `tx.time` expressions but
never source-event fields or `tx.date`. Command entity-binding key expressions,
and instantiated command partition/conflict derivations must use only validated
inputs and deterministic constants. After discarding parentheses, a v1
idempotency expression must be a direct reference to exactly one required,
nonoptional `string<N>` input with `1 <= N <= 128`; constants, compound
expressions, and other types fail compilation. That field alone is omitted from
the canonical input record before `CanonicalInputHash`, and its exact nonempty
UTF-8 bytes supply the ADR-0005 keyed digest. Projection plans permit constants,
source-event fields, and `tx.date`; projection `tx.date` is the UTC calendar date
deterministically derived from the originating event's fixed committed logical
`tx.time`, never an ambient clock. Projection plans contain no command bindings
or command inputs.

The designated idempotency input is secret-tainted and may be referenced only by
the command's `idempotency_key` declaration. It cannot appear in entity or
aggregate key derivation, a requirement, mutation, event, outcome, explain
value, or diagnostic value. Any other reference fails compilation; only its
bounded public input schema and redaction-safe field identity are generated.
This enforces ADR-0005 and `ID-005` rather than relying on adapters to notice a
leaked value later.

Entity, local-index, aggregate-partition, and aggregate-conflict key schemas and
derivations use only the closed component/composite registry, typed identities,
aggregate ownership convention, envelopes, and validation rules accepted by
ADR-0016. Their component order is semantic and is included in every transitive
command-plan closure. IR validation rejects a key plan whose purpose, owner,
component type, bound, maximum encoded size, composite framing, or root/child
mapping does not match its referenced schema.

Binding modes are read `0x01`, mutate `0x02`, and create `0x03`. A binding plan
contains the entity ID, typed key-expression tuple, accessed field set, and one
declared terminal outcome construction. For read/mutate this is the entity-absent
outcome; for create it is the duplicate-entity outcome. IR v1 has no implicit
not-found error or undeclared binding failure. The corresponding mandatory
read/mutate `else` grammar correction must be accepted in ADR-0015 before WP-040
implementation starts.

`BindingId` order is source declaration order. Snapshot materialization may
collect observations in any internal order but may not choose a business result.
Before requirements, the interpreter examines binding observations in ascending
`BindingId`: absent read/mutate and already-present create observations return
the first corresponding declared outcome. Only after every binding succeeds are
root-validation observations required in ascending `RootValidationReadId`; a
missing root is `ExecutionFault::Integrity`. Requirements are then evaluated in
source order. Storage/integrity failures remain execution failures and are not
converted into binding outcomes.

The v1 forward instruction tags are require `0x01`, set field `0x02`, emit event
`0x03`, and terminal return `0x04`. `require` either falls through or returns its
declared rejection; there are no arbitrary jumps. Exactly one return is last.
Creation is represented by an up-front create binding and its typed nonexistence
dependency, not storage I/O in the interpreter. Event payload and outcome field
expressions are encoded in increasing field-ID order while event occurrences and
instructions retain source semantic order.

Command evaluation uses deterministic mutable working records. Read bindings are
immutable pre-effect snapshots. Mutate bindings begin as snapshot records; create
bindings begin with key fields, optional non-key fields initialized to canonical
null, and required non-key fields uninitialized. A read of an uninitialized field
is rejected by compiler definite-assignment analysis and checked-plan validation.
All requirements execute before effects and observe the initial working records.
Each set right-hand side is evaluated against the current working records and the
assignment is then applied. Emit payloads are evaluated and captured at their
instruction position. Return expressions observe the final working records.
Expression nodes are pure templates evaluated at each reference and are not
cached across instructions. Key fields cannot be set, a command cannot set the
same `(BindingId, FieldId)` twice, and every required create field must be set
exactly once before any successful terminal return, whether or not the return or
an earlier emit observes the created record.

Projection aggregation tags are count `0x01` and checked sum `0x02`; the only v1
frontier tag is transactionally ordered `0x01`. Count has result type `u64`.
Sum accepts exactly nonoptional `i64`, `u64`, decimal, or money and retains the
exact operand type; accumulation is checked and a fault degrades the projection
without advancing its frontier under ADR-0010. Projection filters accept only
same-type equality, enum-value equality, and Boolean `&&` as accepted by
ADR-0002; `!=`, ordering, `||`, and arithmetic filter nodes fail projection-plan
validation. Group keys accept nonoptional Boolean, integer, decimal, money,
string, bytes, timestamp, date, UUID, or enum values. Accepted ADR-0017 owns their
canonical derived-state key/payload semantics; WP-060 freezes semantic projection
DTOs, WP-065 their durable envelope, WP-070 persistence, and WP-170 worker/query
behavior.

Execution classification is read-only `0x01` or idempotent mutation `0x02`.
The command locality and required-binding rules are those of ADR-0016. The only
v1 capability
requirement is derived `InvokeCommand(lineage, CommandId)` with tag `0x01`; no
contract-authored policy or retry clause is encoded.

Unknown or zero type, expression, operator, binding, instruction, aggregation,
frontier, execution-class, capability, ledger, bundle, grammar, or IR tags fail
closed. Decoders reject duplicate values, noncanonical order, invalid references,
non-topological arenas, type-invalid instructions, hash mismatch, unexpected
trailing bytes, and unsupported versions without returning a partial plan.

IR v1 has no named `QueryPlan`, state-machine instruction, random operation,
host callback, policy expression, dynamic dispatch, loop, or recursion slot.
Generic bounded query metadata derives from schema IR outside the executable
command stream. Adding any deferred executable form requires IR version 2 or a
later accepted compatibility decision; it is never accepted as an unknown v1 tag.

### Bounds

Bounds are checked before allocation and before multiplication/addition used for
capacity calculations:

| Boundary | v1 limit |
|---|---:|
| Encoded bundle bytes | 15 MiB (15,728,640 bytes) |
| Name bytes | 256 |
| Compiler-version bytes | 64 |
| Type/expression nesting | 32 |
| Active semantic declarations and total expression nodes | 131,072 each |
| Stable-ID ledger entries including tombstones | 262,144 |
| Commands, entities, events, enums, aggregates, or projections | 4,096 per kind |
| Fields, variants, outcomes, bindings, instructions, or measures in one owner | 4,096 |
| Tuple/object/list entries in one encoded IR value | 1,024 |
| One generated JSON Schema artifact | 1 MiB |
| Compiler diagnostics returned | 32 |

In addition to the independent declaration bounds above, every mutating command
plan is rejected before hashing when a conservative maximum successful v1 shape
can exceed any of these pre-sequence semantic limits:

| Derived command boundary | v1 limit |
|---|---:|
| Index-entry mutations | 4,096 |
| Mutation-affected index-prefix targets | 4,096 |
| Binding + root-validation + mutation-affected-prefix validation positions | 4,096 |
| Affected-prefix targets or their current epoch observations | 16 MiB |

The estimator uses checked arithmetic and the declared mutable bindings,
assigned fields, index fields, component maxima, and complete leading-prefix
semantics. It may sum possible non-whole prefixes across bindings even when
particular runtime values would deduplicate; that conservative lower acceptance
limit is intentional. Whole-index targets are counted once per affected stable
`IndexId`, and unchanged leading components before the earliest possibly
assigned index component are not double-counted for one replacement. No value
expression is evaluated and no runtime-value equality is assumed.

These checks are constructor validation, not new serialized plan data. Existing
bundle decoding re-enters checked constructors and therefore rejects a formerly
constructible oversized v1 plan. That is an intentional semantic tightening;
the IR-v1 byte layout and hash framing do not change. Runtime and storage retain
the same exact incremental limits as defense in depth.

The catalog independently owns these active-lineage limits:

| Catalog-resolved boundary | v1 limit |
|---|---:|
| Active bundles from genesis through active | 4,096 |
| Sum of exact active-lineage canonical bundle bytes | 64 MiB (67,108,864 bytes) |
| Process-local lineage-materialization proof charge | 2 MiB (2,097,152 bytes) |

The canonical-byte sum uses checked arithmetic over each exact
`ContractBundle::canonical_bytes().len()` and excludes envelopes, keys,
historical-page framing, and allocator overhead. These bounds neither replace
nor relax the 15 MiB per-bundle limit. The 4,096 active-lineage limit is a
semantic bound below the generic 65,535 catalog-bundle backup bound; the 64 MiB
aggregate is distinct from the 15 MiB bundle and 16 MiB evidence-page bounds.

The 1 MiB headroom below ADR-0006's absolute 16 MiB payload/envelope ceiling is
reserved for the catalog Protobuf wrapper and `StoredEnvelope`. WP-050 must prove
the 15 MiB semantic bundle bound with a maximum-size fixture. WP-065, which owns
the durable Protobuf wrapper and `StoredEnvelope`, must prove with that fixture
that the complete encoded envelope remains at or below 16 MiB. A semantic owner
may impose a lower bound where the source grammar already does.

### Compiler and execution version policy

The compiler embeds one nonempty ASCII semantic-version string constant owned by
`riffdb-contract-compiler`; callers cannot override it. Any change that can alter
canonical bundle output for equal inputs must change that compiler version and
regenerate compatibility fixtures. Git revisions, dirty state, and build time are
not compiler identity and do not enter bundle bytes.

The POC compiler emits only bundle format 1, grammar 1, and IR 1. The POC
validator and runtime execute exactly IR 1 and reject every other version. There
is no implicit migration, best-effort downgrade, or previous-version execution
window. An IR semantic or tag change creates a new version and requires an
explicit reader/executor decision.

Activated historical bundles are immutable and retained while referenced by a
pending reservation, persisted outcome, commit, or provenance record. Pending
work resolves the exact bundle by lineage, application version, bundle hash, and
command plan hash. Missing bundle, unsupported IR, or any hash mismatch fails
closed. Bundle garbage collection is outside the POC.

### Hash topology

ADR-0011 remains the sole owner of SHA-256, domain labels, and outer hash framing.
Before WP-040, ADR-0014 must explicitly extend its registry with the distinct
`ProjectionPlanHash` domain `riffdb.projection-plan/v1` and
`ContractPlanRootHash` domain `riffdb.contract-plan-root/v1`, with matching
newtypes and typed hash functions in `riffdb-types`. The existing `PlanHash` and
`riffdb.plan/v1` remain command-plan-only. This ADR defines the canonical payload
supplied to each function.

- `SourceHash` hashes the exact validated UTF-8 source bytes, including comments,
  whitespace, and line endings. No normalization is applied.
- Each emitted JSON Schema artifact has a `SchemaHash` over its canonical UTF-8
  bytes. Contract structural schema IR uses the same schema domain with the
  distinct internal prefix `RIFFDB-SCHEMA-IR\0`, IR version, and canonical
  structural-schema bytes.
- A command `PlanHash` hashes `RIFFDB-COMMAND-PLAN\0`, IR version, `CommandId`,
  and the canonical transitive semantic closure of that command.
  The closure includes referenced type/key layouts, input/outcome schema hashes,
  partition/conflict derivations, bindings and read templates, invariant and
  commit-check plans, instructions, outcomes, events, execution class, and
  capability requirement. It excludes names used only for display, spans,
  source hash, compiler/application versions, compatibility report, and the
  stored plan hash itself.
- A `ProjectionPlanHash` hashes `RIFFDB-PROJECTION-PLAN\0`, IR version,
  `ProjectionId`, source-event schema, filter, key, measures, and frontier.
- The bundle's `ContractPlanRootHash` hashes `RIFFDB-CONTRACT-PLAN-ROOT\0`, IR
  version, the structural schema hash, and ordered `(stable ID, typed plan hash)`
  pairs for every command and projection. Unrelated source-only spelling does not
  change it; adding or changing executable/schema semantics does.
- `ContractBundleHash` hashes the complete canonical bundle bytes. It is stored
  beside or around the bundle, never inside its own preimage.

Stored command, projection, and root hashes are recomputed during checked
decoding. Canonical bundle bytes include the root hash but never the external
bundle hash, so no hash is self-referential. Adding an unrelated command changes
the root and bundle hashes but does not change an existing command's transitive
plan hash.

### Deterministic JSON Schema

Schema artifacts use JSON Schema draft 2020-12. The compiler first builds a
closed ordered schema IR; it does not construct schemas through hash maps. The
canonical emitter writes no insignificant whitespace, orders every object key by
raw ASCII byte order, preserves semantic array order, emits integers in shortest
base-10 form, writes lowercase Boolean/null, leaves `/` and non-control Unicode
unescaped, and uses JSON short escapes or lowercase `\u00xx` for control bytes.
Generated keys are ASCII. Duplicate object keys are impossible by construction.
Every root artifact contains the exact dialect URI
`https://json-schema.org/draft/2020-12/schema` in `$schema`.

`riffdb-contract-ir` checks in `JSON_SCHEMA_FORMAT.md`, generated from the same
closed declarative schema-format registry used by the schema-IR validator and
emitter. It enumerates the complete object tree, keyword inventory, extension
keyword types, required-array order, and inlining rule for every artifact kind
and value type. Registry data, validator, emitter, document, and golden schemas
must byte-compare in CI. The initial registry/document is a public interface and
requires maintainer review in the first WP-040 IR/schema interface PR before any
schema fixture merges.

Schema v1 is fully inline: it emits no `$id`, `$anchor`, `$defs`, `$ref`,
`title`, `description`, `examples`, `format`, `unevaluatedProperties`, or
unlisted keyword. Only an artifact root has `$schema`; nested schemas do not.
Primitive/value construction is exactly Boolean `{"type":"boolean"}`;
integers with `type`, exact `minimum`, and exact `maximum`; decimal and money as
the string forms and extension metadata below; bounded string with `type` and
`x-riffdb-maxUtf8Bytes`; bytes with `type`, `contentEncoding`, and
`x-riffdb-maxDecodedBytes`; date as the bounded integer; UUID as a string with
the exact lowercase-hyphenated pattern
`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`; enum as a
string `enum` array in `EnumVariantId` order; optional as `oneOf` containing the
nonoptional inner schema then `{"type":"null"}`; and list as an array with
`items` and `maxItems`. Record/object construction and timestamp are fixed below.
The registry rejects any semantically equivalent alternate construction.

The v1 value mapping is Boolean to JSON Boolean and `i64`/`u64` to JSON integer
with exact numeric minimum/maximum. Decimal and money use a scale-preserving JSON
string. For decimal `<P,0>` the pattern is
`^-?(0|[1-9][0-9]{0,P-1})$`; for `0<S<P` it is
`^-?(0|[1-9][0-9]{0,P-S-1})\.[0-9]{S}$`; and for `S=P` it is
`^-?0\.[0-9]{S}$`, with the symbolic bounds expanded to literal integers by the
emitter. Negative zero is accepted at input and normalizes to canonical positive
zero before hashing. Money uses the precision-38, scale-2 rule above.

String maps to JSON string with `x-riffdb-maxUtf8Bytes`. Bytes use the standard
padded RFC 4648 base64 alphabet in a JSON string with
`contentEncoding: "base64"` and `x-riffdb-maxDecodedBytes`. Timestamp is the
closed object `{seconds, nanos}`, where seconds is a canonical signed decimal
string matching `^-?(0|[1-9][0-9]*)$` with string extension
`x-riffdb-integerType: "i64"`, and nanos is an integer in
`0..=999999999`; its `required` array is exactly `["seconds","nanos"]` and it
has `additionalProperties: false`. Date is an integer day count in the full
`i32` range; UUID is the lowercase hyphenated 36-byte form; enum is
its closed display-name enum; optional is inner-or-null; list is a bounded array;
and record is an object keyed by exact contract field names. Decimal schemas
carry the exact pattern plus integer `x-riffdb-decimalPrecision` and
`x-riffdb-decimalScale`. Money carries those same decimal keywords plus exact
string `x-riffdb-moneyCurrency`. Runtime conversion enforces all compiled byte,
precision, normalization, and numeric bounds. The RiffDB schema validator must
enforce the `x-riffdb-*` bounds; they are not documentation-only extension
keywords.

Command input objects, declared outcome payloads, and emitted active-version
record objects are closed with `additionalProperties: false`. Required arrays and
outcome `oneOf` entries are ordered by stable field/outcome ID. Optional input
properties are absent from `required` and carry `default: null`; omission and an
explicit JSON null both normalize to a present `CanonicalValue::Null` field
before canonical input hashing or evaluation. The same null fill applies to
omitted optional create, event, and outcome object fields. Canonical JSON output
emits those fields explicitly as null. Entity records, durable-event payloads,
projection results, and outcome variants require every declared property in
their output schemas, with optional values represented by a nullable property;
source construction may omit an optional event/outcome field only because the
compiler inserts canonical null before execution.

The direct idempotency input property additionally has JSON Schema
`minLength: 1` and `x-riffdb-minUtf8Bytes: 1`; runtime validation uses the byte
bound. Every command outcome union is a root `oneOf` whose variants are flat,
closed objects. Each variant has a required property named `type` whose schema is
`{"const":"<exact outcome name>"}`, followed by every declared payload
property. `type` is first in the variant's `required` array and payload fields
follow in `FieldId` order; all payload properties are required even when their
value schema is nullable, because canonical output emits explicit null. A source
outcome payload field named exactly `type` fails compilation. Variant entries are
ordered by `OutcomeId`, and no nested `payload` wrapper or alternate
discriminator is emitted.

The projection-result artifact describes one public result row, not the later
query response envelope. Its root is a closed object with exactly `key` and
`measures`, both required in that order. `key` is a fixed-length JSON array whose
schema has `prefixItems` in projection source order, `items: false`, and equal
`minItems`/`maxItems` set to the key-component count. `measures` is a closed
object whose properties use exact measure names and result schemas; every measure
is required in `FieldId` order. The root and measures objects both have
`additionalProperties: false`. Query pagination, frontier, wait status, and row
collection envelopes are owned later by the shared service/WP-170 and are not
part of this hashed row schema.

Every source-declared outcome field is present in the schema; generators must not
copy the illustrative SPEC schema that omits `Allocated.budget` and permits
arbitrary extras. Historical unknown durable fields are retained internally but
are not silently exposed through a contract version that does not declare them.

Each command bundle entry carries canonical input and declared-outcome schema
artifacts plus its execution classification. The outcome union is WP-040's
command output schema. The shared service owns one later versioned generic
operation-envelope schema and MCP mechanically composes it with the bundle's
outcome union; neither service nor MCP maintains a parallel command-specific
schema. WP-040 does not invent a sequence for an unjournaled read-only command.
Accepted ADR-0004, ADR-0007, and ADR-0012 close grammar-v1 read-only execution as
unjournaled and outcome-recovery-free; the service result is `ReadOnlyExecuted`,
and accepted ADR-0006 reserves public status `EXECUTED_READ_ONLY = 3` with exact
absence sentinels. A durable read-only result or fault requires a future accepted
ADR.

### Compatibility

The closed compatibility classes are `Compatible` tag `0x01`,
`RequiresExplicitVersion` tag `0x02`, and `Incompatible` tag `0x03`; the report's
overall class is its most restrictive entry. The comparator uses stable IDs,
canonical type/key layouts, typed plan hashes, and lineage metadata rather than
only names.

A genesis bundle has no parent, an empty entry list, and explicit overall class
`Compatible`; this is the defined empty-report identity. A successor with no
semantic change has exactly one contract-root `RDB-K001` entry. A successor with
another reported change omits `RDB-K001`, and its explicit overall class must
equal the most restrictive nonempty entry or validation fails.

#### Active-lineage proof and exact omission authority

Compatible optional-field evolution has one exact normalization rule backed by
catalog-proved ancestry. Both `ActiveCatalogSnapshot::read` and
`resolve_executable_plan` load the current active pointer, walk active to genesis
through exact parent `(ContractVersion, ContractBundleHash)` references, then
reverse and revalidate every adjacent successor with the same pure compatibility
comparator used for activation. Gaps, cycles, repeated versions, hash
substitution, lineage mismatch, unsupported versions, or an active-lineage
count/byte excess reject. Numeric version comparison is never ancestry proof.
The executing bundle and plan must be an exact chain member, allowing a pending
historical plan to consume records written by validated ancestors or descendants
without admitting a foreign writer.

Successful resolution returns a `ResolvedExecutablePlan` carrying one shared
`Arc<LineageMaterializationProofV1>` owned and privately constructed by
`riffdb-catalog`. It is process-local, nonserializable, non-durable,
non-Protobuf, absent from canonical bundle bytes and every hash, and forbidden
across a storage trait. Its exact accounting frame is:

```text
u8 proof_version (= 1)
u32 lineage_len || lineage
u32 bundle_count
repeated(u64 ContractVersion || [u8; 32] BundleHash)
u16 executing_bundle_ordinal
u32 owner_count
repeated(
  u8 owner_tag { entity = 1, event = 2 }
  || u32 owner_id
  || u32 field_count
  || repeated(u32 FieldId || u16 introduced_at_ordinal)
)
```

Integers are big-endian. Ordinals are zero-based and at most 4,095;
`BundleHash` is the exact 32-byte `ContractBundleHash`. Bundle entries are
genesis-to-active; owners are unique and ordered by
`(owner_tag, owner_id)`; fields are unique and ordered by `FieldId`. The lineage
is at most 256 bytes, there are at most 8,192 entity/event owners, and field
entries use the existing 262,144 lineage-ledger ceiling. The exact worst-case
charge is:

```text
1 + (4 + 256) + 4 + (4_096 * 40) + 2 + 4
  + (8_192 * 9) + (262_144 * 6)
= 1_810_703 bytes
```

That leaves 286,449 bytes below the fixed 2 MiB proof cap. Checked arithmetic
and compile-time assertions freeze the relationship; changing a constituent
bound so the assertion fails requires human review. The proof derives each
field's first introduction ordinal from the revalidated parent and child
schemas, never from a compatibility report alone. A synthetic checked-charge
calculator test accepts exactly 2 MiB and rejects one byte more; a separate
valid-v1 maximum test proves 1,810,703 bytes and the 286,449-byte headroom. The
synthetic cap test is not evidence that a valid v1 proof can reach 2 MiB.

For an exact stored writer and executing descendant schema, the proof produces
an opaque bit mask over executing fields in ascending `FieldId` order. Its exact
length is `ceil(field_count / 8)`, unused high bits are zero, and its maximum is
512 bytes for 4,096 fields. The existing maximum 4,096 combined binding and
aggregate-root positions therefore bounds transient masks independently at
an exact 2 MiB structural maximum. It is not additional capacity: retained
normalized `ReadSnapshot` semantic bytes plus every retained nonempty mask byte
of bitset payload must together fit the existing 16 MiB command-snapshot
ceiling. Binding/root and mask vectors are aligned, so position is implicit and
has no separate metadata charge. Exact-equality and one-byte-over combined-
charge tests freeze that rule. Neither a mask nor its cache is encoded or hashed.

Checked materialization supplies `CanonicalValue::Null` if and only if the
writer is an exact chain member,
`writer_ordinal < field_introduction_ordinal <= executing_ordinal`, and the
executing field is optional-with-null-default. An omission from the exact writer
schema, a descendant writer, or a genesis-declared field cannot be filled. A
missing required field, foreign lineage/version/hash, malformed mask, or any
other unproved omission is integrity. Each valid inserted null adds exactly six
canonical bytes: `FieldId:u32` plus the canonical value-version and null-tag
bytes. Existing 1 MiB record, 16 MiB snapshot, and evaluation limits remain in
force.

Fields present in storage but unknown to the exact historical plan being
executed are not visible to its expressions and are never silently discarded:
normalization retains their canonical values, and runtime carries those values
unchanged into each complete canonical mutation post-image unless the historical
plan explicitly addresses a field it knows. Storage still receives and validates
only complete post-images; it performs no field-level merge. Immutable events are
never persistently decoded and re-encoded merely to add null fields.

For projection application, `riffdb-catalog` resolves the exact projection
bundle and plan and owns an opaque process-local event-materialization view. It
combines that exact resolution with the enclosing commit's exact
`ExecutablePlanRef` and immutable durable event. The view is nonserializable,
non-durable, non-Protobuf, excluded from every canonical byte string and hash,
and forbidden across a storage trait. It applies the same introduction ledger
and null-fill rule above: only a strict-ancestor writer can authorize an omitted
optional-with-null-default field. A foreign writer, or an exact, descendant,
genesis, or required-field omission, is rejected. A complete descendant event
may retain fields unknown to the resolved projection plan; those values remain
unchanged and invisible to its expressions. Both live catch-up and rebuild use
this view. Neither catalog nor projection modifies or replaces the original
payload, event bytes, `EventHash`, or stored copies, and storage remains IR-blind.

Activation preparation walks the exact current chain and checks, with checked
arithmetic, `current_count + 1`, current canonical bytes plus the candidate's
exact canonical bytes, and the rebuilt proof charge before coordinator
submission. Concurrent activation is still resolved by the coordinator's exact
expected-active comparison. Count excess maps through the service to one root
`ValidationCode::TooManyItems`; canonical/proof byte excess maps to one root
`ValidationCode::TooLong`. Both use public `Validation` plus `CorrectRequest`
and ordinary authenticated service audit. A storage read remains
`CatalogError::Storage`.

Startup reconstructs and validates the same exact chain, compatibility edges,
three ceilings, and proof. A broken or over-limit active history is
`InvalidHistoricalEvidence`, with readiness false and a redacted public
`InternalDefect`. A proof or cap invariant reached after an admitted activation
is also internal integrity, never `ExecutionFault::ResourceLimit`.

New admission validates and hashes input with the active plan. If idempotency
lookup finds an existing pending or terminal identity, the application service
loads its exact stored bundle/plan before final input comparison and normalizes
against that historical input schema. Fields added later along the validated
compatible lineage are accepted by that historical normalization only when they
are absent or explicit null and are then omitted; a non-null value or unrelated
unknown field fails without execution. The resulting historical canonical input
record, with its historical idempotency field omitted, must reproduce the stored
`CanonicalInputHash`. Active-schema null fill is never allowed to alter an
existing reservation's identity or hash.

The exhaustive grammar-v1 `Compatible` changes are no semantic change (including
comments/whitespace), adding a command, adding an event type, adding a
projection, and adding an optional entity, event-payload, or command-input field
whose only v1 default is null. Adding an outcome or an optional field to an
existing outcome is `RequiresExplicitVersion`. Because the POC does not yet have
one explicit-version mechanism shared by gRPC, MCP, CLI, and SDK calls, WP-050
refuses activation of that class; a later accepted shared-service/MCP decision
may permit it only when every caller explicitly pins the new application version.

An optional entity-field addition that becomes newly visible through a complete
entity record already present in an existing command outcome is also
`RequiresExplicitVersion`, not `Compatible`; it changes that command's public
outcome union transitively. The comparator follows record references when making
this classification. Adding an optional entity field remains `Compatible` only
when no existing command outcome transitively exposes the changed complete
record.

Every other change is `Incompatible`, including removal, tombstone resurrection,
ID reuse, a new entity/enum/variant/aggregate/index/invariant, type or key change,
changed idempotency/partition/conflict derivation, invariant change, outcome
payload change other than the conditional optional addition above, event meaning
change, projection-result shape change, IR-version change, or a semantic change
to an existing stable plan that is not solely caused by an explicitly compatible
optional-field addition. Compilation may produce such a report for review, but
WP-050 refuses activation under the POC policy.

The stable entry-code registry is `RDB-K001` no semantic change, `RDB-K010` added
command, `RDB-K011` added event, `RDB-K012` added projection, `RDB-K013` added
optional field, `RDB-K020` added outcome requiring explicit version,
`RDB-K021` added optional outcome field requiring explicit version,
`RDB-K100` removed identity, `RDB-K101` tombstone resurrection, `RDB-K102` ID
reuse, `RDB-K103` type change, `RDB-K104` key-layout change, `RDB-K105`
idempotency change, `RDB-K106` partition/conflict change, `RDB-K107` invariant
change, `RDB-K108` outcome change, `RDB-K109` event change, `RDB-K110` existing
plan change, `RDB-K111` unsupported addition, and `RDB-K112` IR-version change.
Entries identify affected stable paths without copying unbounded source text.
Unknown codes or code/class disagreements are invalid bundle bytes.
For a transitively exposed optional entity field, the report contains its
`RDB-K013` entity-field entry and one `RDB-K021` entry for each affected command
outcome path; the latter raises the overall class to `RequiresExplicitVersion`.

### 2026-07-13 companion clarifications

ADR-0004's `ExecutablePlanRef` is the required checked historical command-plan
identity at every admission/snapshot/intent/outcome/commit/provenance boundary:
contract lineage, contract version, `ContractBundleHash`, stable `CommandId`, and
command `PlanHash`. The bundle and plan hashes defined here remain unchanged;
the complete tuple prevents active-plan substitution or ambiguous historical
lookup.

ADR-0012 owns the runtime result and durable admission disposition of the checked
arithmetic/resource faults defined here. Runtime returns no `EvaluatedCommand` or
`CommitIntent` for such a fault. Only the coordinator may dependency-validate and
terminalize it as the closed non-commit `ExecutionFailed` admission state. This
clarification does not turn a fault into a declared business outcome or alter IR
arithmetic semantics.

ADR-0017's immutable `ProjectionGroupSchema` contains the nonzero
`ProjectionId`, group/measure types, codecs, and bounds, but not
`ProjectionPlanHash` or `ProjectionIdentity`: its canonical bytes are already in
the projection-plan-hash preimage. After that hash is computed or verified, the
checked bundle exposes `BoundProjectionGroupSchema`, pairing the exact schema
with contract lineage, projection ID, and plan hash. This avoids a self-reference
and is the only projection IR value the storage projection-schema module may
consume; contract IR retains no storage dependency.

## Options Considered

1. **Deterministic allocation plus explicit lineage ledger:** Selected. Source IDs
   would enlarge grammar v1, while name/hash-only reconstruction cannot preserve
   tombstones across evolution.
2. **Checked custom IR codec:** Selected. It preserves IR ownership and the
   WP-040 dependency graph; a later Protobuf catalog record carries rather than
   redefines the bytes.
3. **Protobuf as the IR itself:** Rejected for v1 because WP-040 does not depend on
   WP-020 and semantic IR crates must not depend on generated transport/durable
   DTOs.
4. **One POC execution version:** Selected. A compatibility window without an
   existing historical format would be speculative.
5. **Typed command/projection hashes plus aggregate root:** Selected. One
   monolithic hash would invalidate unrelated command identity and make
   uncertainty recovery and projection rebuild identity coarser.
6. **Compiler-derived ordered schema IR:** Selected. Hand-maintained MCP schemas
   violate `MCP-020`, and generic map serialization is not canonical.

## Consequences

- WP-040 must add required path dependencies and update the root lockfile; its
  allowed paths need an explicit `Cargo.lock` correction before implementation.
- ADR-0015 and the canonical contract must add declared read/mutate absence
  outcomes before binding plans freeze; no implementation may synthesize one.
- ADR-0014 and the WP-010 types must add the projection-plan and contract-root
  hash domains/newtypes before WP-040 uses them.
- ADR-0016 and the WP-010 types must add typed partition identity and the closed
  compiled key-component registry before WP-040 publishes key schemas.
- A later proto-owner interface PR defines the catalog payload that carries
  canonical bundle bytes and their external hash under `StoredEnvelope`.
- Stable-ID, codec, tag, order, bound, schema-mapping, and hash changes require a
  new compatible format/version decision and fixtures.
- Source-only edits may change source/bundle hashes while leaving transitive plan
  hashes unchanged.
- Tombstones make lineage metadata grow monotonically, bounded by the v1 ledger
  and bundle limits.
- Catalog resolution now has one bounded, process-local ancestry proof shared by
  active and historical execution. This adds no durable/protocol field, bundle
  byte, IR tag, plan-hash input, or storage dependency.
- WP-050 owns exact active-chain walking, forward comparator replay, activation
  admission, startup validation, field-introduction derivation, proof framing,
  opaque masks, and the opaque process-local projection event-materialization
  view. WP-100 owns the two command-record applications; WP-080 receives only
  normalized records and rejects every remaining omission. WP-170 depends
  directly on WP-050 and consumes the catalog view for both live catch-up and
  rebuild; it does not interpret raw durable payloads against contract IR.
- Custom codec code is security-sensitive and requires malformed-byte property or
  fuzz coverage in addition to golden vectors.

## Security

All lengths and counts are validated against both remaining bytes and hard limits
before allocation. Checked arithmetic prevents length overflow. Decoder failures
are typed and bounded and do not expose source values, contract data, raw keys, or
internal error chains. Content hashes are integrity identities, not signatures or
authorization. A syntactically valid but unsupported or hash-inconsistent plan
never reaches runtime.

An untrusted stored writer version, a numerically plausible version, or a stored
compatibility report cannot authorize a null fill. Only the catalog's exact
parent/hash chain and schema-derived introduction ledger can do so. Proof and
mask lengths are checked before allocation, and diagnostic/public-error paths
never disclose lineage, contract bytes, field values, or hashes.

## Testing

- Golden genesis/successor ledgers, tombstones, allocation-bound rejection, canonical
  bundle bytes, schema bytes, and every hash layer.
- Repeated compilation equality with the same source/compiler/lineage,
  plus permutation tests proving declaration order does not alter genesis IDs.
- Unchanged-command hash stability when unrelated commands are added.
- Operator-matrix, contextual-literal, exact numeric boundary, short-circuit,
  path-resolution, optional/null, and checked-arithmetic-fault tests.
- Decoder properties for arbitrary bytes, boundary lengths/counts, every unknown
  tag/version, duplicate/out-of-order IDs, bad references, non-forward arenas,
  trailing bytes, and stored-hash mismatch.
- Stable diagnostic snapshots plus semantic assertions for duplicate/unknown
  names, type errors, hidden reads, non-input conflict derivation,
  cross-partition mutation, undeclared events/outcomes/fields, invalid creation,
  unsupported projection operations, and incompatible evolution.
- Exact schema snapshots for every value type and all budget outcomes, including
  the flat discriminator, complete `Allocated` payload, metadata keywords, and
  reserved-name rejection; round-trip input/outcome conversion tests.
- Registry/document reproducibility and exact projection-row snapshots covering
  fixed tuple keys and count/decimal/money measure objects.
- Ancestor-record/event null materialization, missing-required failure,
  historical-plan unknown-field preservation, deployment-between-retry hashing,
  and transitive full-record outcome compatibility fixtures.
- Projection catch-up/rebuild fixtures proving strict-ancestor optional event
  null materialization, foreign-writer and exact/descendant/genesis/required
  omission rejection, unknown-field preservation, and byte-for-byte immutable
  durable event payloads and hashes.
- Catalog boundary tests cover 4,096 versus 4,097 active bundles, exactly 64 MiB
  versus one byte more summed canonical bundle bytes, a synthetic checked proof-
  charge calculator at exactly 2 MiB and one byte more, and the separately valid
  1,810,703-byte v1 maximum with its 286,449-byte headroom. Snapshot accounting
  accepts normalized semantic bytes plus retained nonempty mask bitset payload
  bytes at exactly 16 MiB and rejects one byte more; aligned vectors add no
  position-metadata charge. Compile-time assertions
  bind proof and mask arithmetic to their IR constituent limits.
- Lineage fixtures cover gaps, cycles, repeated versions, parent-hash
  substitution, wrong lineage, exact ancestor/equal/descendant writer masks,
  genesis and exact-writer omissions, required omissions, malformed masks,
  unknown-field retention, and activation/startup error classification.
- Multiple binding failures proving ascending-`BindingId` outcome priority.
- Root-validation equality fixtures proving `ExprId` and arena-allocation
  independence, shared-versus-duplicated DAG equality, ordered binary structure,
  lowest-child grouping/ID order, and lowest matching source-root suppression.
- Unsupported IR rejection and historical exact-plan lookup tests in downstream
  catalog/runtime packages.

## Requirements and Work Packages

- **Requirements:** `ID-003`, `ID-005`, `CMP-001`, `CMP-020` through `CMP-022`,
  `DSL-003` through `DSL-012`, `ENT-002`, `ENT-003`, `TXN-001`, `TXN-002`,
  `TXN-012`, `TXN-030`, `MCP-020`, `REC-001`, and `REC-002`
- **Defines or blocks:** focused foundational `WP-010` follow-up, `WP-040`,
  `WP-050`, `WP-060`, formal durable-schema `WP-065`, `WP-080`, and formal
  public-schema `WP-127`, plus `WP-170` projection consumption
- **Final evidence:** `WP-140`, `WP-200`

## Decision Deadline

The exact text must be accepted before WP-040 publishes stable IDs, schema
artifacts, executable IR, plan hashes, or bundle fixtures. ADR-0003, ADR-0010,
ADR-0014, ADR-0015, and ADR-0016 must also be Accepted and listed by
`work_packages.yaml` before implementation starts. The accepted lineage
clarification must be present before WP-050 publishes active/historical plan
resolution, before WP-100 publishes record normalization, and before WP-170
publishes event consumption or rebuild semantics.
