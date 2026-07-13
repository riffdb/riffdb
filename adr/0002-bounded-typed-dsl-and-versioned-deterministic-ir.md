# ADR-0002: Bounded Typed DSL and Deterministic Compilation Boundary

- **Status:** Accepted
- **Direction approved:** 2026-07-12
- **Exact text accepted:** 2026-07-12
- **Contextual-keyword clarification accepted:** 2026-07-12
- **Decision deadline:** Before WP-030 grammar implementation

The human maintainer accepted this exact grammar, dependency, and compilation
boundary on 2026-07-12.

## Context

SPEC Sections 7.2 and 23.1 now contain the same canonical LegalSpend source.
Earlier drafts disagreed, but SPEC 0.2 reconciled them before this decision.
The remaining ambiguity is intentional: the specification assigns the complete
grammar to `riffdb-contract-syntax` and leaves several possible future surfaces
without productions. Parser, compiler, catalog, runtime, schema generation, and
compatibility work need one bounded initial language rather than guessed aliases.

The specification also requires a statically analyzable command binding for
initial entity creation, but did not fix its syntax or duplicate-key outcome.
That binding must be explicit before the grammar freezes because direct storage
or administration seeding is forbidden.

## Decision

The initial language is grammar version 1 and uses exactly one UTF-8 source
document containing exactly one top-level contract:

```text
contract Ident version UInt { declaration* }
```

There are no modules, imports, includes, scalar aliases, multiple-contract
documents, semicolons, or alternate top-level spellings. Whitespace is
insignificant. `//` line comments are accepted; block comments are rejected.
Identifiers are case-sensitive ASCII `[A-Za-z_][A-Za-z0-9_]*`, are at most 256
bytes, and are not normalized. Lowercase reserved keywords may not be used as
identifiers except for the contextual keyword `idempotency_key`, which is an
identifier in identifier positions and the clause keyword immediately before an
idempotency expression. This exception is required by the canonical LegalSpend
source and does not apply to any other reserved keyword.

Unsigned integer literals are base-10 with no sign, exponent, radix prefix, or
separator. Fixed decimal literals contain decimal digits, one dot, and at least
one digit on both sides. Negative values use unary `-`. Boolean and null literals
are `true`, `false`, and `null`. String literals use double quotes and JSON escape
rules. Source spelling and spans are retained in the AST; WP-040 performs typed
numeric, string, and schema-bound validation.

### Declarations

The exact grammar-version-1 declaration set is:

```text
declaration = entity | event | enum | aggregate | command | projection

entity = "entity" Ident "{" entity_item* "}"
entity_item = key | field | invariant | index
key = "key" "(" typed_field_list ")"
field = "field" Ident ":" type
invariant = "invariant" Ident ":" expr
index = "index" Ident "(" ident_list ")"

event = "event" Ident "{" typed_field* "}"
typed_field = Ident ":" type

enum = "enum" Ident "{" ident_list "}"

aggregate = "aggregate" Ident "{" aggregate_item* "}"
aggregate_item = root | child | partition_by | conflict_key | invariant
root = "root" Ident
child = "child" Ident
partition_by = "partition_by" expr
conflict_key = "conflict_key" "(" expr_list ")"

projection = "projection" Ident "{"
    source_event where_clause? projection_key measure+ frontier
"}"
source_event = "source" "event" Ident
where_clause = "where" expr
projection_key = "key" "(" expr_list ")"
measure = "measure" Ident "=" ("count" "(" ")" | "sum" "(" expr ")")
frontier = "frontier" "transactionally_ordered"
```

`typed_field_list`, `ident_list`, `expr_list`, and object field lists are
comma-separated, nonempty, and accept one trailing comma. Enum variant lists
are also nonempty. Entity and aggregate item order is source-oriented; WP-040
rejects missing singleton items, duplicate singleton items, duplicate names,
unknown fields, unsupported key shapes, or invalid aggregate ownership.
Projection `where` expressions compile only from equality/enum-value filters
and Boolean conjunctions; other parsed expression shapes fail compilation.

The type syntax is exactly:

```text
bool | i64 | u64 | timestamp | date | uuid
decimal<UInt, UInt> | money<CURRENCY>
string<UInt> | bytes<UInt>
optional<type> | list<type, UInt> | Ident
```

`CURRENCY` is exactly three uppercase ASCII letters. A named `Ident` type may
resolve only to a declared enum in grammar version 1. Precision, scale, length,
collection, recursive-type, and compatibility rules are compiler semantics,
not parser guesses.

### Commands and creation

Command items occur in fixed phases:

```text
command = "command" Ident "{"
    input* idempotency? binding* require* effect* return
"}"
input = "input" Ident ":" type
idempotency = "idempotency_key" expr
binding = read | mutate | create
read = "read" Ident "(" expr_list ")" "as" Ident
mutate = "mutate" Ident "(" expr_list ")" "as" Ident
create = "create" Ident "(" expr_list ")" "as" Ident
         "else" outcome_expr
require = "require" Ident ":" expr "else" outcome_expr
effect = set | emit
set = "set" path "=" expr
emit = "emit" Ident object
return = "return" Ident object
outcome_expr = Ident object
object = "{" object_field_list? "}"
object_field = Ident ":" expr
```

A `create` binding is an up-front mutable binding and a nonexistence dependency.
WP-040 must prove its key expressions are computable from validated command
inputs, key fields cannot be assigned, every required non-key field is assigned
exactly once on the creation path, and duplicate existence returns the declared
rejection outcome. Creation uses the compiled command and commit-coordinator
path; it is not a storage or administration bypass.

A command containing `mutate`, `create`, `set`, or `emit` must compile with
exactly one idempotency declaration. `read` and `mutate` bindings retain the
canonical Section 7.2 spelling. `set` and `emit` may interleave in their effect
phase. Exactly one success `return` is final. There is no other control flow or
effect syntax.

### Expressions

Primary expressions are literals, identifier paths, and parenthesized
expressions. Paths are identifiers separated by dots and include the reserved
transaction paths `tx.time` and `tx.date` where their compiler context permits.
Object expressions occur only after `emit`, `else`, or `return` and are not
general maps or records.

Operator precedence from tightest to loosest is unary `!` and `-`; `*` and `/`;
`+` and `-`; `==`, `!=`, `<`, `<=`, `>`, and `>=`; `&&`; then `||`. Binary
operators are left-associative. Generic function calls, dynamic dispatch,
indexing, assignment expressions, loops, recursion, callbacks, host escapes,
list literals, map literals, and arbitrary record literals are syntax errors.
`count()` and `sum(expr)` exist only in projection measure productions.

### Explicit deferrals

Grammar version 1 has no named contract-query declaration. POC entity, exact
prefix index, projection, commit, and provenance queries remain bounded generic
application-service operations. The compatibility prose about adding a query
does not create an unowned surface production. A named query language requires
a later accepted grammar and QueryPlan decision.

State-machine source syntax is deferred as SPEC permits for the first vertical
slice; WP-040 may reserve a typed IR concept but must not invent executable
state-machine instructions. Contract-authored capability and approval clauses
are also deferred: the POC command capability derives from stable contract and
command identity, while approval remains external policy. Optional fields have
only the explicit null default in grammar version 1; there is no `default`
clause. Adding any deferred construct is a compatibility-reviewed grammar change.

### Bounds, spans, and diagnostics

Parser limits are immutable grammar-version-1 safety boundaries unless a later
accepted ADR revises them:

| Boundary | Limit |
|---|---:|
| UTF-8 source bytes | 1,048,576 |
| Identifier bytes | 256 |
| Delimiter/expression nesting | 32 |
| Tokens and total AST nodes | 131,072 each |
| Top-level declarations or items in one declaration | 4,096 |
| Arguments, tuple entries, variants, or object fields | 1,024 |
| Diagnostics returned by one parse | 32 |
| Expected-token alternatives in one diagnostic | 16 |

Spans are half-open UTF-8 byte offsets stored as checked `u32` values. Line and
column rendering is derived from the bounded source and is not identity. The
stable syntax diagnostic registry is:

| Code | Meaning |
|---|---|
| `RDB-S001` | Source byte limit exceeded |
| `RDB-S002` | Token or AST-node limit exceeded |
| `RDB-S003` | Invalid token, escape, or literal representation |
| `RDB-S004` | Unexpected token |
| `RDB-S005` | Unexpected end of source |
| `RDB-S006` | Nesting limit exceeded |
| `RDB-S007` | Declaration, item, argument, variant, or object-field limit exceeded |
| `RDB-S008` | Recognized but unsupported or deferred syntax |

Diagnostics contain only a code, static summary, bounded expected-token names,
one primary span, and optional static help. They never retain an internal error
source or accept parser/library debug text as public output.

### Compilation boundary

Source parses into a source-oriented spanned AST, then WP-040 resolves a typed
HIR and validates a versioned executable IR. The AST stores names, literal
lexemes, type syntax, item order, and spans. It is never canonical, durable,
typed HIR, or executable input. Runtime code never evaluates the AST.

Every executable plan must eventually declare bounded reads, writes, conflict
keys, invariants, effects, outcomes, and locality. `ContractBundle` canonical
content excludes `generated_at`; compilation and deployment timestamps belong
in non-canonical catalog audit metadata. Exact stable-ID allocation, IR tags and
encoding, supported execution-version window, canonical plan-hash framing, and
bundle bytes are deliberately not decided here. ADR-0013 is required before
WP-040 publishes those interfaces.

### Reviewed dependencies and fuzzing

WP-030 uses Logos 0.16.1 with default features disabled and
`export_derive,std,forbid_unsafe`; LALRPOP-util 0.23.1 with default features
disabled; and build-only LALRPOP 0.23.1 with default features disabled. The
external Logos token stream means LALRPOP's built-in lexer and Unicode regex
features are unnecessary. First-party code remains `#![forbid(unsafe_code)]`.
Miette is not part of the syntax semantic boundary; presentation adapters may
be reviewed later without changing diagnostic identity.

Parser fuzzing uses cargo-fuzz 0.13.2, libfuzzer-sys 0.4.13, and
`nightly-2026-07-12` in an independent, nonproduction fuzz workspace on Linux
x86-64/AArch64. The maintainer approved libfuzzer-sys's bundled C++17 code, `cc`
build, upstream unsafe, and composite `(MIT OR Apache-2.0) AND NCSA` license only
for this fuzz workspace. This resolves the deferral recorded by ADR-0006; it
does not permit unsafe or native code in first-party or production crates. CI
checks the independent lockfile, license exception, and 30-second smoke run.

## Options Considered

1. **The exact bounded grammar above:** Accepted.
2. **Only parse the LegalSpend example:** Leaves stated enum/index/projection
   surfaces unowned and forces later accidental grammar changes.
3. **Add named queries, state machines, or policy clauses now:** Their semantics
   are not specified tightly enough and would broaden the POC.
4. **Union with historical syntax:** Adds aliases and ambiguity without semantic
   value.
5. **Execute the AST:** Loses type, dependency, compatibility, and deterministic
   execution validation.
6. **Property tests without the required fuzz target:** Useful supplementary
   evidence but does not satisfy WP-030's explicit acceptance command.

## Consequences

- SPEC Sections 7.2 and 23.1 are the same canonical valid fixture.
- Unsupported and deferred constructs fail with bounded, source-spanned codes.
- Creation is a typed command operation with a declared duplicate outcome.
- Grammar version changes require an accepted ADR and compatibility corpus.
- WP-040 remains blocked on ADR-0013 even after WP-030 completes.
- Native and NCSA-licensed code remains isolated from the root production graph.

## Compatibility

Accepted tokens, productions, precedence, source spelling, bounds, span units,
and diagnostic codes are compiler compatibility boundaries. AST Rust layout and
LALRPOP-generated Rust are internal and are not durable compatibility formats.

## Security

Static syntax has no I/O, wall clock, randomness, callback, host-language, or
unbounded-control-flow surface. Inputs, nesting, tokens, collections, and
diagnostics are bounded before parser amplification. Diagnostics do not relay
library errors or caller-controlled messages.

## Testing

Use valid/invalid source corpora, diagnostic span snapshots with semantic
assertions, exact language-reference examples, delimiter and collection boundary
tests, arbitrary-byte Proptest coverage, and the pinned parser fuzz smoke. WP-040
adds stable-ID, typed-IR, bundle, plan-hash, and repeated-build evidence only
after ADR-0013 acceptance.

## Requirements and Work Packages

- **Requirements:** `DSL-001`, `DSL-002`, `DSL-006`, `DSL-008`, `CMP-001`, and
  parser prerequisites for the remaining `DSL-*` and `CMP-*` requirements
- **Defines or blocks:** `WP-030`; AST input for `WP-040`
- **Final evidence:** `WP-040`, `WP-140`, `WP-200`

## Decision Deadline

The WP-030 grammar deadline was satisfied by exact-text acceptance on
2026-07-12. ADR-0013 must be accepted before WP-040 publishes stable IDs,
executable IR, plan hashes, or bundle fixtures.
