# RiffDB contract language v1

The [complete grammar-version-1 parser reference](LANGUAGE.md) is generated
directly from the parser grammar. The crate-local and public copies are checked
byte-for-byte in CI.

A contract is compiled before deployment. Parsing alone grants nothing and
does not create an executable plan. Compilation must prove:

- every symbol and expression has one exact static type;
- every entity belongs to one aggregate and has a complete key;
- every command is confined to one statically derivable partition;
- mutating commands have one direct bounded-string idempotency input;
- relationship and uniqueness changes are validated transaction-current;
- required fields are definitely initialized and each mutation target is
  legal;
- every declared outcome and emitted event has one consistent typed shape; and
- all source, nesting, collection, schema, key, row, and plan bounds hold.

If any proof is missing, compilation fails with a stable source-spanned
diagnostic. There is no fallback to a callback, SQL statement, unrestricted
transaction, cross-partition write, or runtime-only integrity convention.

## Source bounds

| Input | Inclusive limit |
|---|---:|
| Contract source | 1,048,576 UTF-8 bytes |
| Identifier | 256 bytes |
| Tokens | 131,072 |
| AST nodes | 131,072 |
| Nesting | 32 |
| Items in one declaration | 4,096 |
| Items in one local list | 1,024 |
| Diagnostics | 32 |

The authoritative constants live in the parser and compiler. The generated
language reference is checked against the parser grammar in CI.

## Command index-work bounds

Mutating plans prove secondary-index work before plan hashing. The compiler
keeps physical index-entry removals and additions at 4,096, permits at most
65,535 exact affected prefix epochs and complete validation positions, and
also requires their correlated charge to fit:

```text
index-entry deltas + affected prefix epochs + validation positions <= 65,535
```

Affected targets and their current epoch observations must independently fit
the 16 MiB command read-state ceiling. A bounded collection shares its proved
partition prefix across elements, but the compiler assumes no equality among
caller values, entity keys, other index fields, or different elements. When a
collection declares `aggregate_bytes`, the same checked aggregate bound may
limit copies of element-sourced bytes in index prefixes; count and work charges
are never reduced by byte correlation.

An exceeded known ceiling is reported as `RDB-C020` with a closed resource
identity and the checked `actual` and `maximum` integers. These diagnostics
contain compiler-owned plan metadata only—never submitted values, keys, stored
rows, or backend details. There is no application-selectable work budget,
runtime fallback, partial command, or automatic command split.

## Author workflow

```text
edit riffdb/contract.riff
riffdb application check
riffdb application lock --write
riffdb application lock --check
```

Authors use names. Stable IDs, hashes, command plans, capability requirements,
field visibility, and generated transport code belong to the compiler-owned
lock and generated tree.

See [First application authoring](AUTHORING.md) for reserved identifiers,
namespace rules, supported comments, natural CLI/MCP JSON values, and installed
deployment inputs.

See [the command and invariant cookbook](COMMAND-INVARIANT-COOKBOOK.md) for
safe patterns and [the Museum example](examples/museum/) for a complete
multi-entity application.
