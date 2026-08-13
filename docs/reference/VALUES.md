# Values and Identifiers

Generated schemas and clients preserve RiffDB values exactly. Do not substitute
JSON floating-point values for exact decimals or infer internal numeric
identities.

| Value | Public representation | Notes |
|---|---|---|
| UUID | Canonical lowercase UUID string | Generated clients use native UUID types where available |
| Decimal | Exact generated decimal value/object | Never a JSON float |
| Money | Currency plus fixed-scale amount | Currency and scale are checked |
| Signed/unsigned integer | Checked integer | Low-level CLI tagged `i64`/`u64` values use decimal strings; generated schema-directed inputs may accept JSON integers |
| Vector | Finite binary32 component array | Length and declared field dimension are checked; no bytes alias is accepted |
| Timestamp | Nanosecond-precision value | No implicit operating-system clock in command evaluation |
| Date | Exact epoch-day domain | Python supports values outside `datetime.date` |
| Bytes | Generated binary representation | Bounded before decoding or persistence |
| Enum | Declared variant name | Internal enum and variant IDs are compiler-owned |
| Record/list/map | Immutable generated structure | Maps and sets are canonically ordered before hashing or persistence |

## Public names and internal IDs

Applications select contract lineages, commands, queries, outcomes, and fields
by generated symbolic names. The compiler assigns nonzero internal IDs and pins
them in the exact bundle. Application source must not copy or override those
values.

## Canonical input

Command input is materialized under the selected contract schema and encoded in
a canonical order before hashing. The hash is part of idempotency identity: the
same key with a different canonical input is rejected.

## Vector transport shapes

The low-level typed Protobuf value branch is
`Value.vector_value = 15`, containing `VectorValue.components = 1` as repeated
binary32 values. CLI output uses the exact JSON shape:

```json
{"type":"vector","components":[0.0,1.5,-2.25]}
```

Hosted MCP uses its existing tagged-value convention:

```json
{"kind":"vector","components":[0.0,1.5,-2.25]}
```

Every transport accepts 1 to 4,096 finite components, converts each value to
IEEE-754 binary32, and canonicalizes negative zero to positive zero. Command
materialization additionally requires the component count to equal the selected
contract field's declared dimension. A vector is never represented as bytes or
another value kind. Canonical durable value encoding uses the internal tag
`0x0e`; that tag is not an application-facing spelling.

The low-level transport branch does not by itself provide complete embedding
persistence or stable generated application-facade support. See
[Known Limitations](../known-limitations.md).

## Limits

All values, collections, recursion, input and output messages, scans, waits, and
diagnostics have explicit bounds. The generated schema is the authoritative
public shape; an adapter must reject rather than truncate an out-of-range value.
