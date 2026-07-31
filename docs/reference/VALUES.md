# Values and Identifiers

Generated schemas and clients preserve RiffDB values exactly. Do not substitute
JSON floating-point values or infer internal numeric identities.

| Value | Public representation | Notes |
|---|---|---|
| UUID | Canonical lowercase UUID string | Generated clients use native UUID types where available |
| Decimal | Exact generated decimal value/object | Never a JSON float |
| Money | Currency plus fixed-scale amount | Currency and scale are checked |
| Signed/unsigned integer | Checked integer | MCP accepts an ordinary JSON integer when its generated schema permits it |
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

## Limits

All values, collections, recursion, input and output messages, scans, waits, and
diagnostics have explicit bounds. The generated schema is the authoritative
public shape; an adapter must reject rather than truncate an out-of-range value.
