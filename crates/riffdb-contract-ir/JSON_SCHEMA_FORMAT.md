# RiffDB Generated JSON Schema v1

Status: **Accepted**

This generated review artifact is derived from the production schema-artifact and keyword registries. Artifacts use draft 2020-12 and are hashed as exact canonical UTF-8 bytes.

## Canonical JSON

- Root `$schema` is exactly `https://json-schema.org/draft/2020-12/schema`.
- Object keys are ordered by raw ASCII bytes with no insignificant whitespace.
- Semantic arrays preserve their specified order.
- Integers use shortest base-10 spelling; Boolean and null are lowercase.
- String escaping uses JSON short escapes where defined, lowercase `\u00xx` for other control bytes, and leaves `/` and non-control Unicode unescaped.
- Schema v1 is fully inline and emits no unregistered keyword.
- One artifact is at most 1,048,576 UTF-8 bytes. An exact checked-arithmetic size traversal rejects a larger expansion before its JSON tree or output string is materialized; serialization must reproduce the preflight byte count exactly.

## Artifact Keys

The five-byte key is `u8 tag || u32 stable_id`. Artifacts order by this key and encode `key || u32 JSON_byte_length || JSON_bytes || 32-byte SchemaHash`.

| Tag | Artifact |
|---:|---|
| `0x01` | entity record |
| `0x02` | durable event payload |
| `0x03` | command input |
| `0x04` | command outcome union |
| `0x05` | projection result row |

## Exact ValueType Construction Registry

Templates are canonical no-whitespace JSON with angle-bracketed semantic parameters. Object keys appear in exact emitted ASCII order; the accompanying rule fixes parameter derivation and semantic array order.

### `0x01` bool

```text
{"type":"boolean"}
```

Rule: no parameters.

### `0x02` i64

```text
{"maximum":9223372036854775807,"minimum":-9223372036854775808,"type":"integer"}
```

Rule: bounds are inclusive JSON integers.

### `0x03` u64

```text
{"maximum":18446744073709551615,"minimum":0,"type":"integer"}
```

Rule: bounds are inclusive JSON integers.

### `0x04` decimal

```text
{"pattern":"<decimal(P,S)>","type":"string","x-riffdb-decimalPrecision":<P>,"x-riffdb-decimalScale":<S>}
```

Rule: P=1..38, S=0..P; S=0 pattern ^-?(0|[1-9][0-9]{0,P-1})$; S=P pattern ^-?0\.[0-9]{S}$; otherwise ^-?(0|[1-9][0-9]{0,P-S-1})\.[0-9]{S}$.

### `0x05` money

```text
{"pattern":"^-?(0|[1-9][0-9]{0,35})\\.[0-9]{2}$","type":"string","x-riffdb-decimalPrecision":38,"x-riffdb-decimalScale":2,"x-riffdb-moneyCurrency":"<3 uppercase ASCII currency>"}
```

Rule: precision and scale are exactly 38 and 2; currency is the ValueType currency.

### `0x06` string

```text
{"type":"string","x-riffdb-maxUtf8Bytes":<maximum_utf8_bytes>}
```

Rule: maximum is the nonzero bounded ValueType byte limit.

### `0x07` bytes

```text
{"contentEncoding":"base64","type":"string","x-riffdb-maxDecodedBytes":<maximum_bytes>}
```

Rule: maximum applies after strict padded RFC 4648 base64 decoding.

### `0x08` timestamp

```text
{"additionalProperties":false,"properties":{"nanos":{"maximum":999999999,"minimum":0,"type":"integer"},"seconds":{"pattern":"^-?(0|[1-9][0-9]*)$","type":"string","x-riffdb-integerType":"i64"}},"required":["seconds","nanos"],"type":"object"}
```

Rule: properties use ASCII key order; required preserves semantic seconds,nanos order.

### `0x09` date

```text
{"maximum":2147483647,"minimum":-2147483648,"type":"integer"}
```

Rule: signed i32 days since Unix epoch.

### `0x0a` uuid

```text
{"pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$","type":"string"}
```

Rule: lowercase hyphenated network-order UUID bytes.

### `0x0b` enum

```text
{"enum":["<variant name in EnumVariantId order>"...],"type":"string"}
```

Rule: the referenced enum must exist; names are exact source names.

### `0x0c` optional

```text
{"oneOf":[<inner ValueType schema>,{"type":"null"}]}
```

Rule: inner schema is first and null is second.

### `0x0d` list

```text
{"items":<element ValueType schema>,"maxItems":<maximum_entries>,"type":"array"}
```

Rule: maximum is the bounded ValueType list limit.

### `0x0e` record

```text
{"additionalProperties":false,"properties":{<ASCII-name-ordered field schemas>},"required":["<all field names in FieldId order>"...],"type":"object"}
```

Rule: only entity and event references are legal inline; the inline record omits $schema.

## Exact Record, Outcome, and Projection Shapes

### entity or event output record

```text
{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{<ASCII-name-ordered field schemas>},"required":["<every field name in FieldId order>"...],"type":"object"}
```

Rule: every field is required, including fields whose ValueType is optional.

### command input record

```text
{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{<ASCII-name-ordered field schemas>},"required":["<nonoptional field names in FieldId order>"...],"type":"object"}
```

Rule: optional fields add default:null to their type-schema object and are omitted from required; the direct idempotency string is nonoptional, bounded 1..=128 bytes, and additionally adds minLength:1 and x-riffdb-minUtf8Bytes:1.

### command outcome union

```text
{"$schema":"https://json-schema.org/draft/2020-12/schema","oneOf":[{"additionalProperties":false,"properties":{<ASCII-name-ordered payload schemas plus "type":{"const":"<outcome name>"}>},"required":["type","<payload field names in FieldId order>"...],"type":"object"}<in OutcomeId order>...]}
```

Rule: the discriminator property is exactly type with the exact outcome name; type is first in required even when ASCII property order places it elsewhere; a source outcome payload field named type is invalid.

### projection tuple key

```text
{"items":false,"maxItems":<component_count>,"minItems":<component_count>,"prefixItems":[<component ValueType schemas in group order>...],"type":"array"}
```

Rule: items is Boolean false; the nonzero component count is at most 1,024 and minItems equals maxItems exactly.

### projection result row

```text
{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"key":<projection tuple key>,"measures":<inline output record>},"required":["key","measures"],"type":"object"}
```

Rule: key and measures are the only properties; measures omits its own $schema.

## Closed Keyword Type and Semantics Registry

| Keyword | JSON value type | Emitted semantics |
|---|---|---|
| `$schema` | `string` | exact draft 2020-12 dialect URI |
| `additionalProperties` | `boolean` | always false on emitted closed objects |
| `const` | `string` | exact outcome discriminator name |
| `contentEncoding` | `string` | exactly base64; decoders use strict base64 |
| `default` | `null` | only direct optional command-input fields |
| `enum` | `array<string>` | variant names in EnumVariantId order |
| `items` | `schema object or boolean` | list element schema, or false for a closed projection tuple |
| `maxItems` | `integer` | nonnegative exact collection upper bound |
| `maximum` | `integer` | inclusive canonical integer maximum |
| `minItems` | `integer` | projection tuple arity, equal to maxItems |
| `minLength` | `integer` | exactly 1 for a direct idempotency string |
| `minimum` | `integer` | inclusive canonical integer minimum |
| `oneOf` | `array<schema object>` | ordered optional variants or OutcomeId-ordered outcome variants |
| `pattern` | `string` | exact registered decimal, integer-string, or UUID pattern |
| `prefixItems` | `array<schema object>` | projection group components in declared group order |
| `properties` | `object<string,schema object>` | property keys in raw ASCII order |
| `required` | `array<string>` | semantic field order defined by the enclosing shape |
| `type` | `string` | one of array, boolean, integer, null, object, or string |
| `x-riffdb-decimalPrecision` | `integer` | decimal precision in 1..=38 |
| `x-riffdb-decimalScale` | `integer` | decimal scale in 0..=precision |
| `x-riffdb-integerType` | `string` | exactly i64 for timestamp seconds |
| `x-riffdb-maxDecodedBytes` | `integer` | nonzero ValueType bytes bound, at most 1,048,576 |
| `x-riffdb-maxUtf8Bytes` | `integer` | nonzero ValueType string bound, at most 1,048,576 |
| `x-riffdb-minUtf8Bytes` | `integer` | exactly 1 for a direct idempotency string |
| `x-riffdb-moneyCurrency` | `string` | exact three-byte uppercase ASCII currency |

No other keyword or alternate object construction is valid in v1.
