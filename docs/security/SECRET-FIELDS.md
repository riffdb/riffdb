# Secret Field Classification

A stored entity field may be declared `secret`:

```riff
entity Session {
  key (org_id: uuid, session_id: uuid)
  field secret token_hash: string<256>
  field secret refresh_secret: string<256>
  field expires_at: timestamp
  unique token (org_id, token_hash)
}
```

The classification marks values whose accidental display would be a security
incident — session tokens, verification tokens, credential hashes, provider
refresh secrets. It is declared once, in the contract, and every present and
future display surface inherits it (ADR-0118).

`secret` is a contextual modifier: it is an ordinary identifier everywhere
else, so a field *named* `secret` stays valid, and adopting the keyword is a
compatible contract evolution. Only stored entity fields accept it — key
fields, event fields, and command inputs cannot carry the classification by
construction. A contract that never uses the keyword keeps its exact prior
encoding and bundle hash.

## The promise

A secret-classified field's value does not appear in:

- tracing, log, metric, or health output,
- typed public errors and diagnostics (which are value-free by construction),
- MCP tool output text,
- CLI rendering of records,
- provenance and audit summaries,
- generated example and documentation output.

Where a record rendering would have shown the value, surfaces emit the one
stable marker instead:

```text
[redacted:token_hash]
```

Redaction is structural, not reviewed-for: the service's single release
point wraps every secret value in a type whose display and debug forms emit
only the marker and which no serializer can consume; the only way a value
leaves the wrapper is a named reveal method whose call sites an architecture
test enumerates. A new surface cannot obtain an unredacted rendering by
accident, because it never receives the value at all.

## Field visibility defaults deny

Capability field-visibility grants never reveal a secret field through their
ordinary field list — enumerating every field (the shape every role default
produces) is inert for secrets. Revealing one requires naming it explicitly
through the grant's dedicated secret-field naming, and:

- requesting or projecting an unrevealed secret fails with a typed
  field-visibility denial, never a silent narrowing;
- a delegated capability can only name secrets its parent names;
- scanning an index whose entries embed a secret field's bytes requires the
  same explicit naming, because released index keys are a projection of the
  value.

Everything that *uses* the value without returning it works with no
visibility at all: predicates on the field, uniqueness enforcement, and
index participation run inside the command and query engines against full
records.

## What is stored

Durable records, backups, exports, and changelog frames carry secret fields
at **full fidelity**. A backup that silently dropped or masked secret fields
would restore a broken application; what an auth workload stores is already
the appropriate at-rest form (hashes, opaque tokens). The classification
governs display surfaces, not storage.

## What `secret` does not promise

`secret` is a display-and-visibility classification, **not a cryptographic
promise**. It does not encrypt the value at rest or in transit, does not
manage keys, and does not hash anything for you. Store secrets in an
appropriate at-rest form — hash session tokens, keep provider refresh
secrets opaque — exactly as you would without the keyword. The
classification itself is not a secret: diagnostics may name a field as
secret-classified.

See the unsafe-pattern entry in
[Unsafe Patterns and Corrections](../contracts/examples/NEGATIVE-EXAMPLES.md)
for the misuse this non-promise guards against.

## Current alpha limitations

- Remote gRPC responses carry secret fields by omission (fail-closed): the
  wire record simply lacks them, and the remote CLI shows absence rather
  than the `[redacted:…]` marker. In-process surfaces (MCP, embedded
  rendering) show the marker. Wire-level marker carriage is follow-up work.
- Generated language bindings do not yet mark secret fields in their types.
- Copying a secret field's value into an event payload or a command outcome
  is visible in the contract source and is the author's explicit choice; the
  classification follows the declared field, not the value's copies.
