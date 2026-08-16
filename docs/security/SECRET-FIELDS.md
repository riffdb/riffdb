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

## Revealing a secret explicitly

The escape hatch is a capability whose field-visibility entry names the
secret field in its dedicated `secret_field_ids` list (distinct from the
ordinary `field_ids`, which never reveals). The naming survives storage at
full fidelity (capability record V6) and delegation can only narrow it. A
read that explicitly selects the named field under such a capability
returns the value; the same read one grant short fails with a typed
field-visibility denial. When creating a capability through the CLI or the
admin API, add `secret_field_ids` to the field-visibility entry:

```json
{
  "contract_lineage": "myapp",
  "entity_type_id": 3,
  "field_ids": [1, 2],
  "secret_field_ids": [7]
}
```

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

## The classification is sticky

The compiler follows a secret-classified value through expressions. Copying
one into a non-secret stored field, durable event field, or command outcome
is a compile error unless the exact flow site names every secret source with
`reveals`:

```riff
set session.public_token = session.token_hash reveals session.token_hash

emit TokenIssued {
  token: session.token_hash reveals session.token_hash
}

return Issued {
  token: session.token_hash reveals session.token_hash
}
```

A value derived from multiple secrets names each source separately:

```riff
return Issued {
  score: row.secret_score + row.secret_offset
    reveals row.secret_score
    reveals row.secret_offset
}
```

The annotation is static contract identity, not an application permission.
It is checked into source, carried in the executable plan and bundle hash,
shown by command explain/catalog surfaces, and cannot be supplied by a
caller at runtime. It does not grant read visibility to either source field.
Copying a secret into another secret-classified stored field remains sticky
and requires no disclosure annotation.

Named RiffQL reads use the same explicit-review principle at a returned leaf:

```riffql
return Found {
  session: session {
    token_hash reveals session.token_hash
  }
}
```

Unlike a command-flow annotation, this declaration also becomes an exact
application-role authority atom when—and only when—the role selects that
immutable named query. The compiler places the stable field ID in the grant's
dedicated secret list; callers cannot add it through parameters or ad-hoc
RiffQL. A missing or stale role returns `RDB-AUTH-0214` with no partial result.
MCP and reactive catalogs omit the query entirely; typed application SDK and
gRPC execution remain available under the exact role.

`RDB-C046` rejects missing, wrong, duplicate, or excess declarations. Its
diagnostic identifies both the destination flow site and the secret source
field declaration, so an author can either remove the copy or make the
intentional disclosure reviewable. Once disclosed into an event or outcome,
the destination is ordinary visible data; `reveals` does not encrypt it or
redact it after delivery.

## Current alpha limitations

- Remote gRPC responses carry secret fields by omission (fail-closed): the
  wire record simply lacks them, and the remote CLI therefore shows
  **absence, never the `[redacted:…]` marker**. The marker appears on the
  MCP surface, which renders in-process from the service's views. Wire-level
  marker carriage is follow-up work.
- Generated language bindings do not yet mark secret fields in their types.
