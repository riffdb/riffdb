# Symbolic application inspection

Inspection explains compiler-owned consequences; it does not ask application
authors to copy those consequences into source.

## Check source without writing

```text
riffdb application check
riffdb --output json application check
```

The human and JSON forms contain the same stable stage, code, local source
path, byte span, symbolic path, cause, suggested fixes, file disposition, and
retry classification. They do not include submitted values, credentials,
hidden schema, or arbitrary internal messages.

## Review the exact generation

```text
riffdb application lock --write
git diff -- riffdb.application.lock.json generated/
riffdb application lock --check
riffdb application generate --locked
```

`lock --write` is the explicit authority/plan acceptance point. The lock
records exact source, contract, query plans, symbolic role definitions,
compiler format versions, and generated artifact hashes. Edit the symbolic
source to change intent; never edit a hash or generated file to force a match.

## Inspect roles

```text
riffdb role check riffdb.application.json --role MuseumApplication
riffdb role describe riffdb.application.json --role MuseumApplication
```

For a tenant-scoped role, add `--tenant <tenant>`. The description names
queries, commands, fields, bounds, contract lineage, and exact derived
authority. It grants nothing. Binding is a separate authenticated operation.

## Inspect generated application operations

The Rust and TypeScript generated clients expose typed parameter structures,
result/outcome unions, cursor types, retry-safe commands, and checked public
errors. `generated/mcp/tools.json` uses the least-sufficient V2 through V5
profile when it has no command and V6 whenever its retained command registry is
nonempty. V6 records the exact predecessor profile and adds the hosted MCP name,
compiler input/outcome identities, service envelope identity, composed output
identity, and closed discovery descriptor to each command. Its visible named
queries carry the same closed hosted descriptor. The catalog remains generated
expectation evidence, never dispatch or authorization authority.

Regenerating an older command-bearing package rotates the MCP artifact hash and
therefore its enclosing application-lock identity without advancing the lock
schema. Review `riffdb application preview`, accept with `application lock
--write`, and verify with `application lock --check`. Existing exact V2 through
V5 catalogs remain readable but make no hosted-descriptor parity claim.
Application code imports those operations, not Protobuf or kernel requests.

The exact compatibility manifest at
`generated/riffdb.application.exact.json` is compiler-owned deployment input.
It is useful for auditing, but its hashes and IDs are never author inputs.

## Explain a failure

Use the diagnostic code and symbolic path:

1. Correct the referenced author-owned source.
2. Run `riffdb application check`.
3. Review `riffdb application lock --write`.
4. Verify with `riffdb application lock --check`.

Do not broaden a role, switch to a kernel API, omit a requested field, or
replace a bounded query with client-side RPC composition merely to make the
failure disappear.
