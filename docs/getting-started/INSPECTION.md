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
errors. `generated/mcp/tools.json` carries schemas for the same operation set.
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
