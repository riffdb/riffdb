# Agent application quickstart

The normal RiffDB application path is contract and RiffQL text compiled into
generated application operations. The kernel gRPC protocol is not an
application-development API.

```text
$ riffdb new order-desk
created application `order-desk` at order-desk

$ cd order-desk
$ riffdb dev --seed
generated exact application bindings
application boundary check passed
riffdb-dev-seed-v1  1  application-manifest
riffdb-dev-ready-v1 http://127.0.0.1:... ... OrderDeskApplication
```

`riffdb new` compiles before it writes and refuses to overwrite an existing
path. It creates:

```text
riffdb.application.json
riffdb/
  contract.riff
  queries/item_page.riffq
  seed/01-CreateItem.jsonl
generated/
  rust/client.rs
  typescript/client.ts
  mcp/tools.json
src/
```

The manifest pins the exact contract bundle and query-module identities. A
source change cannot silently retain stale bindings: `riffdb application
generate` fails closed until the manifest describes the newly compiled
identities.

`riffdb dev` starts one local server, waits for explicit readiness, deploys the
exact contract and module, compiles and binds the symbolic application role,
regenerates all bindings, checks the application boundary, executes seed JSONL
through ordinary idempotent commands, and shuts down its child on failure or
interrupt. Credentials live only in a mode-protected temporary directory.

## The application boundary

Handwritten application code may use the stable application facade and
compiler-generated operations. It may not:

- depend on kernel, storage, service, protobuf, gRPC, Tonic, or Prost packages;
- construct raw entity/index requests, field masks, numeric schema IDs, or
  encoded keys;
- pack or decode named-query records by hand;
- create handwritten RiffDB transport wrappers;
- place handwritten code under `generated/`.

Run the mandatory check with:

```text
scripts/check-application-boundary .
```

Generated bindings contain a compiler marker and are the only files allowed to
contain protocol adaptation. That exception is narrow: application authors
cannot obtain a trusted marker by moving handwritten code into the generated
tree.

Administrative and kernel credentials remain separate from the generated
application role. The normal scaffold has no kernel dependency, feature, role,
credential, MCP tool, or documentation path. Kernel access is an explicit
source-repository operational workflow and must not reuse an application
credential.
