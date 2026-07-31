# RiffDB MCP Agent Cookbook

RiffDB exposes policy-filtered database operations through MCP. Use
`tools/list` after initialization to discover the commands authorized for the
current credential and database. Tool names contain lowercase letters, digits,
and underscores only.

## Values

- Send fixed-scale decimals as strings, for example `"1250.00"`. Do not send a
  JSON floating-point number.
- Send UUID values as canonical lowercase strings, for example
  `"01900000-0000-7000-8000-000000000001"`.
- Follow each tool's `inputSchema`; undeclared properties are rejected.

## Mutating Commands

Choose a fresh caller-owned `idempotency_key` for each logical command. Preserve
that key until the result is known. Never generate a new key merely because a
connection was interrupted.

A declared business rejection is a successful MCP tool call with a typed
outcome. Branch on its declared outcome field rather than parsing text or
treating it as a protocol failure.

If a connection fails after submission, the command may have committed. Retry
the same command with the same input and idempotency key, or resolve the
returned outcome URI. Reusing a key with different canonical input fails
closed.

## Discovery and Documentation

`tools/list` is the authorized catalog for the selected database and current
credential. A missing command may be absent, stale, or unauthorized; do not
infer which.

For a command resource:

- `riffdb://command/<lineage>/<command-id>/docs` is the invocation guide with
  inputs, a complete MCP call example, declared outcomes, and retry guidance.
- `riffdb://command/<lineage>/<command-id>/plan` is the compiler-oriented
  executable plan and generated schema detail.

Use `/docs` to call a command. Use `/plan` when inspecting exact compiler or
dependency behavior.

## Input Errors

Invalid tool arguments return one redacted
`riffdb.mcp.input-error/v1` diagnostic. Its `path` is a JSON Pointer, `code`
classifies the first deterministic violation, and `expected` describes public
schema shape. The error never echoes the submitted value.

## Database Selection

Each MCP connection is bound to one configured database alias. Stdio selects it
in `mcp.toml`; hosted MCP uses the `riffdb-database` header. Credentials are
database-bound and cannot authorize another database.
