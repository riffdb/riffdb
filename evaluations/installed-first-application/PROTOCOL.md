# WP-392 installed-agent protocol

The evaluator supplies one sealed public bundle, this brief, an empty
application directory, an empty evidence directory, and an evaluator-created
shell environment pointing at two already installed and bootstrapped database
aliases. The agent starts in a fresh context with no implementation source.

The environment exposes only paths and routing facts. It does not grant a
combined credential: the operator configurations for the two databases remain
separate, and application deployment must create a distinct least-authority
credential and MCP configuration.

The agent may source the supplied environment file, but must not edit any
server, client, MCP, or credential configuration. It may write only its
application and evidence directories. Network access is disabled.

`events.jsonl` contains compact JSON objects in chronological order with these
members only:

```json
{"schema":"riffdb-installed-agent-event/v1","sequence":1,"kind":"public_docs","status":"passed","elapsed_ms":0}
```

`sequence` starts at one and increases by one. `kind` is one of `public_docs`,
`database_isolation`, `application_check`, `application_lock`,
`application_generate`, `application_deploy`, `cli_named_query`, `mcp_list`,
`mcp_named_query`, `boundary_check`, or `complete`. `status` is `passed` or
`failed`; `elapsed_ms` is a non-negative integer. Events contain no submitted
or returned application values, credentials, endpoints, paths, or source.

`report.json` must conform to `report-schema.json`. Copy the completed
`riffdb.application.lock.json` byte-for-byte to the evidence directory. The
report's lock, contract, module, and role hashes must come from public generated
or deployment state, never from implementation source or inference.
