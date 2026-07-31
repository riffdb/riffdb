# Immutable query modules

A query module is a versioned, content-addressed set of canonically formatted
named RiffQL documents. It is pinned to one exact retained contract lineage,
version, and bundle hash.

Deployment recompiles every source document at the catalog trust boundary,
derives the canonical module hash, and submits an audited compare-and-swap
activation through the control-plane coordinator. The module bytes and active
pointer are committed atomically. Startup replays and validates the durable
module history before readiness.

Named execution always submits the exact module hash emitted by the generated
binding or manifest. Selecting the ambient active module is an inspection and
development action, not an execution identity: omitting the hash fails closed.
Cached plans are bounded and keyed by immutable identity. The server authorizes
the complete compiler-derived application-query request for every invocation;
a module or generated client conveys no authority.

## Generated artifacts

The compiler emits:

- Rust parameter, result-union, command-input, and operation types;
- TypeScript equivalents;
- exact contract and module identity constants;
- response-identity verification helpers; and
- optional module-qualified MCP read tools with domain-shaped JSON Schemas.

For TicketDesk, the generated MCP names are:

```text
ticketdesk_list_tickets
ticketdesk.project_members
ticketdesk.project_summary
ticketdesk_ticket_page
```

Generated MCP operations are read-only and non-destructive. They complement
the general `riffdb.query` operation; they do not replace ad-hoc RiffQL.
Mutation tools remain separate compiled-command operations with mutation risk
metadata.

The checked fixtures are:

- `fixtures/query-modules/ticketdesk.rs`
- `clients/typescript/ticketdesk/client.ts`
- `fixtures/query-modules/ticketdesk.mcp.json`

`scripts/generate-query-clients --check` regenerates all three, compares exact
bytes, compiles the Rust fixture, and validates the TypeScript source.
