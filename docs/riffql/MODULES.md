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

Ordinary modules retain the original version-1 byte layout and identities.
A module containing an operational optional-predicate query uses the additive
version-2 representation. It stores the complete finite plan family, its
authorization union, maximum cost, and family identity. Generated clients pin
that family identity; the service selects one exact member only from parameter
presence and returns the family identity on the response. Strict decoding
recompiles source against the exact contract and compares canonical bytes.

Operational families currently execute through the named-query application
surface. Version-1 reactive/live-query modules do not substitute a representative
member: compilation excludes an operational family until a versioned reactive
presence-selection surface exists.

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

`scripts/generate-query-clients --check` regenerates the checked Rust, Go,
TypeScript, Python, and MCP artifacts, compares exact bytes, and validates their
language-specific build boundaries.
