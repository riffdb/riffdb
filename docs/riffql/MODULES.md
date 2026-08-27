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

A module containing bounded exact aggregates uses additive version 3. Its
aggregate descriptors, result schemas, authorization fields, group ceiling,
cost, source map, explanation, and family identity are all compiler-owned and
strictly recompiled on decode. Version-3 aggregate modules are deployable and
execute only through the one-snapshot operational evaluator. The service passes
the sealed descriptors alongside the selected family member; an adapter that
does not implement this exact boundary fails closed with `InvalidProgram`
instead of falling back to the ordinary source-row result.

A module using the additive exact aggregate core uses version 11 paired with
RiffQL language version 8 and query IR version 11. Its canonical bytes include
the new closed function tags and the independently sealed distinct, state, and
arithmetic budgets. Strict decode recompiles all three identities; module
versions 1 through 10 retain their existing readers and exact bytes.

Operational families currently execute through the named-query application
surface. Version-1 reactive/live-query modules do not substitute a representative
member: compilation excludes an operational family until a versioned reactive
presence-selection surface exists.

A module containing a compiler-checked secret output uses additive version 4.
Its canonical query plan carries each exact query, entity, field, stable field
identity, result slot, and declaration span. Strict decode recompiles those
requirements against the exact contract. Selecting such a query in a symbolic
application role derives a V4 role identity with ordered
`query/entity/field` atoms and dedicated secret-field visibility; selecting an
ordinary query does not inherit another query's secret authority. Modules and
roles without these declarations keep their previous V1 through V3 bytes.

Secret-bearing queries are generated for typed Rust, Go, TypeScript, Python,
and gRPC application execution. Each language publishes the same exact
symbolic secret-output metadata and a language-appropriate redacted diagnostic
surface. SDK operation identities are generated directly from the complete
named-query module; they are not inferred from the MCP inventory. Secret
queries remain deliberately absent from generated MCP tool catalogs and every
reactive query catalog.

The additive identity transition is frozen in
`fixtures/riffql/operational-identity-rotation-v1.json`. That generated receipt
records the v1/v2/v3 language, IR, plan/family, module, manifest, role,
application-lock, and Rust/Go/TypeScript/Python/MCP artifact hashes. It also
binds the unchanged ordinary-v1 TicketDesk and agent-alpha closures. The normal
generated-client check regenerates the receipt, so a partial rotation or stale
example identity fails the repository gate.

## Generated artifacts

The compiler emits:

- Rust parameter, result-union, command-input, and operation types;
- TypeScript equivalents;
- exact per-query `query/entity/field` secret-output metadata and redacted
  debug/string/representation helpers when required;
- exact contract and module identity constants;
- response-identity verification helpers; and
- optional module-qualified MCP read tools with domain-shaped JSON Schemas for
  queries that do not return secret-classified fields.

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
