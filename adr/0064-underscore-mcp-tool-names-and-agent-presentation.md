# ADR-0064: Underscore MCP Tool Names and Agent Presentation

- **Status:** Accepted
- **Date:** 2026-07-30
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `MCP-020`, `MCP-021`, `MCP-023`, `MCP-040`,
  `MCP-043`, `MCP-045`, `MCP-049`
- **Related work packages:** `WP-380`, `WP-381`, `WP-385`
- **Amends:** ADR-0008, ADR-0020, ADR-0040, ADR-0046, ADR-0047

## Context

Some MCP hosts, including the maintainer's target Grok integration, reject or
fail to expose tool names containing dots. The pre-alpha RiffDB MCP catalog
currently uses dotted fixed names and
`riffdb.cmd.<contract-segment>.<command-segment>` for generated command tools.
The semantic MCP path works, but invalid argument errors do not identify the
failed field and generated command documentation emphasizes executable IR facts
over invocation guidance.

The human maintainer explicitly approved an underscore-only incompatible
pre-alpha naming cut and the complete MCP ergonomics improvement set in the
current Codex session on 2026-07-30.

## Decision

### Exact command-tool name

ADR-0020's v1 dotted command name is superseded for newly generated and active
pre-alpha artifacts. Every command tool has exactly:

```text
riffdb_cmd_<contract-segment>_<command-segment>
```

The segment normalization, source-identifier restrictions, compiler ownership,
stable command-ID ordering, collision rejection, and 128-byte complete-name
limit remain exactly as defined by ADR-0020. The two separators between prefix,
contract, and command are literal underscores. The primary golden is:

```text
source contract: Legal_Spend
source command:  Allocate_Budget2
tool name:       riffdb_cmd_legal_spend_allocate_budget2
```

The compiler rejects collisions under the complete underscore form. The catalog
revalidates it, and MCP consumes it verbatim. There are no dotted compatibility
aliases. Because tool names also occur inside accepted outcome locators, all
pre-alpha generated bundles and fixtures are regenerated as one compatibility
cut. The locator grammar itself does not otherwise change.

Every fixed RiffDB MCP tool name also replaces dots with underscores, for
example `riffdb_server_health`, `riffdb_contract_validate`, and
`riffdb_contract_get_active`. Fixed and dynamic names must match
`[a-z][a-z0-9_]{0,127}`. Resource URIs remain unchanged.

The same complete-name grammar applies to every other tool advertised by a
RiffDB MCP server. Symbolic query tools use their existing words separated by
underscores, generated application tools use
`<module-segment>_<operation-segment>`, and contract-builder tools use the
`riffdb_builder_` prefix. Their semantic identities, authorization operations,
schemas, and resource URIs do not otherwise change. No advertised tool name
contains a dot and no dotted compatibility alias is served.

### Actionable, redacted input diagnostics

Schema validation returns at most one deterministic first violation in canonical
schema traversal order. MCP invalid-parameter data uses
`riffdb.mcp.input-error/v1` and contains only:

- a stable public error code;
- a bounded JSON Pointer to the invalid or missing location; and
- a bounded public expectation derived from schema structure.

Supported codes include missing required property, unexpected property, wrong
JSON kind, failed constraint, and unmatched union. Diagnostics never echo a
submitted value, credential, unrestricted schema fragment, internal source, or
stack detail. Invalid schema documents and output-schema violations remain
generic internal failures.

### Agent-facing command documentation

The existing command `/docs` resource becomes an invocation guide derived only
from compiler-owned public metadata. It includes the exact tool name, bounded
summary, input field/type/constraint table, complete JSON call example,
declared outcome names and examples, retry/idempotency guidance, and a link to
the corresponding `/plan` resource.

Execution stages, expression dumps, dependency keys, and other detailed typed IR
remain in `/plan`; they are not duplicated into `/docs`. Generated documentation
must not invent business prose or conditions absent from contract/compiler
metadata.

The repository includes an agent cookbook covering string presentation for
fixed-scale decimals and UUIDs, caller-owned idempotency keys, declared
business rejections, uncertain outcomes, policy-filtered discovery, and the
distinction between `/docs` and `/plan`.

`tools/list` remains the first-class authorized command catalog. No redundant
list-authorized-commands tool is added.

## Compatibility

The underscore change is an intentionally incompatible pre-alpha public MCP and
compiled-bundle compatibility cut. No dotted alias is served. It changes no
command semantics, authorization, durable command identity, storage record, or
gRPC method. Checked fixtures make stale dotted bundles and invocations fail
closed rather than reinterpret them.

## Security

Tool discovery and invocation remain policy-filtered shared-service operations.
Improved diagnostics reveal only public schema shape and bounded paths.
Documentation derives from already-authorized public command metadata and
retains redaction and output bounds.

## Testing

- Compiler and catalog golden, collision, length, stale-name, and compatibility
  fixtures for underscore names.
- Fixed registry, generated schema, resource locator, gRPC bridge, stdio, hosted
  MCP, SDK generator, and end-to-end fixture regeneration.
- A repository-wide generated-artifact check that rejects public MCP tool names
  containing dots or bytes outside the canonical grammar.
- Per-keyword schema-validation tests asserting exact bounded redacted
  diagnostics and absence of submitted values.
- Golden command docs and cookbook link checks.
- Grok-compatible `tools/list` smoke evidence using underscore-only names.

## Requirements and Work Packages

- **Requirements:** `MCP-025`, `MCP-026`, `MCP-034`
- **Defines or blocks:** `WP-380`, `WP-381`
- **Final evidence:** `WP-385`
