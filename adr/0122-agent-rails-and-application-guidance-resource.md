# ADR-0122: Agent rails and application guidance resource

- Status: Accepted
- Date: 2026-08-14
- Decision owners: RiffDB maintainers
- Scope: WP-603

## Context

ADR-0120 requires a repository-local agent bootstrap and application-specific
MCP guidance. ADR-0008 deliberately froze the v1 resource registry, and
ADR-0046 requires hosted and stdio presentation parity. The maintainer approved
an additive compatibility checkpoint and the narrow WP-603 scope expansion on
2026-08-14.

## Decision

### Repository-local installation

`riffdb agent init` operates only in the invoking repository. It preflights and
then publishes these three managed surfaces as one logical operation:

- `.agents/skills/riffdb/SKILL.md`, whose bytes are generated in the CLI;
- one `<!-- riffdb-agent:start -->` through `<!-- riffdb-agent:end -->` block in
  root `AGENTS.md`; and
- one `mcpServers.riffdb` object in root `.mcp.json`, invoking `riffdb-mcp` with
  the endpoint and database selected by the bounded project configuration.

The command never writes a credential. Authentication remains in the existing
protected environment or config path. A rerun is byte-idempotent. Symlinks,
duplicate markers, conflicting managed files or server entries, invalid JSON,
unsafe paths, and any failed preflight cause no write. Unknown `.mcp.json`
members and unrelated MCP server entries are retained semantically; successful
publication uses canonical pretty JSON with a trailing newline. Unmanaged
`AGENTS.md` bytes are retained exactly apart from the appended managed block.

### Application guidance MCP resource

The additive v2 resource registry adds exactly one concrete, non-subscribable
resource:

- URI: `riffdb://application/guide`
- descriptor branch: `application_guidance`
- title: `Application guide`
- description: `Authorization-filtered version-exact application guidance.`
- MIME type: `text/markdown`
- converter: `riffdb.mcp.resource.application-guidance/v1`

It is listed only when the active-contract descriptor is visible. A read
performs fresh authorization through the existing bounded discovery operations
and renders only the complete same-fence, policy-visible catalog: exact active
contract identity, visible entity-schema links, visible command plan and
documentation links, compiler-owned tool names, and visible consistency or
staleness resources. It includes no source text, credentials, authorization
encoding, hidden counts, inferred permissions, or storage access. Dynamic text
uses the existing injection-safe Markdown builder.

Concrete discovery reserves one item from the existing 500-item page ceiling
so inserting guidance beside the active-contract descriptor can never emit 501
items. Template pagination remains unchanged. An over-limit or nonconforming
backend page fails closed.

The adapter owns composition; no new service operation, RPC, durable encoding,
IR encoding, storage key, or authority edge is introduced. Hosted HTTP and
stdio use the same renderer and equivalent discovery inputs. The accepted v1
registry fixture remains byte-for-byte retained; v2 receives its own checked
fixture and identity.

### Single-source teaching artifacts

One bounded structured source under `fixtures/agent-guidance/` generates the
embedded skill, managed AGENTS block, `docs/llms.txt`, and handbook quickstart.
`scripts/check-generated` referees drift. WP-603 produces package-ready
canonical assets; WP-601 owns copying them into ecosystem distribution
packages when that package layout is available.

## Interface-safety design test

The new CLI surface can install only fixed generated rails and cannot express
credentials or relax a database guarantee. The new MCP surface is read-only,
policy-filtered, bounded, and derived entirely from existing safe discovery.
Neither surface allows an application or agent to opt out of authorization,
idempotency, scoping, freshness, boundedness, or acknowledged durability.

## Consequences

The resource registry changes additively from v1 to v2 and therefore requires
the new human-accepted fixture. WP-603 may edit `crates/riffdb-mcp-stdio/**` to
preserve transport parity. Ecosystem package placement remains a WP-601 task;
the generated source artifacts and drift check are complete in WP-603.
