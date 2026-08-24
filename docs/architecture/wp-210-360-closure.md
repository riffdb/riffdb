# WP-210 through WP-360 closure audit

This audit closes the text-first application-authoring arc against current
`main`. It covers the RiffQL compiler and executor, public application service,
generated clients, roles and errors, development workflow, unfamiliar-domain
evidence, and the original four-run Agent Application Alpha gate.

## Product result

The shipped application path has the intended hierarchy:

```text
.riff contracts + .riffq named queries
  -> exact application lock and generated facade
  -> API-neutral application service
  -> authorization / command runtime / commit coordinator
```

Applications do not name numeric entity, field, index, permission, or command
input IDs. Reads are bounded named RiffQL operations; writes are symbolic
commands with declared outcomes and idempotency. Roles compile into exact
capabilities. Rust, Go, TypeScript, Python, CLI, and MCP consume the same locked
operation identities without a kernel or handwritten transport escape.

The implementation includes the bounded dependent-key batch needed for the
TicketDesk label junction, closed query authority and execution proofs,
whole-query cost/fuel, declared same-partition relationships and uniqueness,
structured public semantic errors, resumable command batches, canonical
scaffolding and development orchestration, source-spanned diagnostics, exact
application locking, offline TypeScript/builder-MCP support, and generated
Rust/TypeScript application facades.

## Evidence

- `release/evidence/agent-application-alpha-gate-v3.json` records an
  `eligible` original Agent Application Alpha gate.
- `docs/agent-application-alpha.md` records campaign 02 as passed and preserves
  the exact boundaries, language observations, limitations, and successor
  six-run campaign-03 requirement.
- `release/evidence/agent-application-alpha-rehearsal-v1.json` and
  `release/evidence/agent-application-alpha-package-campaign-v1.json` retain
  public-only rehearsal and package-first evidence.
- Current `scripts/agent-application-alpha-package-acceptance` passes the
  reproducible sealed package and live Rust, Go, Python, and TypeScript cells.
- Current application binding, boundary, Python runtime/package, TypeScript,
  scaffold, generated-artifact, requirement, handbook, workspace-test,
  formatting, Clippy, documentation, and dependency-policy gates pass.

## Historical and successor boundaries

This closure does not claim the deployable alpha is released. WP-340 is the
original four-run Rust/TypeScript gate; the accepted later roadmap expands the
final claim to installed package-first Rust, Go, TypeScript, and Python cells.
That successor gate remains WP-579. Likewise, current cloud performance is
owned by WP-623/WP-674 and endurance by WP-578.

The original POC release packages WP-200 and WP-205 remain open because their
old external benchmark source revisions are not currently reproducible. They
are not silently closed by this application-layer evidence.
