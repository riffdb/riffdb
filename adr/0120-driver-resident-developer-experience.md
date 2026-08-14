# ADR-0120: Driver-Resident Developer Experience

- **Status:** Proposed
- **Direction approved:** 2026-08-13 (maintainer, in session)
- **Exact text accepted:** No
- **Decision deadline:** Before any driver package is published to a public
  registry, and before the alpha's release-polish tier freezes CLI verbs

## Context

RiffDB's product identity for application developers is a cross between a
database driver and an ORM: it installs into a new or existing project,
manages schema and migrations, and then lets the application connect and
query. It is not an application framework — it does not own the project's
layout, HTTP surface, or lifecycle.

Today's developer path contradicts that identity. The binaries build only
from this repository's source; no runtime package is published to any
registry (generated TypeScript vendors its runtime into the project); the
schema lives inside an application-manifest project identity; installing a
schema into a running database takes four commands whose acceptance ceremony
fires even when the tool itself authored everything being accepted; and the
four language paths each tell the story differently. The sealed agent-gate
evidence (ADR-0056) predates most of the current surface and was measured
from a repository checkout — a path no package-arrival user will ever walk.

The maintainer set the bar: a hello-world in any supported language is
schema → install → generate → query, with one consistent story across all
four drivers, arriving through each ecosystem's own package manager.

## Proposed Decision

### 1. One story, four ecosystems

Every driver ecosystem delivers the same four-step story through its native
package manager, with identical verbs and semantics:

| | get the tools | runtime package |
|---|---|---|
| TypeScript | `npm install -D @riffdb/cli` | `@riffdb/client` |
| Python | `pip install riffdb-cli` (pipx-friendly) | `riffdb` |
| Go | `go install` (module) or the binary installer | published Go module |
| Rust | `cargo install riffdb-cli` / crates.io | published crate |

The CLI packages carry prebuilt platform binaries (the esbuild pattern);
no user needs a Rust toolchain or this repository. The runtime packages are
the same first-party code that today ships by vendoring; generated code
imports them normally. Rust remains the sole implementation of transport
trust and semantics per ADR-0106 — the packages are distribution vessels,
never reimplementations.

### 2. The verb set

`riffdb init` writes a minimal `riffdb.toml` (database URL or alias, schema
path, generator targets) and an empty schema into a NEW OR EXISTING project;
it does not scaffold an application. `riffdb push` installs the schema into
the configured database: check, lock, and deploy in one verb, surfacing the
explicit-acceptance ceremony ONLY when compiler-owned identities actually
change — the ceremony's value is review of change, and a fresh scaffold or a
no-op push has nothing to review. `riffdb generate` emits the SDK for the
configured targets against the exact lock. `riffdb status` and
`riffdb diff` report installed-versus-local schema state. `riffdb migrate`
surfaces the existing staged-migration machinery for changes `push` refuses
(incompatible successors), with the same acceptance gates it has today.
`riffdb dev` remains the disposable local daemon — the "have a database
running" prerequisite, like running Postgres locally. `riffdb agent init`
installs the agent rails into the current repository: the skill file, an
AGENTS.md section describing the schema-push-generate-query loop, and MCP
configuration pointing at the configured database.

No verb weakens a guarantee: push wraps the same check/lock/deploy path,
migrations keep their gates, and generated code keeps identity pinning,
typed outcomes, and exact decimal handling unchanged.

### 3. The database describes itself

The daemon serves guidance as MCP resources alongside the existing catalog:
version-exact, application-specific documentation (this application's
entities, named operations, staleness surfaces) discovered over the same
connection agents already use. Published packages carry the generic surface
distillation (llms.txt convention) and README quickstarts; the versioned
docs site carries the same generated artifacts. All teaching artifacts are
generated from one in-repo source with a check-generated referee, so they
cannot drift from the surface.

### 4. Consistency is a conformance surface

A cross-driver conformance matrix proves the four ecosystems tell one story:
the same schema, pushed and generated in each language, produces clients
whose operation identities, outcomes, idempotency behavior, and public
errors agree (extending the existing four-language golden-workload
precedent), and whose quickstart step count is pinned — a documented step
added to any driver's hello-world is a red conformance run, not a docs
drift. The sealed agent-gate campaign (ADR-0056) is re-run starting from
package installation in an empty directory, never from a repository
checkout, with time-to-first-committed-row, ceremony count, and rescue
count recorded as standing evidence.

### 5. Editor tooling is part of the driver product

The contract language gets first-class editor support, built on the
compiler rather than beside it: a tree-sitter grammar for `.riff` and
`.riffq` (highlighting in editors and on forges), and an LSP server
(`riffdb lsp`) shipping inside the same CLI packages, serving diagnostics
by running the same check path the compiler owns — spans and messages
identical to `riffdb push`, so editor feedback can never drift from the
authority. Beyond diagnostics: hover types, go-to-definition across
entities, fields, commands, and queries, and completions fed by the schema.
This serves agents as directly as humans: agent harnesses consume LSP
diagnostics natively, so a contract error surfaces in the loop the moment
it is written instead of at the next push.

### 6. Boundaries

The application starter (`riffdb new`) remains as optional greenfield sugar
and is not the identity. Framework integrations remain in dedicated
repositories per ADR-0117. The repository boundary is unchanged: driver
packages are first-party product; integration packages are not.

## Options Considered

1. **Framework-shaped onboarding** (app scaffold as the front door):
   rejected by the maintainer — RiffDB does not own the application.
2. **Per-ecosystem idiomatic divergence** (each driver telling its own
   story): rejected — four stories is four times the teaching surface and a
   conformance hole.
3. **Keep vendored runtimes** (no registry publication): rejected — it makes
   the package-arrival path structurally impossible and every generated
   repo a snowflake.
4. **One story, four ecosystems, driver-resident tooling** — proposed.

## Consequences

- Registry publication creates release obligations (versioning, yanking,
  supply-chain posture) the repo has not carried before; the durable-format
  compatibility statement (ADR-0112) becomes user-facing at the package
  boundary.
- `push` folding check/lock/deploy narrows the ceremony to change-review;
  CI flows keep the explicit verbs.
- The step-count conformance pin makes onboarding regressions loud.
- Binary distribution requires a platform build matrix in release
  machinery.

## Compatibility

Additive CLI verbs; existing verbs unchanged. `riffdb.toml` is new,
optional, and never required by the existing application-manifest flow.
Published packages start at pre-1.0 versions matching the alpha's
compatibility statement. No wire, durable, or contract-language change.

## Security

Platform binaries are checksummed and the release process signs artifacts;
the packages carry no credentials or endpoints. `agent init` writes only to
the invoking repository. The MCP guidance resources expose only what the
bound role may already discover through the catalog — documentation follows
authorization, never precedes it.

## Standing Design Tests

- **Interface safety:** no new verb or package exposes an operation the
  public surface does not already govern; push/migrate wrap existing gated
  paths; guidance resources are authorization-filtered.
- **Scale:** distribution and generation are per-project offline concerns;
  the daemon's guidance resources are bounded generated artifacts.

## Testing

- Cross-driver conformance matrix (same schema, four languages, agreeing
  identities/outcomes/errors, pinned step counts).
- Package-arrival smoke per ecosystem in CI-shaped scripts: install from a
  local registry mirror, init, push, generate, query.
- Ceremony tests: push with no identity change prompts nothing; push with a
  changed lock requires acceptance; incompatible change refuses toward
  migrate.
- Campaign-03 sealed runs from package install with recorded cliff metrics.
- LSP diagnostics parity: the same broken contract produces byte-identical
  primary diagnostics through riffdb push and the LSP; grammar snapshot
  corpus over the language reference's examples.

## Requirements and Work Packages

- **Requirements:** to be registered as a `DX-*` family at package time
  (one-story conformance, package-arrival path, ceremony-on-change-only,
  self-describing daemon).
- **Defines or blocks:** WP-601 (distribution and published runtimes),
  WP-602 (init/push/status/diff/migrate verbs and riffdb.toml), WP-603
  (agent init, MCP guidance resources, generated teaching artifacts),
  WP-604 (cross-driver conformance matrix and campaign-03), WP-605
  (tree-sitter grammar and LSP server).

## Decision Deadline

Exact acceptance before WP-601 publishes any package or WP-602 freezes verb
semantics.
