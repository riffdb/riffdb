# ADR-0155: Selectively Declared Generated Application Surfaces

- **Status:** Accepted
- **Direction approved:** 2026-08-26
- **Exact text accepted:** Yes, 2026-08-26
- **Accepted:** 2026-08-26
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Codex session for upstream commit `03eea78c`
- **Decision deadline:** Before WP-702 adds a sparse generation declaration or
  successor application-source, manifest, or lock identity
- **Requires:** ADR-0052, ADR-0055, ADR-0057, ADR-0075, ADR-0112, ADR-0120,
  ADR-0121, ADR-0124, and ADR-0148
- **Amends if accepted:** ADR-0121's project-selected materialization by allowing
  the exact symbolic application itself to declare only the generated surfaces
  it owns
- **Defines or blocks:** WP-702 and WP-703

This record is authoritative for WP-702 and WP-703.

## Context

RiffDB supports generated application clients for Rust, Go, TypeScript, and
Python plus a generated MCP tool catalog. Application source V6 requires all
five output paths even when an application repository uses only one language.
The compiler unconditionally renders Rust, TypeScript, and MCP, conditionally
renders the version-required Go and Python artifacts, and records their hashes
in the exact compiler-owned application lock.

ADR-0121 added a local `[project].generators` selection. That selection avoids
materializing an absent unselected language file, while preserving and checking
an unselected file that already exists. It does not make the symbolic
application itself selective: source V6 still requires every target path, the
compiler still constructs and hashes every target, and MCP is classified as a
non-language artifact and is always emitted. Consequently a Go library package
cannot truthfully be Go-only. It must declare placeholder Rust, TypeScript,
Python, and MCP destinations and its exact lock remains coupled to generators
and artifacts it neither ships nor tests.

A real external adapter now demonstrates the cost. Its source repository is a
Go module and its application package needs generated Go bindings. Requiring
npm/TypeScript, Rust facade, Python, and MCP artifacts adds irrelevant files,
toolchains, lock churn, publication dependencies, and failure modes. Omitting
those artifacts after lock compilation is not honest because the retained lock
claims their exact hashes.

This is a general packaging and exact-identity issue, not permission to move
database semantics into a target language. Every selected generated facade
must still be produced by the first-party Rust compiler, call the shared Rust-
owned driver/application service, and pass the same semantic corpus. Omitting a
surface must not alter a command, query, role, capability, server-side MCP
availability, authorization, or application behavior.

The selected surface set participates in application source and lock identity.
Making existing required fields nullable in place or letting `riffdb.toml`
silently control lock contents would reinterpret durable release-significant
formats and allow two machines to compile different exact locks from the same
symbolic source. The change therefore needs explicit least-sufficient source,
manifest, and lock successors.

## Proposed Decision

### 1. Make generated surfaces a closed sparse source declaration

Application source V7 will replace V6's required complete `generation` object
with one nonempty closed sparse map whose permitted keys are exactly:

- `rust` for the generated Rust facade;
- `go` for the generated Go facade;
- `typescript` for the generated TypeScript facade;
- `python` for the generated Python facade; and
- `mcp` for the generated MCP tool catalog.

Each present key maps to one existing validated workspace-relative output path.
Absent keys mean that surface is not part of this application package and must
not be generated, materialized, hashed as a generated artifact, checked for
freshness, required for installation, or pulled in as a package-manager
dependency. At least one surface is required. Keys are canonically ordered,
paths are unique, and unknown keys, duplicate normalized paths, unsafe paths,
empty maps, and target aliases fail source parsing.

For example, a Go-only application declares:

```json
"generation": {
  "go": "generated/go/client.go"
}
```

An MCP-only application may declare only `mcp`; a polyglot application may
declare any finite subset. This sparse declaration controls generated package
artifacts only. It is not a runtime feature flag and does not alter deployed
commands, queries, roles, row policies, hosted MCP authorization, or database
capabilities.

### 2. Keep one symbolic source as the identity authority

The V7 source declaration is the sole authority for which optional generated
surfaces participate in the exact application package and lock. Local
`riffdb.toml` may select which declared surfaces to materialize in a particular
checkout, but it cannot add an undeclared target or cause the compiler-owned
lock to vary.

During compilation, the Rust compiler deterministically renders every surface
declared by V7 in memory and records exactly those generated-artifact hashes in
the lock. It need not write every declared artifact to the current checkout.
Project generation writes only the intersection requested by local project
configuration and the source declaration, while checking every declared
artifact that is already present. Selecting an undeclared surface is a typed
configuration error with guidance to amend and accept the symbolic source;
silently inventing a lock member is forbidden.

The canonical application manifest, compiler-owned contract bundle where
required, reactive modules, migration artifacts, and other non-optional
compiler/runtime artifacts remain mandatory under their owning formats. They
are not generated SDK surfaces and cannot be suppressed through the sparse map
or project configuration.

### 3. Generate, lock, install, and check exactly the declared set

Every generation path must derive from one compiler-owned declared-surface
registry. Source parsing, canonicalization, in-memory generation, artifact-kind
mapping, application-lock compilation, local materialization, freshness
checking, installation planning, development publication, scaffold rendering,
and diagnostics may not maintain divergent required-target lists.

For each declared surface:

- the first-party Rust generator renders its complete deterministic bytes;
- the lock binds its kind, validated path, and domain-separated content hash;
- local generation and check commands enforce exact bytes when materialized;
- installation and publication require only the ecosystem artifacts actually
  used by that declared surface; and
- the existing language or MCP semantic conformance remains unchanged.

For each absent surface, those paths perform no generation, write, freshness
read, toolchain discovery, package installation, artifact hash, or lock
requirement. A stale file left at an undeclared former target is not silently
deleted. Migration or project tooling reports it as an unowned file and leaves
removal to an explicit user action.

An application cannot submit generated bytes to influence the authoritative
lock. The compiler always derives hashes from its own generated output. A
source target removal therefore produces a changed proposed source and lock
identity and follows the existing diff/review/acceptance ceremony before
deployment.

### 4. Add least-sufficient version successors

Selective declarations require:

- `riffdb.application-source/v7`, whose generation object is the sparse closed
  map above;
- `riffdb.application-manifest/v5`, which carries exactly the declared surface
  paths needed for exact deployment and introspection; and
- `riffdb.application-lock/v8`, whose artifact inventory requires exactly the
  V7-declared generated set plus its independently mandatory compiler-owned
  artifacts.

These are release-significant application-artifact identities and must be
registered in the version topology with source locators, reader/writer windows,
fixtures, activation compatibility, upgrade guidance, and retirement posture.
No storage, entity, command, query, event, cursor, Protobuf, driver protocol, or
contract-bundle encoding changes.

Application sources V1 through V6, manifests V1 through V4, and locks V1
through V7 remain readable and writable for their registered compatibility
windows. Their complete required target sets and byte-exact canonicalization do
not change. Existing V6 source stays V6; a newly authored or explicitly
migrated sparse declaration uses V7/V5/V8. V7 also permits the complete five-
surface set so an application can add its final missing surface without a
schema downgrade. No old field becomes optional in place, no old artifact
requirement is relaxed, and no decoder is retired.

An explicit migration helper may propose a V7 source whose selected set matches
the repository's configured and present intended targets. It must be read-only
unless the user requests a write, must show the exact removed target set and
proposed source/lock hashes, and must not infer intent from installed
toolchains. Deployment of the changed identity retains the existing acceptance
ceremony.

### 5. Make Go-only scaffolding and development reproducible

`riffdb init` with only the Go language selected will create a V7 application
source declaring only Go, a project configuration selecting only Go, and no
Rust, TypeScript, Python, or MCP generated paths. Generation, check, push,
development publication, `riffdb dev --run`, and clean package installation
must succeed in a fresh Go repository without npm, a TypeScript compiler, a
Python package, or a Rust application facade.

This does not remove the RiffDB CLI/server binaries or the Rust-owned Go driver
host from the toolchain. It means the application package consumes only the Go
distribution artifacts required by the accepted driver architecture. A
selected Go facade cannot own transport trust, authorization, retries,
idempotency, cursor binding, command semantics, or any other logic reserved to
first-party Rust.

The same acceptance applies symmetrically to Rust-only, TypeScript-only,
Python-only, and MCP-only applications. Cross-language conformance continues to
qualify each generator globally; one application need not carry all languages
to benefit from that qualification.

### 6. Separate artifact selection from runtime authority

Omitting `mcp` suppresses only the checked-in/generated MCP catalog artifact.
It does not disable the RiffDB MCP server, widen or narrow a role, hide a
deployed command from an otherwise authorized hosted discovery operation, or
change application-service behavior. Runtime protocol availability and
authorization remain governed by their existing ADRs and exact role/capability
identities.

Likewise, omitting a language facade does not remove its server protocol or
prevent a separately compiled compatible client from invoking an authorized
named operation. The sparse set describes the compiler-owned artifacts shipped
with this exact application package, not the universe of clients capable of
speaking the public protocol.

Generated-surface presence must not enter row policy, command/query planning,
runtime branching, storage keys, cost class, outcome selection, or observable
data semantics. The compiler may use the set only for deterministic artifact
generation, identity, installation, diagnostics, and documentation.

### 7. Preserve bounded and reviewable evolution

The declared set contains at most the five closed V7 kinds. Generation work,
artifact bytes, lock entries, diagnostics, and filesystem paths remain under
their existing per-artifact and aggregate limits. Compiler validation and
generator selection are paid once per source compilation, never per operation,
row, query page, or runtime request.

Adding a new generated language or presentation surface requires a later
accepted ADR to define its first-party generator boundary, driver/runtime
ownership, artifact kind and format, source/manifest/lock version impact,
distribution, conformance, and retirement. V7 decoders reject unknown keys;
they do not silently ignore a future target.

## Options Considered

1. **Keep every target mandatory:** rejected because single-language packages
   remain coupled to unused files, toolchains, artifact hashes, and publication
   failures.
2. **Let `riffdb.toml` determine the lock inventory:** rejected because local
   configuration would make identical symbolic source compile to different
   authoritative locks on different machines.
3. **Keep placeholder paths but omit their files:** rejected because the lock
   would either claim nonexistent artifacts or cease to be an exact complete
   identity.
4. **Make V6 fields optional in place:** rejected because it reinterprets
   canonical source, manifest, and lock compatibility without a versioned
   decoder boundary.
5. **Always keep MCP mandatory:** rejected because an MCP catalog is a generated
   package surface, not a prerequisite for safe Go, Rust, TypeScript, or Python
   application bindings.
6. **Add a sparse V7 declaration with V5/V8 successors:** proposed because one
   reviewed symbolic source remains authoritative while applications ship only
   the compiler-owned surfaces they actually use.

## Consequences

- Go-only and other single-surface application repositories become truthful,
  reproducible packages without unrelated generated files or toolchains.
- Removing or adding a generated surface becomes an explicit reviewed
  application identity change rather than local filesystem behavior.
- Three application-artifact version families gain successors and incur their
  full topology, decoder, fixture, compatibility, and eventual retirement
  obligations.
- The compiler and CLI must consolidate currently duplicated target lists.
- Existing V1-V6 applications retain their current complete artifact behavior
  until explicitly migrated.
- Omitting a generated artifact does not disable a server protocol or change
  runtime authority.

## Compatibility

This Proposed ADR changes no current bytes or behavior. After acceptance,
source V7, manifest V5, and lock V8 are additive successors. Existing source,
manifest, lock, generated artifact, protocol, and storage decoders remain
registered and byte-exact. No automatic rewrite or decoder retirement occurs.

A selective migration intentionally changes application source, manifest,
lock, and artifact-set identities and therefore requires the existing review
and deployment acceptance flow. Contract, query-module, command-plan, data,
event, cursor, and driver-protocol identities change only if their own source
changes; generator selection alone must not rotate them.

## Security

The sparse declaration can only remove or add compiler-generated package
artifacts. It cannot change named-operation authority, role grants, row policy,
secret reveals, transport trust, idempotency, durability, cursor semantics, or
runtime protocol exposure. All declared outputs are still generated by first-
party Rust and hashed into the exact lock.

Paths remain workspace-relative, unique, bounded, symlink-safe at publication,
and written atomically through existing protected filesystem operations.
Diagnostics may name target kinds and safe paths but must not expose source
secrets, credentials, generated secret values, environment contents, or
registry tokens.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** applications choose only which
  safe compiler-generated facades or catalogs belong to their package. They
  cannot supply generated semantics, replace the Rust-owned driver, enable raw
  transport, alter authority, or opt out of database guarantees through target
  selection.
- **Scale:** the source contains at most five target entries and compilation
  generates each declared artifact once beneath existing byte/path limits.
  Selection causes no runtime state, row scan, database rewrite, or operation-
  proportional work.

## Testing

- Canonical source V7, manifest V5, and lock V8 fixtures for each singleton
  surface and representative multi-surface sets, with unordered-input
  canonicalization and exact artifact inventories.
- Negative fixtures for empty maps, unknown targets, duplicate paths, unsafe
  paths, undeclared local selection, missing declared hashes, extra artifacts,
  and mismatched source/manifest/lock sets.
- Byte-exact V1-V6 source, V1-V4 manifest, and V1-V7 lock reader/writer
  regression plus old-runtime activation refusal and explicit migration review.
- Fresh Rust-only, Go-only, TypeScript-only, Python-only, and MCP-only project
  initialization, generation, check, diff, push-preview, installation, and
  development loopback tests.
- A Go-only external package test proving clean generation, `go test ./...`,
  package-manager installation, local driver execution, and application lock
  validation without npm, TypeScript, Python, or a generated Rust facade.
- Global four-language and MCP semantic conformance proving omission from one
  package does not weaken a generator or runtime surface.
- Filesystem tests for existing undeclared files, symlinks, path collisions,
  interrupted atomic publication, and value-free diagnostics.
- Architecture checks proving one declared-surface registry, no target-
  language semantics, no local-config-derived lock, no unconditional MCP path,
  and no operation-time target branching.

## Requirements and Work Packages

- **Future requirements after exact acceptance:** `DX-042` through `DX-049`
- **Versioned source, manifest, lock, and compiler registry:** WP-702
- **CLI, scaffolding, packaging, conformance, and external acceptance:** WP-703

## Decision Deadline

Exact human acceptance is required before WP-702 adds a sparse generation map
or source V7, manifest V5, or lock V8 identity. Making an old field optional in
place, deriving lock contents from local configuration, deleting an undeclared
file automatically, moving semantics into a generated facade, changing hosted
protocol authority, or retiring an existing decoder requires separate exact
human review.
