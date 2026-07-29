# Agent Application Alpha

Status: accepted post-WP-300 milestone under ADR-0056. WP-305 provides the
canonical manifest and complete generated TicketDesk bindings. The document
still describes a target gate that may not be claimed before WP-340 passes.

## Gate

A fresh coding agent can build an unfamiliar application from an empty
repository using only RiffDB's public textual, generated, CLI, and MCP surfaces.

This gate precedes single-node operational hardening, replication, and
partitioning.

## Required result

| Area | Gate requirement |
|---|---|
| Application code | No raw entity, field, index, or command-input IDs |
| Reads | One exact named RiffQL operation per list or detail screen |
| Writes | Symbolic compiled commands with typed declared outcomes |
| Generated clients | No handwritten parameter maps, encoders, decoders, status parsers, or RPC wrappers |
| Authorization | Symbolic roles; no field IDs, masks, raw grants, or `ReadContract` ritual |
| Errors | Bounded structured context names the operation and actionable symbolic cause without leaking hidden data |
| Local development | `riffdb new`, then `riffdb dev` performs bootstrap, deploy, role bind, generation, seed, and watch |
| Bulk data | Bounded resumable concurrent command batches with per-item identities and typed outcomes |
| Boundaries | No kernel package, request, permission, or encoded key in application code |
| Languages | Rust and TypeScript complete the same golden workload |
| Generality | Blog/CMS and orders/inventory both succeed |
| Agent evaluation | Four sealed fresh-agent runs each rate the experience at least 8.5/10 |

## Work sequence

1. WP-305 completes generated bindings and the application manifest.
2. WP-310 compiles symbolic roles into exact application authority.
3. WP-315 preserves bounded semantic error context across every public surface.
4. WP-320 adds resumable command batches and uses them for development seed.
5. WP-325 adds the canonical scaffold, dev orchestrator, application facade,
   and mandatory kernel-boundary lint.
6. WP-330 proves a real TypeScript web application against the same manifest
   and observations as Rust.
7. WP-335 builds the two unfamiliar domains, records every unsupported shape,
   and implements only separately approved bounded RiffQL additions.
8. WP-340 runs the sealed independent evaluation and publishes the gate report.

WP-305, WP-310, WP-315, WP-320, and WP-325 are intentionally ordered because
the scaffold must consume complete clients, roles, errors, and batches rather
than grow temporary alternatives. RiffQL expansion occurs after both language
paths are usable so application evidence is about the query language rather
than missing client plumbing.

WP-315's public contract and disclosure rules are documented in
`docs/getting-started/APPLICATION-ERRORS.md`. Application examples and generated
clients must use that semantic error object; exposing the compatible kernel
error or reconstructing context from numeric IDs is a boundary failure.

WP-320's batch rule is documented in
`docs/getting-started/COMMAND-BATCHES.md`. A batch is only a bounded client
scheduler for separately authorized exact commands; it never grants collection
atomicity or a generic mutation path.

## WP-325 handoff

Work package: WP-325

Requirement IDs: AAA-008, AAA-009

ADRs consulted: ADR-0007, ADR-0008, ADR-0040, ADR-0041, ADR-0052,
ADR-0055, ADR-0056

Upstream revision: `f079800`

Allowed paths used: CLI and Rust application facade crates, application
templates and fixtures, `examples/agent-alpha`, development and boundary
scripts, getting-started and alpha documentation, root Cargo metadata, and
README.

Behavior added or changed: deterministic compiled `riffdb new`; exact offline
application regeneration; generic manifest-driven `riffdb dev`; protected role
binding and resumable seed; compiler-owned Rust, TypeScript, and MCP outputs;
mandatory application-boundary linting; generated Rust write and page-read
acceptance.

Compatibility classification: additive application surface. The kernel
protocol is unchanged. `riffdb dev` retains the TicketDesk presets and maps its
new `application` default to `ticketdesk-application` in the source workspace.

Security implications: scaffolding fails before writing on compiler failure and
never overwrites a destination. Regeneration checks exact pinned identities.
The boundary linter rejects kernel/transport packages and handwritten protocol
adaptation. Generic development grants only the manifest-declared symbolic
role; kernel access remains an explicit separate TicketDesk source-workspace
preset and credential.

Generated artifacts checked: `examples/agent-alpha` Rust, TypeScript, MCP, and
manifest artifacts are emitted from one compiled contract and query module.
The acceptance script proves byte-for-byte deterministic scaffolding.

Known limitations: the v1 manifest has one generated output target, so the
offline generator accepts exactly one query module. Watch mode accepts edits
only when the updated manifest pins the new identities; it does not silently
rewrite immutable identities. WP-330 owns TypeScript runtime parity and WP-335
owns unfamiliar-domain evidence.

## WP-330 TypeScript application boundary

The generated TypeScript client now embeds compiler-owned input and result
schemas for every query and command. The first-party `@riffdb/application`
runtime uses those schemas to:

- encode UUIDs and enums without guessing from field names;
- reject extra, missing, malformed, oversized, or wrong-cardinality values;
- decode command outcome records through compiler-emitted field identities;
- decode page-shaped query records through symbolic result names;
- preserve exact contract, module, query, and plan identities;
- preserve a caller-provided idempotency key across the Rust CLI's bounded
  uncertainty recovery;
- carry cursor and read-after-commit options; and
- normalize the CLI application-error envelope before the generated closed
  error decoder accepts it.

The application imports only `@riffdb/application` and its generated client.
It does not import gRPC, protobuf, kernel requests, field masks, value codecs,
or a handwritten adapter. The runtime's CLI transport is a POC application
transport over the same public application service; it is intentionally named
`CliApplicationTransport` and makes no direct-channel latency claim. A
long-lived native TypeScript channel can replace that transport without
changing generated application operations or their safety schemas.

`scripts/agent-application-typescript-acceptance` starts the real local server,
binds only `AgentAlphaApplication`, seeds through command batches, starts an
HTTP web application, executes `CreateItem`, fences `ItemPage` after the
returned commit sequence, and checks the language-neutral observation fixture.
`scripts/check-application-bindings` proves generated identities and rejects a
handwritten transport or kernel dependency.

WP-335 exposed and corrected one WP-330 parity defect before unfamiliar-domain
evidence was accepted: the TypeScript CLI transport originally generated types
for every scalar but could not encode integer, decimal, money, bytes, date, or
timestamp inputs. The transport and public CLI now use closed symbolic tags
with range and shape checks, including decimal coefficients as bounded base64
and integers as decimal strings. No JSON-number precision loss or numeric
contract field ID is involved. This allowed the Orders corpus to retain real
`i64` quantities, exact decimal prices, nonnegative inventory invariants, and
atomic reservation arithmetic instead of weakening the model to strings.

## WP-335 unfamiliar-domain evidence

The Blog/CMS and Orders/Inventory applications compile and generate without a
RiffQL language change.

The Blog application includes symbolic commands for sites, authors, posts,
slug routes, comments, tags, and attachments. Its named reads cover a public
feed, slug lookup, moderation queue, and one-snapshot post page with author,
comments, and tags. Slug lookup uses an explicit `PostSlug` primary-key entity;
it does not use a read-before-write uniqueness check. Creating the post and
creating its route are separate idempotent commands because a declared
relationship must observe an existing target rather than infer integrity from
command ordering.

The Orders application includes customers, products, exact prices, inventory,
orders, lines, and a reservation command. The reservation command checks order
state, line quantity, and available inventory in one compiled command, then
atomically changes the order and inventory. Its named reads cover customer
history, open orders, an inventory dashboard, and one-snapshot order detail
with a bounded line-to-product batch.

Rust and TypeScript generated clients compile from the same exact manifests.
The generated MCP catalogs, roles, manifests, and language-neutral workload
observations are byte-reproducible. The application boundary linter remains
green with zero handwritten transport glue and no kernel dependency.
The domain gate also starts a fresh server for each application, deploys its
exact contract and query module, binds only its generated symbolic role, seeds
all seven commands through resumable batches, and executes the page workload
through both generated Rust and TypeScript clients.

Four deliberately unsafe shapes are retained as executable diagnostic
fixtures:

| Shape | Classification | Diagnostic | Safe remedy |
|---|---|---|---|
| Blog title ordering without a matching index | index | `RDB-QP003` | Declare the suggested index or use the status feed |
| Author lookup without `site_id` | locality | `RDB-QP002` | Supply the partition key |
| Treating a line collection as one scalar product key | cardinality | `RDB-QP007` | Use the bounded dependent-key batch |
| Inventory collection without `take` | bounds | `RDB-QS009` | Add a positive bound and optional cursor |

The canonical report is `fixtures/agent-alpha/gap-report-v1.json`; the gate is
`scripts/agent-domain-evidence --assert-complete`. No repeated missing bounded
construct was found, so WP-335 adds no grammar, IR, plan-hash, authorization,
storage, cursor, or result semantic.

### WP-335 handoff

Work package: WP-335

Requirement IDs: AAA-011

ADRs consulted: ADR-0002, ADR-0013, ADR-0051, ADR-0054, ADR-0055, ADR-0056

Upstream revisions: `046709a`, plus the focused WP-330 correction `6c80d60`

Allowed paths used: contract and query corpora, query-module evidence generator,
Rust/TypeScript/MCP generated domain clients, agent-alpha fixtures and examples,
domain evidence and boundary scripts, and RiffQL/alpha documentation.

Behavior added or changed: two unfamiliar symbolic application corpora,
deterministic manifests and roles, resumable command seed inputs, typed
cross-language clients, language-neutral observations, and source-spanned
negative diagnostic evidence.

Compatibility classification: additive application evidence only. RiffQL v1,
query IR v1, plan hashes, storage, service authorization, cursor encoding, and
result semantics are unchanged.

Security implications: every positive query remains same-partition, indexed,
bounded, whole-query authorized, and one-snapshot. Every write remains a
compiled idempotent command with declared outcomes. The negative fixtures prove
there is no fallback for four common unsafe access patterns.

Generated artifacts checked: manifests, roles, Rust, TypeScript, MCP catalogs,
golden observations, and the gap report regenerate byte-for-byte.

Known limitations: RiffQL still has no unbounded aggregates, general joins,
computed expressions, fragments, or search escape hatch. The two applications
did not produce repeated evidence sufficient to add any of them.

Follow-up issues: WP-340 must run four genuinely independent sealed evaluations;
these corpus checks are product evidence, not a substitute for independent
agent ratings.

## Measurements

Every evaluation run records:

- human interventions;
- attempted and successful kernel use;
- handwritten RiffDB glue lines;
- compiler and runtime failures per completed feature;
- unsupported query shapes;
- time from empty repository to first successful write;
- time to first page-shaped read;
- time to the complete workload;
- implementation-source access; and
- rating with an explanation.

The machine-readable report retains event timestamps, tool invocations, stable
diagnostic/error codes, generated-file hashes, boundary-lint output, and final
acceptance observations. It must not retain credentials or application values
that are outside the checked fixture set.

## Alpha thresholds

Each of four runs—both domains in both Rust and TypeScript—must have:

- zero human product-workaround interventions;
- zero successful kernel use and no kernel import in the final source;
- zero handwritten RiffDB transport, encoding, decoding, or authorization glue;
- no RiffDB implementation-source or TicketDesk-source access;
- a first successful write within 30 minutes;
- a first page-shaped read within 60 minutes;
- no unresolved required query shape; and
- a rating of at least 8.5/10.

The gate report must publish raw measurements and explanations, not only the
aggregate score. A failed run reopens its owning product package and cannot be
waived by modifying the evaluation application.

## WP-340 sealed evaluator status

The release-derived evaluator harness is implemented.
`scripts/agent-application-alpha-package` creates a new
content-hashed bundle containing release binaries, public docs, builder MCP,
the public Rust SDK, the TypeScript runtime/toolchain, four briefs, and
redaction-safe schemas. It rejects TicketDesk artifacts and any non-SDK Rust
source. Its self-test creates a new application, starts the installed
development workflow, grants its symbolic role, seeds through commands, and
executes its generated Rust client with network disabled. Both Rust and
TypeScript toolchains are included and checked before publication.

`scripts/agent-application-alpha-acceptance --runs 4 --sealed --assert-gate`
rebuilds the exact bundle and then requires four raw report/transcript pairs.
It checks unique agent identities, exact domain/language coverage, bundle and
transcript hashes, zero successful kernel use, zero handwritten glue, no
prohibited source access, no unresolved shape, both time thresholds, golden
and boundary gates, and a rating of at least 8.5.

No report is generated from WP-335 or from the harness itself. Those are
product and harness tests, not independent-agent evidence.

Four independent Terra evaluations ran against sealed bundle
`1afb8faf08f43dd5f38a4b2c90a1bfad450eb1ce21663457ebbe4a1c9865827a`
without RiffDB implementation or TicketDesk source access:

| Domain | Language | Rating | Golden | Principal result |
|---|---|---:|---|---|
| Blog | Rust | 1.0 | failed | Scaffolding and the minimal manifest failed without enough public schema diagnostics to author the application |
| Blog | TypeScript | 2.0 | failed | Scaffolding failed before a generated RiffDB application could be written |
| Orders | Rust | 3.0 | failed | The sample write/read worked, but an edited contract could not refresh its pinned identities |
| Orders | TypeScript | 2.0 | failed | The sample write/read worked, but the richer Orders contract and operations could not be generated |

All raw reports and redaction-safe event transcripts are published unchanged
under `evaluations/agent-application-alpha/runs`. Three final applications
passed the boundary checker, and no run used a kernel API, handwritten RiffDB
transport glue, implementation source, or TicketDesk source. These controls
worked as intended. The product gate nevertheless failed because no evaluator
completed its assigned unfamiliar-domain workload or reached the 8.5 rating
threshold.

The repeated product defect is in the authoring loop, not RiffQL execution:
after editing a scaffolded contract or query module, a public application
author has no discoverable command that compiles the source, reports
source-spanned diagnostics, and safely refreshes the manifest's pinned
contract, module, query, and plan identities. The Blog runs additionally
exposed confusing `riffdb new` destination behavior. The next product package
must close those defects before the same evaluation is repeated.

The current machine-readable decision is
`release/evidence/agent-application-alpha-gate-v1.json`: all four required
independent runs are present and the decision remains `not_eligible`.
Operational alpha and replication remain blocked. A failed report cannot be
reclassified or edited into a pass; a future evaluation must use a newly
sealed bundle and fresh agent identities.

## Scope boundary

This milestone does not add replication, partitioning, general SQL, generic
bulk writes, cross-partition transactions, or inferred business invariants.
It completes the application-authoring experience over the safety boundary in
`docs/safety-by-construction.md`.
