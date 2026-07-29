# Agent Application Alpha

Status: proposed post-WP-300 milestone under ADR-0056. It does not describe the
current product surface and cannot start until ADR-0056 is accepted.

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

## Scope boundary

This milestone does not add replication, partitioning, general SQL, generic
bulk writes, cross-partition transactions, or inferred business invariants.
It completes the application-authoring experience over the safety boundary in
`docs/safety-by-construction.md`.
