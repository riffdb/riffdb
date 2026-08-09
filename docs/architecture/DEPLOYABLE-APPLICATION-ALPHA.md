# Deployable Application Alpha Plan

> Status: proposed roadmap. The capabilities on this page are not available
> until their work packages pass. ADR-0105 through ADR-0110 remain Proposed and
> do not override the accepted specification.

The first alpha release must prove more than a good local symbolic API. A real
adapter must be able to install an application, connect from another container,
use a supported language driver, express its bounded operational reads and
writes, coordinate workers safely, rotate authority, and upgrade without a
storage or kernel escape hatch.

## Release gate

The release gate is:

> A fresh OpenFGA-, MLflow-, Payload-, or Woodpecker-shaped adapter can be
> installed and exercised through public symbolic surfaces from a separate
> container, with encrypted authenticated transport, generated supported-
> language bindings, bounded atomic application commands, indexed operational
> RiffQL, fenced workflow concurrency, exact upgrade/provisioning receipts, and
> no raw kernel/storage access.

The gate has six tracks.

| Track | Proposed ADR | Work packages | Required proof |
|---|---|---|---|
| Remote ingress | ADR-0105 | WP-498–WP-499 | TLS-only non-loopback gRPC, proxy interoperability, container health, certificate and capability rotation |
| Drivers | ADR-0106 | WP-500–WP-503 | Stable Go, long-lived TypeScript, expanded Python artifacts, pooling/cancellation/errors/retry/read-after parity owned by Rust |
| Bulk commands | ADR-0107 | WP-504–WP-506 | One compiler-visible bounded atomic collection command; no generic transaction or storage batch |
| Operational queries | ADR-0108 | WP-507–WP-509 | Finite dynamic predicate families, cursors/top-N, declared text indexes, null/existence, exact aggregates, safe catalog pages |
| Workflow concurrency | ADR-0109 | WP-510–WP-511 | Revision checks, legal transitions, fenced leases, service time/IDs, and scheduler-through-commands only |
| Provisioning/evolution | ADR-0110 | WP-512–WP-513 | Programmatic resumable install/upgrade, explicit authority diffs, migration integration, and adapter conformance manifests |

WP-497 freezes accepted text and adds normative requirement IDs before any
track changes a public or durable interface. WP-514 runs the installed final
gate after all tracks and existing correctness/performance blockers close.

## Safety invariants across every track

- Application mutation remains possible only through compiled commands.
- Go and TypeScript do not implement remote transport trust; a first-party Rust
  driver host owns TLS, pooling, retry, uncertainty, cancellation, and error
  validation.
- A bulk command is a distinct statically bounded command plan, not a public
  transaction callback or generic insert/update/delete API.
- Dynamic query behavior selects from a finite compiler-owned plan family. A
  caller cannot submit field names, operators, order expressions, indexes, or
  an arbitrary predicate tree.
- Lease possession never grants authority. Every transition repeats current
  authentication and authorization and requires the current fencing token.
- Installation is a resumable campaign of existing authoritative operations,
  not a false cross-system atomic transaction. Partial completion is always
  typed and observable.
- No adapter manifest can execute server-side code, request numeric IDs/raw
  permissions, or bypass exact application locks and migration preflight.

## Ordered delivery

1. **WP-497 — architecture and requirement freeze.** Accept or revise the six
   ADRs, add exact `NET-*`, `DRV-*`, `BLK-*`, `OQ-*`, `WF-*`, and `APE-*`
   requirements to `SPEC.md`, and freeze compatibility manifests.
2. **Remote and driver foundation.** WP-498–WP-503 make one application
   operation reliable across process/container/language boundaries.
3. **Compiler/runtime capability tracks.** WP-504–WP-511 implement bulk,
   operational query, and workflow semantics independently with negative and
   crash evidence.
4. **Installation and adapter ownership.** WP-512–WP-513 compose exact
   deployment, migration, role/credential rotation, and feature conformance.
5. **WP-514 — installed alpha gate.** Run every adapter shape across the
   supported language/platform matrix, recovery boundaries, and security
   negatives. No waiver may introduce a kernel import, handwritten transport,
   unencrypted remote listener, raw transaction, or unbounded query.

## Final acceptance matrix

At minimum, final evidence includes:

| Adapter shape | Critical semantics |
|---|---|
| OpenFGA | Atomic bounded tuple writes/deletes, revision tokens, indexed tuple lookup, remote Go client |
| MLflow | Metric/parameter batches, exact decimal aggregates, run transitions and leases, Python wheel matrix |
| Payload | Document graph creation, optional/null/prefix queries, cursor pages, TypeScript long-lived transport |
| Woodpecker | Pipeline-plus-step creation, claims/scheduler locks, state transitions, event/reactive worker flow |

Every shape must install from an empty selected database, upgrade a populated
database through the supported evolution class, rotate its application
credential, survive process/network interruption, and pass an adapter-owned
manifest without accessing RiffDB source or kernel APIs.

## Explicit deferrals

This program does not add SQL, arbitrary transactions, callbacks, unbounded
loops, cross-partition writes, online dual-schema migration, global scheduler
locks, proxy-asserted principals, direct browser credentials, pure-Go transport
semantics, or automatic role widening. Replication and partitioning remain
separate later phases.
