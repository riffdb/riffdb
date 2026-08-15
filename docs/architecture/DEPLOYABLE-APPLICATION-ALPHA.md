# Deployable Application Alpha Plan

> Status: proposed roadmap. The capabilities on this page are not available
> until their work packages pass. ADR-0105 through ADR-0112 were accepted
> exactly on 2026-08-09 and now govern their implementation work packages.

The first alpha release must prove more than a good local symbolic API. A real
adapter must be able to install an application, connect from another container,
use a supported language driver, express its bounded operational reads and
writes, coordinate workers safely, rotate authority, and upgrade without a
storage or kernel escape hatch.

## Release gate

The release gate is:

> A fresh OpenFGA-, MLflow-, Better-Auth-, or Woodpecker-shaped adapter can be
> installed and exercised through public symbolic surfaces from a separate
> container, with encrypted authenticated transport, generated supported-
> language bindings, bounded atomic application commands, indexed operational
> RiffQL, compiler-owned per-row policy, compiled framework profiles,
> structural secret-field redaction, fenced workflow concurrency, exact
> upgrade/provisioning receipts, an honest durable-format and exit policy,
> exercised disaster recovery, sustained endurance evidence, and no raw
> kernel/storage access.

The gate has thirteen tracks. Prerequisite, compatibility, disaster, and
endurance work cannot be deferred into release day.

| Track | Proposed ADR | Work packages | Required proof |
|---|---|---|---|
| Release prerequisites | existing ADRs | WP-550–WP-552 | Exact architecture freeze, repaired application-binding closure, and retained idle-host 90-second comparator evidence |
| Remote ingress | ADR-0105 | WP-553–WP-554 | TLS-only non-loopback gRPC, proxy interoperability, container health, certificate and capability rotation |
| Drivers | ADR-0106 | WP-555–WP-558 | Stable Go, long-lived TypeScript, expanded Python artifacts, pooling/cancellation/errors/retry/read-after parity owned by Rust |
| Delete/replication prerequisite | ADR-0100/0107 | WP-559 | Changelog tombstones plus delete-aware validated-prefix and entity-chain proofs before delete activation |
| Bulk commands | ADR-0107 | WP-560–WP-562 | One compiler-visible bounded atomic collection command; no generic transaction or storage batch |
| Operational queries | ADR-0108 | WP-563–WP-565 | Finite dynamic predicate families, cursors/top-N, declared text indexes, shared exact aggregates, safe catalog pages, receipted identity rotation |
| Workflow concurrency | ADR-0109 | WP-566–WP-567 | Revision checks, legal transitions, fenced leases, service time/IDs, and scheduler-through-commands only |
| Provisioning/evolution | ADR-0110 | WP-568–WP-569 | Programmatic resumable install/upgrade, explicit authority diffs, migration integration, and adapter conformance manifests |
| Per-row policy | ADR-0111 | WP-570, WP-572–WP-573 | Closed principal-aware predicates enforced before disclosure and transaction-current on writes; realistic Better Auth and MLflow policy proof |
| Framework and secret safety | ADR-0117/0118 | WP-597–WP-598, WP-600 | Generic compiled framework profiles, structural display-surface redaction, explicit secret reveals, and no generic callback/store escape hatch |
| Format compatibility and exit | ADR-0112/0119 | WP-574–WP-575, WP-599 | Release format manifest, refusal before mutation, snapshot-consistent symbolic export, and compiler-owned workflow-safe reimport |
| Disaster recovery | ADR-0050/0112 | WP-576 | Remote backup, total database-volume loss, verified restore, and full adapter reconciliation |
| Endurance | durability ADRs and ADR-0050 amendment | WP-577–WP-578, WP-609–WP-610 | Reproducible lifecycle harness, receipted bounded backup retirement, and retained 72-hour growth/recycling/recovery evidence |

WP-550 freezes accepted text and adds normative requirement IDs before any
track changes a public or durable interface. WP-551 and WP-552 close the two
already-red release prerequisites rather than hiding them inside the final
gate. WP-579 runs the installed final gate after all tracks close. P10 owns
WP-550 through WP-570, WP-572 through WP-579, and WP-599; WP-571 remains
assigned to the independent repository-ownership program. Active replication
packages must allocate outside those P10 ranges.
The Better Auth rescope added the accepted gate-critical follow-ons WP-597,
WP-598, and WP-600; WP-579 names them directly rather than relying on source
history to imply their completion.

As of 2026-08-09, `./scripts/check-application-bindings` exits nonzero at the
agent-alpha manifest-versus-generated module-hash assertion, and no retained
idle-host 90-second comparator corpus exists. A successful short benchmark or
an unrelated generated-artifact check does not clear either blocker.

## Active replication coordination

Replication is not a later hypothetical phase. RE1 (WP-491) is merged and
RE2–RE4 are active work. This program therefore has two hard coordination
fences:

- WP-559 amends ADR-0100 with delete/tombstone changelog entries and the
  delete-aware durable-validation proof before any bulk delete can compile for
  production.
- RE2's `ShipChangelog` RPC and WP-553 both touch the exact streaming transport
  inventory. WP-550 records the exact RE2 revision or an explicit pre-RE2
  sequence, and WP-553 rebases on that inventory rather than allocating fields
  or replacing architecture pins concurrently.

The physical durability journal remains separate from the replication wire
format. TLS changes transport protection only; it does not change changelog
framing or follower semantics.

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
- Row access is a compiled contract policy over bounded current principal
  facts, not application middleware. It runs before hydration, pagination,
  ranking, aggregation, live delivery, and export, and is rechecked against
  transaction-current and successor state for mutations.
- Lease possession never grants authority. Every transition repeats current
  authentication and authorization and requires the current fencing token.
- Installation is a resumable campaign of existing authoritative operations,
  not a false cross-system atomic transaction. Partial completion is always
  typed and observable.
- No adapter manifest can execute server-side code, request numeric IDs/raw
  permissions, or bypass exact application locks and migration preflight.
- An unsupported durable format is rejected before mutation; it is never
  reset, guessed, or silently upgraded. Physical backup and symbolic export
  remain distinct, receipted recovery mechanisms.
- Export cannot expose raw tables, numeric IDs, storage keys, envelopes, or
  journal/changelog bytes, and reimport cannot bypass compiled commands or
  declared migrations.

## Ordered delivery

1. **WP-550 — architecture and requirement freeze.** Accept or revise the eight
   ADRs, add exact `NET-*`, `DRV-*`, `BLK-*`, `OQ-*`, `WF-*`, `APE-*`,
   `RAP-*`, `AFC-*`, and `EXP-*` requirements to `SPEC.md`, freeze
   compatibility manifests, record the RE2 transport revision, and require
   standing design tests on every package.
2. **Existing red gates.** WP-551 repairs agent-alpha's exact binding closure;
   WP-552 freezes and banks the evidentiary 90-second safe-app comparison.
3. **Remote and driver foundation.** WP-553–WP-558 make one application
   operation reliable across process/container/language boundaries.
4. **Replication deletion prerequisite.** WP-559 closes changelog/bootstrap/
   checkpoint deletion semantics before compiler delete activation.
5. **Compiler/runtime capability tracks.** WP-560–WP-567 implement bulk,
   operational query, and workflow semantics independently with negative,
   rotation, and crash evidence.
6. **Installation and adapter ownership.** WP-568–WP-569 compose exact
   deployment, migration, role/credential rotation, and feature conformance.
7. **Compiled row policy.** WP-570 and WP-572–WP-573 freeze and enforce the
   policy language across authoritative, projected, search, reactive, command,
   workflow, and export surfaces, then prove realistic Better Auth per-user
   row policies (a principal sees only its own sessions and accounts) and
   MLflow ACLs.
8. **Framework and secret safety.** WP-597 carries secret classification and
   structural redaction through every display surface, WP-598 proves a generic
   compiled framework profile without framework-owned transactions or hooks,
   and WP-600 requires an explicit source-naming reveal annotation for every
   intentional one-time secret handout.
9. **Compatibility and exit.** WP-574–WP-575 publish the exact durable-format
   promise, refuse unsupported data before mutation, and provide symbolic
   export plus ordinary compiled reimport. WP-599 closes the workflow-shaped
   reconstitution gap without admitting a normal workflow-state write, then
   proves all-domain export/reimport into an empty database.
10. **Disaster and endurance.** WP-576 exercises remote backup, volume loss, and
   restore for every adapter. WP-577 builds the lifecycle harness; WP-609 closes
   the offline-retention/journal rebase boundary exposed by its installed
   rehearsal; WP-610 closes the unreceipted backup-retirement boundary exposed
   by the repeated-cycle rehearsal; WP-578 then banks the uninterrupted 72-hour
   release receipt. A 24-hour run is rehearsal only.
11. **WP-579 — installed alpha gate.** Run every adapter shape across the
   supported language/platform matrix, recovery boundaries, and security
   negatives. No waiver may introduce a kernel import, handwritten transport,
   unencrypted remote listener, raw transaction, unbounded query, row-policy
   bypass, silent format reset, nonportable data, untested restore, or shortened
   endurance evidence.

## Final acceptance matrix

At minimum, final evidence includes:

| Adapter shape | Critical semantics |
|---|---|
| OpenFGA | Atomic bounded tuple writes/deletes, revision tokens, indexed tuple lookup, remote Go client |
| MLflow | Metric/parameter batches, exact decimal aggregates, per-experiment/per-run policy, run transitions and leases, Python wheel matrix |
| Better Auth | Unique identities and provider/account links, single-use expiring verification tokens with replay refusal, atomic user/account/session workflows, concurrent session refresh and revocation, secret-field classification with guaranteed display-surface redaction, server-owned time and IDs, verification-email outbox intent, plugin schemas resolved at generation time, TypeScript long-lived transport |
| Woodpecker | Pipeline-plus-step creation, claims/scheduler locks, state transitions, event/reactive worker flow |

Payload's adapter shape (document graph creation, per-document
owner/team/public ACLs, optional/null/prefix queries, cursor pages) is
retained as a named post-alpha adapter: every capability it forced has
already landed, and its shape moves down the list rather than out of it
(maintainer scope decision, 2026-08-11). Framework integrations themselves
live in dedicated repositories once the required capabilities exist; this
repository carries only capabilities, design records, and workload shapes
(ADR-0117).

The alpha adapter event corpus deliberately contains no protected deletion
event. Better Auth revokes and expires sessions through commands rather than
protected deletion events; MLflow has no
protected run/experiment deletion event; OpenFGA's tuple deletion has no event
stream; and Woodpecker's protected stream contains only pipeline-start facts.
The bulk restrict-delete conformance case emits no event. Adding a protected
deletion event to any gate adapter requires the separately accepted immutable-
event-policy design rather than weakening current-row anchor semantics.

Every shape must install from an empty selected database, upgrade a populated
database through the supported evolution class, rotate its application
credential, survive process/network interruption, and pass an adapter-owned
manifest without accessing RiffDB source or kernel APIs.

Every shape must also export and reimport its portable application data into an
empty database, take a verified remote backup, lose the original database
volume, restore the backup, and pass the same conformance manifest against the
restored state. The release candidate must then pass a retained 72-hour mixed
load run that forces retention, checkpoint, journal-recycle, backup, rotation,
deployment, and restart cycles and asserts bounded memory, file, queue, and
backlog growth.

The independent authoring gate uses six fresh Terra contexts from one sealed
release bundle: Blog in Go, Rust, and TypeScript, and Orders in Python, Rust,
and TypeScript. This preserves campaign 02's original four cells while adding
direct application-authoring evidence for both additional alpha drivers.

## Explicit deferrals

This program does not add SQL, arbitrary transactions, callbacks, unbounded
loops, cross-partition writes, online dual-schema migration, global scheduler
locks, proxy-asserted principals, direct browser credentials, pure-Go transport
semantics, or automatic role widening. Per-principal request-rate limits and
per-tenant storage/work quotas at the remote application boundary are also
deferred for alpha: global admission, connection, query, and work bounds remain
mandatory, and deployment is limited to trusted design-partner networks. This
deferral must be revisited before opening an untrusted multi-tenant service.
Replication remains its own active arc with the explicit coordination fences
above; partitioning remains a later phase.
