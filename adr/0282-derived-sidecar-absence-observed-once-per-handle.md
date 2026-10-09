---
adr: "0282"
title: Derived Sidecar Absence Observed Once Per Handle
status: proposed
tier: guarantee
date: 2026-10-09
accepted: null
acceptance: null
requires: [ADR-0240, ADR-0246]
amends: []
supersedes: []
requirements: [STO-030]
packages: [WP-830]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - Anything other than this handle's `derived_database` can create the derived sidecar while the handle is open.
  - A derived read can return the empty view after this handle has opened or created the sidecar.
---
# ADR-0282: Derived Sidecar Absence Observed Once Per Handle

## Context

ADR-0240 moved rebuildable derived state into a sidecar redb file that is opened on first derived read or write, so a database that never uses derived
state keeps its closed three-file set (ADR-0246). Every query snapshot opens a derived read. While the sidecar has never been opened, that read stats the
sidecar path to decide between opening it and serving an empty view. A database with no projection never creates the file, so every read pays one `statx`:
1.0 per read and 1.6% of tokio-worker CPU in the TicketDesk read profile on the ADR-0239 host.

## Decision

1. Once a derived read on an open handle observes that the sidecar file does not exist, later derived reads on that handle serve the empty view without
   statting the path again.
2. The observation ends when the handle opens the sidecar. While a handle is open the sidecar is created only by that handle's `derived_database`, which
   publishes the opened sidecar before returning, and every derived read checks for it before the observation. Each offline creator, restore, retirement and
   fixture, requires the database to be stopped.
3. A handle that has opened the sidecar always reads it; the empty view is never served after that.

## Options considered

1. **Keep statting on every read:** rejected; it is a syscall per read that answers the same question for the life of the handle.
2. **Create the sidecar eagerly at open:** rejected; it breaks ADR-0246's closed file set for databases that never use derived state.

## Consequences

- One fewer system call per query snapshot while no derived state exists. No durable byte, file set, ordering or visibility rule changes: a read that would
  have seen an absent file still serves the empty view, and one racing a first creation behaves as before.
- A sidecar placed beside a running database by an external process is not noticed until the handle reopens; doing that was already unsupported, since redb
  holds an exclusive lock on the files it opens and offline tools require the stopped database.

## Standing design tests

- **Interface safety:** nothing on a public surface changes and no caller can select the behaviour.
- **Scale:** constant per handle; it removes one probe per read and adds one atomic load.

## Checks

- `absent_sidecar_is_probed_once_and_a_later_creation_is_read` requires one filesystem probe for sixteen reads of an absent sidecar (sixteen before this
  decision) and requires a sidecar the handle then creates to be read rather than the empty view.
- The ADR-0240 obligation proofs, including `concurrent_first_touch_of_the_sidecar_never_reports_unavailable` and
  `restore_leaves_no_derived_state_from_the_replaced_timeline`, remain unchanged and passing.
