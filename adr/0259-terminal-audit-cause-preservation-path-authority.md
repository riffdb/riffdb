---
adr: "0259"
title: Terminal-Audit Cause Preservation Path Authority
status: proposed
tier: guarantee
date: 2026-09-23
accepted: null
acceptance: null
requires: [ADR-0205]
amends: [ADR-0205]
supersedes: []
requirements: []
packages: [WP-705]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - Any edit under this authority other than capturing the AuditAppendFailure from a finish()
    error and passing its exact cause() to note_audit_failure_with_cause.
  - Any change to a deadline duration, scheduling, cancellation, admission cap, batch size,
    concurrency, retry budget, re-entry, gRPC status, health recovery, or the threshold of
    eight.
  - Any new public surface, durable byte, storage key, encoding, transaction ordering,
    conflict ownership, acknowledgement, or outcome sequencing change.
---
# ADR-0259: Terminal-Audit Cause Preservation Path Authority

## Context

ADR-0205 Decision 7 requires command success and failure completion to pass
the terminal-audit append failure's exact `cause()` to
`note_audit_failure_with_cause`, never replacing it with an inferred or
unconditional subsystem cause. On main, `finish_success` and
`finish_failure_with_link` in `crates/riffdb-service/src/command_operations.rs`
violated it — both discarded the cause and noted subsystem, so one
request-scoped terminal-audit failure (a deadline or capacity event on a
single request) stopped global authoritative readiness immediately instead of
counting toward the accepted eighth consecutive failure. WP-705 repaired the
two authorized sites and proved the mechanism
(`request_scoped_command_terminal_audit_failure_preserves_threshold`,
ba71eb6cf).

The same violation exists at thirty-one further sites across thirteen
sibling operation files: each follows a `finish()` error with an
unconditional `note_audit_failure`, discarding the request-scoped cause. A
single request-scoped terminal-audit failure on an administration, commit,
consumer, event, export, contract, primary-fence, promotion-retry,
installation, query-discovery, reimport, or replication operation therefore
stops global readiness immediately today.

ADR-0205 Decision 9 authorizes WP-705 to edit exactly
`crates/riffdb-service/src/command_operations.rs` and the two test paths, and
states that "a required edit elsewhere stops the package for another exact
human-reviewed amendment." This is that amendment.

## Decision

1. ADR-0205 Decision 9 is amended only to add the following thirteen paths to
   WP-705 implementation authority, solely for Decision 7's cause
   preservation at the enumerated sites: `administration_operations.rs`,
   `commit_operations.rs`, `consumer_operations.rs`,
   `contract_operations.rs`, `event_operations.rs`, `export_operations.rs`,
   `installation_operations.rs`, `primary_fence_operations.rs`,
   `promotion_retry.rs`, `query_discovery_operations.rs`,
   `reimport_operations.rs`, `replication_administration_operations.rs`, and
   `replication_audit.rs`, all under
   `crates/riffdb-service/src/`. Another path or purpose requires a further
   separately accepted amendment.
2. The permitted edit at each site is exactly the shape already landed in
   `command_operations.rs`: capture the `AuditAppendFailure` from the
   `finish()` error and pass `failure.cause()` to
   `note_audit_failure_with_cause` with the same operation identity the site
   already notes. The existing failure mapping of each site — which public
   error it returns, whether capacity overload is preserved, and whether the
   site maps to `storage_unavailable` or `outcome_unknown` — is unchanged.
   Sites whose `note_audit_failure` does not follow a `finish()` error (for
   example a control-plane execution failure, which is a genuine subsystem
   event) are not touched.
3. The enumerated sites, at the revision this record was proposed against,
   are: `administration_operations.rs` lines 1481, 1541, 1616, 1638, 1661,
   1685; `commit_operations.rs` lines 1071, 1093, 1119;
   `consumer_operations.rs` lines 3305, 3327, 3350;
   `contract_operations.rs` line 1835; `event_operations.rs` lines 1564,
   1586, 1612; `export_operations.rs` line 462;
   `installation_operations.rs` line 332; `primary_fence_operations.rs`
   lines 127, 150, 183; `promotion_retry.rs` line 110;
   `query_discovery_operations.rs` lines 4106, 4127, 4149, 4172;
   `reimport_operations.rs` line 1075;
   `replication_administration_operations.rs` lines 107, 130, 163; and
   `replication_audit.rs` line 78. Line numbers identify the sites; a pure
   line shift from an unrelated change does not widen or narrow this
   authority.
4. Nothing else in ADR-0205 changes. Decision 8's threshold of eight, the
   subsystem-cause immediate stop, the successful-receipt reset, and every
   deadline, scheduling, cancellation, admission, batch, concurrency, retry,
   re-entry, gRPC-status, and health-recovery rule remain exact. No public
   surface, durable byte, storage key, encoding, transaction ordering,
   conflict ownership, acknowledgement, or outcome sequencing changes.

## Options considered

1. **A narrow single-purpose path amendment.** Selected. The defect is real
   and reproduced by the same proof shape as the landed repair; the fix is
   mechanical and identical at every site; and a single-purpose record keeps
   the acceptance surface minimal.
2. **Fold into the obligation-retirement record (ADR-0258).** Rejected: each
   record stays single-purpose — one retires stranded proof bindings, this
   one widens implementation authority — so each acceptance is reviewable on
   its own terms.
3. **Record as a hazard and hand to a separately-scoped package.** Rejected:
   the violation is live on main today on thirteen operations; a new package
   adds ceremony without changing the edit.
4. **Fix immediately under D-001's discovered-issue exception.** Rejected:
   ADR-0205 Decision 9's own text makes the edit a stop condition, and D-001
   permits opening records for discovered issues — which is what this is —
   not skipping acceptance.

## Consequences

- Every operation's terminal-audit failure preserves its request-scoped
  cause, so a deadline or capacity event on one request fails that request
  and counts toward the eighth-consecutive stop instead of stopping readiness
  immediately.
- The repair is uniform with the two sites already landed and proven.
- Explicitly deferred: none. This authority is exhausted by the enumerated
  sites and closes with WP-705.

## Standing design tests

- **Interface safety:** no public surface changes; the affected invocation
  already fails closed, and only the readiness accounting of its cause
  changes.
- **Scale:** no new state, allocation, or scheduling; the edit replaces one
  method call with another on the same failure path.

## Checks

- `request_scoped_command_terminal_audit_failure_preserves_threshold` passes
  unchanged before and after the sibling repairs.
- A source scan of `crates/riffdb-service/src/` finds no `note_audit_failure(`
  call that follows a `finish()` error without capturing the cause, and none
  of the thirty-one enumerated sites remains.
- `cargo clippy -p riffdb-service --all-targets --all-features -- -D
  warnings` and the riffdb-service test suite pass.
