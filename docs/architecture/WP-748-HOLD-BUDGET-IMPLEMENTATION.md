# WP-748 follower hold budget implementation

Status: implementation foundation; WP-748 remains open.

ADR-0178 section 6 and REP-006 require a per-registration sequence budget,
typed health degradation on exhaustion, and ceremony-only fence release.
`FollowerHoldBudget` in `riffdb-storage-api` supplies the shared checked
calculation for that budget. It is a semantic value, with no independent
durable format, mutation capability, or application-facing operation.

## Decisions

- A budget is a nonzero application-sequence allowance. Zero is invalid;
  missing configuration never means an unlimited budget or a released hold.
- Lag is the published application head minus the durable acknowledged
  application head, treating before-first as zero. Administration and physical
  receipt sequences consume no application allowance.
- Equality with the allowance is exhaustion. Remaining allowance becomes zero
  without unsigned wrap, including at the maximum representable frontier.
- Only follower-acknowledgement holds qualify. Database, incarnation, epoch,
  retained position bounds and monotone dual frontiers must agree; equal
  physical positions must have identical hashes and frontiers.
- The storage owner still proves same-pin provenance and receipt ancestry.
  This calculation cannot certify an arbitrary intermediate receipt hash.
- Exhaustion is a health observation. It cannot retire a follower, release a
  fence, or grant pruning permission. Actual retirement and configured expiry
  require their separately authorized durable transitions.

## Evidence and remaining work

`follower_hold_budget.rs` covers before-first, exact exhaustion, catch-up,
control/audit-only progress, counter extremes, foreign lineages, wrong hold
owners, stale positions, and substituted boundary hashes. The production
`retention_prune_refuses_to_pass_registered_follower_frontier` test also checks
an exhausted observation before refusing offline pruning; only durable
acknowledgement advancement releases that follower's low-water constraint.

This is not the complete REP-006 proof. Registration must durably carry the
policy, operational health must consume checked observations, and audited
retirement/expiry must enforce their release ceremony. Promotion, fencing,
incarnation/epoch advancement, exact RPO, and the gRPC/Rust-client/CLI surfaces
also remain WP-748 deliverables. No current operator behavior changes, so this
increment does not change the public handbook.

The scoped storage-API suite passes all 449 tests, and the production retention
test passes against redb. These checks do not claim that registration, health
reporting or automatic expiry are implemented.
