# WP-748 follower hold budget implementation

Historical foundation notes; the evidence and remaining-work statements below
record those earlier increments. See the [later lifecycle verification](WP-748-LIFECYCLE-VERIFICATION.md)
for registration, retirement, health and expiry progress. WP-748 remains open.

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


## Versioned registration-policy codec increment

Package: WP-748. Tier: guarantee, under accepted ADR-0178 section 6 and its
compatibility decision naming registration/budget metadata through the registry
migration path, within ADR-0186's existing SourceOnly hold table.

Decisions:

- Add tag 73 revision 2 in the existing hold domain; embed the unchanged V1
  fence instead of reinterpreting its fields, owner tags or keys. V1 and V2
  remain distinct checked codecs. Bootstrap and archive holds cannot carry
  follower registration policy.
- Keep registration phase separate from V1's frozen owner-kind enum. The
  registration predecessor and its checked physical successor bind one
  registration generation within its lineage. This is data, not a token.
- Configured expiry is an optional future application sequence. Budget remains
  a nonzero application-sequence allowance. A persisted degradation observation
  must reach that allowance or configured expiry; it cannot authorize release.
- Registration and acknowledgement points must have comparable monotone shape;
  equal physical positions require exact frontier/hash identity. Pending
  bootstrap pins the registration predecessor. Storage must still prove retained
  ancestry and the actual registration transaction before accepting a row.
- Reuse the existing V1 semantic conversion and bounded structural preflight.
  V2 has a maximum 328-byte payload, with canonical phase-specific fixtures.
- Register the codec now, without selecting a runtime writer or adding an
  automatic migration edge. Existing exact registry markers continue to require
  their matching binary. The runtime must use an explicit governed migration
  before policy bytes can be persisted by registration.

Compatibility: V1 schema hashes, wire vectors, compact identity and writers are
unchanged. The public handbook's Compatibility page documents the new codec and
exact registry refusal. No application or operator operation is activated.

Evidence: V1/V2 tests check round trips, every truncation and single-byte
corruption, cross-version refusal, redacted diagnostics, independently resealed
invalid policy fields, expiry-only degradation, absent expiry/degradation,
maximum counters and the exact 328-byte structural bound. Canonical fixtures
bind all three registration phases.

Hazards and follow-ups: source storage reads, registry migration, audited
registration/retirement/expiry, health consumption, bootstrap/acknowledgement
integration and all promotion deliverables remain unfinished. Codec values
never release an existing retention fence. WP-748 stays open.

Checks: scoped acceptance passes all nine steps, including 635 Protobuf and
storage-API tests, clippy, generated artifacts, topology, handbook, file-size
and panic-allowance checks. A byte comparison verifies all 105 existing
schema-hash and V1 hold artifacts are unchanged. The release manifest retains
every previous record identity/hash and adds only tag 73 revision 2.
