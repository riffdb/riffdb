# WP-748 promotion implementation and fixture review

Package: WP-748. Tier: guarantee. Implementation range:
`05501ec07705576321e4ad8350dbeda38a266a2a..6cba6e8cfb2c1c9fe8c4866d07bf795020a338c6`.
Status: implementation and the complete CI battery verified; human review pending.
The fencing, V2 startup and revised stream-audit amendments are already accepted.
This review covers their implementation and generated compatibility fixtures.

## Result to review

An operator durably fences the source, then asks its registered follower to
promote. The follower obtains current authenticated fence evidence over the
configured TLS connection, drains the received prefix, and closes readers and
the sole applier. The existing exclusive maintenance owner retains authority
through cutover and complete source validation. One transaction installs the
checked successor incarnation and epoch, promotion audit/control linkage and
active source admission. Only complete validation and exact external receipt
reconciliation release application readiness and the successful response.

Source fencing survives restart and has no undo operation. Promotion may fail
after fencing, leaving the old source unavailable for writes. Missing or
contradictory retained evidence refuses readiness. Before committed cutover,
restarting a selected attempt serves only restricted administrative retry:
fresh authorization, the exact immutable operation and current authenticated
proof are required. After committed cutover, recovery reconciles the same
result without restamping or reconnecting to the old source.

Follower-derived projection state remains independently owned under ADR-0248.
The source also refuses a scoped token from an obsolete history incarnation
before projection lifecycle or wait handling. This implements the existing
history-mismatch contract without a new token encoding.

## Public and durable fixture changes

| Surface | Exact change |
| --- | --- |
| Administrative RPCs | Add `AdminService.FenceReplicationPrimary` and `AdminService.PromoteFollower`, preserving prior methods and field numbers |
| Permission | Add administrative-only `FenceReplicationPrimary`, permission tag 34; exclude application roles and MCP |
| Service audit | Add fence 0x3c, stream establishment 0x3d and promotion 0x3e; preserve all prior operation tags |
| Stream negotiation | Add request field 13 for fence evidence and response oneof field 6; exactly one evidence item then EOF, bounded to 2,048 encoded bytes |
| Application refusal | Add `RDB-REP-0102`, `primary_fenced`; public error enum 14 and application error enum 27, with ContactOperator recovery |
| Operator bindings | Add typed Rust client methods and CLI `follower fence-primary` / `follower promote`; caller selects the operation and registration, never the applied head or successor counters |
| Catalog | Add `StoredAuthoritativeStateCatalogV2`, tag 69/revision 2, with required source-only admission metadata |
| Source admission | Add `ReplicationPrimaryAdmissionV1`, tag 76/revision 1 |
| Fence audit link | Add `StoredPrimaryFenceAdministrationV1`, tag 77/revision 1 |
| Promotion audit link | Add `StoredPromotionAdministrationV1`, tag 78/revision 1 |
| External maintenance evidence | Add the accepted bounded promotion receipt V1 under exclusive maintenance custody |

All 111 pre-existing readable registry rows and 88 writable rows remain exact.
The generated descriptor sets, schema hashes, validation, client vectors,
adapter locks, durable manifest and topology reflect the additions above.
Existing V1 catalog fixtures, changelog frame bytes and backup manifest V1 are
preserved. Compact-prefix experimental formats are absent. Old incompatible
catalog/registry markers refuse before mutation; no inferred admission state or
automatic migration is supplied. V2 startup fully validates sources after CLEAN,
as approved, while V1 clean-start behavior is preserved.

## Required proof

- Four production TLS cells exercise eight concurrent command clients in
  Standard and Hardened storage, with caught-up and deliberately lagging
  followers. Every acknowledged command is accounted for at the source fence.
  Receipt RPO equals the measured application-sequence delta in every cell.
- Actual old source tokens, follower discovery cursor/history fence and source
  replication stream positions refuse after promotion. A new command, exact
  idempotent replay and authoritative entity read succeed.
- Ten restricted retry crash cells and four signal shutdown cells retain one
  cutover and exact audit selection. Startup without old-source credentials
  stays restricted; unavailable proof durably fails, then exact retry succeeds
  when proof becomes available.
- Authorization, policy drift, cancellation, dropped responses, owner loss,
  conflicting selection, corrupt audit, source crash/reopen and process-level
  cutover/reconciliation proofs preserve fail-closed behavior.
- Existing registration/retirement/expiry and retention proofs remain part of
  full CI. A lagging registration still prevents pruning its required history.

The [verification report](WP-748-LIFECYCLE-VERIFICATION.md#final-promotion-validation)
records the complete proof inventory and all verification attempts. On
2026-09-21, every `ci-all` step completed successfully at implementation revision
`6cba6e8c`. The initial invocation stopped at a missing worktree Node dependency
after workspace tests and archive CLI checks passed. Installing the unchanged
lockfile dependencies allowed the exact remaining CI steps to pass. Operator
conformance also passed with the pinned Maturin 1.14.1 after an earlier attempt
used the globally installed 1.15.0. No runtime source changed between these runs.
This is a complete battery assembled from the recorded attempts, not a claim
that the original uninterrupted invocation passed.

## PR note

- **Package / Tier:** WP-748 / guarantee.
- **Behavior:** explicit durable source fencing, audited promotion, exact
  pre-cutover retry and post-cutover recovery, stale-history refusal, complete
  V2 startup and established-stream audit.
- **Checks:** all 17 non-CI acceptance steps and the complete `ci-all` battery,
  with the environment repairs and exact remaining-step reruns described above;
  all focused proofs listed above passed.
- **Compatibility:** existing durable bytes and application safety preserved;
  additive operator interfaces require regenerated bindings; incompatible
  durable registry/catalog markers refuse.
- **Handbook:** follower administration, configuration, compatibility,
  backup/restore, remote ingress, errors and known limitations, plus generated
  CLI and wire references.
- **Hazards and follow-ups:** fencing may leave both nodes unavailable until
  exact recovery succeeds; preserve maintenance receipts. WP-749 performance
  qualification and WP-750's release campaign remain separate work.

Human review is required by AGENTS.md's guarantee-tier change protocol and
fixture-review rule. Acceptance of this implementation does not amend the
already approved guarantees.
