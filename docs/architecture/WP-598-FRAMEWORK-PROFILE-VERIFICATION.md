# WP-598 framework-profile verification

WP-598 closes the generic prerequisite review required by ADR-0117. The
repository contains no framework-specific runtime, schema, command, or
dependency. Its acceptance artifact is the framework-neutral profile in
`fixtures/adapters/framework-profile/`.

## Conclusions

| Prerequisite | Conclusion | Named evidence |
|---|---|---|
| Transactional unique identity under concurrent admission | **VERIFIED** | `equal_scoped_unique_values_commit_once_and_loser_replays_without_sequence` runs two colliding admissions through the real redb coordinator: exactly one commits, the loser observes the typed `UniqueConflict`, the replay keeps the failure with a replay disposition, and no refused admission allocates a commit sequence. |
| Sequential re-insert and value release | **VERIFIED** | `create_before_delete_refuses_then_delete_releases_unique_value_for_reinsert` proves a fresh-identity create before deletion receives the same typed refusal against committed state, then the audited bounded delete atomically releases the exact old unique entry for a successful re-insert. |
| Delete-then-reinsert through entity delete | **VERIFIED** | WP-607 retires the audited-path `RDB-C044` seal. `unique_entity_delete_compiles_without_an_input_computable_release_conflict` proves the Better Auth session shape compiles only with an explicit deletion policy and retains source-spanned `RDB-C045` for the unsafe shape. `delete_before_create_releases_unique_value_in_submission_order` and the reverse-order schedule pin ordered unique ownership. `delete_removes_current_index_state_and_emits_reciprocal_v2_tombstone` proves index absence across reopen plus exact changelog-v2 tombstone/head reciprocity; `secret_classified_v8_delete_derives_the_exact_old_index_keys` supplies the exact unique-index derivation proof. |
| Single-use expiring token | **VERIFIED** (boundary delivered) | `framework_profile_token_refuses_fresh_reuse_and_expired_consumption` proves consume-once, fresh-reuse refusal, and expiry refusal. The profile's expiry comparisons are strict (`tx.time < expires_at`), and `framework_profile_token_expiry_boundary_is_exact_to_one_tick` pins the boundary at nanosecond precision: one tick before expiry consumes, at and after expiry refuse typed with zero mutations. |
| One-time secret handout (WP-600 anchor) | **DELIVERED — SMALL** | `IssueVerificationToken` hands out the secret-classified digest exactly once in its success outcome — the secret-to-non-secret flow ADR-0118 Amendment 1 will make a compile error without an explicit `reveals` annotation. `token_handout_flow_is_the_declared_secret_to_outcome_read_wp600_will_annotate` pins the flow as one direct bound-field read so WP-600's annotation lands on a stable anchor. |
| Atomic user/account/session graph, evaluation semantics | **VERIFIED** | `framework_profile_signup_is_one_complete_atomic_graph_with_compiler_initial_state` proves one compiled bounded command evaluates to one three-entity mutation graph with the compiler-owned workflow initial state; `framework_profile_refresh_and_revocation_cannot_both_survive_one_revision` proves the runtime revision-check semantics. |
| Refresh/revoke race through the commit coordinator | **ESCALATED** | Every mutating command on a V7+ bundle fails closed before commit: `derive_grammar_v1_indexes` (`crates/riffdb-commit/src/command_index.rs`) whitelists grammar/IR pairs only through V6 while the bundle registry pairs versions through V10. The gate profile is V9, so its signup, refresh, and revoke mutations cannot commit; the same seal covers V7 (vectors) and V8 (secret classification), and no existing test drives a V7+ mutating commit. Both race schedules (`framework_refresh_wins_the_race_and_revocation_observes_its_declared_stale_outcome`, `framework_revocation_wins_the_race_and_refresh_observes_its_declared_stale_outcome`) are written against the real coordinator with both submission orders, loser replay, and final-state assertions; they land ignore-marked as the ready acceptance evidence for the commit-path version-audit package. `framework_profile_declared_refusal_commits_terminally_on_the_v9_bundle` stays live and pins the current boundary: zero-mutation declared outcomes commit terminally on the V9 bundle today. |
| Keyless upstream retries | **VERIFIED** | `adapter_minted_idempotency_survives_driver_retries_with_fresh_transport_ids` proves an adapter-retained command input survives bounded driver retries while transport request identities rotate; `equal_key_commands_commit_once_and_replay_exactly` proves the admission property the minted identity rides — one commit sequence, byte-equal stored outcome on replay, one provenance record, typed input-mismatch for changed input. |
| Framework-neutral alpha workload | **VERIFIED** | `adapter-framework-profile-acceptance` rejects framework-specific fixture names, runs the compiler, runtime, driver, and real-storage evidence above, and fails if any exact-match schedule resolves to an empty triggering set; `deployable-alpha-acceptance` includes it as a required phase. |

## WP-606 discharge of the coordinator escalation

The refresh/revoke race escalation above is discharged. WP-606 audited the
derivation whitelist per version (audit notes at
`index_derivation_version_supported` in
`crates/riffdb-commit/src/command_index.rs`, per-version evidence tests in
the same module) and both race schedules now run live and pass with their
WP-598 assertions unchanged. Two corrections to the escalation text as
written: grammar/IR V7 is current-row event policy anchors (ADR-0116) —
vector-field specs require IR V6 and were already inside the audited set —
and the whitelist had meanwhile been extended without audit by an
out-of-package commit (`44e4afa0`); WP-606's audit is the retroactive
discharge of both that extension and the original silent parking. The
schedules' initial failure after the extension was not a derivation defect:
the profile harness mixed the audited capsule path with the unaudited
storage-only conformance path on one entity, which startup validation
correctly reports as an Authoritative `MissingCrossLink`; the harness now
uses the audited path consistently, matching public application commands.

## WP-607 discharge of the unique-delete escalation

The compiler no longer asks callers to compute a conflict key from a unique
value that exists only in the checked row being deleted. The ordinary
aggregate conflict preserves submission order, and the sole authoritative
writer removes snapshot-derived index entries before transaction-current
unique occupancy is tested by a later command. This is not a blind delete or
an authorization bypass: the bounded command still requires an accepted
deletion policy, exact predecessor observation, compiled index derivation,
typed outcomes, audit, provenance, idempotency, and one atomic commit.

The recovery evidence composes three independent checks: commit derivation
names every exact old index key (including unique indexes), runtime schedules
prove before/after ownership behavior against redb, and changelog V2 requires
one delete tombstone plus its exact deleted chain head. Reopen proves neither
the materialized entity nor its old index entry survives.

## Corrections to the previous round

The earlier revision of this page recorded the refresh/revoke race as
delivered and the atomic graph as verified without qualification. Both
claims held only at deterministic-runtime evaluation level: the commit
coordinator's index derivation refuses every mutating command on V7, V8,
V9, and V10 bundles, so no gate-profile mutation had ever committed
through real storage. The real-redb unique-identity schedules run on a
low-version contract and were unaffected. The verdicts above supersede
the earlier table.

## Compatibility boundary

Contracts without an initial state or explicit self-transition retain their
least prior IR version and canonical encoding. A contract using either feature
requires V9. `fixtures/compiler/workflow-initial/` pins the V9 bytes and bundle
hash, while the existing pre-V9 compiler fixtures remain byte-frozen and are
checked by `scripts/generate-contract-fixtures --check`.

The profile's expiry semantics are strict: `expires_at` is the first instant
at which a token or session is invalid. This is a fixture-contract semantic,
not an IR change.

## Safety invariants

- Only the compiler injects a declared workflow initial state into creates.
- Callers cannot assign, parameterize, or override workflow state.
- A workflow without `initial` remains ineligible for ordinary create.
- A self-transition remains an exact-revision transition and returns the same
  declared stale and illegal-state outcomes as every other transition.
- The profile exposes only compiled commands. It adds no arbitrary callback,
  transaction, storage, or generic mutation surface.
- Token expiry uses authorized transaction logical time, never runtime wall
  time; comparisons are strict, and token material remains secret-classified.
- The one intentional secret disclosure (issuance handout) is a single
  declared flow site, written so WP-600's `reveals` annotation attaches to it.

## Reproduction

```bash
./scripts/adapter-framework-profile-acceptance
TMPDIR=/home/user/tmp ./scripts/generate-contract-fixtures --check
cargo test --test command_concurrency
cargo test --test storage_recovery_matrix delete_removes_current_index_state
```
