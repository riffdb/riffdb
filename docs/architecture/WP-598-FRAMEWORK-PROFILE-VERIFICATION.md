# WP-598 framework-profile verification

WP-598 closes the generic prerequisite review required by ADR-0117. The
repository contains no framework-specific runtime, schema, command, or
dependency. Its acceptance artifact is the framework-neutral profile in
`fixtures/adapters/framework-profile/`.

## Conclusions

| Prerequisite | Conclusion | Named evidence |
|---|---|---|
| Transactional unique identity under concurrent admission | **VERIFIED** | `equal_scoped_unique_values_commit_once_and_loser_replays_without_sequence` runs two colliding commands through the real redb coordinator and proves that exactly one unique value commits. |
| Single-use expiring token | **VERIFIED** | `framework_profile_token_refuses_fresh_reuse_and_expired_consumption` proves the first consume mutation, replay against consumed state, and transaction-logical-time expiry refusal. |
| Atomic user/account/session graph | **VERIFIED** | `framework_profile_signup_is_one_complete_atomic_graph_with_compiler_initial_state` proves one compiled bounded command returns one three-entity mutation graph whose workflow state is compiler-owned. |
| Exact refresh/revoke concurrency | **DELIVERED — SMALL** | ADR-0109 Amendment 1 adds compiler-owned workflow initialization and explicit self-transitions in bundle/grammar/executable IR V9. `framework_profile_refresh_and_revocation_cannot_both_survive_one_revision` proves both contenders can evaluate at one revision but the loser receives the declared stale outcome after the winner commits. |
| Keyless upstream retries | **VERIFIED** | `adapter_minted_idempotency_survives_driver_retries_with_fresh_transport_ids` proves an adapter-retained command input survives bounded driver retries while transport request identities rotate. |
| Framework-neutral alpha workload | **DELIVERED — SMALL** | `adapter-framework-profile-acceptance` rejects framework-specific fixture names and runs the compiler, runtime, driver, and real-storage evidence above; `deployable-alpha-acceptance` includes it as a required phase. |

No prerequisite was escalated to a separate work package.

## Compatibility boundary

Contracts without an initial state or explicit self-transition retain their
least prior IR version and canonical encoding. A contract using either feature
requires V9. `fixtures/compiler/workflow-initial/` pins the V9 bytes and bundle
hash, while the existing pre-V9 compiler fixtures remain byte-frozen and are
checked by `scripts/generate-contract-fixtures --check`.

## Safety invariants

- Only the compiler injects a declared workflow initial state into creates.
- Callers cannot assign, parameterize, or override workflow state.
- A workflow without `initial` remains ineligible for ordinary create.
- A self-transition remains an exact-revision transition and returns the same
  declared stale and illegal-state outcomes as every other transition.
- The profile exposes only compiled commands. It adds no arbitrary callback,
  transaction, storage, or generic mutation surface.
- Token expiry uses authorized transaction logical time, never runtime wall
  time, and token material remains secret-classified.

## Reproduction

```bash
./scripts/adapter-framework-profile-acceptance
TMPDIR=/home/kevin/tmp ./scripts/generate-contract-fixtures --check
```
