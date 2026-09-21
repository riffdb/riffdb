# WP-748 lifecycle, health and expiry verification

Package: WP-748. Tier: guarantee. Status: registration/retirement and promotion
correctness verified through the complete CI battery; human implementation and
fixture review pending. WP-748 remains open. Promotion resumed independently of the closed
WP-749 tail investigation. This report makes no performance-qualification claim.

## Promotion integration and recovery guard

The saved approved promotion implementation was initially integrated onto main
revision `395e2b693` without its private benchmark evidence or compact-prefix
experiments. Final implementation `6cba6e8c` uses public main `05501ec07` as its base.
The already accepted V2 complete-startup and revised establishment-only stream
amendments are restored in an authority-only commit. Main's Rust 1.98.1 and
process/request defect scoping remain in force.

A regression test reproduced an ordinary-startup hole: a terminal failed attempt
retained its immutable selection but passed maintenance reconciliation. The
repair keeps every selected attempt fenced until this exclusive owner has
completely reconciled an exact successful retry. That proof covers earlier
pre-cutover attempts with the same request and, when present, selection, without
rewriting their audit rows. A second invocation claiming committed or validated
authority remains contradictory. A fresh owner must validate again;
removing the successful receipt or adding another operation's failed selection
refuses. Denial before selection retains ordinary-open behavior.

The 40 storage promotion tests pass, including both process-crash campaigns.
The integrated workspace builds with all targets and features. The first scoped
suite ran 4,263 tests: 4,253 passed, six failed and four timed out. Five failures
exposed integration-generated fixture errors: a wrongly shifted V1 catalog
generator index and a missing primary-fenced error-registry row. Regeneration
restores the frozen V1 bytes; all 111 prior readable registry entries retain
their exact identities and bounds. Only the four approved fencing/promotion
records are added. Stale compact-prefix generator entries were removed.

The remaining failure was a follower readiness timeout. All ten unsuccessful
tests passed in a focused, serial rerun of 765 tests. The two daemon crash
campaigns finished in 38.1 and 34.2 seconds against unchanged 120-second limits.
Nextest now reserves the runner for follower lifecycle and promotion crash
campaigns; no readiness deadline, crash schedule or assertion was relaxed.
The earlier 19 non-test acceptance steps passed, including scoped all-feature
Clippy and all 30 generators, handbook, dependencies and governance. The final
complete battery is recorded below. Human review still precedes merge and closure.

Generated client changes propagate through application locks, adapter and
portability fixtures, and driver manifests. Their regeneration is a fixture
refresh, not a new external-consumer or performance qualification result.

Restricted pre-cutover startup and the under-load zero/nonzero-RPO drills are
implemented and exercised below. Final fixture review remains. The
passing lifecycle path is preserved rather than consolidated. Follower-derived
projection generation and lifecycle belong to the follower under ADR-0248.

### Integration PR note

- **Package / Tier:** WP-748 / guarantee; accepted fencing, complete V2 startup
  and revised establishment-only stream-audit amendments.
- **Behavior:** recover the approved live TLS promotion and committed restart
  path; keep failed or denied selected attempts fenced until exact successful
  reconciliation under the current owner.
- **Checks:** the complete CI battery at `6cba6e8c`, including workspace
  all-target/all-feature Clippy, tests/doctests, real archive CLI, operator
  conformance, generators and installation; 17 non-CI acceptance checks passed.
  Initial failed attempts and environment repairs are disclosed below.
- **Compatibility:** no pre-existing durable identity or V1 bound changed; no
  compact-prefix format is activated. Public fixture review precedes merge.
- **Updated handbook:** follower administration, configuration, compatibility,
  backup/restore, remote ingress, errors and known limitations.
- **Hazards and follow-ups:** human implementation/fixture review remains;
  fencing may leave both nodes unavailable until exact recovery succeeds.

### Final promotion validation

On 2026-09-21, all `scripts/ci-all` steps completed successfully at fixed
implementation `6cba6e8c`, using Rust 1.98.1. The initial command was
`./scripts/acceptance --wp WP-748 --range 8c025c549..HEAD --full`.
Its 17 non-CI checks passed. Workspace lint, tests and doctests, both real
archive CLI cases, documentation, dependency checks and expansion-oracle
checks also passed before operator conformance stopped at the TypeScript build.

That stop was an isolated-worktree environment error: TypeScript was on PATH,
but the worktree lacked its locked `@types/node` package. `npm ci` installed
the unchanged lockfile dependencies. The exact remaining `ci-all` commands then
passed through source installation. Operator conformance was additionally
rerun successfully with the project's pinned Maturin 1.14.1, replacing the
global 1.15.0 used by the first completed attempt. No runtime source or lockfile
changed during these repairs. The complete result uses those retained attempts;
the original uninterrupted invocation is recorded as failed, not relabeled.

The full-run follower binary passed all 25 tests in 672.59 seconds. Storage unit
tests passed 730 with zero failures and one existing ignored scale diagnostic
in 798.76 seconds. Both named WP-748 obligation proofs passed. Existing opt-in
diagnostics, fixture generators and process tests retain their ignore markers;
the two archive CLI cases are explicitly executed later by `ci-all` and passed.
Cargo-machete's two missing-source notices concern unchanged template and
negative-fixture manifests that explicitly document their non-compilable layout.

The final checks also cover Rust, Go, TypeScript, Python, CLI and MCP operational
queries; hosted/stdio cursor authority and redaction; generated fixtures and
topology; requirement/ADR proofs; Helm render, operator and upgrade/rollback
checks; public-source authoring; and source installation. See the
[implementation and fixture review](WP-748-PROMOTION-IMPLEMENTATION-REVIEW.md)
for the exact review range and compatibility inventory.

### Restricted pre-cutover recovery

Startup distinguishes a pending operation from ordinary maintenance readiness.
The exclusive maintenance owner validates the complete promotion ledger and
refuses unrelated maintenance inventory. The managed receiver reopens the local
follower with its retained bootstrap custody, full structural/catalog checks and
a pinned authority snapshot. It opens no peer connection, starts no replication
or derived worker, and publishes no application-ready receipt.

A separate private gRPC route supplies only checked authentication and promotion
admission. A foreign operation or generation is externally audited as denied.
An allowed exact invocation gets fresh source fence proof and a fresh clock and
current-capability check. Closing admission drains the bounded queued requests
with durable outcomes before releasing every local reader and applier. Cutover
and complete source validation retain the same maintenance owner on a blocking
worker. The completion waits for normal source graph installation and readiness.
This does not consolidate or refactor the existing live TLS promotion path.

The storage regression matrix reproduces why preserving an interrupted row used
to block even a later exact success. It covers Attempted, Draining, Offline,
Selected and uncertain CutoverPending, preserves each original row, and requires
a new owner to revalidate success. Negative cases retain the fence for another
operation, a second claimed cutover, or unknown maintenance inventory.

The production TLS process proof passes in both Standard and Hardened profiles:
startup with the old source stopped and its credential absent; no ordinary
Health; durable foreign-operation and insufficient-authority denials; exact
authorized retry; one new incarnation/epoch; unchanged frozen selection; a new
idempotent command; and restart without the old source. Ten crash cells cover
Draining, Offline, Selected, CutoverPending and committed cutover in both profiles.
Each recovers through the same selection and retains one committed cutover.
Four signal-driven shutdown cells additionally prove clean custody release at
Offline and after committed cutover, and
fresh retry, with an explicit entity read before and after source restart.
An authorized retry while source credentials are unavailable records
FenceUnavailable, reopens restricted admission and preserves the selection;
restoring source access then permits the same operation to finish.

This increment adds no durable format, operation identity or public RPC. The
fixture daemon's observation/crash hooks are unavailable in normal builds.
Changed handbook pages are follower administration, configuration, backup/restore
and known limitations. These correctness results do not qualify a performance
gate or close either package.

### Promotion under concurrent commands

`promotion_mints_incarnation_and_refuses_old_lineage_tokens` runs eight bounded
command clients while the primary is fenced, in Standard and Hardened profiles.
Each profile has a caught-up follower and a follower held at application
sequence one by an explicit delivery barrier. Successful command sequences
are unique and exactly account for the primary's frozen application frontier.
The promoted receipt reports the observed applied frontier and its exact delta
from that fence: zero for the caught-up cells and positive for the held cells.
A new command and idempotent replay succeed, followed by an entity read with the
expected value, with the former source stopped.

During fencing, the process-local admission pause returns `StorageUnavailable`;
after its durable receipt it returns `PrimaryFenced`. A paused worker waits on
that receipt and retries its exact input, which must be refused rather than
committed or replayed. No sleep controls command or follower progress. The test
does not equate source and follower projection generations or lifecycle.

The initial process test exposed a missed source-side token check: after
promotion, a causal projected read could report `Building` for an old token.
The shared service now checks history incarnation before either lifecycle or
wait handling on sources as well as followers. The existing service regression
had expected `Lagging` for this detectable mismatch; it now requires the
accepted `RDB-HISTORY-0101` refusal. This preserves the same token and error
identities and follows the accepted WP-747 classification. All 18 projected-read
acceptance tests pass. The under-load process proof also retains an actual
source-issued token, follower discovery continuation and discovery history fence
across promotion, and attempts an old-lineage replication stream. All four cells
pass in 36.0 seconds with the unchanged 120-second watchdog. The old cursor uses
the existing kernel discovery validation refusal; the history fence is a typed
history mismatch and the stream returns ForeignLineage then closes without a
frame. None releases stale rows or starts an implicit fresh page.

### Failure and cancellation coverage

| Boundary | Proof |
| --- | --- |
| Admission, current authorization and durable denial | `promotion_admission_tests`: unavailable policy, revoked authority, foreign target/generation, conflicting selection, corrupt audit storage and bounded handoff |
| Handoff owner is lost | `promotion_handoff_is_bounded_and_receiver_loss_preserves_uncertainty`; bounded admission and a dropped owner report uncertainty |
| Caller drops its response | `dropping_promotion_response_preserves_accepted_audit_custody`; the accepted trigger still persists its original audited terminal outcome |
| Interrupted selected operation | `selected_promotion_restarts_restricted_then_exact_retry_serves_one_new_lineage`, both profiles, no peer or credential at restricted startup |
| Fresh source proof unavailable | The same TLS proof requires a durable FenceUnavailable result and a later exact retry with unchanged selection |
| Signal shutdown | `restricted_promotion_shutdown_releases_custody_before_a_fresh_exact_retry`, both profiles, at Offline and committed cutover; clean join/custody release and subsequent exact retry |
| Durable cutover and reconciliation | Ten restricted process crash cells plus the existing storage cutover/reconciliation crash campaigns; one committed lineage and no restamping |
| Cancelled validation and contradictory retained claims | `committed_promotion_reconciliation_refuses_cancelled_missing_and_terminal_claims` and the exact-success/contradictory-cutover storage matrix |

These are correctness proofs, not latency or availability measurements.

## Implemented behavior

- Registration and retirement use the sole coordinator and a closed consuming
  source-barrier transaction. Current capability facts and a fresh authorization
  clock sample are checked even for exact retries.
- The immutable lineage, hold ID and policy select the original registration
  receipt. Retirement selects its exact generation. Replays cannot resurrect a
  retired identity or release a different registration.
- The administration record, V2 source hold and V3 physical receipt commit
  atomically. Retirement drops matching bootstrap custody while preserving
  independent archive holds.
- Budget exhaustion or configured expiry first records degradation in the
  checked V2 policy using an empty SourceHold receipt. This changes neither the
  application nor administration frontier. A later coordinator turn may expire
  only the matching configured policy, linked to its original registration.
  Budget exhaustion alone never releases retention.
- Primary Health includes reached registration policy limits, including expiry
  with zero application and administration lag. Statistics counters retain their
  existing meaning. Follower health and primary zero-registration rules remain.
- A primary-owned observer releases its source pin before coordinator admission,
  submits at most 16 one-registration transitions per pass, and waits one second
  between passes. Expiry remains sequence-based. Proven unavailability retries;
  uncertain or invalid outcomes stop the observer. An uncertain expiry may
  already have committed a complete audited release; recovery validates that
  persisted result before further work.
- Shutdown cancels pending capacity waits, drains already accepted receipts and
  joins the observer before coordinator close. Followers acquire no local writer.

## Revisions and checks

| Revision | Increment | Focused evidence |
|---|---|---|
| `b7f05212` | Atomic lifecycle writes and current authorization | Seven storage tests, three coordinator tests and current-policy drift tests; full acceptance passed all 12 checks, including `ci-all` |
| `a6ba11f1` | Health before configured expiry | Four production-storage tests and 61 coordinator/protocol/service tests |
| `c9863791` | Bounded scheduler | Four deterministic scheduler tests plus four production-storage tests; 14 static acceptance checks |

The process tests abort before and after health and expiry persistence, reopen
repeatedly, and check whole-state recovery, exact application/administration
heads, original audit provenance and retention. A caught-up follower at expiry
still reports degradation before its audited release. Deterministic scheduler
tests cover bounded passes, cancellation, accepted-work drain, refusal and retry
backoff without using sleeps as correctness synchronization.

Full validation command for the fixed combined revision:

```bash
./scripts/check-allowed-paths --wp WP-748 --base 49a79aa0
RUST_TEST_THREADS=1 NEXTEST_TEST_THREADS=1 ./scripts/acceptance --base ec5fac23 --full
```

Scope is checked after the standalone scope-widening commit; full acceptance
checks every implementation change since current main.

Combined lifecycle/health/scheduler revision `88c1a275` passed all 12 full
acceptance checks on 2026-09-17; its `ci-all` run took 3,223.0 seconds.
The earlier lifecycle-only revision `b7f05212` passed full acceptance on
2026-09-17; its `ci-all` run took 3,849.8 seconds. This is a
correctness-check duration, not benchmark evidence. The intermediate health-only and scheduler full runs were superseded when
main accepted ADR-0236 and removed fresh-locator coverage. Their focused and
static checks passed; neither superseded full run is claimed as a pass. The
combined revision preserves main's write-fencing helper and production commit
order. Nineteen focused tests, including health/expiry process-crash recovery,
passed after replay onto `ec5fac23`.
The combined command includes `scripts/ci-all`: workspace lint/tests/doctests,
real archive CLI tests, documentation, dependency checks, generated artifacts,
operator conformance, deployment-package checks and source-install smoke tests.

## Compatibility and remaining work

These increments preserve durable identities and existing record bytes. The
Health adapter admits the existing Degraded status for a live primary
registration at zero lag; it adds no wire field. Older clients enforcing Healthy
at zero lag refuse that newly admitted shape. Required hold/control links
remain fail-closed, including after crash, replay and physical-history pruning.

The updated handbook pages are [Compatibility](../compatibility.md),
[Replication health and statistics](../operations/REMOTE-INGRESS.md#replication-health-and-statistics),
[Changelog V3](CHANGELOG-V3.md#application-history-retention-fence-wp-748),
[Backup and restore](../backup-restore.md), and [Known limitations](../known-limitations.md).

The production administration dispatcher now selects V3 for checked follower
targets and preserves identical V2 bytes for existing targets. Its follow-up
passed 54 audit-focused tests, including mixed-generation reopen, refusal of
substituted database/incarnation/epoch/hold identity at each terminal phase, and
four process-crash edges with exact physical receipts. This follow-up is separate
from the fixed lifecycle revision's full validation above.

The shared-service current-policy port and production server adapter also forward
complete lifecycle requests to the existing authorization preparation. Two
focused tests prove the real capability-view path rejects expiry/revocation and
the port rejects missing or unavailable current facts, clock outage, stream-only
authority and unsupported adapters. Final transaction-current authorization
remains independently required by the coordinator. Final prerequisite revision
`6510d052`, including the V3 writer and policy adapter, passed all 12 full
acceptance checks on 2026-09-17; its `ci-all` run took 3,278.8 seconds.

The follow-up operator increment implements registration and retirement with
shared-service Started/terminal auditing, exact action/target result linkage,
distinct operation identities, and gRPC/Rust-client/CLI adapters. Its production
daemon test covers new-request-ID retries across restart, historical replay
after retirement and persisted audit links. Three focused service tests prove
revocation between safe points, pre-admission cancellation, follower refusal and
MCP refusal. Wire tests cover malformed lineage, zero policy, duplicate nested
fields, ambiguous responses and retirement-generation substitution. CLI tests
cover explicit selection, canonical hold IDs and full-width decimal output.
See the [public fixture review](WP-748-PUBLIC-LIFECYCLE-REVIEW.md) for the exact
additive surface. Public implementation revision `43a88c7e` passed all 12 full
acceptance checks on 2026-09-17 with
`./scripts/acceptance --base 6510d052 --full`; its `ci-all` run took 2,906.1
seconds. The full suite includes the daemon lifecycle regression and all service
policy, audit-link, wire and CLI checks. The maintainer accepted the exact public fixture and adapter diff on
2026-09-17 with "i approve"; see the linked review for the acceptance record. The implementation and generated artifacts are fixed at
that revision; this report is a documentation follow-up.

Primary fencing, authenticated fence proof, promotion, incarnation/epoch
advancement and exact RPO are implemented and verified above. Human review and
the resulting closure record remain for WP-748. WP-749 still requires archive
qualification; WP-750 depends on both open packages.
