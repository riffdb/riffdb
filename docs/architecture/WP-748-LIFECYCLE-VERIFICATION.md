# WP-748 lifecycle, health and expiry verification

Package: WP-748. Tier: guarantee. Status: full validation in progress;
WP-748 remains open. Benchmark qualification remains paused at the maintainer's
request; this report makes no performance-qualification claim.

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

Combined full validation result: pending. The lifecycle-only revision passed
full acceptance on 2026-09-17; its `ci-all` run took 3,849.8 seconds. This is a
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

Public registration/retirement remain unactivated. They still need shared-service
Started/terminal V3 audit selection and exact result linkage, distinct operation
identities, and gRPC/Rust-client/CLI adapters with authorization and cancellation
proofs. Primary fencing, authenticated fence proof, offline promotion,
incarnation/epoch advancement and exact RPO remain WP-748 work. WP-749 still
requires its held qualification; WP-750 depends on both open packages.
