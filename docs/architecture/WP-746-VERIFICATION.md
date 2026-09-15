# WP-746 verification and review

**Package:** WP-746 — Replication stream, capability, follower applier, and gap-free resume

**Tier:** guarantee

**Status:** implementation, obligation proofs and full CI verified on 2026-09-15.

## Behavior added

`riffdbd --mode follower` bootstraps from one exact V3 fence, validates the
complete database and required derived state, and exposes a writer-free service
graph. One applier persists each frame atomically, replays derived state and
acknowledges the completed prefix before publishing immutable reader snapshots.
Continuous tail connections resume from durable state after interruption.

The administrative `ReplicateChangelog` permission guards the single
`ReplicationService.StreamChangelog` RPC. Verified transport and current
authorization precede byte release. Exact lineage, epoch, catalog, position and
retention checks produce typed refusals. Application roles and bootstrap
convenience grants cannot carry this permission.

Commands, administration, migration, maintenance, export and other authoritative
operations refuse before writer or durable audit admission on a follower.
Policy-required audited reads also refuse. The accepted follower audit amendment
allows Health/Statistics and authenticated denied reads to use bounded redacted
telemetry; primary auditing and all replicated audit bytes are preserved.

## Exact obligation evidence

All three tests run in the ordinary `riffdb-server` integration target
`replication_follower`, using separate real daemons and verified TLS.

| Obligation | Test | Assertion |
| --- | --- | --- |
| OBL-0178-1 | `follower_applies_exact_prefix_byte_faithfully` | Complete app-baseline smoke seed and four command probes, 408 commands with exact replay of each; every replicated-authoritative namespace matches an independent receipt-replay model at the exact compared V3 position |
| OBL-0178-2 | `replication_stream_resumes_gap_free_after_repeated_kills` | Three deterministic mid-workload kills, exact durable resume requests, full startup validation and independent comparison of four increasing durable prefixes |
| OBL-0186-3 | `bootstrap_to_tail_fence_is_gap_free_across_crash` | Two source and two receiver crashes preserve the original bootstrap image/fence, reject a foreign incarnation and attach the retained tail without missing or duplicating state |

The test relay forwards the real primary's authenticated requests and bytes.
Explicit barriers hold frames or attachment requests; sleeps do not establish
correctness. The namespace model checks every namespace end, canonical ordering,
all prior-value conditions and strict receipt succession before mutation. Its
unit tests reject omitted namespaces, substituted values, duplicate/gapped
receipts and partial updates after late validation or budget failure.

Native storage process tests separately cover page/progress atomicity, bootstrap
hold creation, publication link/marker/sync boundaries, receipt/root/commit
atomicity, incremental indexes, projection replay, local acknowledgement and
scratch retirement. `RECOVERY_SCENARIOS` registers the applier, bootstrap and
stream-kill arms. The simulator inventory guard names their native process
coverage explicitly; it claims no simulated follower schedule exploration.

## Checks run

- Package acceptance: 17 steps passed, 3,611 tests passed, eight existing skips,
  before the recovery inventory registration. Log:
  `/tmp/wp-746-kill-acceptance.log`.
- Four daemon integration tests passed together in 46.26 seconds.
- The simulator inventory guard failed on the missing replication classification
  and passed after all three cases were classified.
- Recovery report regenerated through its Rust renderer; linked checksum and
  release evidence verified with `scripts/release-evidence --verify`.
- Formatting, diff checks and allowed-path checks passed. The root manifest is
  an allowed internal-tier consequence of the test workload dependency.
- Full `scripts/ci-all` passed, including workspace tests, doctests, Rustdoc,
  dependency checks, operator conformance, generated artifacts, Helm checks,
  public authoring acceptance and source-install smoke. Log:
  `/tmp/wp-746-ci-all.log`.
- Final closure tests: 3,818 passed, with 16 existing skips across the expanded
  affected set. The first closure acceptance run found only a missing handbook
  summary entry for this page. After adding it, acceptance reruns with
  `--no-tests`: all Rust tests already passed and the correction changes only
  documentation. Logs: `/tmp/wp-746-closure-acceptance.log` and
  `/tmp/wp-746-closure-docs-acceptance.log`.

## Compatibility and accepted review

The storage format, V3 frame encoding, journal, backup manifest, primary commit
acknowledgement and application export behavior retain their accepted semantics.
Bootstrap uses the accepted separate manifest/page/progress identities. RPC,
capability, configuration and typed error additions have regenerated schema,
client and application-error fixtures. Dependency versions are unchanged.

The maintainer accepted follower lifecycle isolation in `96dcc300` and the
follower audit boundary in `1a9ce841`. Scope widening precedes implementation;
the final scope baseline is `a74a9d50`. Commit `aa33c150` supplies the accepted
consequence-path rules.

Handbook pages updated: [configuration](../configuration.md),
[security](../security.md), [limitations](../known-limitations.md),
[errors](../reference/ERRORS.md), and [V3 implementation](CHANGELOG-V3.md).
The implementation page is reachable from the handbook summary.

## Implementation decisions and follow-ups

- Keep source construction and follower activation as separate ownership paths;
  follower startup never acquires a source lifecycle writer or coordinator.
- Reuse the canonical namespace catalog, complete structural/catalog validators,
  projection evaluator and read adapters. Do not normalize authoritative bytes
  or copy source-only receipt history into followers.
- Publish reader roots only after durable apply, required local replay and
  acknowledgement. Terminal receiver failure withdraws admission; existing
  historical pins drain under the normal lifecycle.
- Retain source holds and private receiver evidence through exact attachment;
  retire only known scratch files after durable handoff under live exclusion.
- Reuse the existing app-baseline dataset and generated TicketDesk facade in
  tests. Exclude its nested core package from root workspace membership so its
  existing workspace retains ownership.
- Initial connection/bootstrap failure terminates startup with readiness absent.
  The active tail worker retries transient failures while retaining the same
  applier, using bounded backoff; terminal failures require operator recovery.
- WP-747 owns complete follower freshness/provider coverage and sequence-lag
  reporting. WP-748 owns promotion and hold retirement; WP-749 owns archives;
  WP-750 owns the complete receipted acceptance campaign. No automatic failover,
  quorum, cascading followers or performance-freeze lift is claimed here.
