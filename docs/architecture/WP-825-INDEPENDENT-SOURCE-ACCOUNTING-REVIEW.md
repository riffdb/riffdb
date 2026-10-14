# Review: independent read-only source accounting

WP-825 remains open. No eight-hour or 24-hour rehearsal has started.

The maintainer accepted the exact text of [ADR-0301](https://github.com/riffdb/riffdb/blob/main/adr/0301-read-only-source-byte-accounting-independent-of-deletion-proofs.md) and its WP-825 registration in session on 2026-10-14. Its accepted Decision is unchanged; accounting grants no deletion authority. The maintainer also requested another main merge before the rehearsal.

The retained population contains an initial source interval of roughly 290,000 receipts. A native component scan of 275,313 newly eligible receipts now completes in 28.00 seconds under public 100-command/400-read load. That four-minute diagnostic completes 24,000 fresh commands and 96,000 reads without retries, but latency overflow prevents service-level qualification.

The corrected shipping-profile server then emits no source-byte samples within 95 seconds on the same saved population, without the extra scan process. It has validated 71,168 initial rows with no invalidations or admission refusals. Code inspection confirms that initial checkpoint observation and byte capture are reached only after the complete paced source/audit proof. The proposal separates byte-census readiness from mutation authority, rather than changing a gate or pausing foreground work.

The independent census must still establish actual finite checkpoint coverage, current hold inputs, a complete canonical interval, exact byte counts, lineage and native qualification. Its private coverage type cannot initialize or replace the deletion license or validation proof. Every deletion still waits for full source/audit, registration, fence and promotion validation and ordinary writer admission.

Capture may occur during mutation rest, but cannot advance mutation proof or alter its rest deadline. The existing two-pin and 64-KiB bookkeeping bounds, 256-row pages, 60-second cadence, 30-second capture freshness and 95-second sample gap remain. If evidence or a reader slot is unavailable, accounting stays unknown; no zero or default sample is invented.

The proposal adds a deterministic fail-first checkpoint/append schedule while the deletion proof is unfinished. It also requires exact pinned-byte equivalence, unchanged refusal under hold/classification/lineage changes, proof that census completion cannot authorize deletion, and explored capture/rest/pin retirement schedules. The populated actual native-worker check must then pass before final CI and either rehearsal qualify.

The isolated implementation draft now connects exact current-hold validation, independent finite checkpoint coverage and the native worker. It reuses existing validators, checks appended registration/release audit input before reusing a completed hold certificate, and releases obsolete pins. Its private coverage cannot initialize a deletion license or advance the mutation proof. Native refusal, own-cut/acknowledgement, cancellation and worker-rest tests pass: 119 related tests plus five focused schedule/custody checks on the latest-main base. The combined bookkeeping retains its compiled 64-KiB assertion.

The required-host comparison includes local main through revision `3b851020`, merged as `48726d12`. That committed server still emits no source-byte sample within 95 seconds on the saved population; it completes 84,992 initial validation rows without invalidations or admission refusals. The unmerged accounting prototype emits four valid samples during the same four-minute public load. Its first complete interval has 290,605 receipts, 1,953,952,604 canonical receipt bytes and 2,324,840 key bytes, collected in 15.90 seconds across 1,136 bounded pages. Subsequent qualified captures reuse the exact interval at the unchanged cadence. All 24,000 fresh commands and 96,000 reads complete without retries.

That unprofiled diagnostic fails foreground p99: 2,574 requests exceed the existing 250 ms histogram ceiling, so p99 is refused. Its p95 of 3.653 ms passes ADR-0265’s 100 ms limit. An earlier private diagnostic comparison incorrectly used 3/40 ms thresholds; that mistake does not change the p99 failure. Sampling success does not qualify the workload, recovery gates or either rehearsal. A separate timing diagnostic preserves the original histogram and reconciles every completion into fixed five-second bins. Of its 1,880 overflowed requests, 1,870 finish in the first five seconds; 1,832 incurred queue waits above 250 ms, and 48 exceeded that bound after dispatch. The longest dispatched request takes 3.98 seconds. Investigate native startup and first-use work before assuming steady CPU pressure is the sole cause. Diagnostic timing instrumentation cannot qualify the campaign.

A focused 60-second diagnostic attributes the startup stall to the existing cold transient-index rebuild: 6.520 seconds under the mutation gate with 27,973 command rows counted. The longest dispatched request takes 6.550 seconds. Journal initialization takes about a millisecond. The diagnostic completes 6,000 fresh commands and 24,000 reads without retries and collects a valid source sample; its wrapper subsequently refuses an incorrectly small diagnostic-line inventory cap. Preserve that failed receipt and its measurements. No database check or workload threshold was changed to obtain them.

This cold-start diagnostic is not a normal five-minute campaign window. The frozen checker applies normal-window latency requirements outside predetermined fault windows. ADR-0265 still counts a blocked first write as recovery time. Keep the measured cache cost inside the actual 45-second release and 60-second recovery checks before deciding whether a cache change is necessary for this milestone. No extra warm-up, timing exclusion or stricter latency target is introduced.

Complete acceptance and `ci-all` pass on the isolated latest-main implementation: 7,089 workspace tests, doctests, linting, generated artifacts and the remaining repository checks. Rust, Go, TypeScript and Python operational-query and remote-driver conformance pass, and four ordinary release binaries are frozen from the same source. These are review results for the proposed implementation, not final accepted-source qualification.

The wider checks exposed an observation-owner regression: a plain retention step acquired an accounting reader whose independent callbacks it did not drive, preventing source reclamation. Capture now belongs to the worker's observation callbacks and the explicit observation API. The three previously failing production tests pass unchanged, along with all 20 retained-extent tests and the workspace battery. The offline crash matrix also now consumes the existing complete owned-stage evidence instead of opening a second engine. Its parent requires actual `SIGABRT`; all ten original process-crash cases and the native publication crash-cut test pass.

A focused public restore diagnostic uses the retained backup and archive roots used by the campaign controller. Shutdown, creation of the empty data/projection directories, recovery admission, native materialization and validation remain inside the recovery clock. Original acknowledged-command replay and full reconciliation of all 65,536 tickets at the 150,000-command saved prefix finish at 43.256 seconds. The diagnostic waits for the actual 45-second release point; a fresh committed write completes at 45.115 seconds. Exact replay and full state reconciliation after that write finish at 47.812 seconds, inside the unchanged 60-second deadline. No live archive database is copied. This diagnostic does not establish retention of the original campaign workers, the complete fixed fault schedule, normal service windows or long-run resource bounds.

At that diagnostic checkpoint the ADR was unaccepted and the implementation remained isolated. Exact acceptance is now recorded above. Qualification still requires the merged-source checks and bindings, the actual scheduled restore/release gates, and both complete rehearsals.

The maintainer subsequently accepted the exact ADR-0301 and its registration in WP-825 on 2026-10-14. The accepted accounting-validation boundary grants no deletion authority; the broader approval for necessary native fixes remains in force.


The requested final main refresh incorporates WP-796 and accepted ADR-0244.
Native restore no longer installs or requires the retired durable locator tables.
Its completed-registry and native-checkpoint checks remain mandatory; operational
startup must prove the replacement derived locator index. Logical byte accounting
counts superseded inline locators and excludes the retired physical lookup keys.
Regression tests cover owned complete and bounded restore with both historical
locator-table inventories and their absence, immutable backup custody, exact byte
counts and unchanged refusal of corrupt inline locators. The first merged build
exposed obsolete locator references; its failed evidence is retained. The corrected
merged source still requires its saved restore recheck and full C3D acceptance
before the eight-hour rehearsal starts.


The saved native restore recheck then exposes a reader compatibility regression
from locator retirement before the recovery clock starts. The image's table
inventory and checkpoint roots pass, but its last retained command frame cannot
be decoded, causing a bounded-header sequence-discontinuity finding. An
independent regression confirms that decoding previously supported journal table
tags 18 through 20 returns `UnknownTable`.

Retirement applies to fresh table creation and new command staging. Historical
journal tags, exact composite mutation representation, closed key/value validation
and replay remain supported without changing existing frame bytes, checksums or
frontiers. The regression covers all supported journal frame versions, exact
re-encoding, shared overlay conversion and refusal of unknown tags. Existing
fresh-image restore tests still require that retired tables are not recreated.
The original saved header and whole restore gates must pass on the correction;
the failure is retained and no rehearsal qualification is inferred from builds.


The final workspace replay then passes 7,090 of 7,091 tests, with one seeded
crash-corpus finding. Locator-table retirement at `9ee66abf` removes engine
writes and moves physical crash placement. The complete unchanged corpus audit
shows three outdated movement records: `0x51c2c067` no longer reaches its
30-torn-decision territory, while `0x51c2c200` and `0x51c2c301` reach their
original territories again. Every oracle completes cleanly; the existing
terminal witnesses `0x51c2c30a`, `0x51c2c420` and `0x51c2c508` still prove
all original predicates with their unchanged thresholds.

The internal follow-up uses accepted ADR-0113's movement protocol. It preserves
every seed, generator version, campaign configuration, expected territory and
prior receipt, adds the exact causal commit and 2026-10-14 review date, and keeps
the existing forward successor chains. A third displacement explicitly retains
all three earlier restorations. The original corpus replay, complete audit, receipt-preservation test and
seeded-campaign lint check pass on the required host. An explicit
review test also passes twelve identical complete reports for all three affected
coordinates and all three terminal witnesses.
This changes no shipping behavior, persistent encoding, public interface or
rehearsal gate. WP-825 registers these narrow test paths in a preceding commit.


A fresh ordinary release build after the next main refresh produces different
server, CLI and load-worker bytes, so its prior restore result is not reused.
The original populated restore test reconciles at 46.710 seconds and correctly
fails the unchanged 45-second pre-release gate. The old primary's graceful-close
barrier takes 4.198 seconds instead of 1.109 seconds; native restore after
admission is slightly faster, at 34.692 seconds instead of 35.126 seconds.
The failed attempt and its media remain retained.

A native regression confirms that graceful close creates a spare extent and
immediately deletes it, even when no future write is possible. Normal production
spares have 320-MiB capacity and retain ADR-0280's 2-MiB sync bound. Close must
still establish and verify its valid empty active extent, complete existing
sealing jobs and perform the final Immediate CLEAN transaction under ADR-0157
and ADR-0188. Its private initialization path does not request new writer scratch.
The empty runtime is released outside its mutex so an interrupted close cannot
leave a later writer reusing initialization that omitted its spare. Ordinary
writer initialization, existing sealing work, chunk and final syncs, publication
order, durable bytes, frontiers and validation remain unchanged. The maintainer's
standing authorization covers this necessary native fix; no guarantee, clock,
fault placement or fixture is weakened.

The regression inspects actual media creation, the active journal header, both
authoritative frontiers and the CLEAN lifecycle. Separate guards require normal
writer spare preparation and preservation of that behavior after a refused close.
The original native graceful-close crash and refusal tests, bounded-zero-fill
tests and complete seeded corpus remain required, followed by the closest saved
45/60-second restore test on the new ordinary binaries before full CI. The
[Startup Integrity handbook](../operations/STARTUP-INTEGRITY.md) records the
shutdown behavior. No long-run qualification follows from this component repair.
