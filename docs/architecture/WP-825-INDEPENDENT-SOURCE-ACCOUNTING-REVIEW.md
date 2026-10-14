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

The proposed ADR remains unaccepted and this implementation remains isolated. Qualification still requires exact acceptance, the merged-source checks and bindings, the actual scheduled restore/release gates, and both complete rehearsals.

Acceptance request: accept the exact ADR-0301 and its registration in WP-825. AGENTS.md requires a full guarantee record whose exact text a human accepts, and review when an accepted guarantee changes. Broad approval for necessary fixes remains in force; this request concerns the explicit accounting-validation boundary.


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
