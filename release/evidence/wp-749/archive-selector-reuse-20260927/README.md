# WP-749 archive selector I/O census

The production exact-retry/reopen test persists two new frames. Baseline
10c312d74 syncs frame.tmp, manifest.tmp and current.tmp twice each. The
candidate on integrated base be4e1a25 removes only the two current.tmp
syncs. All other archive file and directory sync counts are identical.
The duplicate selector write is 386 bytes per frame. This is local syscall
count evidence, not a benchmark or a latency verdict.

The baseline and candidate raw traces/test logs are retained privately and
bound by SHA-256. sync-comparison.json binds all changed runtime/test source
bytes at measurement. targeted-tests.json records the 39 passing archive
tests, including legacy selectors, substitution refusal, ten process-crash
edges and ten uncertain-I/O edges. No durable fixture bytes changed.

Candidate f77159c0 passes all thirteen full-acceptance steps, including ci-all.
The earlier scoped run passes 1,051 storage tests; its duplicate fixture-import
lint was corrected, and Clippy and the full plan then pass without suppressions.
qualification.json records that history and the successful command/steps.
full-proof-audit.json binds 86 named archive/restore/selector proofs and 24
source files to the successful full-CI log. Both dedicated CLI restore tests
are included. The ignored shutdown_checkpoint_build_scale_evidence test is an
existing manual timing experiment, not a missing correctness proof.

Neither this local qualification nor the syscall counts award a performance
pass. The control/candidate comparison has not run and the package is open.

Reproduce each revision's count with the command in sync-comparison.json.
Count successful fsync calls within the fixture's archive directory, grouped
by entry name; include the directory itself, retries and reopen validation.
Do not treat the traced test's wall time as performance evidence.

The next comparison requires a released exclusive measurement host. WP-749's
acknowledgement-latency gate remains unchanged and unmet.
