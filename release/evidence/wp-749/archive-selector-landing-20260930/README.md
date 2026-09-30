# WP-749 archive selector landing qualification

The maintainer approved landing the selector implementation and historical evidence
on 2026-09-30. Integrated source `6d26f3b93` includes main `49fb4c291`.
The archive implementation is unchanged from candidate `f77159c0`.

All thirteen full-acceptance steps pass, including `ci-all`. The proof audit binds
86 archive/restore/selector tests, the bootstrap recovery test and 26 source files
to the retained private log. Both real CLI restore tests ran in their dedicated
CI step. This qualifies correctness; it is not a latency measurement.

## Integration repairs and retained failures

Two full attempts failed the first 30-second held-attachment watchdog in
`bootstrap_to_tail_fence_is_gap_free_across_crash`; their other 24 replication
tests passed. An unchanged focused run passed, and a temporary probe observed
one checked attachment at 27,015 ms while all 25 replication tests passed.
The probe did not observe a late attachment after 30 seconds and does not prove
the cause of the earlier timeouts.

The normal fixture already allowed 60 seconds for debug startup and populated
recovery. Commit `b9b5ac260` shares that bounded startup budget with the two
bootstrap attachment waits. Channels still hold the exact crash boundary; all
prefix, lineage and recovery assertions remain. Command progress, kill and RPC
deadlines are unchanged. No product deadline changed. The temporary probe was
removed before final full qualification; the failing logs remain retained.

Another attempt reached the storage architecture guard with the new cache
implementation but its old assertion. Main `29933f6e` corrected that guard to
check both explicit cache budgets; final qualification includes it. Three earlier
runs were stopped as main changed and are not counted as passing qualification.

Existing manual timing tests keep their existing ignore annotations. The two
CLI tests ignored by the general suite run under `check-archive-cli`. The
handbook's existing large-search-index warning remains informational.

## Later benchmark-only integration

Main `5c3dfd9f3` subsequently added an opt-in numeric error probe in the
`benchmark-support` feature. Integration `e6c06a6a2` preserves the archive code
and all 26 audited proof sources byte-for-byte. Storage Clippy checks all targets
and features; all 13 benchmark-support tests and the authority-snapshot
corruption/refusal integration test pass. This follow-up is separate from full
CI on `6d26f3b93`; it does not reattribute that full run to later source.

The first documentation follow-up caught a link to evidence outside the generated
handbook. The report now names that repository-relative evidence directory as
text, consistent with its other evidence paths. The failed check is retained.

## Limits

The original syscall census and failed latency measurements remain in their
September 27 packets, bound to their original sources. This record adds no
performance verdict. WP-749 remains incomplete and its latency gate remains unmet.

Run `sha256sum -c SHA256SUMS` in this directory to check the reviewed records.
Raw logs remain private; only their digests and reviewed results are published.
