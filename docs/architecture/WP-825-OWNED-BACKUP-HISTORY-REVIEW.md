# WP-825: owned backup history review

ADR-0299 and its WP-825 registration were accepted on 2026-10-13.
The implementation replaces ADR-0293's copied private sealed extents with
owned read-only original backup descriptors. Required-host qualification and
shipping timing remain in progress. It also permits the actual verified backup and
publication owners to continue through fresh same-process startup consumption.
It grants no durable proof, public mode or relaxed validation or timing gate.

## Current verified checkpoint

Local main's bundle reuse and fused history validation are merged. The unchanged
saved primary-loss check passes on ordinary shipping binaries at `aacaaf54`:
first write 54.59 seconds; exact first-write and original-command replays plus
all 65,536 tickets 58.07 seconds. A separate actual process kill after a new
post-restore acknowledgement preserves that write, both earlier command
identities, outcomes and provenance, the actual target inode and the full state
oracle. The selected backup and maintenance receipt bytes remain unchanged.
These are focused checks, not full CI, the scheduled population or a campaign.

The later CI-only main fixes are merged too. All four ordinary shipping binaries
were rebuilt together from fresh committed source `42a524b2`; all 5,197 tracked
source hashes and copied binary hashes were verified. Required-host ADR form,
index and obligation checks pass. Full handbook checks pass after two historical
review-link corrections. Final-source full CI remains outstanding.

## Measured remaining work

Private instrumentation of `42a524b2` preserves every checksum and validator;
it is excluded from shipping gate binaries and cannot qualify timing. Production
source was restored and all tracked hashes rechecked after each probe build.

| Phase | Seconds | Relationship |
| --- | ---: | --- |
| Restore component | 47.77 | Excludes backup creation, target setup and replay/oracle |
| Private materialization | 35.36 | Includes copying and private readiness below |
| Initial materialization | 18.95 | Includes sealed staging and hydration below |
| Sealed extent staging | 8.82 | Copy, hash and existing frame checks |
| Surviving command hydration | 1.56 | Included in initial materialization |
| Preparation before validation | 2.00 | Included in private readiness |
| Complete retained-history capture | 11.29 | Included in watermark preparation/readiness |
| Receipt-authorized stamp and close | 4.70 | Includes final seal and source retirement |
| Native publication | 3.70 | Includes immutable backup check and full target readback |
| Cold publication-owner reconstruction | 2.04 | About one second each for backup and target hashes |

Nested phases must not be added together. Large-file hashes across the operation
read 18,761,596,928 bytes and take 13.11 seconds. Fresh target rehashing alone is
about one second, so retaining just that owner is insufficient. The retained-
history walk is complete required evidence and stays on the critical path.

Verified phase-probe receipt SHA-256:
`c34053454cc8bd4dfdefe74350f797d07a6d4ee41dcc2a06e118c265fb69ca31`.
An independent hash-only probe reads the same byte count in 12.55 seconds:
`a6b4ea91fe57c4c85cc9d4b8ee71ddcbdc117ecebffa61abe03c014425088a65`.
The 8.82-second staging phase includes work the proposal retains; it is not a
promise of an 8.82-second saving or a passing 45-second result.

## Concrete implementation boundary

1. Build one private owned backup inventory with full verification of every
   artifact, including sealed extents. Today the primary/journal/marker helper
   and the copied sealed-extent validator perform separate parts of that work;
   a reusable combined owner must prove the complete set before granting reuse.
2. Preserve primary/journal/marker copy and verification. Bind temporary history
   resolution to the original sealed descriptors, exact generation and original
   seals, with pinned directories and continuity checked around every read.
3. Retain complete history validation, fresh authorization and the existing
   one-engine receipt-authorized stamp. Prove all surviving values self-contained
   before dropping borrowed leases. Original backup files are never changed.
4. Preserve complete backup revalidation at publication, final full source seals
   and independent full transferred-target readback. Continue actual fresh owners
   through consumption when their custody remains intact; cold recovery rebuilds
   no ephemeral proof and retains complete required checks.
5. Keep all existing descriptor and other ceilings. Explicitly account for peak
   overlap and cancellation; use complete copying or refuse unsupported states.
   Keep frozen intent/receipt/database encodings and all application interfaces.

## Review and remaining qualification

The maintainer accepted the exact owned-backup text (originally ADR-0298, now ADR-0299) and its WP-825 registration
in session on 2026-10-13. Its Decision section is unchanged from that review.
The maintainer's standing approval for necessary fixes covers implementation
paths.
No fixture change is proposed. If implementation requires one, its concrete
diff must receive the separate human review required by the repository.

The fixed 45-second campaign window remains a blocker. The saved fixture has
1,500 seconds of offered work, not the scheduled 5,400-second population. No
deadline, workload, resource gate or earlier failure may be reclassified.
ADR-0292's campaign-clock integration, final-source full acceptance/CI,
scheduled-population checks and the eight-hour rehearsal remain required before
the 24-hour run. Neither measured campaign has started.

## Implementation checkpoint

Eligible receipt-stamped native restoration retains a closed original backup
inventory before its first copy. Fixed artifacts copy through the original
read-only descriptors with bounded scratch; sealed history uses exact private
generation bindings and positional reads. All original SHA-256 checks and
journal, marker, frame and footer validation remain. Immutable handoffs check
retained identities, inventory and write observations; mutation and cancellation
invalidate the owner permanently within that operation.

History hydration and complete retained-chain validation still precede the
receipt-authorized stamp. Surviving stubs or original read leases refuse source
retirement. Retirement drops only borrowed leases, preserving original files.
The original backup owner continues through full pre-transfer revalidation and
independent target readback. Fresh startup currently uses the existing cold
publication-owner reconstruction; the optional retained startup handoff remains
unimplemented.

A new process-recovery regression exposed that the old native Prepared/Published
path did not check original sealed-backup contents. The new path acquires and
fully validates the original bounded inventory before target ownership or any
cleanup. Its regression first failed, then passed for both phases. The consumed
target protocol and acknowledged-write protection remain unchanged.

Required-host regression checkpoint: 12 readiness/custody tests, 73 backup tests
and 249 maintenance tests pass, including process crash cuts. These results
precede final qualification; full CI, latest shipping timing, scheduled
population and the 45-second campaign window remain outstanding. The unchanged
saved whole60 and post-ACK crash results above belong to the earlier shipping
revision, not this implementation.

PR note: Package WP-825; Tier guarantee; Behavior adds accepted ADR-0299 original
backup ownership and borrowed temporary history with complete cold source
checks. Checks include fail-first regressions, required-host readiness, backup
and maintenance tests; remaining checks are tracked above. Compatibility keeps
all durable encodings, fixtures, public application interfaces and resource
ceilings. Hazards and follow-ups include full CI, shipping timing, optional
fresh owner transfer and campaign-clock integration. Handbook page updated:
`docs/backup-restore.md`. WP-825 remains open without a closure claim.

## First shipping retest and targeted correction

The first ADR-0299 shipping build is committed source `dda0d5c7`. All four
ordinary release binaries and all 5,200 tracked source files were independently
verified. The unchanged saved primary-loss gate failed: backup creation took
17.62 seconds before loss, recovery admission took 10.54 seconds after loss,
and the restore had not succeeded within the one 60-second deadline. There was
no post-backup acknowledgement gap. This remains a failed attempt; the earlier
58.07-second result cannot qualify the new source. Failed shipping receipt:
`ab097f9d44bcea49da272ba8404559db6cf60db2968d0c900e694dd9f23b6d74`.

A separate CPU diagnostic also exhausted its unchanged public polling bound
before completion. Its recorded samples show substantial filesystem metadata
and canonical-path work from checks around individual parser reads. It grants
no timing result. Failed diagnostic receipt:
`3b66ae9e0c1f57a46d95e53da9a5250106794c793ed1baa513bb8cbb74328070`.
Recorded-sample report receipt:
`77d112a46d298b3627ea04164bdd8c95e987d0336a5a058925ed7740c685519e`.

The targeted correction treats initial full-artifact parsing as one complete
read interval, with exact descriptor, inventory and write-metadata checks
before and after that parser, as for complete artifact hashing. Positional
cursors remain private to acquisition, check cancellation throughout, and never
escape into history resolution. Later borrowed history reads retain their
individual continuity checks. Deterministic endpoint tests inject same-length
writes with restored bytes and modification time, identical-content replacement,
and cancellation after a valid footer has been parsed. All must refuse before
an owned image can be returned. No hash, frame, footer or generation check is
removed. This correction and its shipping timing remain under qualification.

The targeted correction's required-host checks pass: two endpoint/complete-
artifact proofs, all 12 individual-read readiness regressions, the cold original-
seal regression and Clippy with warnings denied. Receipt:
`29f60caa637b1f637aa2f81ce31a6798da9945fd1ef70f8a2e4da43e9351b79d`.
The next step is a new ordinary shipping build and the same failed whole60 gate;
no broader CI or campaign qualification is inferred from these checks.

## Populated active-history refusal

The next ordinary shipping retest at `c5145c1c` returned the public
`internal_defect` error during restore, before a success observation. Its failed
receipt remains:
`8a2ab71100f9dae8600324ba027c8f9a21d233e0296d5f177aecd4c82c598379`.
A private native probe reproduced `CorruptData` specifically while hydrating
surviving command stubs. The original backup remained byte-identical and all
private instrumentation was removed afterward.

The closed borrowed-media adapter returned an I/O refusal for an absent sealed
candidate, preventing the existing journal reader from trying the active
journal. A checkpointed active journal can retain valid frames before its
anchor despite having an empty recovery suffix. A real frame regression first
failed with `Io`, then passed after restoring authoritative `NotFound` for a
canonical sealed generation absent from the closed inventory. Physical shadows,
changed source ownership, malformed names and every mutation attempt still
refuse; no ambient unknown extent is opened.

Required-host checks pass for that real anchored-frame regression, the three
original-owner tests and Clippy with warnings denied. Receipt:
`d91f56b66a89e2864a269a09b24b6719de4c8fbabe218600457e053e217d317b`.
The closest populated native materialization retest now passes on the same
150,000-update source with the checked fix; its original backup remains
unchanged. This proves only materialization, not full restore or timing. The
next step remains a new shipping build and the unchanged whole60 gate.
The handbook's complete-journal behavior is unchanged by this adapter correction.

## Closed namespace reuse for private journal operations

The next shipping attempt at `e9756799` still exceeded the unchanged whole60
clock, with no successful restore or post-restore acknowledgement. Backup
creation took 15.89 seconds before primary loss; admission took 11.19 seconds
after loss. Its failed receipt remains
`ba99d228fe7b07b7ed6075b200bbc0136e72a15c66a70e1eea8652df6d7406c8`.

A focused regression then proved repeated complete original inventory walks
while resolving the copied private journal and absent canonical sealed names.
Private operations now reuse the acquired closed inventory while checking the
pinned original namespace, directory write metadata and manifest descriptor
continuity. Namespace changes invalidate the owner permanently. Reads of
original extents still perform their individual full custody checks; complete
inventory and content checks remain at acquisition and publication handoffs.
No semantic walk, artifact checksum, deadline or resource limit changes.

The same regression, real anchored-active-frame regression, four original-owner
tests and Clippy with warnings denied pass on the required host. Receipt:
`5e120f05389c64c209e1478c293f110a160582262c8b0d74c9fe7e6a1d316d44`.
This is a focused implementation check, with shipping timing still unqualified.
The handbook's public behavior is unchanged.

## Shipping failure and supported capacity conversion

Shipping revision `666ddde2` still exceeded the unchanged whole60 gate after
closed namespace reuse. Backup creation took 18.24 seconds before loss;
recovery admission took 12.82 seconds after loss. Restore had not succeeded
within the original deadline. Original backup bytes and all shipping sources
were independently rechecked. Failed receipt:
`580c175f1413a5f31c1b7a9418e738a4315e41b39e5d479a9a6b8e58aac10a1d`.

A temporary populated native unit probe revealed an additional compatibility
case: test builds configure 40-MiB extents while the shipping fixture uses
320 MiB. Capacity-changing recovery needs another private sealed source;
that cannot be admitted into an already closed borrowed inventory. The probe's
readiness refusal is therefore not a shipping-capacity timing result.
The small native capacity regression first proved incorrect borrowing:
`f6ef7891e801669ba0e884d2313e8ba36c0c1eb6a0ab3f5788eb419717926236`.

Supported capacity changes now select complete copying and complete history
hydration. After hydration has removed every source reference, the unpublished
journal is renewed at the configured capacity through its retained descriptor.
The existing paced physical zero-fill and file synchronization remain; this
avoids normal operational recovery replacing the journal inode behind restore
custody. The original backup is never changed. Required physical, complete
history, authorization and final publication checks still follow. Ordinary
operational capacity conversion retains its atomic replacement protocol.

An injected process abort after synchronized private conversion writes proves
that the partial stage cannot be accepted as empty or publish a primary. The
original backup remains valid, and a fresh private retry passes readiness.
On merged main `0779d0b0`, nextest passes the capacity regression, process-crash
regression and all 15 restore-readiness tests without retry; Clippy passes with
warnings denied. Receipt:
`67d5da091d7cd83cb18b98535ffa19bb63e3c75ae2b254c5cd9249438b23af6b`.
An earlier run passed the 13 readiness tests but failed one test-only needless
borrow lint; its failure remains preserved. No shipping timing or campaign
qualification follows from these checks. Handbook: `docs/backup-restore.md`.

## One retained-frame read interval

A real retained-frame regression first proved repeated full custody checks for
individual header/body syscalls, rather than one bounded decoded-frame read.
Fail-first receipt:
`19d293930c9aabdb180a70e5a17f31427ccb9037b33542f3b10cf4d906feb005`.
The private immutable cursor now checks full original descriptor/path, namespace,
link, length, write metadata, cancellation and stage-shadow custody before that
frame and after complete framing/checksum/database validation, before returning
its decoded value. Cancellation remains checked throughout byte reads. The
cursor must finish the interval; unfinished/error exits invalidate its owner.
A generation mismatch also finishes its interval before trying another source.
Raw reads retain their individual checks. Ordinary and simulated media preserve
their existing operations through default no-op completion; original content
seals and full publication readback remain unchanged.

Six deterministic endpoint cases refuse same-byte/restored-time writes,
identical-content replacement, cancellation, stage shadows, hard-link aliases
and namespace changes. A bad frame hash invalidates future use; a checked other
extent generation does not invalidate an unchanged owner. Required-host nextest
passes the work regression, all eight owned-history tests, all 15 readiness
regressions and the retained-frame journal tests; Clippy passes with warnings
denied. Receipt:
`31f075c0106b47203bb66c2aabf6821e315daf2de36cdadae39aaf55a940a42f`.
No public behavior, encoding, provider admission or handbook contract changes.
A new ordinary shipping build and unchanged whole60 gate remain required.

## Release attribution after the retained-frame correction

Ordinary shipping source `ca7ec010` still fails the unchanged saved whole
60-second primary-loss gate. Backup creation completes before loss in 21.96
seconds; replacement setup and recovery admission take 13.86 seconds after
loss. Restore has not reached durable Succeeded at the deadline. No first
acknowledgement or full-state oracle qualifies this attempt. Preserve its
verified receipt `55637dabb0b2c32d1c6e7c4e54480ca48db763d5bfaa9cce9c704f4b736907bc`.

Separate private timing probes on the same ordinary release profile complete
restore and readiness in 71.77 seconds, excluding replacement setup and
admission. They change no validator or checksum, and production source is
restored and all 5,203 tracked hashes checked after building. This diagnostic
has a longer component timeout and qualifies neither recovery deadline.

| Phase | Seconds | Relationship |
| --- | ---: | --- |
| Restore component | 71.77 | Excludes target setup, admission and replay/oracle |
| Initial materialization | 20.47 | Includes the next three rows |
| Original backup acquisition | 3.82 | Includes complete artifact hashes and journal validation |
| Fixed-artifact copies | 7.56 | Primary, active journal and marker, including synchronization |
| Surviving command hydration | 8.05 | Complete required hydration |
| Complete retained-history capture | 30.86 | Included in watermark preparation and private readiness |
| Private materialization and readiness | 56.51 | Includes initial materialization and retained-history capture |
| Receipt-authorized stamp and close | 4.65 | Includes final seal and source retirement |
| Native publication | 5.54 | Includes full original backup and target readback |
| Cold publication-owner reconstruction | 4.90 | Includes fresh original-backup acquisition |

Nested phases are not additive. Large-file hashing across this operation takes
16.88 seconds, also nested in the phases above. The independently checked phase
receipt is `600522b2f562a773724b98d4f32de7bf4288cbc8461568dd3144a558f8fe5d34`.
The independently checked private build receipt is
`c8a7f0265e9a9026d8300239c8e90327bb3294d0284816506cb8792488486559`.

A same-source diagnostic control changes only original-backup borrowing to
use the retained complete-copy protocol, with the same backup, release profile,
probes, validators and polling limits. Restore and readiness take 48.70 seconds;
complete retained-history capture takes 11.13 seconds and surviving-command
hydration 1.55 seconds. Initial materialization takes 19.27 seconds, including
7.29 seconds staging copied sealed extents. Cold publication reconstruction
remains 4.93 seconds. This isolates a 23.07-second regression in the borrowed
path; it does not qualify the whole recovery clock or prove that all of the
additional time belongs to any one syscall.

The independently checked copied-control phase receipt is
`bafe24b49cadbbcea0555a812fbce7d36c02ffa53f154017df1c4309dc067bca`;
its private build receipt is
`69c1603f83f3348a3cccfb6152dc066b2fb81a6eadbe057edf078e35556d0aa5`.
Both probes preserve all eight selected original backup artifacts and grant no
shipping or campaign qualification. No validation, workload, clock or campaign
gate has been relaxed. Full final-source CI, scheduled population, the
45-second campaign window, resources and both measured campaigns remain
outstanding.

## Accepted bounded private read intervals

The maintainer accepted ADR-0300 and its WP-825 registration on 2026-10-13.
The registration is a separate prerequisite commit. Its exact Decision amends
ADR-0299's custody-check boundary only for the bounded private input protocol.

Original borrowing now has a closed private read owner for at most 256 candidate
frames and 64 MiB of reserved physical reads per interval. The reservation
includes both extent-header slots and bounds actual file reads. Missing or
wrong-generation candidates consume that conservative reservation. Before and
after each interval, the original exact inventory, manifest, descriptor/name
identity, links, lengths, write observations, cancellation and private shadow
absence remain checked. Reads retain independent positions and all existing
framing, checksum, generation, database, admission and semantic checks.

A scoped resolver is not registered for ambient lookup. Only the actual native
preparation or complete-validation caller installs it on its synchronous
thread; a same-database alternate cannot rescue failed custody. Cross-thread
use and overlapping scopes refuse. Completed history roots escape only after
all original read intervals and native generation checks finish. Hydration
finishes read validation before its existing private transaction writes, then
rechecks the finished original owner and the same native preparation engine
before commit. Unfinished drop, cancellation, observed I/O failure or changed
source permanently invalidates that owner. No original source is resealed.

The original work-bound regression fails semantically before batching; receipt
`39040376fa6b61a58913d3990936c875c0818834e4205355796c606ddc5951ae`.
Required-host focused checks subsequently pass without retries: the work-bound
regression, 17 owned-source tests, native generation fences, 15 readiness tests,
three retained-frame tests and Clippy. Actual child-process crashes during
unfinished reads and before hydration commit leave all original artifact hashes
unchanged, preserve the unhydrated durable row and refuse reuse of the partial
stage. Changed-source injection immediately before commit also aborts that
hydration batch. Receipt:
`5f4bf75a9b37ae66a9026888e2c2a0dc7f3cda691cb96f5ef2a9460c0f4e3a41`.
All 5,204 committed source hashes outside the 14 implementation/test drafts,
including frozen compatibility inputs, remain unchanged on the required host.

Original full publication verification, complete final source seals and
independent transferred-target readback remain. Ordinary readers and unsupported
complete-copy protocols retain their prior checks. No durable encoding, fixture,
application surface or operator contract changes; the existing backup/restore
handbook contract remains accurate. Shipping whole60 retesting, complete scoped
acceptance, full final-source CI, scheduled population, campaign45, resources and
both measured campaigns remain outstanding. These focused checks establish no
performance or campaign qualification.
