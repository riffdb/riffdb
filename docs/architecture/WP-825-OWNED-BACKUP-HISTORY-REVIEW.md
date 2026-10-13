# WP-825: owned backup history review

ADR-0298 and its WP-825 registration were accepted on 2026-10-13.
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

The maintainer accepted the exact ADR-0298 text and its WP-825 registration
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

PR note: Package WP-825; Tier guarantee; Behavior adds accepted ADR-0298 original
backup ownership and borrowed temporary history with complete cold source
checks. Checks include fail-first regressions, required-host readiness, backup
and maintenance tests; remaining checks are tracked above. Compatibility keeps
all durable encodings, fixtures, public application interfaces and resource
ceilings. Hazards and follow-ups include full CI, shipping timing, optional
fresh owner transfer and campaign-clock integration. Handbook page updated:
`docs/backup-restore.md`. WP-825 remains open without a closure claim.

## First shipping retest and targeted correction

The first ADR-0298 shipping build is committed source `dda0d5c7`. All four
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
