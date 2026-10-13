# WP-825: owned backup history review

ADR-0298 and its WP-825 registration were accepted on 2026-10-13.
Implementation remains outstanding. The accepted record replaces ADR-0293's copied private sealed extents with owned read-only
original backup descriptors. It also permits the actual verified backup and
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
