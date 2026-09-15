# WP-784: fresh-locator segment witness in linear time

## What regressed

Commit `a493f162` (2026-09-05, "verify locator-bearing command successors",
WP-778) added a witness that runs on every epoch seal and on every direct group
commit. For each command in the sealed segment it resolved the durable member
through `command_member_at_access`, which reads the segment row from the
`COMMITS` table and decodes the complete n-command segment. Sealing an
n-command group therefore decoded the same row n times, and the writer thread
became CPU-bound with a cost quadratic in the group size. Group commit stopped
amortizing: the write path was fastest with one client and collapsed under
concurrency.

The WP-762 evidence run on N1 (revision `f5f79195`, 2026-09-15) surfaced it.
Mean group-commit time from the daemon's own writer evidence, write-only
sweep, safe-application harness, 90-second windows:

| clients | mean batch | mean commit | RiffDB ops/s |
|---:|---:|---:|---:|
| 1 | 1.0 | 1.9 ms | 255 |
| 8 | 4.0 | 5.1 ms | 614 |
| 32 | 16.0 | 38 ms | 371 |
| 128 | 63.7 | 600 ms | 103 |

The journal fsync inside each commit stayed at 0.7 to 2 ms and the redb final
apply at 0.2 to 5 ms; the remainder was the witness. Interactive c32 on N1 fell
to 2,474 ops/s (0.075x safe-application PostgreSQL) against the WP-644 N1
baseline of 7,223 ops/s (0.88x).

## What changed

`exact_fresh_locator_segment` now takes a resolver for the whole durable
segment instead of a per-member identity resolver. The queued path resolves
through `command_segment_at_access` and the direct path through
`command_segment_at_write_access`: one bounded exact-key read of the row keyed
at the segment's first commit sequence, decoded once. Each member is then
checked by ordinal against that row, and the manifest's idempotency entries are
indexed once per segment instead of being filtered once per member. Every check
the witness made before is retained; a row keyed at the first sequence that
does not start there, or a durable member whose sequence disagrees with its
ordinal, is still `CorruptData`. No durable byte, key, identity, ordering, or
public surface changes.

The test controller counts durable segment resolutions.
`grouped_fresh_locator_seal_resolves_the_durable_segment_once` seals a
64-command direct group and a queued single-command epoch on a fresh database
and proves exactly one resolution per seal with terminal identities, novel
absence without a history fallback scan, and zero transient rebuilds.

## Measured effect

E2 (`e2-standard-8`), app-baseline `--smoke --load write_only --skip-postgres`,
20-second windows, one repetition, release builds from the same worktree.
Mean group-commit time and RiffDB throughput per client level:

| revision | c=8 | c=32 | c=128 |
|---|---:|---:|---:|
| `9bf995d9` (2026-08-30, before) | 2.6 ms / 892 ops/s | 4.8 ms / 1,550 | 11.7 ms / 1,917 |
| `8a9a5f0f` (parent of `a493f162`) | 3.2 ms / 683 | 5.2 ms / 1,373 | 13.0 ms / 1,635 |
| `a493f162` | 3.2 ms / 656 | 25.3 ms / 506 | 370 ms / 153 |
| `f5f79195` (current, before fix) | 5.5 ms / 555 | 38 ms / 371 (N1, full) | 600 ms / 103 (N1, full) |
| `f5f79195` + WP-784 | | 7.8 ms / 1,151 | 21.5 ms / 1,365 |

Smoke windows are diagnosis, not evidence; the WP-762 bank reruns the full
harness on both profiles after this lands. The remaining gap to the
2026-09-05 parent (about 1.5x at c=32 and c=128) is linear per-command work
added since, chiefly the WP-772 receipt capture, and is not part of this
package.
