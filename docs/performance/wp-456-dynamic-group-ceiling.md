# WP-456 — Dynamic groups under a static safety ceiling

Date: 2026-08-05

## Decision and implementation

The authoritative physical-transaction count ceiling is now 256 commands. It
is a compiled safety invariant, not a requested or runtime-tunable batch size.
The scheduler still selects the currently ready FIFO prefix and stops at the
first count, byte, eligibility, cancellation, deadline, or hard-barrier bound.
The 16 MiB staged-write ceiling, public 16-command transport batch, and oldest
command's 200-microsecond deadline are unchanged.

The widened count required 16-bit internal batch counts and application/audit
sequence allocation. It did not change a durable encoding or public protocol.
Completion telemetry is a fixed 256-element vector whose positions represent
exact group sizes 1 through 256; it introduces no high-cardinality labels.

## Safety evidence

- Scheduler tests prove 257 ready commands form a 256-command group followed
  by a one-command group, while smaller ready prefixes retain their actual
  cardinality.
- The coordinator actor drains the exact 256-item internal boundary without a
  timer wait, and existing FIFO, deferrable-observation, hard-barrier,
  cancellation, shutdown, and byte-reservation schedules remain green.
- Memory and redb reject count 257 before staging beyond the bound. Audit
  allocation accepts exactly 512 Started-plus-terminal records and rejects
  513.
- Process-kill recovery executes an actual 256-command redb transaction. A
  pre-commit kill leaves all 256 identities, entities, and commits absent; a
  post-commit kill recovers all 256 complete stored outcomes and reciprocal
  command graphs.
- Unknown status still fences admission and resolves identities independently;
  no response is released from a partially classified physical group.

## Full-seed sweep

All runs used the same 19,220-command full dataset and public symbolic command
path. They are single-run diagnostic evidence, not a stability-qualified
release comparison.

| In-flight commands | Seed | Physical commits | Mean commands/commit | Largest observed group | Flush total | Validation/encode/stage |
|---:|---:|---:|---:|---:|---:|---:|
| 32 | 8.079 s | 1,063 | 18.11 | 32 | 6.201 s | 1.219 s |
| 64 | 5.452 s | 641 | 30.04 | 55 | 3.955 s | 1.097 s |
| 128 | 4.043 s | 341 | 56.47 | 112 | 2.512 s | 1.125 s |

Evidence files are
`target/app-baseline/wp456-ceiling256-c{32,64,128}.json` in the producing
worktree. At 128 in-flight commands the dynamic selector used nonzero group
sizes from 1 through 112; it did not wait for 256. Relative to the immediately
preceding WP-455 run, physical commits fell from 358 to 341, while seed time
moved from 3.915 to 4.043 seconds and unary `create_comment` p50 moved from
2.021 to 2.159 milliseconds. Those one-run changes are too small and noisy to
claim a throughput win or regression.

## Result

The larger ceiling is valid bounded headroom, but it does not reach the
sub-3-second seed target under the then-current public client limit of 128 in-flight
commands. The writer is fed dynamically; at that concurrency it does not have
enough simultaneously ready eligible work to approach 256. Widening the public
generated-batch concurrency is a separate application-interface decision and
was deliberately not smuggled into this storage work package.

The remaining full-seed critical path is about 2.5 seconds of durable flushes
plus 1.1 seconds of validation, encoding, and staging. Future performance work
should reduce those costs or improve upstream preparation overlap based on
stage evidence, rather than make the physical safety bound configurable or
extend unary waiting.

## Documentation impact

No handbook change. This work changes an internal bounded physical grouping
mechanism and observability vector width; public command semantics, transport
bounds, durability, and application interfaces are unchanged.
