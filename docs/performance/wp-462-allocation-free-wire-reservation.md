# WP-462: Allocation-free sequence-free wire reservation

RiffDB reserves the complete durable command graph before assigning a commit
sequence. The reservation deliberately charges maximum-width future sequence
and entity-version varints, then the final encoder proves that exact bytes fit
inside that conservative bound.

WP-462 keeps those two independent checks but removes intermediate vectors that
held only computed field lengths. The sizing pass folds directly over the
already bounded semantic command collections. It does not cache untrusted
input, skip overflow checks, change Protobuf bytes, or alter the 16 MiB staged
write ceiling.

## Evidence

The exact parent revision is `a35539f6`. Its adjacent full public TicketDesk
report measured a 6.381 s median seed. The allocation-free candidate measured
6.257 s across three internal samples, a 1.9% improvement. A separate
single-sample writer trace measured 6.017 s total with 1.293 s in
validation/encoding/staging across 19,220 commands. Representative unary
`create_comment` measured 4.01-4.64 ms versus 5.04 ms in the adjacent parent
report; no unary regression was observed.

The shared host still shows substantial redb flush variation, so this is a
retain-or-revert result rather than a new release baseline. The directly
targeted result is structural: seven per-command intermediate length vectors
are absent, while all exact-bound, one-over-limit, semantic, durable-codec, and
architecture tests pass unchanged.
