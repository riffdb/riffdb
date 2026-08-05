# WP-451 commutative child appends

Status: implemented under accepted ADR-0094; renewed cross-backend alpha parity
remains WP-454 work.

## Product rule

An aggregate conflict key is an invariant boundary, not a scheduler hint.
RiffDB therefore shares one aggregate conflict lease only when the checked
command plan proves all writes are input-keyed child creates and the plan has
no root or existing-row mutation, uniqueness domain, requirement predicate,
root/range validation, commit invariant, or unclassified dependency.

The coordinator separately rejects exact read/write and write/write overlap.
Every unproved command retains strict FIFO conflict ownership. Grouped items
still have independent authorization, idempotency, outcomes, events,
provenance, audit, sequences, acknowledgement, and uncertainty recovery.

For TicketDesk, `CreateComment` and `AttachLabel` qualify. `CreateTicket`,
`CloseTicketWithComment`, and `SwapMemberRoles` do not.

## Evidence surface

The closed writer evidence line now includes
`compatibility_commutative_shared_groups`. The number counts physical
compatibility groups selected for compiler-proved shared conflict authority;
it is recorded before commit and contains no command, tenant, key, principal,
or submitted value.

Use the isolated profile to reproduce the proof path:

```bash
./benchmarks/run-app-baseline --smoke --load append_only --load-clients 32 \
  --load-duration-secs 2 --load-warmup-secs 1 --skip-postgres
```

The 2026-08-05 short diagnostic completed 4,512 measured child appends at 2,249
operations per second with no failures. Of 425 physical completion groups, 212
compatibility groups were selected for the compiler-proved shared lease; the
successful completion distribution was 213 singletons and 212 groups of 31,
for 10.62 commands per physical commit overall.

The mixed `write_only` probe completed 2,051 commands at 1,026 operations per
second. It formed 182 proved shared groups but retained 656 strict conflict
cuts around root-mutating/root-creating commands. This is the intended safety
boundary, not a reason to broaden the proof.

The expanded full 19,220-command public seed completed in a median 3.773
seconds across the local three-repetition run. That is a substantial
improvement over the older generation but does not meet the separate 3.000
second target; this work package does not waive that gate.

These are local causal diagnostics, not immutable release evidence. WP-454
must rerun same-machine PostgreSQL minimal and safe-app comparisons with the
complete stability and semantic gates.

## Safety regression found during implementation

The first mixed-load run correctly stopped readiness: after a proved shared
subgroup formed, the partitioner admitted a later disjoint but unproved root
mutation into the same group, and the acquisition layer refused it. The
partition invariant now prevents any unproved member from joining once shared
authority is selected. A deterministic regression test freezes that rule, and
the corrected mixed run completed with zero unavailable or error outcomes.
