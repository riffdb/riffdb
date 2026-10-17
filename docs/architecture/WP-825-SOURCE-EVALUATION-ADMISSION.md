# WP-825 source evaluation admission

The source worker must reserve every additional allocation before constructing
it under ADR-0316's aggregate 64-MiB allowance. Existing canonical values can
contain nested lists, records, strings, byte buffers and vectors. Their encoded
size alone does not bound the allocation graph made by `Clone`.

The shared types crate now supplies allocation-free clone-work bounds for an
existing canonical value or record. The walk counts each actual cloned buffer
using the same allocator and alignment allowance as owned-capacity inspection.
It bounds recursion, total visits and arithmetic, and distinguishes the input's
retained spare capacity from the new clone's requested capacity. The helper
grants no reservation, source authority or publication permission.

Three focused proofs on the qualification host cover every canonical tag,
nested record admission and release, and independently retained input capacity.
The types crate's all-target, all-feature Clippy check and package scope, file
size and panic guards also pass. The receipt is
`caf73429efabf33e9baaffacc28d881650a357572c2b5a219497c610642bc4ee`.
The common allocation formula additionally passed the source-row codec's
preflight and ownership proofs in receipt
`4fa8db3b6f85bc145b7bf9334262863100e65efc650daa658a11ac08d1ba8699`.

The projection crate now uses these bounds to admit pure evaluation before
catalog event normalization, writer-plan resolution, expression cache controls
and cached values, group-key construction and per-command grouped deltas.
It calls the existing ordinary evaluator with its complete-command aggregation
order and checked arithmetic. Sizing refusal returns storage unavailable;
the sizing walk cannot invent an authoritative corruption or semantic limit.
The move-only result borrows its original plan, schema, command and ledger
through physical destruction. Evaluated data drops before its reservation.

The composition with the latest merged main passed five semantic proofs on the
qualification host: pressure refusal with ordinary evaluation independence;
ordinary group/filter/empty-command parity; complete-command overflow
association; original shared catalog custody; and sizing-refusal classification.
The actual borrowed-input compilation proof, catalog/invariant/projection
all-target Clippy with default and all features, formatting, scope, file-size
and panic guards also passed. Receipt:
`46135cf8f5e8a33e41bf5005d2618068e5bf43761887ff30d2910b5779b0f4ec`.

This component grants no native capture, source candidate or publication right.
Staging updates and the complete reader/evaluator/map working set still need
composition with the actual native owner. Source retention, full-population
restore, full CI and rehearsal qualification remain open.

Package: WP-825. Tier: guarantee under accepted ADR-0316. Behavior changed:
first-party allocation sizing and admitted pure evaluation; applications,
encodings and fixtures are
unchanged. Checks: the focused proofs and guards above; combined acceptance and
full CI remain pending. Compatibility: no durable or application interface
change. Handbook: no update is needed for these hidden first-party helpers.
Hazards and follow-ups: compose evaluation with native capture, staging and
publication while retaining each reservation until its allocation releases.
