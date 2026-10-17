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

This is a prerequisite. Complete evaluator admission still needs to count
catalog event normalization, writer-plan resolution, expression cache controls
and cached values, group-key construction, per-command grouped deltas and
staging updates. It must preserve the existing per-command aggregation order
and checked arithmetic. It does not establish source-worker, source retention,
full-population restore or rehearsal qualification.

Package: WP-825. Tier: guarantee under accepted ADR-0316. Behavior changed:
first-party allocation sizing only; applications, encodings and fixtures are
unchanged. Checks: the focused proofs and guards above; combined acceptance and
full CI remain pending. Compatibility: no durable or application interface
change. Handbook: no update is needed for these hidden internal sizing helpers.
Hazards and follow-ups: wire the bounds into the actual evaluator and retain
each reservation until its corresponding allocation physically releases.
