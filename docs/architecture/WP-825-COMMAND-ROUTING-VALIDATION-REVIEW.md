# Native command membership and damaged routing rows

Package: WP-825. Tier: guarantee, within the accepted native dependency and
complete-validation decisions. No durable format or application surface changes.

The public offline scrub regression first completes a healthy scrub, then plants
an invalid standalone event-route row that bounded startup deliberately does not
inspect. Its second public scrub previously failed before admission: the live
command-directory constructor discarded an independently valid command directory
when its separate routing accelerator decoded the damaged row.

Construction now retains that valid command directory and the original typed
routing corruption separately. Only a failure witnessed inside event-route row
decoding can take this path. Source traversal, I/O, capacity, cancellation and
canonical command failures retain their existing refusals. The incomplete routing
map is discarded; routing reads return CorruptData rather than absence. Later
command publication still updates valid outbox membership while routing is failed.

The unchanged public scrub test now completes its healthy phase and reports the
planted damage as ValidationFailed in its second phase. Three focused native
checks prove valid command/idempotency membership and exact new outbox events
alongside routing corruption, refusal of genuine command corruption with the
older protected source intact, and refusal of a route traversal-bound violation
that occurs outside row decoding. Scoped server/native Clippy, allowed-path and
file-size guards pass on the same candidate. An earlier candidate also passed the
existing builder and initial-admission schedule tests; complete source CI remains
required after integration.

Handbook: Startup Integrity and Clean Restarts describes the independent
construction boundary. Authorization, durable audit, readiness, background scrub,
publication ordering and the public maintenance deadlines remain unchanged.
This fix neither reconstructs pruned projection history nor resolves the separate
online retention refusal. The main integration, remaining native/follower
regressions and campaign qualification remain open.
