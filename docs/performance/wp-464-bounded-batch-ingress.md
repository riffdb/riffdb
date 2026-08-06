# WP-464: Bounded authenticated batch ingress

The public `ExecuteBatch` transport already bounds the complete request before
the handler begins. WP-464 uses that envelope boundary to perform lifecycle
admission, deadline derivation, and credential authentication once per RPC.
Each decoded item then receives its own request identifier, request control,
cancellation guard, and API-neutral `ExecuteCommand` invocation.

This is transport coalescing, not a batch transaction or an authorization
shortcut. Every item still passes the application service's current-policy
authorization and response-release checks and remains an independently
committed, audited, replayable command with its own typed result. One item's
failure does not cancel its siblings, results retain input order, and dropping
the RPC cancels unfinished item controls. The coordinator's ordering,
conflict, durability, acknowledgement, and recovery behavior is unchanged.

The handler polls the bounded item futures directly instead of spawning one
Tokio task and retaining one abort handle per item. It also decodes every item
before authentication or semantic execution begins, so malformed transport
carriage cannot leave an earlier sibling detached.

## Evidence

The exact parent revision was `7d4c98ad`. On the shared development host, with
generated concurrency 128 and three internal full-seed samples, the parent
median was 6.435 s and the candidate median was 6.212 s, a 3.5% improvement.
The physical completion-group mean remained effectively unchanged at 56.14
versus 56.30 commands, while representative unary `create_comment` p50 did not
regress (5.760 ms parent versus 4.844 ms candidate).

Authentication telemetry fell from about one authentication per item (19,443
records during a representative parent seed) to one per bounded transport
envelope (1,429 records during the candidate seed). Those counts describe
transport authentication work; they do not reduce or summarize the
current-policy authorization decisions that still execute independently for
every command.

Filesystem latency on the shared host is variable, so these measurements are
retain-or-revert evidence rather than a release performance baseline. The
deterministic structural result is that a bounded batch performs one transport
authentication, allocates no outer Tokio item tasks, and preserves ordinary
per-command service semantics.
