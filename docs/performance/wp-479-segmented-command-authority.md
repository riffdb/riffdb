# WP-479 segmented command authority

WP-479 is an internal durable-layout transition. It does not change the RiffQL,
command, gRPC, MCP, CLI, or generated-client contract. The safety rule remains:
an acknowledged command has one atomic, typed, idempotent, auditable result and
survives a crash with its events and provenance intact.

## Implemented transition

The durable registry now contains closed, bounded encodings for:

- `StoredCommandCapsuleV2`, which retains the complete immutable events and the
  exact logical index-generation transitions of one command;
- `StoredCommandSegmentV1`, which can own at most 256 contiguous successful
  commands and 16 MiB of canonical content;
- the exact segment manifest used to rebuild idempotency, provenance, audit,
  event-route, and pending-outbox locators; and
- a readiness-gated in-memory directory rebuilt from that canonical manifest.

Current audited successful writes emit one command segment for each compatible
physical group. Decoding is fail closed by record type: corrupt segment or V2
bytes are never retried as older bytes. Historical commits and V1 capsules
remain readable during the pre-alpha transition.

Within one physical command group, the writer validates every logical
index-generation transition in command order against a transaction-local
overlay. It writes only the final post-image for each distinct partition/index
target. Every intermediate transition remains in the owning V2 capsule and in
the durability journal frame. Transaction-local command reads observe the
overlay, not the older physical row.

Point command, outcome, provenance, commit, and linked-audit resolution use a
common predecessor lookup that understands legacy commits, V1 capsules, V2
capsules, and bounded segments. Startup expands segments once into bounded
command and audit caches instead of repeatedly decoding a segment for each
logical member.

Operational readiness also rebuilds an exact in-memory directory from every
validated segment manifest. Publication installs a segment and its derived
keys only after the durability fence publishes the matching read frontier.
Duplicate manifest keys or missing segment members invalidate the accelerator;
they never become a negative lookup result. Successful commands on the new path
write no standalone idempotency, provenance, command-audit, audit-by-request,
event, event-route, or outbox-intent row. Historical per-command rows retain
their existing durable lookup path until the offline migration rewrites them.

Pipelined durability epochs keep a bounded unpublished exact-index overlay.
Later writers consult it so a retry cannot execute twice after the owning redb
root is sealed but before its journal fence is published. Operational readers
never consult that overlay: they see only the directory paired with the
published durability frontier.

The local durability journal counts logical commands inside a segment rather
than equating command count with physical `COMMITS` puts. A frame still proves
the exact contiguous logical sequence interval. This is required for grouped
standard-profile writes: one segment put may advance the command frontier by
as many as 256 commands.

## Safety checks

The transition is covered by the complete redb suite, including process-kill
journal recovery, response-loss replay, retention, backup/restore, validated
prefix recovery, and corruption findings. Retention materializes every cold
view that must survive the watermark, removes complete eligible segments, and
canonically splits and re-chains a segment when the watermark falls inside it.

`COMMITS` point reads, scans, logical-head discovery, backup facts, startup, and
journal recovery now understand one physical row representing many logical
commands. The remaining format-transition limitation is the offline rewrite of
historical ADR-0099 capsules; retained legacy rows remain readable and are not
silently treated as migrated.

## Performance checkpoint

The pre-consolidation checkpoint was about 197 ms for the 404-command smoke
TicketDesk seed and 181 ms for its dead-peer seed at concurrency 32.

The larger full TicketDesk seed contains 19,220 symbolic application commands.
After command-owned rows were consolidated, one same-host three-repetition run
still averaged 3.399 seconds. Profiling showed that a later pipelined writer
could resolve the prior unpublished segment digest from the exact overlay but
could not resolve its terminal audit there. Audit-tail validation therefore
decoded the complete prior segment once per writer group.

The unpublished overlay now serves audit-sequence and audit-request lookups as
well as command identity and segment-tail lookups. The redb fallback remains
for dormant or legacy paths, but a normal pipelined writer no longer reparses a
prior segment. A three-repetition full run then averaged 2.831 seconds, or about
6,790 commands per second. The individual runs remained below 2.85 seconds.
This is a 16.7% elapsed-time reduction from the immediately preceding
single-owner/single-pass segment build and about a 32% reduction from the
roughly 4.16-second pre-optimization checkpoint.

Artifact:
`target/app-baseline/wp479-unpublished-audit-tail-full-3rep.json`.

These are local development measurements, not release evidence. The WP-479 exit
gate still requires the same-run tri-backend sweep after segment migration,
recovery, and retention coverage are complete.
