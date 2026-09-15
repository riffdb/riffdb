# WP-746 follower audit boundary — accepted amendment

Status: accepted 2026-09-15. The maintainer approved the exact text below in
session: "I approve of these changes". ADR-0178 and SPEC §13.5 carry the amendment.

## Conflict found during service composition

SPEC §13.5 requires every explicit authenticated policy denial, including a
standard-read denial, to append a standalone durable audit record. It also
requires durable Started/terminal audit for administrative reads and for
policy-obligated standard reads. `GetStatistics` is an administrative read.

ADR-0178 §2 and REP-002 make the follower applier the sole writer. ADR-0186
classifies service audit and the administration allocator as replicated
authority: locally generated follower audit records would break prefix equality.
ADR-0178 §4 nevertheless requires follower reads and replication health/statistics.
The current shared service always holds primary audit/command executors, so it
cannot simply be installed behind the follower applier.

No accepted decision defines an upstream audit-submission protocol or a separate
durable follower audit log. Adding either would require more protocol, authority,
identity, durability and recovery decisions. Silently dropping required audit,
or deliberately treating every denied follower read as a broken audit subsystem,
would not be a sound implementation of the existing decisions.

## Accepted exact amendment to ADR-0178 §2 and §4, and SPEC §13.5

> **Follower service and audit boundary.** Follower service composition carries
> no command, control-plane, or service-audit writer executor. The follower
> applier remains the sole database writer. Application commands, including
> read-only command invocation and outcome resolution, capability changes,
> contract/query/reactive-module deployment, consumer lease or cursor mutations,
> installation/reimport operations, migration, maintenance, and every export
> operation return the typed follower-mode refusal before execution or durable
> audit admission. No follower refusal allocates an application or administration
> sequence or creates a locally originated authoritative record.
>
> A follower may serve ordinary compiled, projected, catalog, and discovery reads
> under the existing current authorization, redaction, bounds and freshness rules.
> If a standard read's current policy decision requires durable audit, the
> follower refuses it with the typed follower-mode outcome before releasing data;
> it never removes that obligation or reports audited success. Intrinsically
> audited administrative reads are likewise refused, except read-only Health and
> Statistics, which may disclose only their existing authorized operational
> results, including the follower role, applied frontier and replication lag.
>
> Follower Health/Statistics and explicit authenticated denials of follower reads
> use bounded redacted operational telemetry, without a local durable service
> audit append. An authorization denial remains an authorization denial and
> never releases protected data. These are explicit follower-only exceptions to
> SPEC §13.5; a follower supplies no durable local audit guarantee for them. The
> operator handbook documents this limitation. Applications requiring durable
> audit for reads or denials must use the primary. Primary audit behavior and
> all replicated audit bytes remain unchanged.
>
> A follower's telemetry is non-authoritative, is never replication or recovery
> evidence, and cannot satisfy a policy-required durable audit obligation. No new
> durable format, local audit allocator, NodeId, RPC or privilege bypass is added.

## Acceptance mechanics

The exact text above is recorded in ADR-0178 and SPEC §13.5. The maintainer's
in-session acceptance and applicable WP-746/WP-747 deliverables were committed
separately from implementation in `1a9ce841`. The generated ADR index was checked
and was already current.

## Required implementation evidence

- A follower service can be built without any primary writer executor.
- Every write/maintenance/export family returns RDB-REP before its future or
  mutation provider runs; source-side canary audit/command counts remain zero.
- Allowed follower reads still use current policy, redaction and freshness.
- A policy-obligated read refuses with no released data and no claimed audit.
- Repeated denied follower reads stay denied without changing database bytes or
  incorrectly fencing the applier as a failed primary audit coordinator.
- Follower Health/Statistics disclose only the existing authorized operational
  fields; primary durable audit tests remain unchanged and pass.

## Tradeoff

This proposal weakens durable audit coverage specifically for follower operational
reads and denied reads. It keeps the exact-prefix and sole-writer guarantees and
avoids inventing a second audit durability system. If durable audit of follower
denials is required, this proposal must be rejected and a separate durable audit
ownership/transport design accepted before follower read serving is enabled.
