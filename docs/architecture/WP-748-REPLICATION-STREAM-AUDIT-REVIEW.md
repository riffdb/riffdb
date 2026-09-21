# WP-748 — administrative replication stream audit

Status: accepted by the maintainer in this Codex session, 2026-09-18.
Package: WP-748. Tier: guarantee. Restored authority commit: `a5b9cfa6`.

Acceptance: "Approve exact revised amendment", explicitly superseding the earlier
draft's extra late-denial audit row. The exact revised text below is incorporated
in SPEC §13.5, ADR-0007, ADR-0021 and ADR-0178. Implementation and activation
remain pending their required evidence.

## Demonstrated gap

SPEC §13.5 requires durable service audit for administrative reads and explicit
policy denials after authenticated context. ADR-0178 Decision 3 defines the
administrative `StreamChangelog` RPC and its `ReplicateChangelog` permission.
The current replication service checks authority and emits denial telemetry, but
has no service-audit port or operation identity. This also prevents properly
auditing the accepted source fence-proof phase before promotion.

`replication_stream_has_its_own_auditable_service_operation` fails because the
closed operation registry has no `StreamChangelogRequest` declaration. The
shared service-operation inventory ends at `0x3c` and explicitly rejects `0x3d`.
This is registry and composition evidence, not a claimed runtime audit exploit.
The failing log is `/tmp/wp748-replication-audit-identity-red.log`; its reproducible
patch is `/tmp/wp748-replication-audit-identity-red.patch`. The proposed test is
kept outside the active suite until the accepted identity is implemented.

Adding a durable service-operation tag requires review under ADR-0007 and D-003;
this proposal does not treat D-004 as permission to change an already-used format.
The accepted follower-only telemetry exception does not exempt a primary source.

## Exact accepted amendment

> Add `ServiceOperationV1::StreamChangelog` at unused tag `0x3d` to ADR-0007's
> closed service-operation inventory. It identifies the existing single
> `ReplicationService.StreamChangelog` RPC, including its bootstrap, attachment,
> follower acknowledgement, tail and accepted fence-evidence phases. Classify it
> as an intrinsically audited administrative stream with operator-protected
> output. It uses the existing administrative `ReplicateChangelog` permission
> and its current global/all-partition, no-application-role and approval checks.
> No new permission, application operation, MCP tool or separate RPC is added.
>
> The shared API-neutral service owns authenticated context, current-policy
> decisions and audit sequencing. Durable audit goes through the existing sole
> coordinator and service-audit executor. An explicit initial policy denial
> appends standalone `Denied` before returning that denial. An allowed
> establishment appends `Started` before opening the source or changing source
> retention custody. After constructing a bounded, policy-filtered stream
> handle, append `Succeeded` before releasing that handle, including for an
> empty stream. Cancellation, known failure and
> uncertainty use the existing exact phase meanings; a dropped transport future
> cannot turn a possibly committed source-control action into proven cancellation.
> Required audit failure withholds data and returns the existing unavailable
> outcome. There is no unaudited success fallback.
>
> Audit establishment once, not every replicated frame or bootstrap page.
> Continue current-policy checks before and after source admission, waits and
> framing. Preserve ADR-0007 and SPEC §13.5 establishment-only audit: later
> policy denial or failure closes the stream and emits bounded redacted security
> telemetry; it does not append a second terminal record or start another audit
> invocation. The fused stream releases no further items. This creates no
> per-frame audit feedback loop and does not relax freshness, cancellation or
> bounded admission.
>
> Apply ADR-0021's existing request-selected target rule: an explicitly selected
> follower uses the existing lineage-scoped `ReplicationFollower` target; a plain
> tail request uses an empty target list. Derive any attachment target from the
> bounded, checked request manifest before `Started`, never from returned rows
> or source observations. Every record for the invocation retains the same
> checked targets and authenticated principal/capability reference. Result links
> are `None`: source custody or evidence reads do not claim an application commit
> or a fence-administration mutation. No credential, history hash, receipt bytes,
> cursor, business value, network address or transport object enters audit.
>
> Fence evidence travels as one bounded terminal phase/item on the existing
> `StreamChangelog` exchange. It binds the exact selected operation, source
> lineage, follower registration/generation and drained applied position under
> the already accepted promotion-fencing rules. Plain record bytes remain
> unauthenticated evidence. Only the configured exact-CA/name TLS peer path can
> supply authenticated source proof to the promotion owner. The existing
> 1024-byte request and 32 MiB + 1024-byte stream-item ceilings remain; the fence
> evidence item has an additional 2048-byte ceiling and emits once before EOF.
>
> Preserve all existing service-operation tags, record/envelope bytes and their
> meanings. The additive tag is carried by existing versioned service-audit
> records with their existing target codec selection and compatibility rules;
> register it in the policy, operation, wire, telemetry and compatibility
> inventories before activation. Older readers must refuse the unknown tag.
> No metadata domain, frame identity, cryptography or migration is added.
> This amendment does not authorize a follower service-audit writer or change
> its already accepted local telemetry exceptions. External promotion-attempt
> audit and offline cutover remain separately required by the accepted amendment.

## Required evidence before activation or closure

- Real source establishment and denials retain exact operation, principal,
  request-selected targets and closed phase records in both storage profiles.
- No source access/custody change precedes required Started durability; no
  stream handle, protected item or successful empty stream precedes required
  Succeeded durability.
- Revocation and establishment-audit outage at deterministic waits refuse release;
  later denials close established streams with bounded redacted telemetry, no
  further durable audit rows and no further items.
- Cancellation/unknown-outcome tests preserve phase meanings and source custody;
  source crash/reopen retains durable audit without reopening primary admission.
- Real configured TLS accepts only exact source/candidate evidence and refuses
  substituted identities, invalid trust, stale authority and unavailable proof.
- Existing tag/encoding fixtures remain byte-exact; generator, scope, package
  acceptance and full merge checks pass; operator and compatibility docs update.

## Interface safety

Applications gain no authority, bypass, proof constructor or raw writer. This
repairs missing audit coverage on an existing operator surface. Audit failure
reduces availability; it never permits data release or promotion. Exact acceptance
is recorded in authority-only commit `f6bed7aa`; the tag and wire phase remain
inactive until implementation and its required evidence are complete.
