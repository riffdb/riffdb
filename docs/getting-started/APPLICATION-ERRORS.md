# Application errors

RiffDB application operations fail with one versioned semantic error object.
The object is the same checked domain value on gRPC, Rust, TypeScript, CLI, and
MCP. Presentation casing may follow the host language, but codes, categories,
recovery actions, fixes, operation identity, and optional context have identical
meaning.

Application code should branch on `code`, `recovery_action`, or `fixes`. It
must not parse the human message.

```json
{
  "type": "application",
  "code": "RDB-AUTH-0214",
  "message": "application operation is not authorized",
  "category": "authorization",
  "recovery_action": "obtain_permission",
  "operation": "ExecuteQuery",
  "contract_lineage": "ticketdesk",
  "contract_version": "18",
  "operation_symbol": "TicketPage",
  "fixes": ["bind_application_role"],
  "trace_id": "019bf6aa-a640-7de6-89c9-8a7f70bbbd23"
}
```

The message and remediation text are selected from a closed registry. The
complete v1 registry is frozen in
`fixtures/application-errors/registry-v1.tsv`.

`RDB-AUTH-0215` means the presented credential reciprocally matched a retained
capability that has been irreversibly revoked. Generated clients can therefore
distinguish replacement/role-rebinding work from an ordinary `RDB-AUTH-0214`
policy denial. Unknown, malformed, expired, or boundary-mismatched credentials
remain the generic authentication/authorization class; the revoked code is not
an oracle for credentials that did not establish an exact retained match.

Exact result sets and projected vector reads use four projection codes. `RDB-PROJECTION-0101`
reports that the declared providers cannot prove one common epoch;
`RDB-PROJECTION-0102` reports that a requested snapshot has retired; and
`RDB-PROJECTION-0103` reports that no retained snapshot satisfies the requested
freshness floor. `RDB-PROJECTION-0104` reports that a retained nearest-query
artifact predates the required compiler-owned projected source; regenerate,
deploy, and pin the current query module. The first and third permit a bounded retry. The retired-
snapshot response requires a new first-page/current-snapshot request. None of
these codes authorizes client-side filtering, page walking, or weaker
consistency.

## Safety boundary

An application error may contain:

- the closed operation, code, category, recovery action, and fix codes;
- exact contract lineage and version already visible to the caller;
- a query, command, module, or role symbol the caller submitted or was
  authorized to discover;
- a bounded path of similarly visible symbols;
- a bounded source span into source the caller submitted;
- the request ID as an opaque trace ID; and
- an opaque incident ID for operator correlation.

It can never contain submitted application values, credentials, principals,
tenant secrets, raw entity/field/index IDs, capability masks, storage errors,
server source locations, arbitrary server prose, or a symbol inferred from a
context-free infrastructure failure.

This restriction is intentional. For example, a denial of `TicketPage` may
name `TicketPage` because the caller invoked it. It may name
`Ticket.description` only when that symbol is already safe to release after
authorization filtering. RiffDB omits unavailable context rather than
fabricating a more detailed but potentially disclosive explanation.

## Wire compatibility

`riffdb.app.v1` RPCs carry `riffdb.app.v1.ApplicationError` directly as their
bounded gRPC details payload. Application clients reject a kernel error payload,
unknown tag, unknown enum, inconsistent redundant field, oversized context,
status-code mismatch, or message mismatch.

Kernel and administrative RPCs continue to carry the existing
`riffdb.v1.PublicError` bytes unchanged. This preserves low-level protocol
compatibility while making the normal application surface symbolic and
actionable.

The sole exception is emergency internal containment when the server cannot
obtain a real incident ID. That response remains the accepted details-free
internal error; RiffDB never invents a trace or incident identity.

## Retry behavior

Recovery actions are guidance constrained by the operation:

- `correct_request`: change the rejected input before another submission;
- `retry`: use an explicit bounded caller retry policy;
- `resolve_with_same_idempotency_key`: never invent a new mutation identity;
- `obtain_permission`: bind the declared symbolic application role;
- `refresh_contract`: refresh exact contract/module metadata;
- `contact_operator`: report only the opaque trace and incident IDs; and
- `none`: do not automatically resubmit.

Generated command helpers retain RiffDB's uncertainty rules. Translating a
kernel command failure into application context does not make an unsafe retry
safe.
