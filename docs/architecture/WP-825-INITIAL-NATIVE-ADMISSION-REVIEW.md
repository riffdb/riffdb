# WP-825: Initial native dependency admission review

Status: ADR-0323, SPEC section 13.5 and the package registration were accepted
in session on 2026-10-16. The implementation passes the original service
replay regression, all 10 service admission checks, the server bridge check,
13 native admission/cancellation checks, and scoped Clippy on the required host.
The final native check receipt is
`549c58eba096e2ebc88822ea99574ee17af9b4a1a9d9e8b7227e8a9b4ec01fc9`.
The unchanged public restore gate and complete source qualification remain
required; this note claims no successful rehearsal.

## Problem and authority

Bounded restored startup can reach Ready with a Pending exact native locator.
The first original acknowledged-command replay then returns typed storage
unavailable. Its required terminal audit needs the same locator generation.
Once accepted by the coordinator, an audit storage error correctly stops or
fences authoritative service; ignoring that error would weaken durable audit.
The observed runtime shutdown also overlaps diagnostic cleanup, so a deterministic
regression establishes the admission failure independently of that timing.
On the required host, the unchanged service submits one required audit request
while the fixture dependency is Pending; the new assertion expects zero and
fails at exactly that boundary. The fixture preserves a real committed command
and uses the existing test-only audit actor simulator to isolate this phase;
it does not claim native durability or spontaneous shutdown evidence.
The real public restore remains the native failure evidence.
Baseline receipt:
`e4593edc29134feb5d22826329e265919b830cce3a765fe327e1cec7307760ae`.

SPEC section 13.5 and ADR-0007 require audit for structurally valid authenticated
mutations and administrative reads, including prestart cancellation. ADR-0071's
unaudited pre-admission exception covers capacity only. ADR-0319 permits Pending
refusal but grants no service-audit exception. The proposal explicitly amends
these boundaries; it is not an implementation deviation.

## Exact proposal

Read [ADR-0323](../../adr/0323-request-scoped-native-dependency-admission-before-durable-audit.md)
with the [SPEC section 13.5](../../SPEC.md) and [WP-825 registration](../../work_packages.yaml).

- Every ordinary authenticated request entering the live-source writer-service
  composition uses initial native dependency admission before operation work,
  idempotency, audit input or coordinator submission. This includes standard
  reads and authenticated health because denial or policy obligations can
  require their first audit. No caller or operation-name bypass is introduced.
- Only a request whose initial phase identified Pending may return the existing
  Cancelled, DeadlineExceeded or Unavailable result without audit, and only
  before any operation/audit/coordinator work was accepted. Complete-source
  requests admit immediately and retain their ordinary prestart Cancelled audit.
  Invalid or corrupt proof retains existing systemic failure behavior.
- Waiting uses the original RequestControl absolute deadline and cancellation,
  holds no writer lease, actor or audit permit, and retains charged bounded
  waiter/source ownership. It performs no population scan or operation retry.
- Current-source completeness and fresh policy are required after the wait.
  An older completed root cannot authorize another source or prove absence.
  All post-admission audit, stop/fence, atomicity and uncertainty rules remain.
- Bounded Ready, native metadata-only ports, pre-bootstrap health, recovery-only
  access and follower/archive/static authority retain their existing boundaries.
  Ordinary authenticated health can wait at the same initial gate. No budget,
  deadline, 45/60-second gate, harness retry or readiness workload is increased.

## Registration and validation

The package-owned requirement SEC-005 records the new phase and narrow audit
exception. WP-825 additionally binds API-001 and SEC-001, retains REC-001 and
TXN-042, and requires ADR-0007, ADR-0071 and ADR-0323 alongside ADR-0319.
Its scope gains SPEC.md, work_packages.yaml, adr/README.md, the new ADR,
tests/service/** and crates/riffdb-service/Cargo.toml. These last two paths own
the genuine service regression and its existing test-only commit simulator;
this proposal does not activate new production semantics through that feature.
The package remains open and its rehearsal gates remain unchanged.

No durable format, protobuf field, frozen fixture, critical dependency or
application input changes are proposed. Front-matter obligations remains empty;
register only actual existing test symbols after they are written. Planned
checks in the ADR are not proof claims.

Required deterministic checks cover the original first replay while the actual
builder is paused Pending, zero accepted audit/coordinator work and a free
mutation gate while waiting, exact-source completion, cancellation/deadline
release, completion-before-subscription and source-change schedules, fresh
policy denial/revocation, invalid completion, complete-source prestart audit,
and unchanged genuine accepted-audit failure/unknown-commit stop/fence.
Use hooks/barriers rather than sleeps. Then rerun the original public restore
and first replay before broader acceptance, CI or rehearsal.

The implementation PR must update the reachable startup-integrity handbook
page and known limitations in the same change. Normal native startup may be
Ready while a request waits; documentation must distinguish this admission
state from an accepted audit/storage failure and disclose the narrow unaudited
initial Pending refusal.

## Why an audit-only Pending successor is insufficient

A service-audit append preserves command membership, so a checked native net
mutation plan could publish another Pending source without inventing a complete
directory. That publication property does not prove audit lifecycle validity.
[service_audit_sequences_for](../../crates/riffdb-storage-redb/src/store.rs)
merges bounded physical `audit_by_request` rows with exact command-owned
request/audit sequences from the captured generation and private epoch deltas.
Command Started/terminal records can exist only inside canonical command
capsules; absence from the physical request table cannot prove request absence.
Skipping the command-owned merge could admit a duplicate Started or terminal.

The [administration tail fallback](../../crates/riffdb-storage-redb/src/administration.rs)
uses the known final application sequence to inspect only its owning command
segment. It cannot exclude an arbitrary request ID from an older retained
capsule. A successful command-result link also validates its provenance member
and reciprocal command identity. A known commit sequence can bound a positive
lookup but cannot supply the missing request-wide negative proof.

The existing physical request-index migration backfills physical AUDIT rows;
it does not materialize all command-owned capsule entries. Request-ID freshness,
current-epoch absence or an incomplete directory is not that durable proof.
A whole-history fallback per audit would violate the bounded point-read design;
a new persisted index or independently complete audit-only capability would
require another architecture decision and its own lifecycle/accounting proof.
Consequently, exempting audit from the Pending guard or preserving a Pending
metadata successor alone is not a correctness-preserving native-only fix.

## Current native restore diagnostic

The ordinary shipping build reaches readiness in 16.469 seconds on the saved
65,536-ticket, 100-update population; the immediate original acknowledgement
replay remains the native Pending admission failure. A separate debug build of
the same source confirms actual public terminal restore success and readiness
in 97.271 seconds, without reproducing the earlier debug-only refusal. Its
original backup, archive and controls remain unchanged. This debug observation
uses private numeric diagnostics, is not shipping timing qualification and
proves no first replay or 45/60-second gate. Preserve the historical refusal;
this observation does not establish its sole cause.
Diagnostic receipt:
`d49cf11c819145ae2e92e4ee3600c314488eeb9dcf2c74558c724fd7b6dd6d1d`.
