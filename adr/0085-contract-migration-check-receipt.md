# ADR-0085: Contract Migration Check Receipt

- **Status:** Accepted
- **Date:** 2026-08-01
- **Decision owners:** RiffDB maintainers
- **Acceptance reference:** Human maintainer approval in the WP-409 implementation session on 2026-08-01
- **Depends on:** ADR-0076, ADR-0078, ADR-0079
- **Amends:** ADR-0079

## Context

ADR-0079 requires `CheckContractMigration` to run one complete read-only
preflight under a caller-stable operation identity and to use the external
migration receipt for audit and uncertainty recovery. The receipt introduced
by WP-408 represents only an apply operation: its legal path begins
`Accepted -> Draining -> Preflight` and a successful preflight must continue to
backup publication. It cannot represent a successful check without either
performing forbidden apply work or inventing a failure.

The existing apply receipt is already a versioned durable format with checked
compatibility fixtures. Its meaning and canonical bytes must remain stable.

## Decision

The V1 external contract-migration receipt gains a closed operation kind:
`Check` or `Apply`. Durable omission of the new kind means `Apply`, preserving
the meaning and canonical bytes of every WP-408 receipt. Encoders continue to
omit the kind for `Apply` and encode it only for `Check`. Unknown encoded kinds
fail closed.

The operation kind selects one exact transition graph:

- `Check`: `Accepted -> Preflight -> Succeeded`, with `FailedClosed` permitted
  from `Accepted` or `Preflight`;
- `Apply`: the unchanged WP-408 graph beginning
  `Accepted -> Draining -> Preflight -> BackupPublished` and continuing through
  staging, publication, validation, success, or rollback.

A check receipt cannot contain backup identity, stage identity, transforming or
publication phases, or an apply confirmation. A check never drains the
database, creates a backup, creates a stage, publishes state, allocates an
administration sequence, or becomes `ServiceOperationV1::ApplyContractMigration`.
Its successful terminal `Succeeded` phase means only that complete read-only
preflight accepted the exact bound artifacts against the observed predecessor.

Public migration observations include the closed operation kind. Start retries
and observation authorize the exact operation as required by ADR-0079. The
canonical input hash binds the operation kind, so one operation ID cannot be
replayed between check and apply. Unknown public operation kinds fail closed.

## Consequences

- WP-409 can expose durable, retryable checks without weakening the apply
  receipt or creating a second receipt subsystem.
- Existing apply receipt compatibility vectors remain byte-identical.
- Check receipt vectors and operation-specific invalid-transition tests become
  required fixtures.
- Recovery dispatches by checked operation kind; it never resumes a check in
  the apply driver.

## Compatibility

The durable field is additive and optional. Absence has the sole legacy meaning
`Apply`. Existing apply encoders preserve omission, so old canonical bytes and
hashes do not change. Decoders reject unknown values and any receipt whose
operation-specific transition graph or attached evidence is invalid.

## Security

Separating the transition graphs prevents a read-only check request from
crossing the drain, backup, stage, transform, or publication boundaries. Kind
is covered by operation identity hashing and returned only after current
authorization and redaction.

## Testing

- Preserve every existing apply receipt golden byte-for-byte.
- Round-trip check receipts and reject unknown kinds.
- Reject cross-kind replay under one operation ID and input hash.
- Reject every apply-only phase or artifact on a check receipt.
- Prove a successful public check reaches `Succeeded` without drain, backup,
  stage, mutation, or administration sequence.

## Requirements and Work Packages

- **Requirements:** `MIG-006`, `MIG-008`, `MIG-009`, `MIG-010`, `MIG-017`
- **Defines or blocks:** `WP-409`
- **Final evidence:** `WP-413`
