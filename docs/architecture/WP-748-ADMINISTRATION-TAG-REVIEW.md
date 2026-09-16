# WP-748 — replication administration tag collision

Status: proposed; exact maintainer acceptance required before implementation.
Package: WP-748. Tier: guarantee.

## Conflict

The exact registration-audit amendment accepted in `05d2938b` assigns
`StoredReplicationAdministrationV1` durable tag 74, revision 1. Main commit
`60509ee2` assigns that same identity to
`StoredApplicationExportPageCommitmentV1`. Two different record types cannot
share one durable tag/revision identity. The replication administration record
has not been implemented or written. The existing export identity must retain
its bytes and meaning.

## Exact proposed amendment

> In the accepted WP-748 registration-audit amendment in SPEC §13.5,
> incorporated by ADR-0021 and ADR-0178, replace the durable identity of
> `StoredReplicationAdministrationV1` from tag 74, revision 1 to tag 75,
> revision 1. Reserve tag 74, revision 1 for the already implemented
> `StoredApplicationExportPageCommitmentV1`. All other text, fields, actions,
> authority boundaries, atomicity, retry, expiry, audit and compatibility
> requirements remain unchanged. No existing record bytes are reinterpreted.

## Interface safety and acceptance

This correction introduces no public operation or guarantee change. It assigns
a distinct unused durable tag to the already accepted record before its first
writer exists. V3 service audit remains tag 22, revision 3.

AGENTS.md's authoritative-file conflict rule and standing directive D-003
require exact human acceptance because the accepted decision explicitly names
tag 74. After acceptance, record the correction in a standalone authority-only
commit before implementing the replication administration codec.
