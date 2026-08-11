# ADR-0114: Row-Policy Capability Successor and Current-Fact Binding

- **Status:** Accepted
- **Direction requested:** 2026-08-11
- **Exact text accepted:** 2026-08-11
- **Decision deadline:** Before WP-572 changes a durable capability record or
  enables any protected application operation
- **Requires:** ADR-0006, ADR-0007, ADR-0009, ADR-0055, ADR-0089, ADR-0111,
  and ADR-0112
- **Amends:** ADR-0089's closed capability successor chain and
  ADR-0110's installation-only `CapabilityRecordV3`
- **Defines or blocks:** WP-572, WP-573, and WP-579

## Context

WP-570 froze row-policy grammar, executable IR, application-role identity, and
bounded capability fact values. Its fail-closed rollout deliberately withholds
ordinary query, command, and reactive permissions from roles touching a
protected entity. The first WP-572 increment now provides one shared pure
evaluator and move-only proof bound to exact capability revision and current /
successor row hashes.

Protected execution still cannot be enabled safely because current durable
capabilities retain neither the role's exact selected policy bindings nor the
principal fact set. `CapabilityRecordV1`, `CapabilityGrantV1`, migration-only
`CapabilityRecordV2`, and installation-capable `CapabilityRecordV3` are
schema-hash-bound durable formats. Adding a field to any of them would rotate
its descriptor hash and make previously valid database rows unreadable.
ADR-0089 established the correct pattern: preserve the frozen record and
introduce an own-file successor containing explicit extensions. The
implementation briefly violated that rule by appending installation directly
to V2; the WP-568 compatibility correction restored V2 byte-for-byte and moved
installation authority to V3 before this decision was accepted.

## Decision

### A distinct V4 durable record

Keep `CapabilityRecordV1`, `CapabilityGrantV1`, the V1 permission enum,
ADR-0089's migration extension and `CapabilityRecordV2`, and ADR-0110's
installation extension and `CapabilityRecordV3` byte-for-byte unchanged. Add
one own-file schema with these exact semantic shapes:

```protobuf
enum CapabilityRowPolicyOperationV1 {
  CAPABILITY_ROW_POLICY_OPERATION_UNSPECIFIED = 0;
  CAPABILITY_ROW_POLICY_OPERATION_READ = 1;
  CAPABILITY_ROW_POLICY_OPERATION_CREATE = 2;
  CAPABILITY_ROW_POLICY_OPERATION_UPDATE = 3;
  CAPABILITY_ROW_POLICY_OPERATION_DELETE = 4;
}

message CapabilityRowPolicyBindingV1 {
  string contract_lineage = 1;
  string policy_name = 2;
  uint32 entity_type_id = 3;
  repeated CapabilityRowPolicyOperationV1 operations = 4 [packed = true];
}

message CapabilityRowPolicyGrantExtensionV1 {
  bytes application_role_hash = 1;
  bytes canonical_principal_facts = 2;
  repeated CapabilityRowPolicyBindingV1 policies = 3;
}

message CapabilityRecordV4 {
  CapabilityRecordV1 base = 1;
  CapabilityMigrationGrantExtensionV1 migration = 2;
  CapabilityInstallationGrantExtensionV1 installation = 3;
  CapabilityRowPolicyGrantExtensionV1 row_policy = 4;
}
```

The new definitions live outside the frozen V1, V2, and V3 source files. V4
requires `base` and `row_policy`. Migration and installation remain optional
and retain their exact ADR-0089 and ADR-0110 meanings. A V4 record with a
missing or empty row-policy
extension is corrupt; a capability without row-policy authority continues to
encode as the least applicable V1, V2, or V3 record.

### Canonical extension semantics

The row-policy extension has exactly one 32-byte application-role hash, one
complete nonempty versioned `CapabilityPrincipalFactsV1` document (the empty
fact *set* has a valid nonempty canonical document), and one through 1,024
policy bindings. Bindings are ordered by lineage canonical bytes, entity type
ID, then policy-name bytes. Duplicate lineage/entity bindings are forbidden,
matching role compilation's one selected policy per protected entity.

Each binding has one through four strictly increasing operation tags. The
policy name is a checked contract symbol. The entity and operations must match
the exact policy in the exact contract bundle selected for execution. Unknown
tags, missing facts, extra facts, wrong fact types, noncanonical fact bytes,
unknown policy names, entity mismatch, missing operation coverage, and any
ordering or bound error fail closed.

The base grant must contain exactly one matching
`ApplicationRoleIdentity(application_role_hash)` permission. V4 decoding
rejects no role identity, multiple role identities, or a different role hash.
The normalized in-memory grant may expose the extension only to trusted auth,
policy, service, and commit owners; it is never a public request predicate or a
field mask.

The complete extension participates in the existing one-MiB capability
semantic bound, equality, create/replay identity, redacted audit summary, and
transaction-current revision. Fact and policy values never enter logs,
diagnostics, metrics, cursor bytes, or generated application calls.

### Issuance and delegation

Role compilation emits the exact policy bindings and fact schemas. Role bind /
application provisioning accepts fact **values** only through a bounded typed
operator input checked against those schemas; ordinary operation requests can
never supply or override facts. A protected role cannot be provisioned without
one complete valid fact set, including an explicit empty set when it declares
no facts.

Delegation may drop policy bindings or operation tags and may narrow principal
facts according to `CapabilityPrincipalFactsV1::is_narrowing_of`. It cannot add
a binding, change a selected policy, add an operation, add/change a scalar fact,
or widen a list fact. Existing tenant, partition, permission, audience,
lifetime, approval, and field-visibility subset checks remain conjunctive.

Because WP-570 never enabled protected application permissions, no existing
V1/V2/V3 protected capability has executable authority to migrate in place.
Rebinding a protected role issues a new V4 capability and retires the prior
non-executable binding through the existing receipted credential-rotation path.
There is no startup rewrite of V1/V2/V3 records.

`principal.id` remains the WP-570 UUID operand. A selected policy that refers to
it can be bound only to an `ActorId` containing canonical lowercase UUID text;
binding rejects any other actor ID before capability creation. Policies that do
not reference `principal.id` retain the existing bounded `ActorId` space.

### Transaction-current use

Authentication reconstructs one `PrincipalFactBindingV1` only from the current
stored capability record. The shared service resolves the policy selection from
the exact application role and contract identity before lower access. Query,
projection, search, reactive, export, bulk, workflow, scheduler, MCP, and agent
paths receive no caller-selected policy object.

Before protected release and inside the authoritative write transaction, the
policy/commit verifier reloads the exact capability ID and revision and compares
the role hash, canonical facts, selected policy binding, current relationship
evidence, and current/successor row hashes. Any change denies with no partial
output or mutation. A cached plan may retain policy identity and required field
union, but never a principal-specific allow decision or fact value.

## Compatibility

V1, V2, and V3 sources, descriptors, schema hashes, payload goldens, envelope
bytes, readable/writable registry entries, and behavior remain exact. V4
receives its own FQN, descriptor closure, schema hash, payload/envelope goldens,
conservative bound, and durable-format manifest entry. Older binaries refuse V4
as an unknown registered tuple without mutating the database. Current binaries
read V1, V2, V3, and V4 and write the least version capable of representing the
grant.

Backup, restore, export receipts, and alpha format manifests name V4 support.
No automatic down-conversion exists because removing the row-policy extension
would remove authority facts while leaving operation permissions ambiguous.

## Security

Default is deny. Possessing a role hash, policy name, fact document, or prior
proof is not authority. Every protected safe point requires the current active
capability and exact revision. Public errors may name the authorized role,
policy, entity, operation, and missing fact schema, but never fact values,
policy branches, hidden-row existence, or raw durable bytes.

The format introduces no middleware policy, request predicate, storage bypass,
generic transaction, or broader administrator mode. The existing administrative
backup authority remains separate from principal-filtered application export.

## Standing Design Tests

- **Interface safety:** generated applications supply only ordinary operation
  values. Policy selection and facts come from one exact durable role binding,
  and no public request has a skip, predicate, fact, or bytecode channel.
- **Scale:** at most 1,024 local policy bindings, 32 facts, 64 members per fact,
  64 KiB fact bytes, and one MiB complete capability semantics. Evaluation is
  finite, partition-local, and index-backed.

## Testing

- Freeze V1/V2/V3 source, descriptor, schema-hash, payload, envelope, and
  registry literals before adding V4.
- Golden V4 fixtures cover policy-only, policy plus migration, policy plus
  installation, and all three extensions.
- Reject every missing, empty, duplicate, unordered, unknown, mismatched,
  over-bound, noncanonical, wrong-role, wrong-entity, and wrong-operation case.
- Prove create/replay/revoke, backup/restore, startup validation, and delegation
  preserve exact facts and policy bindings without formatting values.
- Race capability revision/fact/policy/relationship/current-row/successor-row
  changes between evaluation and release/commit and prove no output/mutation.
- Rebind a WP-570 non-executable protected role into V4 and prove the old
  credential remains unable to execute protected operations.

## Requirements and Work Packages

- **Requirements:** `SEC-001` through `SEC-003`, `RAP-005`, `RAP-008` through
  `RAP-014`, `AFC-001` through `AFC-004`
- **Defines or blocks:** WP-572, WP-573, and WP-579
- **Final evidence:** WP-573 and WP-579

## Acceptance

The human maintainer accepted this exact revised V4 text on 2026-08-11 after
the frozen V2 correction assigned installation authority to V3. WP-572 may now
add the V4 Protobuf, durable registry tuple, normalized grant extension,
capability issuance input, and protected-operation enforcement without changing
V1, V2, or V3.
