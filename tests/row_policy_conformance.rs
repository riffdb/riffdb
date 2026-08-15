#![forbid(unsafe_code)]

//! Cross-operation differential acceptance for the shared row-policy evaluator.

use std::num::{NonZeroU16, NonZeroU64};
use std::path::PathBuf;

use riffdb_auth::PrincipalFactBindingV1;
use riffdb_catalog::validate_catalog_history;
use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{ContractBundle, RowPolicyOperationV1, RowPolicyPlanV1};
use riffdb_policy::{
    AuthorizationClock, AuthorizationClockError, AuthorizedQueryRowPolicyContextV1,
    CurrentAuthorizer, Decision, EventConsumerOperationTarget, EventPolicyCandidateV1,
    EventPolicyReleaseDecisionV1, IndexedRelationshipEvidenceV1, NoopAuthorizationTelemetry,
    OperationRequest, OperationTenantScope, RowPolicyDecisionV1, evaluate_row_policy,
    evaluate_row_transition, resolve_authorized_event_row_policy_context,
    revalidate_event_release_proof, revalidate_row_policy_proof,
};
use riffdb_storage_api::{
    DatabaseInitializationPort, DatabaseInitializationResult, EventConsumerIdentityV1,
    EvidencePageLimit, ReadableCapabilityDigestInventory, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, StartupValidationInputs, StructuralEvidenceCursor,
    StructuralEvidenceOpen, StructuralEvidencePage, StructuralEvidenceSession,
};
use riffdb_storage_redb::{
    ProtectedEventConsumerLeaseValidationResultV1, ProtectedEventConsumerLeaseValidationV1,
    RedbDormantPorts, RedbOperationalPorts, RedbStore,
};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_types::{
    ActorId, ActorKind, AggregateTypeId, ApplicationRoleHash, Audience, CanonicalRecord,
    CanonicalString, CanonicalValue, CapabilityGrantV1, CapabilityId, CapabilityPermissionV1,
    CapabilityPermissionsV1, CapabilityPrincipalFactV1, CapabilityPrincipalFactsV1,
    CapabilityRowPolicyBindingV1, CapabilityRowPolicyGrantV1, CapabilityRowPolicyOperationV1,
    CommitSequence, ContractVersion, DatabaseId, DigestKeyId, EntityFieldVisibilityV1, EntityKey,
    Environment, EventConsumerName, EventDeliveryAttempt, EventId, EventLeaseToken,
    PartitionKeyBuilder, PartitionScopeV1, QueryParameterHash, ReactiveModuleHash,
    ReactiveOperationName, RowPolicyName, TenantScope, Timestamp,
};

const SOURCE: &str = include_str!("../fixtures/compiler/row-policy/valid/document-access.riff");
const RELATIONSHIP_SOURCE: &str =
    include_str!("../fixtures/compiler/row-policy/valid/document-grant.riff");
const BETTER_AUTH_SOURCE: &str = include_str!("../fixtures/adapters/better-auth/contract.riff");
const OWNER: [u8; 16] = [0x11; 16];
const TEAM: [u8; 16] = [0x22; 16];
const OUTSIDER: [u8; 16] = [0x33; 16];

struct FixedAuthorizationClock(Timestamp);

impl AuthorizationClock for FixedAuthorizationClock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        Ok(self.0)
    }
}

#[test]
fn better_auth_rows_are_owned_by_the_authenticated_user_for_every_operation() {
    let bundle =
        compile_contract_source(BETTER_AUTH_SOURCE).expect("Better Auth contract compiles");
    let owner = principal(OWNER, &[]);
    let outsider = principal(OUTSIDER, &[]);

    for (policy_name, entity_name) in [
        ("UserAccess", "User"),
        ("AccountAccess", "Account"),
        ("SessionAccess", "Session"),
        ("VerificationTokenAccess", "VerificationToken"),
    ] {
        let policy = bundle
            .row_policies()
            .policies()
            .iter()
            .find(|policy| policy.name() == policy_name)
            .expect("Better Auth row policy");
        let row = better_auth_row(&bundle, entity_name, OWNER);

        for operation in [
            RowPolicyOperationV1::Read,
            RowPolicyOperationV1::Create,
            RowPolicyOperationV1::Update,
            RowPolicyOperationV1::Delete,
        ] {
            assert!(
                evaluate_row_policy(policy, operation, &row, &owner, &[]).is_allowed(),
                "{policy_name} must allow its authenticated owner to {operation:?}",
            );
            assert!(
                evaluate_row_policy(policy, operation, &row, &outsider, &[]).is_denied(),
                "{policy_name} must deny another principal from {operation:?}",
            );
        }
    }
}

#[test]
fn owner_public_and_capability_facts_agree_for_every_operation_class() {
    let bundle = compile_contract_source(SOURCE).expect("policy contract compiles");
    let policy = policy(&bundle);
    let private = document(&bundle, OWNER, Some(TEAM), "Private");
    let public = document(&bundle, OUTSIDER, None, "Public");

    let owner = principal(OWNER, &[]);
    let teammate = principal(OUTSIDER, &[TEAM]);
    let outsider = principal(OUTSIDER, &[]);

    assert!(
        evaluate_row_policy(policy, RowPolicyOperationV1::Read, &private, &owner, &[]).is_allowed()
    );
    assert!(
        evaluate_row_policy(policy, RowPolicyOperationV1::Read, &private, &teammate, &[])
            .is_allowed()
    );
    assert!(
        evaluate_row_policy(policy, RowPolicyOperationV1::Read, &public, &outsider, &[])
            .is_allowed()
    );
    assert!(
        evaluate_row_policy(policy, RowPolicyOperationV1::Read, &private, &outsider, &[])
            .is_denied()
    );

    assert!(
        evaluate_row_transition(
            policy,
            RowPolicyOperationV1::Create,
            None,
            Some(&private),
            &owner,
            &[]
        )
        .is_allowed()
    );
    assert!(
        evaluate_row_transition(
            policy,
            RowPolicyOperationV1::Delete,
            Some(&private),
            None,
            &owner,
            &[]
        )
        .is_allowed()
    );
    assert!(
        evaluate_row_transition(
            policy,
            RowPolicyOperationV1::Delete,
            Some(&private),
            None,
            &outsider,
            &[]
        )
        .is_denied()
    );
}

#[test]
fn update_requires_both_transaction_current_and_successor_authority() {
    let bundle = compile_contract_source(SOURCE).expect("policy contract compiles");
    let policy = policy(&bundle);
    let current = document(&bundle, OWNER, Some(TEAM), "Private");
    let retained = document(&bundle, OWNER, None, "Private");
    let escaped = document(&bundle, OUTSIDER, None, "Public");
    let owner = principal(OWNER, &[]);

    let allowed = evaluate_row_transition(
        policy,
        RowPolicyOperationV1::Update,
        Some(&current),
        Some(&retained),
        &owner,
        &[],
    );
    assert!(allowed.is_allowed());
    let RowPolicyDecisionV1::Allow(proof) = allowed else {
        panic!("allowed update carries a proof");
    };
    assert!(proof.current_row_hash().is_some());
    assert!(proof.successor_row_hash().is_some());

    assert!(
        evaluate_row_transition(
            policy,
            RowPolicyOperationV1::Update,
            Some(&current),
            Some(&escaped),
            &owner,
            &[],
        )
        .is_denied()
    );
    assert!(
        evaluate_row_transition(
            policy,
            RowPolicyOperationV1::Update,
            Some(&escaped),
            Some(&retained),
            &owner,
            &[],
        )
        .is_denied()
    );
}

#[test]
fn relationship_policy_accepts_only_exact_index_evidence() {
    let bundle =
        compile_contract_source(RELATIONSHIP_SOURCE).expect("relationship policy compiles");
    let policy = policy(&bundle);
    let document = relationship_document(&bundle, false);
    let principal_id = [0x44; 16];
    let principal = principal_without_facts(principal_id);
    let grant = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "DocumentGrant")
        .expect("grant entity");
    let index = grant
        .indexes()
        .iter()
        .find(|index| index.name() == "by_document_subject")
        .expect("grant index");
    let exact = IndexedRelationshipEvidenceV1::new(
        grant.id(),
        index.id(),
        vec![
            CanonicalValue::Uuid([0x01; 16]),
            CanonicalValue::Uuid([0x02; 16]),
            CanonicalValue::Uuid(principal_id),
        ],
        true,
    );

    assert!(
        evaluate_row_policy(
            policy,
            RowPolicyOperationV1::Read,
            &document,
            &principal,
            std::slice::from_ref(&exact),
        )
        .is_allowed()
    );
    assert!(
        evaluate_row_policy(
            policy,
            RowPolicyOperationV1::Read,
            &document,
            &principal,
            &[]
        )
        .is_denied()
    );
    let wrong = IndexedRelationshipEvidenceV1::new(
        grant.id(),
        index.id(),
        vec![
            CanonicalValue::Uuid([0x01; 16]),
            CanonicalValue::Uuid([0x02; 16]),
            CanonicalValue::Uuid([0x55; 16]),
        ],
        true,
    );
    assert!(
        evaluate_row_policy(
            policy,
            RowPolicyOperationV1::Read,
            &document,
            &principal,
            &[wrong],
        )
        .is_denied()
    );
}

#[test]
fn final_safe_point_rejects_capability_revision_and_row_drift() {
    let bundle = compile_contract_source(SOURCE).expect("policy contract compiles");
    let policy = policy(&bundle);
    let current = document(&bundle, OWNER, Some(TEAM), "Private");
    let successor = document(&bundle, OWNER, None, "Private");
    let principal = principal_at_revision(OWNER, &[], 1);
    let RowPolicyDecisionV1::Allow(proof) = evaluate_row_transition(
        policy,
        RowPolicyOperationV1::Update,
        Some(&current),
        Some(&successor),
        &principal,
        &[],
    ) else {
        panic!("initial policy evaluation");
    };
    assert!(
        revalidate_row_policy_proof(
            &proof,
            policy,
            Some(&current),
            Some(&successor),
            &principal,
            &[],
        )
        .is_allowed()
    );
    let revised = principal_at_revision(OWNER, &[], 2);
    assert!(
        revalidate_row_policy_proof(
            &proof,
            policy,
            Some(&current),
            Some(&successor),
            &revised,
            &[],
        )
        .is_denied()
    );
    let changed = document(&bundle, OWNER, Some(TEAM), "Public");
    assert!(
        revalidate_row_policy_proof(
            &proof,
            policy,
            Some(&changed),
            Some(&successor),
            &principal,
            &[],
        )
        .is_denied()
    );
}

#[test]
fn event_release_proof_binds_event_key_row_and_capability_revision() {
    let bundle = compile_contract_source(SOURCE).expect("policy contract compiles");
    let policy = policy(&bundle).clone();
    let row = document(&bundle, OWNER, Some(TEAM), "Private");
    let key = document_key(&bundle, &row);
    let event_id = EventId::new(CommitSequence::first(), 0);
    let context = AuthorizedQueryRowPolicyContextV1::test_fixture(
        principal(OWNER, &[]),
        vec![policy.clone()],
        bundle.schema(),
    )
    .expect("event policy context");
    let candidate = EventPolicyCandidateV1::new(
        event_id,
        key.clone(),
        RowPolicyName::new(policy.name()).expect("policy name"),
    );

    let EventPolicyReleaseDecisionV1::Allow(proof) =
        context.authorize_event_release(&candidate, &row, &[])
    else {
        panic!("owner receives an event/key-bound proof");
    };
    assert_eq!(proof.event_id(), event_id);
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Document")
        .expect("document entity");
    let wrong_key = entity
        .primary_key()
        .encode_entity(&[
            CanonicalValue::Uuid([0x01; 16]),
            CanonicalValue::Uuid([0x09; 16]),
        ])
        .expect("different document key");
    assert!(
        context
            .authorize_event_release(
                &EventPolicyCandidateV1::new(
                    event_id,
                    wrong_key,
                    RowPolicyName::new(policy.name()).expect("policy name"),
                ),
                &row,
                &[],
            )
            .is_denied()
    );
    assert!(
        revalidate_event_release_proof(&proof, &context, event_id, &key, &row, &[]).is_allowed()
    );
    assert!(
        revalidate_event_release_proof(
            &proof,
            &context,
            EventId::new(CommitSequence::first(), 1),
            &key,
            &row,
            &[],
        )
        .is_denied()
    );

    let revised = AuthorizedQueryRowPolicyContextV1::test_fixture(
        principal_at_revision(OWNER, &[], 2),
        vec![policy.clone()],
        bundle.schema(),
    )
    .expect("revised context");
    assert!(
        revalidate_event_release_proof(&proof, &revised, event_id, &key, &row, &[]).is_denied()
    );
    let outsider = AuthorizedQueryRowPolicyContextV1::test_fixture(
        principal(OUTSIDER, &[]),
        vec![policy],
        bundle.schema(),
    )
    .expect("outsider context");
    assert!(
        outsider
            .authorize_event_release(&candidate, &row, &[])
            .is_denied()
    );
}

#[test]
fn protected_reaction_safe_point_denies_when_durable_capability_is_absent() {
    let bundle = compile_contract_source(SOURCE).expect("policy contract compiles");
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Document")
        .expect("document entity");
    let owner_field = entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "owner_id")
        .expect("owner field")
        .id();
    let role = ApplicationRoleHash::from_bytes([0x71; 32]);
    let module = ReactiveModuleHash::from_bytes([0x72; 32]);
    let operation = ReactiveOperationName::new("DocumentAgent").expect("operation");
    let facts = CapabilityPrincipalFactsV1::new(vec![
        CapabilityPrincipalFactV1::new(
            "team_ids",
            CanonicalValue::list(Vec::new()).expect("empty team set"),
        )
        .expect("team fact"),
    ])
    .expect("principal facts");
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::ConsumeContextualSubscription(
                bundle.lineage().clone(),
                module,
                operation.clone(),
            ),
            CapabilityPermissionV1::ApplicationRoleIdentity(role),
        ])
        .expect("permissions"),
        vec![
            EntityFieldVisibilityV1::new(bundle.lineage().clone(), entity.id(), vec![owner_field])
                .expect("field visibility"),
        ],
        NonZeroU16::new(8).expect("row limit"),
        Vec::new(),
    )
    .expect("base grant")
    .with_row_policy(
        CapabilityRowPolicyGrantV1::new(
            role,
            facts,
            vec![
                CapabilityRowPolicyBindingV1::new(
                    bundle.lineage().clone(),
                    RowPolicyName::new("DocumentAccess").expect("policy name"),
                    entity.id(),
                    vec![CapabilityRowPolicyOperationV1::Read],
                )
                .expect("policy binding"),
            ],
        )
        .expect("row-policy grant"),
    )
    .expect("V4 grant");
    let database_id = protected_database_id();
    let environment = Environment::new("test").expect("environment");
    let audience = Audience::new("riffdb-row-policy-test").expect("audience");
    let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
        database_id,
        environment.clone(),
        ActorId::new(uuid_text(OWNER)).expect("principal"),
        ActorKind::Human,
        audience,
        AuthorizationFixtureTimes::new(
            Timestamp::new(1, 0).expect("issued"),
            Timestamp::new(10, 0).expect("expires"),
            Timestamp::new(5, 0).expect("authenticated"),
        ),
        grant,
    ))
    .expect("authorization fixture");
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
    partition
        .push_uuid(&[0x01; 16])
        .expect("partition component");
    let parameter_hash = QueryParameterHash::from_bytes([0x73; 32]);
    let consumer_name = EventConsumerName::new("worker").expect("consumer name");
    let target = EventConsumerOperationTarget::new(
        bundle.lineage().clone(),
        ContractVersion::new(1).expect("contract version"),
        bundle.bundle_hash(),
        module,
        operation.clone(),
        parameter_hash,
        consumer_name.clone(),
        OperationTenantScope::global_only(),
        partition.finish().expect("partition"),
    );
    let resolver = fixture.current_capability_resolver();
    let decision = CurrentAuthorizer::new(
        &resolver,
        &FixedAuthorizationClock(Timestamp::new(5, 0).expect("authorization time")),
        &NoopAuthorizationTelemetry,
        database_id,
        environment,
    )
    .authorize(
        fixture.authenticated_principal(),
        OperationRequest::execute_contextual_reaction(target),
    )
    .expect("authorization evaluates");
    let Decision::Allow(authorization) = decision else {
        panic!("protected reaction is authorized at the service boundary");
    };
    let policy_context =
        resolve_authorized_event_row_policy_context(&authorization, &bundle, &[entity.id()])
            .expect("policy context resolves")
            .expect("protected policy context");

    let (_scope, mut ports) = empty_operational_database(database_id);
    let result = ports
        .validate_protected_event_consumer_lease(ProtectedEventConsumerLeaseValidationV1 {
            identity: EventConsumerIdentityV1::new(
                database_id,
                module,
                operation,
                parameter_hash,
                consumer_name,
            ),
            partition_hash: riffdb_types::PartitionKeyHash::from_bytes([0x74; 32]),
            event_id: EventId::new(CommitSequence::first(), 0),
            attempt: EventDeliveryAttempt::first(),
            token: EventLeaseToken::from_bytes([0x75; 32]),
            history_incarnation: 1,
            observed_at: Timestamp::new(5, 0).expect("observed time"),
            policy: policy_context,
        })
        .expect("missing current capability is a closed denial, not a storage failure");
    assert_eq!(
        result,
        ProtectedEventConsumerLeaseValidationResultV1::Denied
    );
}

fn policy(bundle: &ContractBundle) -> &RowPolicyPlanV1 {
    bundle
        .row_policies()
        .policies()
        .iter()
        .find(|policy| policy.name() == "DocumentAccess")
        .expect("document policy")
}

fn document_key(bundle: &ContractBundle, row: &CanonicalRecord) -> EntityKey {
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Document")
        .expect("document entity");
    let values = entity
        .primary_key_fields()
        .iter()
        .map(|field_id| {
            row.fields()
                .binary_search_by_key(field_id, |(candidate, _)| *candidate)
                .ok()
                .map(|index| row.fields()[index].1.clone())
                .expect("complete document key")
        })
        .collect::<Vec<_>>();
    entity
        .primary_key()
        .encode_entity(&values)
        .expect("canonical document key")
}

fn document(
    bundle: &ContractBundle,
    owner: [u8; 16],
    team: Option<[u8; 16]>,
    visibility: &str,
) -> CanonicalRecord {
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Document")
        .expect("document entity");
    let value = |name: &str| -> CanonicalValue {
        match name {
            "organization_id" => CanonicalValue::Uuid([0x01; 16]),
            "document_id" => CanonicalValue::Uuid([0x02; 16]),
            "owner_id" => CanonicalValue::Uuid(owner),
            "team_id" => team.map_or(CanonicalValue::Null, CanonicalValue::Uuid),
            "visibility" => {
                let enumeration = bundle
                    .schema()
                    .enums()
                    .iter()
                    .find(|enumeration| enumeration.name() == "Visibility")
                    .expect("visibility enum");
                let variant = enumeration
                    .variants()
                    .iter()
                    .find(|variant| variant.name() == visibility)
                    .expect("visibility variant");
                CanonicalValue::Enum {
                    type_id: enumeration.id(),
                    variant_id: variant.id(),
                }
            }
            _ => panic!("unexpected document field"),
        }
    };
    CanonicalRecord::new(
        entity
            .record()
            .fields()
            .iter()
            .map(|field| (field.id(), value(field.name())))
            .collect(),
    )
    .expect("canonical document")
}

fn relationship_document(bundle: &ContractBundle, published: bool) -> CanonicalRecord {
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Document")
        .expect("document entity");
    CanonicalRecord::new(
        entity
            .record()
            .fields()
            .iter()
            .map(|field| {
                let value = match field.name() {
                    "organization_id" => CanonicalValue::Uuid([0x01; 16]),
                    "document_id" => CanonicalValue::Uuid([0x02; 16]),
                    "published" => CanonicalValue::Bool(published),
                    _ => panic!("unexpected relationship document field"),
                };
                (field.id(), value)
            })
            .collect(),
    )
    .expect("relationship document")
}

fn better_auth_row(
    bundle: &ContractBundle,
    entity_name: &str,
    user_id: [u8; 16],
) -> CanonicalRecord {
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == entity_name)
        .expect("Better Auth entity");
    CanonicalRecord::new(
        entity
            .record()
            .fields()
            .iter()
            .map(|field| {
                let value = match field.name() {
                    "organization_id" => CanonicalValue::Uuid([0x01; 16]),
                    "user_id" => CanonicalValue::Uuid(user_id),
                    "account_id" | "session_id" | "verification_token_id" => {
                        CanonicalValue::Uuid([0x02; 16])
                    }
                    "email" => CanonicalValue::String(
                        CanonicalString::new("owner@example.test").expect("email"),
                    ),
                    "provider" => {
                        CanonicalValue::String(CanonicalString::new("oidc").expect("provider"))
                    }
                    "provider_account_id" => CanonicalValue::String(
                        CanonicalString::new("subject-1").expect("provider account"),
                    ),
                    "token_digest" => CanonicalValue::String(
                        CanonicalString::new("digest").expect("token digest"),
                    ),
                    "expires_at" | "issued_at" => {
                        CanonicalValue::Timestamp(Timestamp::new(5, 0).expect("timestamp"))
                    }
                    "consumed" => CanonicalValue::Bool(false),
                    "state" => {
                        let enumeration = bundle
                            .schema()
                            .enums()
                            .iter()
                            .find(|enumeration| enumeration.name() == "SessionState")
                            .expect("session-state enum");
                        let variant = enumeration
                            .variants()
                            .iter()
                            .find(|variant| variant.name() == "Active")
                            .expect("active session state");
                        CanonicalValue::Enum {
                            type_id: enumeration.id(),
                            variant_id: variant.id(),
                        }
                    }
                    name => panic!("unexpected Better Auth field {name}"),
                };
                (field.id(), value)
            })
            .collect(),
    )
    .expect("canonical Better Auth row")
}

fn principal(id: [u8; 16], teams: &[[u8; 16]]) -> PrincipalFactBindingV1 {
    principal_at_revision(id, teams, 1)
}

fn principal_at_revision(
    id: [u8; 16],
    teams: &[[u8; 16]],
    revision: u64,
) -> PrincipalFactBindingV1 {
    let facts = CapabilityPrincipalFactsV1::new(vec![
        CapabilityPrincipalFactV1::new(
            "team_ids",
            CanonicalValue::list(teams.iter().copied().map(CanonicalValue::Uuid).collect())
                .expect("bounded team list"),
        )
        .expect("team fact"),
    ])
    .expect("fact set");
    PrincipalFactBindingV1::new(
        CapabilityId::from_unix_milliseconds_and_random(1, [0x10; 10]).expect("capability"),
        NonZeroU64::new(revision).expect("revision"),
        DatabaseId::from_unix_milliseconds_and_random(1, [0x20; 10]).expect("database"),
        Environment::new("test").expect("environment"),
        ActorId::new(uuid_text(id)).expect("principal"),
        ActorKind::Human,
        vec![Audience::new("riffdb-row-policy-test").expect("audience")],
        TenantScope::Global,
        Timestamp::new(1, 0).expect("issued"),
        Timestamp::new(10, 0).expect("expires"),
        facts,
    )
    .expect("principal binding")
}

fn principal_without_facts(id: [u8; 16]) -> PrincipalFactBindingV1 {
    PrincipalFactBindingV1::new(
        CapabilityId::from_unix_milliseconds_and_random(1, [0x40; 10]).expect("capability"),
        NonZeroU64::new(1).expect("revision"),
        DatabaseId::from_unix_milliseconds_and_random(1, [0x41; 10]).expect("database"),
        Environment::new("test").expect("environment"),
        ActorId::new(uuid_text(id)).expect("principal"),
        ActorKind::Human,
        vec![Audience::new("riffdb-row-policy-test").expect("audience")],
        TenantScope::Global,
        Timestamp::new(1, 0).expect("issued"),
        Timestamp::new(10, 0).expect("expires"),
        CapabilityPrincipalFactsV1::empty(),
    )
    .expect("principal binding")
}

fn uuid_text(bytes: [u8; 16]) -> String {
    bytes
        .iter()
        .enumerate()
        .fold(String::new(), |mut output, (index, byte)| {
            if matches!(index, 4 | 6 | 8 | 10) {
                output.push('-');
            }
            use std::fmt::Write as _;
            write!(output, "{byte:02x}").expect("write UUID");
            output
        })
}

struct ProtectedTestDatabase {
    root: PathBuf,
}

impl Drop for ProtectedTestDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn protected_database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1, [0x52; 10]).expect("database ID")
}

fn empty_operational_database(
    database_id: DatabaseId,
) -> (ProtectedTestDatabase, RedbOperationalPorts) {
    let test_root = std::env::var_os("RIFFDB_TEST_TMP_ROOT").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"),
        PathBuf::from,
    );
    assert!(
        test_root.is_absolute() && !test_root.is_symlink(),
        "row-policy test root must be an absolute non-symlink path"
    );
    let root = test_root
        .join("wp572-event-policy")
        .join(format!("reaction-safe-point-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create test database directory");
    let path = root.join("db.redb");
    let mut store = RedbStore::open(&path).expect("open test database");
    assert_eq!(
        store
            .initialize_database(database_id)
            .expect("initialize test database"),
        DatabaseInitializationResult::Installed(database_id)
    );
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).expect("digest key ID"));
    let startup = StartupValidationInputs::new(
        Timestamp::new(1, 0).expect("startup time"),
        ReadableCapabilityDigestInventory::new(vec![key]).expect("capability keys"),
        ReadableIdempotencyDigestInventory::new(vec![key]).expect("idempotency keys"),
    );
    let mut session = store
        .begin_structural_evidence(startup)
        .expect("begin structural validation");
    let open_session_id = session.open_session_id();
    let mut cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
    let limit = EvidencePageLimit::new(64).expect("evidence page limit");
    let structural_end = loop {
        match session
            .read_structural_evidence(cursor, limit)
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty(), "unexpected findings: {findings:?}");
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (history, historical_end) = validate_catalog_history(&mut session)
        .expect("validate empty catalog history")
        .into_parts();
    let opened = session
        .finish(structural_end, historical_end)
        .expect("finish structural validation");
    let riffdb_catalog::CatalogHistoryOutcome::Ready(history) = history else {
        panic!("empty database must have ready catalog history");
    };
    let riffdb_storage_api::StructuralOpenOutcome::Clean(opened) = opened else {
        panic!("empty database must open cleanly");
    };
    assert!(history.matches(opened.database_id(), opened.open_session_id()));
    let (_, _, _, dormant): (_, _, _, RedbDormantPorts) = opened.into_parts();
    let ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate test database");
    (ProtectedTestDatabase { root }, ports)
}
