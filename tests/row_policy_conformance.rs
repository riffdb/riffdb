#![forbid(unsafe_code)]

//! Cross-operation differential acceptance for the shared row-policy evaluator.

use std::num::NonZeroU64;

use riffdb_auth::PrincipalFactBindingV1;
use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{ContractBundle, RowPolicyOperationV1, RowPolicyPlanV1};
use riffdb_policy::{
    AuthorizedQueryRowPolicyContextV1, EventPolicyReleaseDecisionV1, IndexedRelationshipEvidenceV1,
    RowPolicyDecisionV1, evaluate_row_policy, evaluate_row_transition,
    revalidate_event_release_proof, revalidate_row_policy_proof,
};
use riffdb_types::{
    ActorId, ActorKind, Audience, CanonicalRecord, CanonicalValue, CapabilityId,
    CapabilityPrincipalFactV1, CapabilityPrincipalFactsV1, CommitSequence, DatabaseId, EntityKey,
    Environment, EventId, TenantScope, Timestamp,
};

const SOURCE: &str = include_str!("../fixtures/compiler/row-policy/valid/document-access.riff");
const RELATIONSHIP_SOURCE: &str =
    include_str!("../fixtures/compiler/row-policy/valid/document-grant.riff");
const OWNER: [u8; 16] = [0x11; 16];
const TEAM: [u8; 16] = [0x22; 16];
const OUTSIDER: [u8; 16] = [0x33; 16];

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

    let EventPolicyReleaseDecisionV1::Allow(proof) =
        context.authorize_event_release(event_id, &key, &row, &[])
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
            .authorize_event_release(event_id, &wrong_key, &row, &[])
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
            .authorize_event_release(event_id, &key, &row, &[])
            .is_denied()
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
