#![forbid(unsafe_code)]

//! Inference regressions for policy-before-shape behavior.

use std::num::NonZeroU64;

use riffdb_auth::PrincipalFactBindingV1;
use riffdb_contract_compiler::compile_contract_source;
use riffdb_policy::{filter_authorized_rows, policy_visible_count};
use riffdb_types::{
    ActorId, ActorKind, Audience, CanonicalRecord, CanonicalValue, CapabilityId,
    CapabilityPrincipalFactV1, CapabilityPrincipalFactsV1, DatabaseId, Environment, TenantScope,
    Timestamp,
};

const SOURCE: &str = include_str!("../fixtures/compiler/row-policy/valid/document-access.riff");

#[test]
fn hidden_rows_do_not_consume_limit_or_change_count() {
    let bundle = compile_contract_source(SOURCE).expect("policy contract compiles");
    let policy = &bundle.row_policies().policies()[0];
    let principal = principal([0x44; 16]);
    let rows = [
        document(&bundle, [0x11; 16], "Private", 1),
        document(&bundle, [0x44; 16], "Private", 2),
        document(&bundle, [0x22; 16], "Public", 3),
    ];

    let visible = filter_authorized_rows(policy, &principal, rows.iter(), &[]);
    assert_eq!(visible.len(), 2);
    assert_eq!(
        policy_visible_count(policy, &principal, rows.iter(), &[]),
        2
    );

    let page = visible.into_iter().take(2).collect::<Vec<_>>();
    assert_eq!(page.len(), 2, "limit is applied after policy");
    assert_eq!(uuid_field(page[0], "document_id", &bundle), [2; 16]);
    assert_eq!(uuid_field(page[1], "document_id", &bundle), [3; 16]);
}

fn document(
    bundle: &riffdb_contract_ir::ContractBundle,
    owner: [u8; 16],
    visibility: &str,
    id: u8,
) -> CanonicalRecord {
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Document")
        .expect("document");
    let enumeration = bundle
        .schema()
        .enums()
        .iter()
        .find(|item| item.name() == "Visibility")
        .expect("visibility");
    let variant = enumeration
        .variants()
        .iter()
        .find(|item| item.name() == visibility)
        .expect("variant");
    CanonicalRecord::new(
        entity
            .record()
            .fields()
            .iter()
            .map(|field| {
                let value = match field.name() {
                    "organization_id" => CanonicalValue::Uuid([1; 16]),
                    "document_id" => CanonicalValue::Uuid([id; 16]),
                    "owner_id" => CanonicalValue::Uuid(owner),
                    "team_id" => CanonicalValue::Null,
                    "visibility" => CanonicalValue::Enum {
                        type_id: enumeration.id(),
                        variant_id: variant.id(),
                    },
                    _ => panic!("unexpected field"),
                };
                (field.id(), value)
            })
            .collect(),
    )
    .expect("record")
}

fn uuid_field(
    row: &CanonicalRecord,
    name: &str,
    bundle: &riffdb_contract_ir::ContractBundle,
) -> [u8; 16] {
    let field = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Document")
        .expect("document")
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == name)
        .expect("field");
    let (_, CanonicalValue::Uuid(value)) = row
        .fields()
        .iter()
        .find(|(id, _)| *id == field.id())
        .expect("value")
    else {
        panic!("UUID field")
    };
    *value
}

fn principal(id: [u8; 16]) -> PrincipalFactBindingV1 {
    PrincipalFactBindingV1::new(
        CapabilityId::from_unix_milliseconds_and_random(1, [0x31; 10]).expect("capability"),
        NonZeroU64::new(7).expect("revision"),
        DatabaseId::from_unix_milliseconds_and_random(1, [0x32; 10]).expect("database"),
        Environment::new("test").expect("environment"),
        ActorId::new(uuid_text(id)).expect("actor"),
        ActorKind::Human,
        vec![Audience::new("riffdb-row-policy-test").expect("audience")],
        TenantScope::Global,
        Timestamp::new(1, 0).expect("issued"),
        Timestamp::new(10, 0).expect("expires"),
        CapabilityPrincipalFactsV1::new(vec![
            CapabilityPrincipalFactV1::new(
                "team_ids",
                CanonicalValue::list(Vec::new()).expect("empty bounded team list"),
            )
            .expect("team fact"),
        ])
        .expect("fact set"),
    )
    .expect("principal")
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
            write!(output, "{byte:02x}").expect("UUID");
            output
        })
}
