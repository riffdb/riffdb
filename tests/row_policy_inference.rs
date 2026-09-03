#![forbid(unsafe_code)]

//! Inference regressions for policy-before-shape behavior.

use std::num::NonZeroU64;

use riffdb_auth::PrincipalFactBindingV1;
use riffdb_contract_compiler::compile_contract_source;
use riffdb_policy::{filter_authorized_rows, policy_visible_count};
use riffdb_query_module::{
    ApplicationRoleErrorKind, ApplicationSourceManifest, QueryModule, QueryModuleCandidate,
    QueryModuleName, QueryModuleVersion, compile_application_role,
};
use riffdb_types::{
    ActorId, ActorKind, Audience, CanonicalRecord, CanonicalValue, CapabilityId,
    CapabilityPrincipalFactV1, CapabilityPrincipalFactsV1, DatabaseId, Environment, TenantScope,
    Timestamp,
};

const SOURCE: &str = include_str!("../fixtures/compiler/row-policy/valid/document-access.riff");

// req: BLK-040
#[test]
fn initialized_transition_authority_requires_create_and_update_policy_coverage() {
    let contract_source = |rules: &str| {
        format!(
            r#"
contract InitializedPolicySurface version 1 {{
  entity Document {{
    key (organization_id: uuid, document_id: uuid)
    field owner_id: uuid
    field value: u64
  }}
  aggregate Documents {{
    root Document
    partition_by organization_id
    conflict_key (organization_id, document_id)
  }}
  row policy DocumentAccess on Document {{
    {rules}
  }}
  command PutDocument {{
    input request_id: uuid
    input organization_id: uuid
    input document_id: uuid
    input owner_id: uuid
    input value: u64
    idempotency_key request_id
    init_or_mutate Document(organization_id, document_id) as document initialize {{
      owner_id: owner_id,
      value: 0,
    }}
    set document.owner_id = owner_id
    set document.value = value
    return Written {{}}
  }}
}}
"#
        )
    };
    let manifest_source = r#"{
      "application":"init-policy",
      "contract":{"lineage":"InitializedPolicySurface","source":"contract.riff","version":1},
      "generation":{"go":"generated/go/client.go","mcp":"generated/mcp/tools.json","python":"generated/python/client.py","rust":"generated/rust/client.rs","typescript":"generated/typescript/client.ts"},
      "migrations":[],
      "query_modules":[{"name":"empty","queries":[],"version":1}],
      "reactive_modules":[],
      "roles":[{"agent_subscriptions":[],"commands":["PutDocument"],"environment":"development","event_streams":[],"name":"DocumentWriter","queries":[],"row_policies":["DocumentAccess"],"tenant_scope":"global","watch_queries":[]}],
      "schema":"riffdb.application-source/v6",
      "seed_inputs":[]
    }"#;
    let compile_role = |rules: &str| {
        let contract = compile_contract_source(&contract_source(rules)).expect("contract compiles");
        let module = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("empty").expect("module name"),
                QueryModuleVersion::new(1).expect("module version"),
                vec![],
            )
            .expect("module candidate"),
            &contract,
        )
        .expect("empty module");
        let manifest = ApplicationSourceManifest::parse(manifest_source)
            .expect("source manifest")
            .exact_manifest_v2(&contract, std::slice::from_ref(&module), &[])
            .expect("exact manifest");
        compile_application_role(
            &manifest,
            "DocumentWriter",
            None,
            &contract,
            std::slice::from_ref(&module),
        )
    };

    for incomplete in [
        "allow create when owner_id == principal.id",
        "allow update when owner_id == principal.id",
    ] {
        assert_eq!(
            compile_role(incomplete)
                .expect_err("one operation must not authorize both alternatives")
                .kind(),
            ApplicationRoleErrorKind::PolicyCoverage
        );
    }
    let role = compile_role(
        "allow create when owner_id == principal.id\n    allow update when owner_id == principal.id",
    )
    .expect("the complete create/update authority union is accepted");
    assert_eq!(role.row_policies()[0].operations().len(), 2);
}

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
