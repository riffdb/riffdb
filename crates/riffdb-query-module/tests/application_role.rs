#![forbid(unsafe_code)]

//! Symbolic application-role authority tests.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_ir::SymbolicCatalog;
use riffdb_query_module::{
    ApplicationManifest, ApplicationRoleErrorKind, ApplicationRoleOperationKind,
    ApplicationSourceManifest, NamedQuerySource, QueryModule, QueryModuleCandidate,
    QueryModuleName, QueryModuleVersion, compile_application_role, generate_go_application_client,
    generate_python_application_client, generate_rust_application_client,
    generate_typescript_application_client,
};
use riffdb_types::{
    ActorId, CanonicalValue, CapabilityPermissionKindV1, CapabilityPermissionV1,
    CapabilityPrincipalFactV1, CapabilityPrincipalFactsV1, PartitionScopeV1, TenantId, TenantScope,
};

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
const MANIFEST: &str = include_str!("../../../fixtures/application-manifests/ticketdesk-v1.json");
const QUERIES: [(&str, &str); 11] = [
    (
        "BoardPage200",
        include_str!("../../../queries/ticketdesk/board_page_200.riffq"),
    ),
    (
        "BoardPage450",
        include_str!("../../../queries/ticketdesk/board_page_450.riffq"),
    ),
    (
        "BoardPage50",
        include_str!("../../../queries/ticketdesk/board_page_50.riffq"),
    ),
    (
        "GetTicket",
        include_str!("../../../queries/ticketdesk/get_ticket.riffq"),
    ),
    (
        "GetUser",
        include_str!("../../../queries/ticketdesk/get_user.riffq"),
    ),
    (
        "ListComments",
        include_str!("../../../queries/ticketdesk/list_comments.riffq"),
    ),
    (
        "ListTickets",
        include_str!("../../../queries/ticketdesk/list_tickets.riffq"),
    ),
    (
        "ListTicketsByAssignee",
        include_str!("../../../queries/ticketdesk/list_tickets_by_assignee.riffq"),
    ),
    (
        "ProjectMembers",
        include_str!("../../../queries/ticketdesk/project_members.riffq"),
    ),
    (
        "ProjectSummary",
        include_str!("../../../queries/ticketdesk/project_summary.riffq"),
    ),
    (
        "TicketPage",
        include_str!("../../../queries/ticketdesk/ticket_page.riffq"),
    ),
];

fn exact_application() -> (
    ApplicationManifest,
    riffdb_contract_ir::ContractBundle,
    QueryModule,
) {
    let manifest = ApplicationManifest::decode_canonical(MANIFEST.as_bytes()).expect("manifest");
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("ticketdesk").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        QUERIES
            .into_iter()
            .map(|(name, source)| NamedQuerySource::new(name, source).expect("query"))
            .collect(),
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &contract).expect("module");
    (manifest, contract, module)
}

#[test]
fn v4_role_identity_covers_symbolic_policy_and_fact_schema() {
    let contract = compile_contract_source(include_str!(
        "../../../fixtures/compiler/row-policy/valid/document-access.riff"
    ))
    .expect("policy contract");
    let query_source = r#"
query GetDocument(
    $organization_id: Document.organization_id,
    $document_id: Document.document_id,
) {
    one document from Document
        where organization_id == $organization_id
          && document_id == $document_id
        else NotFound
    return Found {
        document: document { document_id owner_id team_id visibility }
    }
    outcomes Found | NotFound
}
"#;
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("policy_surface").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![NamedQuerySource::new("GetDocument", query_source).expect("query")],
        )
        .expect("candidate"),
        &contract,
    )
    .expect("module");
    let source = |policies: &str| {
        format!(
            r#"{{
  "application": "policy-surface",
  "contract": {{"lineage": "PolicySurface", "source": "contract.riff", "version": 1}},
  "generation": {{"go": "generated/go/client.go", "mcp": "generated/mcp/tools.json", "python": "generated/python/client.py", "rust": "generated/rust/client.rs", "typescript": "generated/typescript/client.ts"}},
  "migrations": [],
  "query_modules": [{{"name": "policy_surface", "queries": [{{"name": "GetDocument", "source": "queries/get_document.riffq"}}], "version": 1}}],
  "reactive_modules": [],
  "roles": [{{"agent_subscriptions": [], "commands": [], "environment": "development", "event_streams": [], "name": "DocumentReader", "queries": ["GetDocument"], "row_policies": {policies}, "tenant_scope": "global", "watch_queries": []}}],
  "schema": "riffdb.application-source/v6",
  "seed_inputs": []
}}"#
        )
    };
    let exact = ApplicationSourceManifest::parse(&source("[\"DocumentAccess\"]"))
        .expect("source")
        .exact_manifest_v2(&contract, std::slice::from_ref(&module), &[])
        .expect("exact manifest");
    let role = compile_application_role(
        &exact,
        "DocumentReader",
        None,
        &contract,
        std::slice::from_ref(&module),
    )
    .expect("compiled role");

    assert_eq!(exact.schema(), "riffdb.application-manifest/v4");
    assert_eq!(role.row_policies().len(), 1);
    assert_eq!(role.row_policies()[0].name(), "DocumentAccess");
    assert_eq!(role.row_policies()[0].entity(), "Document");
    assert_eq!(role.principal_fact_schemas().len(), 1);
    assert_eq!(role.principal_fact_schemas()[0].name(), "team_ids");
    assert_eq!(
        role.principal_fact_schemas()[0].value_type(),
        "List<Uuid,32>"
    );
    let catalog = SymbolicCatalog::from_bundle(&contract).expect("safe catalog");
    let policy = catalog.row_policy("DocumentAccess").expect("policy symbol");
    assert_eq!(policy.entity(), "Document");
    assert_eq!(policy.operations().len(), 4);
    let fact = catalog.principal_fact("team_ids").expect("fact schema");
    assert_eq!(fact.value_type(), "List<Uuid,32>");
    let safe_catalog = format!("{policy:?} {fact:?}");
    assert!(!safe_catalog.contains("field_id"));
    assert!(!safe_catalog.contains("team-a"));
    let generated = [
        generate_rust_application_client(&module, &contract, &[]),
        generate_typescript_application_client(&module, &contract, &[]),
        generate_python_application_client(&module, &contract, &[]).expect("Python client"),
        generate_go_application_client(&module, &contract, &[]),
    ];
    for client in generated {
        let client = client.to_ascii_lowercase();
        assert!(!client.contains("row_policy"));
        assert!(!client.contains("principal_fact"));
        assert!(!client.contains("policy_bypass"));
    }
    assert!(
        !role
            .internal_grant()
            .permissions()
            .as_slice()
            .iter()
            .any(|permission| matches!(permission, CapabilityPermissionV1::ExecuteNamedQuery(..)))
    );

    let team = CanonicalValue::Uuid([7; 16]);
    let facts = CapabilityPrincipalFactsV1::new(vec![
        CapabilityPrincipalFactV1::new(
            "team_ids",
            CanonicalValue::list(vec![team]).expect("bounded fact list"),
        )
        .expect("principal fact"),
    ])
    .expect("fact set");
    let bound = role
        .bind_principal_facts_for(
            &ActorId::new("00000000-0000-0000-0000-000000000007").expect("UUID principal"),
            facts,
        )
        .expect("trusted role binding");
    assert!(bound.internal_row_policy().is_some());
    assert!(
        bound.permissions().as_slice().iter().any(|permission| {
            matches!(permission, CapabilityPermissionV1::ExecuteNamedQuery(..))
        })
    );
    assert_eq!(
        role.bind_principal_facts_for(
            &ActorId::new("not-a-uuid").expect("bounded actor"),
            CapabilityPrincipalFactsV1::empty(),
        )
        .expect_err("missing required fact must deny")
        .kind(),
        ApplicationRoleErrorKind::PrincipalFacts
    );
    assert_eq!(
        role.bind_principal_facts_for(
            &ActorId::new("not-a-uuid").expect("bounded actor"),
            CapabilityPrincipalFactsV1::new(vec![
                CapabilityPrincipalFactV1::new(
                    "team_ids",
                    CanonicalValue::list(vec![CanonicalValue::Uuid([7; 16])])
                        .expect("bounded fact list"),
                )
                .expect("principal fact"),
            ])
            .expect("fact set"),
        )
        .expect_err("principal.id policy must require a canonical UUID")
        .kind(),
        ApplicationRoleErrorKind::PrincipalFacts
    );

    let missing = ApplicationSourceManifest::parse(&source("[]"))
        .expect("source")
        .exact_manifest_v2(&contract, std::slice::from_ref(&module), &[])
        .expect("exact manifest");
    let error = compile_application_role(&missing, "DocumentReader", None, &contract, &[module])
        .expect_err("missing protected policy must deny");
    assert_eq!(error.kind(), ApplicationRoleErrorKind::PolicyCoverage);
}

#[test]
fn compiled_role_resolves_enum_principal_facts_by_symbol_only() {
    let contract = compile_contract_source(
        r#"
contract EnumPolicySurface version 1 {
  enum Visibility { Private, Team, Public }

  principal fact allowed_visibility: Visibility
  principal fact allowed_visibilities: list<Visibility, 3>

  entity Document {
    key (organization_id: uuid, document_id: uuid)
    field visibility: Visibility
  }

  aggregate Documents {
    root Document
    partition_by organization_id
    conflict_key (organization_id)
  }

  row policy DocumentAccess on Document {
    allow read when visibility == principal.fact.allowed_visibility
      || visibility in principal.fact.allowed_visibilities
  }
}
"#,
    )
    .expect("enum policy contract");
    let query = r#"
query GetDocument(
    $organization_id: Document.organization_id,
    $document_id: Document.document_id,
) {
    one document from Document
        where organization_id == $organization_id
          && document_id == $document_id
        else NotFound
    return Found { document: document { document_id visibility } }
    outcomes Found | NotFound
}
"#;
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("enum_policy_surface").expect("module"),
            QueryModuleVersion::new(1).expect("version"),
            vec![NamedQuerySource::new("GetDocument", query).expect("query")],
        )
        .expect("candidate"),
        &contract,
    )
    .expect("module");
    let source = r#"{
      "application":"enum-policy-surface",
      "contract":{"lineage":"EnumPolicySurface","source":"contract.riff","version":1},
      "generation":{"go":"generated/go/client.go","mcp":"generated/mcp/tools.json","python":"generated/python/client.py","rust":"generated/rust/client.rs","typescript":"generated/typescript/client.ts"},
      "migrations":[],
      "query_modules":[{"name":"enum_policy_surface","queries":[{"name":"GetDocument","source":"queries/get_document.riffq"}],"version":1}],
      "reactive_modules":[],
      "roles":[{"agent_subscriptions":[],"commands":[],"environment":"development","event_streams":[],"name":"DocumentReader","queries":["GetDocument"],"row_policies":["DocumentAccess"],"tenant_scope":"global","watch_queries":[]}],
      "schema":"riffdb.application-source/v6",
      "seed_inputs":[]
    }"#;
    let exact = ApplicationSourceManifest::parse(source)
        .expect("source")
        .exact_manifest_v2(&contract, std::slice::from_ref(&module), &[])
        .expect("exact");
    let role = compile_application_role(
        &exact,
        "DocumentReader",
        None,
        &contract,
        std::slice::from_ref(&module),
    )
    .expect("role");

    let scalar = role
        .internal_resolve_principal_fact_enum("allowed_visibility", "Team")
        .expect("scalar enum symbol");
    let list_member = role
        .internal_resolve_principal_fact_enum("allowed_visibilities", "Team")
        .expect("list enum symbol");
    assert_eq!(scalar, list_member);
    assert!(matches!(scalar, CanonicalValue::Enum { .. }));
    assert!(
        role.internal_resolve_principal_fact_enum("allowed_visibility", "Missing")
            .is_none()
    );
    assert!(
        role.internal_resolve_principal_fact_enum("unknown_fact", "Team")
            .is_none()
    );
}

#[test]
fn symbolic_role_lowers_only_to_exact_application_operations() {
    let (manifest, contract, module) = exact_application();
    let role = compile_application_role(&manifest, "TicketDeskAgent", None, &contract, &[module])
        .expect("role");

    assert_eq!(role.application_name(), "ticketdesk");
    assert_eq!(role.role_name(), "TicketDeskAgent");
    assert_eq!(role.environment().as_str(), "development");
    assert_eq!(role.tenant_scope(), &TenantScope::Global);
    assert_eq!(role.operations().len(), 22);
    assert!(role.operations().iter().any(|operation| {
        operation.kind() == ApplicationRoleOperationKind::Query && operation.name() == "TicketPage"
    }));
    assert!(role.operations().iter().any(|operation| {
        operation.kind() == ApplicationRoleOperationKind::Query && operation.name() == "BoardPage50"
    }));
    assert!(role.operations().iter().any(|operation| {
        operation.kind() == ApplicationRoleOperationKind::Query
            && operation.name() == "BoardPage200"
    }));
    assert!(role.operations().iter().any(|operation| {
        operation.kind() == ApplicationRoleOperationKind::Query
            && operation.name() == "BoardPage450"
    }));
    for command in [
        "CloseTicketWithComment",
        "OpenTicketWithLabels",
        "SwapMemberRoles",
    ] {
        assert!(role.operations().iter().any(|operation| {
            operation.kind() == ApplicationRoleOperationKind::Command && operation.name() == command
        }));
    }

    let grant = role.internal_grant();
    assert!(matches!(grant.partition_scope(), PartitionScopeV1::All));
    assert!(grant.approval_required().is_empty());
    assert!(!grant.field_visibility().is_empty());
    assert!(grant.permissions().as_slice().iter().all(|permission| {
        matches!(
            permission,
            CapabilityPermissionV1::Unparameterized(CapabilityPermissionKindV1::ReadContract)
                | CapabilityPermissionV1::ExecuteNamedQuery(..)
                | CapabilityPermissionV1::InvokeCommand(..)
                | CapabilityPermissionV1::ApplicationRoleIdentity(..)
        )
    }));
    assert!(grant.permissions().as_slice().iter().any(|permission| {
        matches!(
            permission,
            CapabilityPermissionV1::ApplicationRoleIdentity(hash) if *hash == role.identity()
        )
    }));
    assert!(
        grant
            .permissions()
            .contains_kind(CapabilityPermissionKindV1::ReadContract)
    );
    for forbidden in [
        CapabilityPermissionKindV1::ReadEntity,
        CapabilityPermissionKindV1::ScanIndex,
        CapabilityPermissionKindV1::CheckAdHocQuery,
        CapabilityPermissionKindV1::ExplainAdHocQuery,
        CapabilityPermissionKindV1::ExecuteAdHocQuery,
        CapabilityPermissionKindV1::CreateCapability,
        CapabilityPermissionKindV1::AdministerCapabilities,
    ] {
        assert!(!grant.permissions().contains_kind(forbidden));
    }
}

#[test]
fn role_identity_and_authority_fail_closed_on_substitution() {
    let (manifest, contract, module) = exact_application();
    let original = compile_application_role(
        &manifest,
        "TicketDeskAgent",
        None,
        &contract,
        std::slice::from_ref(&module),
    )
    .expect("original");
    let application = compile_application_role(
        &manifest,
        "TicketDeskApplication",
        None,
        &contract,
        std::slice::from_ref(&module),
    )
    .expect("second role");
    assert_ne!(original.identity(), application.identity());

    let stale = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("ticketdesk").expect("name"),
            QueryModuleVersion::new(2).expect("version"),
            QUERIES
                .into_iter()
                .map(|(name, source)| NamedQuerySource::new(name, source).expect("query"))
                .collect(),
        )
        .expect("candidate"),
        &contract,
    )
    .expect("stale module");
    let error = compile_application_role(&manifest, "TicketDeskAgent", None, &contract, &[stale])
        .expect_err("stale module rejected");
    assert_eq!(error.kind(), ApplicationRoleErrorKind::ModuleMismatch);

    let error = compile_application_role(
        &manifest,
        "TicketDeskAgent",
        Some(TenantId::new("injected").expect("tenant")),
        &contract,
        &[module],
    )
    .expect_err("scope injection rejected");
    assert_eq!(
        error.kind(),
        ApplicationRoleErrorKind::TenantBindingMismatch
    );
}
