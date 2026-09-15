//! The same compiled plans and provider engines on independently launched nodes.
// req: REP-002, REP-003
use super::support::*;
use riffdb_client_rust::app_v1;
use riffdb_client_rust::{
    ApplicationContract, ApplicationUuid, ApplicationValue, BearerCredential, CallMetadata,
    NamedQuery, NamedQueryResult, QueryOptions, RiffDbClient, StableApplicationClient, v1,
};
use std::collections::BTreeMap;
use std::time::Duration;

const CONTRACT: &str = r#"
contract FollowerDocuments version 1 {
  entity Document {
    key (org_id: uuid, doc_id: uuid)
    field title: string<256>
    field state: string<32>
    index by_state (org_id, state, doc_id)
    pattern_index by_pattern(
      title, profile unicode_fold_v1,
      operators (equals, starts_with, ends_with, contains, like, ilike, not_like, not_ilike),
      max_source_bytes 500, max_matched_bytes 9000, max_rows 500,
      max_total_matched_bytes 268435456, max_grams_per_row 500,
      max_distinct_grams 65535, max_postings 1048576, max_postings_bytes 67108864,
      max_pattern_bytes 500, max_pattern_atoms 500, max_candidates 500,
      max_verification_bytes 268435456, max_results 499, staleness_slo 60,
      replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000,
      retained_generations 8
    )
    index by_title (org_id, title, doc_id) text_key(title, binary_utf8_v1)
    text_index search((title weight 4), analyzer standard_v1, staleness_slo 60, replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000, result boolean_v1, max_terms 16, max_candidates 10000, max_results 1000)
  }
  aggregate Documents {
    root Document
    partition_by org_id
    conflict_key (org_id, doc_id)
  }
  command CreateDocument {
    input request_key: uuid
    input org_id: uuid
    input doc_id: uuid
    input title: string<256>
    idempotency_key request_key
    create Document(org_id, doc_id) as stored else AlreadyExists {}
    set stored.title = title
    set stored.state = "visible"
    return Created {}
  }
}
"#;

const QUERIES: [(&str, &str); 4] = [
    (
        "ContainsDocuments",
        r#"
query ContainsDocuments($org_id: Document.org_id, $needle: Document.title,
    $limit: Limit<499> = 25, $offset: u64 = 0) {
  many documents from Document where org_id == $org_id && title contains $needle
    order by title asc, doc_id asc take $limit offset $offset
  aggregate total from documents { exact_count() as value }
  return Found { documents: documents { org_id doc_id title } total: total { value } }
  outcomes Found
}"#,
    ),
    (
        "SearchDocuments",
        r#"
query SearchDocuments($org_id: Document.org_id, $needle: Document.title,
    $limit: Limit<499> = 25, $offset: u64 = 0) {
  many documents from Document where org_id == $org_id
    matching(search, conjunction, $needle)
    order by doc_id asc take $limit offset $offset
  return Found { documents: documents { org_id doc_id title } }
  outcomes Found
}"#,
    ),
    (
        "PredicateDocuments",
        r#"
query PredicateDocuments($org_id: Document.org_id, $needle: Document.title,
    $states: Set<Document.state>, $limit: Limit<499> = 25, $offset: u64 = 0) {
  many documents from Document where org_id == $org_id
    && title contains $needle && state not_in $states
    order by state asc, doc_id asc take $limit offset $offset
  aggregate total from documents { exact_count() as value }
  return Found { documents: documents { org_id doc_id title } total: total { value } }
  outcomes Found
}"#,
    ),
    (
        "PatternDocuments",
        r#"
query PatternDocuments($org_id: Document.org_id, $needle: Document.title,
    $limit: Limit<499> = 25, $after: Cursor?) {
  candidates matching_documents: Document.doc_id
    from intersect {
      Document.doc_id using by_state where org_id == $org_id && state == "visible",
      Document.doc_id using by_pattern where org_id == $org_id && title contains $needle,
    }
    within 500 else IntegrityFailure
  many documents from Document where org_id == $org_id && doc_id in matching_documents
    order by title asc, doc_id asc take $limit after $after else IntegrityFailure
  return Found { documents: documents { org_id doc_id title } }
  outcomes Found | IntegrityFailure
}"#,
    ),
];

async fn deploy(client: &mut RiffDbClient, metadata: &CallMetadata) -> [u8; 32] {
    let deployed = client
        .deploy_contract(
            v1::DeployContractRequest {
                request_id: request_id(20),
                source: CONTRACT.into(),
                ..Default::default()
            },
            metadata,
        )
        .await
        .unwrap();
    let Some(v1::deploy_contract_response::Result::Activated(contract)) = deployed.result else {
        panic!("document contract activation failed: {deployed:?}")
    };
    let mut queries = QUERIES
        .iter()
        .map(|(name, source)| app_v1::NamedQuerySource {
            name: (*name).into(),
            source: (*source).into(),
        })
        .collect::<Vec<_>>();
    queries.sort_by(|left, right| left.name.cmp(&right.name));
    let bundle = riffdb_contract_compiler::compile_contract_source(CONTRACT).unwrap();
    let candidate = riffdb_query_module::QueryModuleCandidate::new(
        riffdb_types::QueryModuleName::new("documents").unwrap(),
        riffdb_types::QueryModuleVersion::new(1).unwrap(),
        queries
            .iter()
            .map(|query| {
                riffdb_query_module::NamedQuerySource::new(&query.name, &query.source).unwrap()
            })
            .collect(),
    )
    .unwrap();
    let compiled = riffdb_query_module::QueryModule::compile(candidate, &bundle).unwrap();
    for query in compiled.queries() {
        match query.name() {
            "ContainsDocuments" => assert!(query.exact_text_result().is_some()),
            "SearchDocuments" => assert!(query.tokenized_text_result().is_some()),
            "PredicateDocuments" => assert!(query.exact_predicate_result().is_some()),
            "PatternDocuments" => assert!(query.ordinary_program().unwrap().steps().iter().any(
                |step| matches!(
                    step.access(),
                    riffdb_query_ir::QueryAccessKind::LongPatternCandidate { .. }
                )
            )),
            other => panic!("unexpected query {other}"),
        }
    }
    let result = client
        .deploy_query_module(
            app_v1::DeployQueryModuleRequest {
                contract: Some(app_v1::ContractSelector {
                    lineage: "FollowerDocuments".into(),
                    version: 1,
                    bundle_hash: contract.bundle_hash,
                }),
                module_name: "documents".into(),
                module_version: 1,
                queries,
                request_id: request_id(21),
                expected_active: Some(
                    app_v1::deploy_query_module_request::ExpectedActive::AbsentActive(true),
                ),
            },
            metadata,
        )
        .await
        .unwrap();
    assert_eq!(
        result.outcome,
        app_v1::QueryModuleDeploymentOutcome::Activated as i32,
        "{result:?}"
    );
    result.module.unwrap().module_hash.try_into().unwrap()
}

async fn authority(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    module: [u8; 32],
) -> CallMetadata {
    let bundle = riffdb_contract_compiler::compile_contract_source(CONTRACT).unwrap();
    let mut permissions = vec![v1::CapabilityPermission {
        permission: Some(v1::capability_permission::Permission::InvokeCommand(
            v1::LineageScopedStableId {
                contract_lineage: "FollowerDocuments".into(),
                stable_id: bundle.commands()[0].command_id().get(),
            },
        )),
    }];
    let mut names = QUERIES.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    names.sort_by_key(|name| (name.len(), *name));
    permissions.extend(names.iter().map(|name| v1::CapabilityPermission {
        permission: Some(v1::capability_permission::Permission::ExecuteNamedQuery(
            v1::NamedQueryPermission {
                contract_lineage: "FollowerDocuments".into(),
                query_module_hash: module.to_vec(),
                query_name: (*name).into(),
            },
        )),
    }));
    permissions.push(v1::CapabilityPermission {
        permission: Some(
            v1::capability_permission::Permission::ApplicationRoleIdentity(vec![0x55; 32]),
        ),
    });
    let result = client
        .create_capability(
            v1::CreateCapabilityRequest {
                request_id: request_id(22),
                mode: v1::CapabilityCreateMode::Normal as i32,
                capability_id: capability_id(22).as_bytes().to_vec(),
                principal_id: "document-reader".into(),
                actor_kind: v1::ActorKind::Service as i32,
                requested_lifetime_seconds: 3600,
                audiences: vec!["replication-process-test".into()],
                grant: Some(v1::CapabilityGrant {
                    tenant_scope: Some(v1::TenantScope {
                        scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
                    }),
                    partition_scope: Some(v1::PartitionScope {
                        scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
                    }),
                    permissions,
                    field_visibility: bundle
                        .schema()
                        .entities()
                        .iter()
                        .map(|entity| v1::EntityFieldVisibility {
                            contract_lineage: "FollowerDocuments".into(),
                            entity_type_id: entity.id().get(),
                            field_ids: entity
                                .record()
                                .fields()
                                .iter()
                                .map(|field| field.id().get())
                                .collect(),
                            secret_field_ids: vec![],
                        })
                        .collect(),
                    max_scan_rows: 1000,
                    ..Default::default()
                }),
            },
            metadata,
        )
        .await
        .unwrap();
    let Some(v1::create_capability_response::Result::Normal(result)) = result.result else {
        panic!("normal capability")
    };
    let Some(v1::normal_create_capability_result::Result::Created(result)) = result.result else {
        panic!("created capability")
    };
    CallMetadata::authenticated(BearerCredential::new(&result.token).unwrap())
}

async fn write(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    id: u8,
    org: u8,
    title: &str,
) -> u64 {
    let fields = [
        ("doc_id", v1::value::Kind::UuidValue(vec![id; 16])),
        ("org_id", v1::value::Kind::UuidValue(vec![org; 16])),
        ("request_key", v1::value::Kind::UuidValue(vec![id; 16])),
        ("title", v1::value::Kind::StringValue(title.into())),
    ]
    .into_iter()
    .map(|(name, kind)| v1::ValueField {
        name: name.into(),
        field_id: None,
        value: Some(v1::Value { kind: Some(kind) }),
    })
    .collect();
    let request = v1::ExecuteCommandRequest {
        request_id: request_id(id),
        command_name: "CreateDocument".into(),
        expected_contract_version: Some(1),
        input: Some(v1::Value {
            kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord { fields })),
        }),
    };
    let response = client.execute(request.clone(), metadata).await.unwrap();
    assert_eq!(response.outcome_type, "Created");
    assert_eq!(
        response.status,
        v1::execute_command_response::CompletionStatus::Committed as i32
    );
    assert!(!response.provenance_uri.is_empty());
    let mut replay_request = request;
    replay_request.request_id = request_id(id + 100);
    let replay = client.execute(replay_request, metadata).await.unwrap();
    assert_eq!(
        replay.status,
        v1::execute_command_response::CompletionStatus::Replayed as i32
    );
    assert_eq!(response.commit_sequence, replay.commit_sequence);
    assert_eq!(response.outcome, replay.outcome);
    response.commit_sequence
}

async fn query(
    client: &mut StableApplicationClient,
    metadata: &CallMetadata,
    name: &str,
    module: [u8; 32],
    head: u64,
) -> NamedQueryResult {
    let mut parameters = BTreeMap::from([
        (
            "org_id".into(),
            ApplicationValue::Uuid(ApplicationUuid::from_bytes([0x11; 16])),
        ),
        ("needle".into(), ApplicationValue::String("alpha".into())),
    ]);
    if name == "PredicateDocuments" {
        parameters.insert(
            "states".into(),
            ApplicationValue::List(vec![ApplicationValue::String("hidden".into())]),
        );
    }
    let query = NamedQuery::new(
        ApplicationContract::Active,
        name,
        Some(module),
        parameters,
        None,
    )
    .unwrap()
    .with_options(
        QueryOptions::new()
            .read_after_commit(head)
            .at_least_admission_head(),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match client.execute_named_query(query.clone(), metadata).await {
                Ok(result) => break result,
                Err(error) => {
                    assert_eq!(
                        error.semantic_error().map(|error| error.code()),
                        Some(riffdb_errors::ApplicationErrorCode::QueryUnavailable),
                        "{error:?}"
                    );
                    tokio::task::yield_now().await;
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{name} provider did not reach application head {head}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_exact_providers_match_primary_after_tail_and_restart() {
    let fixture = Fixture::new();
    let mut initial = fixture.start("primary", None);
    stop(&mut initial);
    let (lineage, admin, _) = seed_primary(&fixture.database("primary"));
    let baseline = super::oracle::baseline(&fixture.database("primary"));
    let mut primary = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    let replication = create_replication_capability(&mut client, &admin).await;
    let module = deploy(&mut client, &admin).await;
    let reader = authority(&mut client, &admin, module).await;
    write(&mut client, &reader, 30, 0x11, "alpha document").await;
    write(&mut client, &reader, 31, 0x11, "beta document").await;
    let mut head = write(
        &mut client,
        &reader,
        32,
        0x22,
        "alpha private other partition",
    )
    .await;
    fixture.configure_follower(lineage, &replication);
    let mut follower = fixture.start("follower", Some("follower"));
    super::wait_for_commit(&fixture, &admin, head).await;
    let mut primary_queries = fixture.application_client().await;
    let mut follower_queries = fixture.application_client_for("follower").await;
    for phase in 0..3 {
        if phase == 1 {
            head = write(&mut client, &reader, 33, 0x11, "alpha newer document").await;
            super::wait_for_commit(&fixture, &admin, head).await;
        }
        if phase == 2 {
            stop(&mut follower);
            follower = fixture.start("follower", Some("follower"));
            follower_queries = fixture.application_client_for("follower").await;
        }
        for (name, _) in QUERIES {
            let expected = query(&mut primary_queries, &reader, name, module, head).await;
            let actual = query(&mut follower_queries, &reader, name, module, head).await;
            assert_eq!(expected.application_head, head);
            assert_eq!(actual.application_head, head);
            assert_eq!(actual.fields, expected.fields);
            assert_eq!(actual.identity, expected.identity);
            assert_eq!(actual.outcome, "Found");
            assert_eq!(
                actual.fields["documents"].records.len(),
                if phase == 0 { 1 } else { 2 }
            );
            assert_eq!(
                actual.fields["documents"].records[0].fields["title"],
                ApplicationValue::String("alpha document".into())
            );
        }
    }
    stop(&mut follower);
    stop(&mut primary);
    super::oracle::compare(
        baseline,
        &fixture.database("primary"),
        &fixture.database("follower"),
        head,
    );
}
