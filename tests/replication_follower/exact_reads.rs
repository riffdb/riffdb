//! The same compiled plans and provider engines on independently launched nodes.
// req: REP-002, REP-003, REP-004, PRJ-001, PRJ-002, PRJ-004
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
    field rank: optional<u64>
    delete_policy no_inbound
    index by_state (org_id, state, doc_id)
    index by_rank (org_id, rank, doc_id) presence(rank)
    index by_state_title (org_id, state, title, doc_id) text_key(title, binary_utf8_v1)
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
    set stored.rank = 1
    return Created {}
  }
  command UpdateDocument {
    input request_key: uuid
    input org_id: uuid
    input doc_id: uuid
    input title: string<256>
    input state: string<32>
    idempotency_key request_key
    mutate Document(org_id, doc_id) as stored else Missing {}
    set stored.title = title
    set stored.state = state
    return Updated {}
  }
  command SetRank {
    input request_key: uuid
    input org_id: uuid
    input doc_id: uuid
    input rank: optional<u64>
    idempotency_key request_key
    mutate Document(org_id, doc_id) as stored else Missing {}
    set stored.rank = rank
    return Ranked {}
  }
  command DeleteDocument {
    input request_key: uuid
    input org_id: uuid
    input doc_id: uuid
    idempotency_key request_key
    delete Document(org_id, doc_id) as stored else Missing {}
    return Deleted {}
  }
}
"#;

const QUERIES: [(&str, &str); 6] = [
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
        "FilteredDocuments",
        r#"
query FilteredDocuments($org_id: Document.org_id, $needle: Document.title,
    $state: Document.state?, $limit: Limit<499> = 25, $offset: u64 = 0) {
  many documents from Document where org_id == $org_id
    && when $state { state == $state } && title contains $needle
    order by title asc, doc_id asc take $limit offset $offset
  aggregate total from documents { exact_count() as value }
  return Found { documents: documents { org_id doc_id title } total: total { value } }
  outcomes Found
}"#,
    ),
    (
        "NullableDocuments",
        r#"
query NullableDocuments($org_id: Document.org_id, $needle: Document.title,
    $states: Set<Document.state>, $limit: Limit<499> = 25, $offset: u64 = 0) {
  many documents from Document where org_id == $org_id
    && title contains $needle && state not_in $states
    order by rank desc nulls last, doc_id asc take $limit offset $offset
  aggregate total from documents { exact_count() as value }
  return Found { documents: documents { org_id doc_id title rank } total: total { value } }
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
    deploy_source(client, metadata, CONTRACT).await
}

async fn deploy_source(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    source: &str,
) -> [u8; 32] {
    let deployed = client
        .deploy_contract(
            v1::DeployContractRequest {
                request_id: request_id(20),
                source: source.into(),
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
    let bundle = riffdb_contract_compiler::compile_contract_source(source).unwrap();
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
            "FilteredDocuments" => assert!(query.exact_text_result().unwrap().filter().is_some()),
            "NullableDocuments" => assert!(query.nullable_exact_predicate_result().is_some()),
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
    authority_source(client, metadata, module, CONTRACT).await
}

async fn authority_source(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    module: [u8; 32],
    source: &str,
) -> CallMetadata {
    let bundle = riffdb_contract_compiler::compile_contract_source(source).unwrap();
    let mut permissions = bundle
        .commands()
        .iter()
        .map(|command| v1::CapabilityPermission {
            permission: Some(v1::capability_permission::Permission::InvokeCommand(
                v1::LineageScopedStableId {
                    contract_lineage: "FollowerDocuments".into(),
                    stable_id: command.command_id().get(),
                },
            )),
        })
        .collect::<Vec<_>>();
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
                principal_id: "11111111-1111-7111-9111-111111111111".into(),
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
                    row_policy: (!bundle.row_policies().policies().is_empty()).then(|| {
                        v1::CapabilityRowPolicyGrant {
                            application_role_hash: vec![0x55; 32],
                            principal_facts: vec![],
                            policies: bundle
                                .row_policies()
                                .policies()
                                .iter()
                                .map(|policy| v1::CapabilityRowPolicyBinding {
                                    contract_lineage: "FollowerDocuments".into(),
                                    policy_name: policy.name().into(),
                                    entity_type_id: policy.entity().get(),
                                    operations: vec![
                                        v1::CapabilityRowPolicyOperation::Read as i32,
                                        v1::CapabilityRowPolicyOperation::Create as i32,
                                        v1::CapabilityRowPolicyOperation::Update as i32,
                                        v1::CapabilityRowPolicyOperation::Delete as i32,
                                    ],
                                })
                                .collect(),
                        }
                    }),
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
    execute_checked(
        client,
        metadata,
        create_request(id, org, title),
        id,
        "Created",
    )
    .await
}

fn create_request(id: u8, org: u8, title: &str) -> v1::ExecuteCommandRequest {
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
    v1::ExecuteCommandRequest {
        request_id: request_id(id),
        command_name: "CreateDocument".into(),
        expected_contract_version: Some(1),
        input: Some(v1::Value {
            kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord { fields })),
        }),
    }
}

async fn execute_checked(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    request: v1::ExecuteCommandRequest,
    id: u8,
    outcome: &str,
) -> u64 {
    let response = client
        .execute(request.clone(), metadata)
        .await
        .unwrap_or_else(|error| panic!("{} fixture request {id}: {error:?}", request.command_name));
    assert_eq!(response.outcome_type, outcome);
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

async fn change_document(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    request: u8,
    doc: u8,
    org: u8,
    replacement: Option<(&str, &str)>,
) -> u64 {
    let mut fields = vec![
        ("doc_id", v1::value::Kind::UuidValue(vec![doc; 16])),
        ("org_id", v1::value::Kind::UuidValue(vec![org; 16])),
        ("request_key", v1::value::Kind::UuidValue(vec![request; 16])),
    ];
    let (command, outcome) = if let Some((title, state)) = replacement {
        fields.push(("state", v1::value::Kind::StringValue(state.into())));
        fields.push(("title", v1::value::Kind::StringValue(title.into())));
        ("UpdateDocument", "Updated")
    } else {
        ("DeleteDocument", "Deleted")
    };
    let fields = fields
        .into_iter()
        .map(|(name, kind)| v1::ValueField {
            name: name.into(),
            field_id: None,
            value: Some(v1::Value { kind: Some(kind) }),
        })
        .collect();
    execute_checked(
        client,
        metadata,
        v1::ExecuteCommandRequest {
            request_id: request_id(request),
            command_name: command.into(),
            expected_contract_version: Some(1),
            input: Some(v1::Value {
                kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord { fields })),
            }),
        },
        request,
        outcome,
    )
    .await
}

async fn set_rank(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    request: u8,
    doc: u8,
    rank: Option<u64>,
) -> u64 {
    let fields = [
        ("doc_id", v1::value::Kind::UuidValue(vec![doc; 16])),
        ("org_id", v1::value::Kind::UuidValue(vec![0x11; 16])),
        (
            "rank",
            rank.map_or(v1::value::Kind::NullValue(0), v1::value::Kind::U64Value),
        ),
        ("request_key", v1::value::Kind::UuidValue(vec![request; 16])),
    ]
    .into_iter()
    .map(|(name, kind)| v1::ValueField {
        name: name.into(),
        field_id: None,
        value: Some(v1::Value { kind: Some(kind) }),
    })
    .collect();
    execute_checked(
        client,
        metadata,
        v1::ExecuteCommandRequest {
            request_id: request_id(request),
            command_name: "SetRank".into(),
            expected_contract_version: Some(1),
            input: Some(v1::Value {
                kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord { fields })),
            }),
        },
        request,
        "Ranked",
    )
    .await
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
    if matches!(name, "PredicateDocuments" | "NullableDocuments") {
        parameters.insert(
            "states".into(),
            ApplicationValue::List(vec![ApplicationValue::String("hidden".into())]),
        );
    }
    if name == "FilteredDocuments" {
        parameters.insert("state".into(), ApplicationValue::String("visible".into()));
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
        let started = std::time::Instant::now();
        let mut refused = 0_u64;
        loop {
            match client.execute_named_query(query.clone(), metadata).await {
                Ok(result) => {
                    eprintln!("exact-provider-read-v1 name={name} head={head} refusals={refused} elapsed_us={}", started.elapsed().as_micros());
                    break result;
                },
                Err(error) => {
                    refused += 1;
                    // Authoritative replication and provider publication have
                    // separate frontiers. A ready prior generation can report
                    // FreshnessUnsatisfied while its successor is prepared;
                    // never accept that generation as a successful result.
                    assert!(
                        matches!(
                            error.semantic_error().map(|error| error.code()),
                            Some(
                                riffdb_errors::ApplicationErrorCode::QueryUnavailable
                                    | riffdb_errors::ApplicationErrorCode::FreshnessUnsatisfied
                            )
                        ),
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
    #[cfg(feature = "test-fixtures")]
    let mut primary = start_observed(&fixture, None);
    #[cfg(not(feature = "test-fixtures"))]
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
    let mut obsolete_cursor = None;
    let mut primary_generations = None;
    for phase in 0..3 {
        if phase == 1 {
            head = write(&mut client, &reader, 33, 0x11, "alpha newer document").await;
            // A sustained sequence of fitting same-specification updates must
            // retain the provider generation. A full partition rebuild assigns
            // a successor generation and therefore fails the oracle below.
            for request in 34..50 {
                head = write(
                    &mut client,
                    &reader,
                    request,
                    0x11,
                    "beta unselected document",
                )
                .await;
            }
            super::wait_for_commit(&fixture, &admin, head).await;
        }
        if phase == 2 {
            stop(&mut follower);
            follower = fixture.start("follower", Some("follower"));
            follower_queries = fixture.application_client_for("follower").await;
            assert_cursor_refused(
                &mut follower_queries,
                &reader,
                module,
                head,
                obsolete_cursor
                    .take()
                    .expect("cursor from the earlier process"),
            )
            .await;
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
        let observed = exact_checkpoint_generations(&fixture.projections("primary"), head);
        if let Some(expected) = &primary_generations {
            assert_eq!(
                &observed, expected,
                "fitting updates must use receipt replay, not full rebuilds"
            );
        } else {
            assert_eq!(
                observed.len(),
                4,
                "binary/filtered text and present/nullable predicate providers"
            );
            primary_generations = Some(observed);
        }
        if phase == 1 {
            let request = pattern_page(module, head, None);
            let primary_page = primary_queries
                .execute_named_query(request.clone(), &reader)
                .await
                .unwrap();
            let follower_page = follower_queries
                .execute_named_query(request.clone(), &reader)
                .await
                .unwrap();
            assert_eq!(primary_page.fields, follower_page.fields);
            assert_eq!(follower_page.fields["documents"].records.len(), 1);
            let primary_cursor = primary_page.next_cursor.expect("primary has a second row");
            let follower_cursor = follower_page
                .next_cursor
                .expect("follower has a second row");
            // The same-lineage source handle is still foreign to this process.
            assert_cursor_refused(
                &mut follower_queries,
                &reader,
                module,
                head,
                primary_cursor.clone(),
            )
            .await;
            let primary_tail = primary_queries
                .execute_named_query(pattern_page(module, head, Some(primary_cursor)), &reader)
                .await
                .unwrap();
            let follower_tail = follower_queries
                .execute_named_query(pattern_page(module, head, Some(follower_cursor)), &reader)
                .await
                .unwrap();
            assert_eq!(primary_tail.fields, follower_tail.fields);
            assert_eq!(follower_tail.fields["documents"].records.len(), 1);
            assert_eq!(
                follower_tail.fields["documents"].records[0].fields["title"],
                ApplicationValue::String("alpha newer document".into())
            );
            assert!(primary_tail.next_cursor.is_none());
            assert!(follower_tail.next_cursor.is_none());
            // Retain an actually published, still-live continuation across restart.
            obsolete_cursor = follower_queries
                .execute_named_query(request, &reader)
                .await
                .unwrap()
                .next_cursor;
            assert!(obsolete_cursor.is_some());
        }
    }
    // A filter-only update keeps the text posting but changes membership in
    // V3/V4/V5; changing it back must not duplicate any posting or count.
    for (request, state) in [(60, "hidden"), (61, "visible")] {
        head = change_document(
            &mut client,
            &reader,
            request,
            33,
            0x11,
            Some(("alpha newer document", state)),
        )
        .await;
        super::wait_for_commit(&fixture, &admin, head).await;
        for (name, _) in QUERIES {
            let expected = query(&mut primary_queries, &reader, name, module, head).await;
            let actual = query(&mut follower_queries, &reader, name, module, head).await;
            assert_eq!(actual.fields, expected.fields);
            let count =
                if state == "hidden" && !matches!(name, "ContainsDocuments" | "SearchDocuments") {
                    1
                } else {
                    2
                };
            assert_eq!(actual.fields["documents"].records.len(), count, "{name}");
        }
    }
    // Null placement and order-only changes pass through the real V5 worker,
    // independently checked against the follower's complete rebuild.
    for (request, doc, rank, first_doc) in [
        (62, 30, None, 33),
        (63, 33, None, 30),
        (64, 33, Some(5), 33),
        (65, 30, Some(10), 30),
    ] {
        head = set_rank(&mut client, &reader, request, doc, rank).await;
        super::wait_for_commit(&fixture, &admin, head).await;
        let expected = query(
            &mut primary_queries,
            &reader,
            "NullableDocuments",
            module,
            head,
        )
        .await;
        let actual = query(
            &mut follower_queries,
            &reader,
            "NullableDocuments",
            module,
            head,
        )
        .await;
        assert_eq!(actual.fields, expected.fields);
        assert_eq!(actual.fields["documents"].records.len(), 2);
        assert_eq!(
            actual.fields["documents"].records[0].fields["doc_id"],
            ApplicationValue::Uuid(ApplicationUuid::from_bytes([first_doc; 16]))
        );
        let changed = actual.fields["documents"]
            .records
            .iter()
            .find(|row| {
                row.fields["doc_id"]
                    == ApplicationValue::Uuid(ApplicationUuid::from_bytes([doc; 16]))
            })
            .unwrap();
        assert_eq!(
            changed.fields["rank"],
            rank.map_or(ApplicationValue::Null, ApplicationValue::U64)
        );
    }
    // The follower has no retained source receipts and reconstructs each
    // frontier from authoritative rows. It is an independent full-rebuild
    // oracle for the primary's replacement/removal replay.
    for (request, replacement, expected_rows) in [
        (70, Some(("beta replaced", "hidden")), 1),
        (71, Some(("alpha restored", "visible")), 2),
        (72, None, 1),
    ] {
        head = change_document(&mut client, &reader, request, 33, 0x11, replacement).await;
        super::wait_for_commit(&fixture, &admin, head).await;
        for (name, _) in QUERIES {
            let expected = query(&mut primary_queries, &reader, name, module, head).await;
            let actual = query(&mut follower_queries, &reader, name, module, head).await;
            assert_eq!(actual.fields, expected.fields);
            assert_eq!(actual.fields["documents"].records.len(), expected_rows);
        }
        assert_eq!(
            Some(exact_checkpoint_generations(
                &fixture.projections("primary"),
                head
            )),
            primary_generations
        );
    }
    // A same-entity write in another partition advances the proven prefix but
    // cannot enter this partition's membership, count or order structures.
    head = change_document(
        &mut client,
        &reader,
        73,
        32,
        0x22,
        Some(("alpha outside changed", "visible")),
    )
    .await;
    super::wait_for_commit(&fixture, &admin, head).await;
    for (name, _) in QUERIES {
        let expected = query(&mut primary_queries, &reader, name, module, head).await;
        let actual = query(&mut follower_queries, &reader, name, module, head).await;
        assert_eq!(actual.fields, expected.fields);
        assert_eq!(actual.fields["documents"].records.len(), 1);
    }
    assert_eq!(
        Some(exact_checkpoint_generations(
            &fixture.projections("primary"),
            head
        )),
        primary_generations
    );
    #[cfg(feature = "test-fixtures")]
    {
        let counts: BTreeMap<String, u64> = serde_json::from_slice(
            &std::fs::read(fixture.projections("primary").with_extension("counts")).unwrap(),
        )
        .unwrap();
        assert_eq!(counts.len(), 4);
        assert!(
            counts.values().all(|reads| *reads == 1),
            "only initial full partition reads are permitted: {counts:?}"
        );
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

// req: PRJ-001, PRJ-002, PRJ-004, RAP-001, RAP-009
#[cfg(feature = "test-fixtures")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_delta_policy_transitions_match_full_rebuild_without_partition_reads() {
    let source = CONTRACT.replacen(
        "  command CreateDocument",
        r#"
  row policy DocumentAccess on Document {
    allow read when state == "visible"
    allow create when true
    allow update when true
    allow delete when true
  }
  command CreateDocument"#,
        1,
    );
    let fixture = Fixture::new();
    let mut initial = fixture.start("primary", None);
    stop(&mut initial);
    let (lineage, admin, _) = seed_primary(&fixture.database("primary"));
    let mut primary = start_observed(&fixture, None);
    let mut client = fixture.client("primary").await;
    let replication = create_replication_capability(&mut client, &admin).await;
    let module = deploy_source(&mut client, &admin, &source).await;
    let reader = authority_source(&mut client, &admin, module, &source).await;
    write(&mut client, &reader, 30, 0x11, "alpha original").await;
    let mut head = write(&mut client, &reader, 31, 0x11, "alpha changed").await;
    fixture.configure_follower(lineage, &replication);
    let mut follower = fixture.start("follower", Some("follower"));
    let mut primary_queries = fixture.application_client().await;
    let mut follower_queries = fixture.application_client_for("follower").await;
    for (request, state, expected_count) in [
        (60, None, 2),
        (61, Some("hidden"), 1),
        (62, Some("visible"), 2),
    ] {
        if let Some(state) = state {
            head = change_document(
                &mut client,
                &reader,
                request,
                31,
                0x11,
                Some(("alpha changed", state)),
            )
            .await;
        }
        super::wait_for_commit(&fixture, &admin, head).await;
        for name in [
            "ContainsDocuments",
            "FilteredDocuments",
            "PredicateDocuments",
            "NullableDocuments",
        ] {
            let expected = query(&mut primary_queries, &reader, name, module, head).await;
            let actual = query(&mut follower_queries, &reader, name, module, head).await;
            assert_eq!(actual.fields, expected.fields);
            assert_eq!(actual.fields["documents"].records.len(), expected_count);
            assert_eq!(
                actual.fields["total"].records[0].fields["value"],
                ApplicationValue::U64(expected_count as u64)
            );
        }
    }
    let counts: BTreeMap<String, u64> = serde_json::from_slice(
        &std::fs::read(fixture.projections("primary").with_extension("counts")).unwrap(),
    )
    .unwrap();
    assert_eq!(counts.len(), 4);
    assert!(counts.values().all(|reads| *reads == 1), "{counts:?}");
    // Exercise the literal-true delete rule through the same committed/replayed
    // provenance checks, then recover both protected provider selections.
    head = change_document(&mut client, &reader, 64, 31, 0x11, None).await;
    super::wait_for_commit(&fixture, &admin, head).await;
    stop(&mut follower);
    stop(&mut primary);
    let mut primary = fixture.start("primary", None);
    let mut follower = fixture.start("follower", Some("follower"));
    let mut primary_queries = fixture.application_client().await;
    let mut follower_queries = fixture.application_client_for("follower").await;
    for name in [
        "ContainsDocuments",
        "FilteredDocuments",
        "PredicateDocuments",
        "NullableDocuments",
    ] {
        let expected = query(&mut primary_queries, &reader, name, module, head).await;
        let actual = query(&mut follower_queries, &reader, name, module, head).await;
        assert_eq!(actual.fields, expected.fields);
        assert_eq!(actual.fields["documents"].records.len(), 1);
        assert_eq!(
            actual.fields["total"].records[0].fields["value"],
            ApplicationValue::U64(1)
        );
    }
    stop(&mut follower);
    stop(&mut primary);
}

// req: PRJ-001, PRJ-002, PRJ-004
#[cfg(feature = "test-fixtures")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_delta_indexed_policy_dependency_rebuilds_unchanged_rows() {
    let source = CONTRACT
        .replacen(
            "  entity Document {",
            r#"
  entity Organization { key (org_id: uuid) }
  entity DocumentGrant {
    key (org_id: uuid, doc_id: uuid)
    delete_policy no_inbound
    index by_document (org_id, doc_id)
  }
  entity Document {"#,
            1,
        )
        .replacen(
            "    root Document",
            "    root Organization\n    child Document\n    child DocumentGrant",
            1,
        )
        .replacen(
            "    conflict_key (org_id, doc_id)",
            "    conflict_key (org_id)",
            1,
        )
        .replacen(
            "  command CreateDocument",
            r#"
  row policy DocumentAccess on Document {
    allow read when state == "visible" || exists DocumentGrant.by_document(org_id, doc_id)
    allow create when true
    allow update when true
    allow delete when true
  }
  command CreateGrant {
    input request_key: uuid
    input org_id: uuid
    input doc_id: uuid
    idempotency_key request_key
    create DocumentGrant(org_id, doc_id) as stored else AlreadyExists {}
    return Granted {}
  }
  command DeleteGrant {
    input request_key: uuid
    input org_id: uuid
    input doc_id: uuid
    idempotency_key request_key
    delete DocumentGrant(org_id, doc_id) as stored else Missing {}
    return Revoked {}
  }
  command CreateDocument"#,
            1,
        );
    let fixture = Fixture::new();
    let mut initial = fixture.start("primary", None);
    stop(&mut initial);
    let (lineage, admin, _) = seed_primary(&fixture.database("primary"));
    let mut primary = start_observed(&fixture, None);
    let mut client = fixture.client("primary").await;
    let replication = create_replication_capability(&mut client, &admin).await;
    let module = deploy_source(&mut client, &admin, &source).await;
    let reader = authority_source(&mut client, &admin, module, &source).await;
    write(&mut client, &reader, 30, 0x11, "alpha original").await;
    let mut head = change_document(
        &mut client,
        &reader,
        60,
        30,
        0x11,
        Some(("alpha original", "hidden")),
    )
    .await;
    fixture.configure_follower(lineage, &replication);
    let mut follower = fixture.start("follower", Some("follower"));
    let mut primary_queries = fixture.application_client().await;
    let mut follower_queries = fixture.application_client_for("follower").await;
    for (phase, expected_rows) in [(0, 0), (1, 1), (2, 0)] {
        if phase > 0 {
            let id = 60 + phase;
            let fields = [("doc_id", 30), ("org_id", 0x11), ("request_key", id)]
                .into_iter()
                .map(|(name, value)| v1::ValueField {
                    name: name.into(),
                    field_id: None,
                    value: Some(v1::Value {
                        kind: Some(v1::value::Kind::UuidValue(vec![value; 16])),
                    }),
                })
                .collect();
            let (command, outcome) = if phase == 1 {
                ("CreateGrant", "Granted")
            } else {
                ("DeleteGrant", "Revoked")
            };
            head = execute_checked(
                &mut client,
                &reader,
                v1::ExecuteCommandRequest {
                    request_id: request_id(id),
                    command_name: command.into(),
                    expected_contract_version: Some(1),
                    input: Some(v1::Value {
                        kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord { fields })),
                    }),
                },
                id,
                outcome,
            )
            .await;
        }
        super::wait_for_commit(&fixture, &admin, head).await;
        for name in [
            "ContainsDocuments",
            "FilteredDocuments",
            "PredicateDocuments",
            "NullableDocuments",
        ] {
            let expected = query(&mut primary_queries, &reader, name, module, head).await;
            let actual = query(&mut follower_queries, &reader, name, module, head).await;
            assert_eq!(actual.fields, expected.fields);
            // The unfiltered query exposes policy membership; the remaining
            // query predicates independently exclude the hidden row.
            let count = if name == "ContainsDocuments" {
                expected_rows
            } else {
                0
            };
            assert_eq!(actual.fields["documents"].records.len(), count);
            assert_eq!(
                actual.fields["total"].records[0].fields["value"],
                ApplicationValue::U64(count as u64)
            );
        }
        let counts: BTreeMap<String, u64> = serde_json::from_slice(
            &std::fs::read(fixture.projections("primary").with_extension("counts")).unwrap(),
        )
        .unwrap();
        assert_eq!(counts.len(), 4);
        assert!(
            counts.values().all(|reads| *reads == u64::from(phase) + 1),
            "dependency changes must recompute unchanged row admission: {counts:?}"
        );
    }
    stop(&mut follower);
    stop(&mut primary);
}

#[cfg(feature = "test-fixtures")]
fn start_observed(
    fixture: &Fixture,
    abort: Option<(&str, u64, &str)>,
) -> riffdb_testkit_server::process::ChildProcessController {
    use riffdb_testkit_server::process::{ChildProcessController, ChildProcessSpec};
    let mut spec = ChildProcessSpec::new(env!("CARGO_BIN_EXE_riffdbd-exact-provider-fixture"))
        .unwrap()
        .clear_environment()
        .arg("--config")
        .unwrap()
        .arg(fixture.config("primary"))
        .unwrap()
        .env(
            "RIFFDB_EXACT_COUNTS",
            fixture.projections("primary").with_extension("counts"),
        )
        .unwrap();
    if let Some((point, head, slot)) = abort {
        spec = spec
            .env("RIFFDB_EXACT_SLOT", slot)
            .unwrap()
            .env("RIFFDB_EXACT_ABORT", point)
            .unwrap()
            .env("RIFFDB_EXACT_ABORT_HEAD", head.to_string())
            .unwrap();
    }
    let process = ChildProcessController::spawn(&spec).unwrap();
    process
        .wait_for_readiness("riffdbd-ready-v1\t", Duration::from_secs(30))
        .unwrap();
    process
}

// Independent observation of the unchanged RXAC V1 envelope. The production
// provider decoders validate each complete payload; the envelope digest covers
// its identity, framing and every payload byte. This test never creates state.
fn exact_checkpoint_generations(root: &std::path::Path, head: u64) -> BTreeMap<String, u64> {
    exact_checkpoints(root)
        .into_iter()
        .map(|path| {
            let (_, generation, frontier) = checkpoint_binding(&path);
            assert_eq!(frontier, head);
            (
                path.file_name().unwrap().to_str().unwrap().to_owned(),
                generation,
            )
        })
        .collect()
}

fn exact_checkpoints(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut paths = Vec::new();
    for directory in ["exact-text-v2", "exact-predicate-v4"] {
        let files = std::fs::read_dir(root.join(directory))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(files.len() <= 256);
        paths.extend(files.into_iter().map(|file| file.path()).filter(|path| {
            matches!(
                path.extension().and_then(|value| value.to_str()),
                Some("rxts" | "rxps" | "rxp5")
            )
        }));
    }
    paths
}

fn checkpoint_binding(path: &std::path::Path) -> (&'static str, u64, u64) {
    use riffdb_projection::{
        ExactPredicatePartitionIndexV4, ExactPredicatePartitionIndexV5, ExactTextPartitionIndexV2,
        ExactTextPartitionIndexV3,
    };
    use riffdb_types::{HashDomain, hash};
    assert!(std::fs::metadata(path).unwrap().len() <= 64 * 1024 * 1024);
    let bytes = std::fs::read(path).unwrap();
    let header = 16 + (32 + 32 + 32 + 1 + 16 + 8) + 4;
    assert_eq!(&bytes[..8], b"RXAC\0\x01\0\0");
    let length = u32::from_be_bytes(bytes[header - 4..header].try_into().unwrap()) as usize;
    let end = header + length;
    assert_eq!(bytes.len(), end + 32);
    assert_eq!(
        hash(HashDomain::ExactResultCheckpoint, &bytes[..end]).as_bytes(),
        &bytes[end..]
    );
    let payload = &bytes[header..end];
    if let Ok(provider) = ExactTextPartitionIndexV2::from_checkpoint_bytes(payload) {
        (
            "ContainsDocuments",
            provider.generation().get(),
            provider.frontier().unwrap().get(),
        )
    } else if let Ok(provider) = ExactTextPartitionIndexV3::from_checkpoint_bytes(payload) {
        (
            "FilteredDocuments",
            provider.generation().get(),
            provider.frontier().unwrap().get(),
        )
    } else if let Ok(provider) = ExactPredicatePartitionIndexV4::from_checkpoint_bytes(payload) {
        (
            "PredicateDocuments",
            provider.binding().generation().get(),
            provider.binding().frontier().get(),
        )
    } else {
        let provider = ExactPredicatePartitionIndexV5::from_checkpoint_bytes(payload).unwrap();
        (
            "NullableDocuments",
            provider.binding().generation().get(),
            provider.binding().frontier().get(),
        )
    }
}

#[cfg(feature = "test-fixtures")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_provider_checkpoint_crashes_recover_complete_source_bound_results() {
    const NAMES: [&str; 4] = [
        "ContainsDocuments",
        "FilteredDocuments",
        "PredicateDocuments",
        "NullableDocuments",
    ];
    let fixture = Fixture::new();
    let mut initial = fixture.start("primary", None);
    stop(&mut initial);
    let (_, admin, _) = seed_primary(&fixture.database("primary"));
    let mut primary = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    let module = deploy(&mut client, &admin).await;
    let reader = authority(&mut client, &admin, module).await;
    let mut head = write(&mut client, &reader, 30, 0x11, "alpha original").await;
    let mut queries = fixture.application_client().await;
    for name in NAMES {
        query(&mut queries, &reader, name, module, head).await;
    }
    stop(&mut primary);
    let mut rows = 1;
    let mut request_id = 80;
    for name in NAMES {
        for point in [
            "BeforeFileSync",
            "AfterFileSync",
            "BeforeSelection",
            "AfterSelection",
        ] {
            let path = exact_checkpoints(&fixture.projections("primary"))
                .into_iter()
                .find(|path| checkpoint_binding(path).0 == name)
                .unwrap();
            let slot = path.file_name().unwrap().to_str().unwrap();
            let next = head + 1;
            let mut crashed = start_observed(&fixture, Some((point, next, slot)));
            let mut client = fixture.client("primary").await;
            let mut queries = fixture.application_client().await;
            // Register this slot and finish restart's source-bound initial
            // rebuild before appending the exact successor which arms the abort.
            query(&mut queries, &reader, name, module, head).await;
            let request = create_request(request_id, 0x11, "alpha added");
            let _uncertain = client.execute(request.clone(), &reader).await;
            let evidence = crashed
                .wait_for_readiness("riffdb-exact-abort-v1\t", Duration::from_secs(30))
                .unwrap();
            assert_eq!(
                evidence.trim(),
                format!("riffdb-exact-abort-v1\t{point}\t{next}")
            );
            assert!(
                !crashed
                    .wait_for_exit(Duration::from_secs(30))
                    .unwrap()
                    .status
                    .success()
            );
            let (_, _, persisted) = checkpoint_binding(&path);
            assert_eq!(
                persisted,
                if matches!(point, "BeforeFileSync" | "AfterFileSync") {
                    head
                } else {
                    next
                }
            );
            head = next;
            rows += 1;
            primary = fixture.start("primary", None);
            let mut client = fixture.client("primary").await;
            let mut retry = request;
            retry.request_id = super::support::request_id(request_id + 100);
            let replay = client.execute(retry, &reader).await.unwrap();
            assert_eq!(
                replay.status,
                v1::execute_command_response::CompletionStatus::Replayed as i32
            );
            assert_eq!(replay.commit_sequence, head);
            assert!(!replay.provenance_uri.is_empty());
            let mut queries = fixture.application_client().await;
            for recovered in NAMES {
                let result = query(&mut queries, &reader, recovered, module, head).await;
                assert_eq!(
                    result.fields["documents"].records.len(),
                    rows,
                    "{name} {point} {recovered}"
                );
                let keys = result.fields["documents"]
                    .records
                    .iter()
                    .map(|row| format!("{:?}", row.fields["doc_id"]))
                    .collect::<std::collections::BTreeSet<_>>();
                assert_eq!(keys.len(), rows, "replay must not duplicate postings");
                assert_eq!(
                    result.fields["total"].records[0].fields["value"],
                    ApplicationValue::U64(rows as u64)
                );
            }
            assert_eq!(
                exact_checkpoint_generations(&fixture.projections("primary"), head).len(),
                4
            );
            stop(&mut primary);
            request_id += 1;
        }
    }
}

fn pattern_page(module: [u8; 32], head: u64, cursor: Option<String>) -> NamedQuery {
    let mut options = QueryOptions::new()
        .read_after_commit(head)
        .at_least_admission_head();
    if let Some(cursor) = cursor {
        options = options.after(cursor);
    }
    NamedQuery::new(
        ApplicationContract::Active,
        "PatternDocuments",
        Some(module),
        BTreeMap::from([
            (
                "org_id".into(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes([0x11; 16])),
            ),
            ("needle".into(), ApplicationValue::String("alpha".into())),
            ("limit".into(), ApplicationValue::U64(1)),
        ]),
        None,
    )
    .unwrap()
    .with_options(options)
    .unwrap()
}

async fn assert_cursor_refused(
    client: &mut StableApplicationClient,
    metadata: &CallMetadata,
    module: [u8; 32],
    head: u64,
    cursor: String,
) {
    let error = client
        .execute_named_query(pattern_page(module, head, Some(cursor)), metadata)
        .await
        .expect_err("foreign or obsolete process cursor must release no page");
    assert_eq!(
        error.semantic_error().map(|error| error.code()),
        Some(riffdb_errors::ApplicationErrorCode::CursorInvalid)
    );
}
