//! Scalar and production-vector views from a real replicated database over TLS.
// req: REP-004, PRJ-005, PRJ-006, PRJ-008, PRJ-009, REC-002

use super::support::*;
use riffdb_client_rust::{
    ApplicationContract, ApplicationUuid, ApplicationValue, BearerCredential, CallMetadata,
    NamedQuery, NamedQueryResult, ProjectedQuery, ProjectedQueryOutcome, ProjectedResponseEncoding,
    QueryOptions, RiffDbClient, StableApplicationClient, app_v1, v1,
};
use riffdb_types::{
    CanonicalVector, CommitSequence, CommitToken, FreshnessPolicy, FrontierPosition,
};
use std::{collections::BTreeMap, time::Duration};

const CONTRACT: &str = include_str!("../../fixtures/vector-exit/documents.riff");
const QUERY: &str = include_str!("../../fixtures/vector-exit/similar_documents.riffq");

async fn deploy(client: &mut RiffDbClient, admin: &CallMetadata) -> [u8; 32] {
    let deployed = client
        .deploy_contract(
            v1::DeployContractRequest {
                request_id: request_id(60),
                source: CONTRACT.into(),
                ..Default::default()
            },
            admin,
        )
        .await
        .unwrap();
    let Some(v1::deploy_contract_response::Result::Activated(contract)) = deployed.result else {
        panic!("contract not activated: {deployed:?}")
    };
    let module = client
        .deploy_query_module(
            app_v1::DeployQueryModuleRequest {
                request_id: request_id(61),
                contract: Some(app_v1::ContractSelector {
                    lineage: "VectorDocuments".into(),
                    version: 1,
                    bundle_hash: contract.bundle_hash,
                }),
                module_name: "vectors".into(),
                module_version: 1,
                queries: vec![app_v1::NamedQuerySource {
                    name: "SimilarDocuments".into(),
                    source: QUERY.into(),
                }],
                expected_active: Some(
                    app_v1::deploy_query_module_request::ExpectedActive::AbsentActive(true),
                ),
            },
            admin,
        )
        .await
        .unwrap();
    assert_eq!(
        module.outcome,
        app_v1::QueryModuleDeploymentOutcome::Activated as i32,
        "{module:?}"
    );
    module.module.unwrap().module_hash.try_into().unwrap()
}

async fn authority(
    client: &mut RiffDbClient,
    admin: &CallMetadata,
    module: [u8; 32],
) -> CallMetadata {
    let bundle = riffdb_contract_compiler::compile_contract_source(CONTRACT).unwrap();
    let entity = &bundle.schema().entities()[0];
    let permission = |permission| v1::CapabilityPermission {
        permission: Some(permission),
    };
    let response = client
        .create_capability(
            v1::CreateCapabilityRequest {
                request_id: request_id(62),
                mode: v1::CapabilityCreateMode::Normal as i32,
                capability_id: capability_id(62).as_bytes().to_vec(),
                principal_id: "columnar-reader".into(),
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
                    permissions: vec![
                        permission(v1::capability_permission::Permission::InvokeCommand(
                            v1::LineageScopedStableId {
                                contract_lineage: "VectorDocuments".into(),
                                stable_id: bundle.commands()[0].command_id().get(),
                            },
                        )),
                        permission(v1::capability_permission::Permission::ReadEntity(
                            v1::LineageScopedStableId {
                                contract_lineage: "VectorDocuments".into(),
                                stable_id: entity.id().get(),
                            },
                        )),
                        permission(v1::capability_permission::Permission::ExecuteAdHocQuery(
                            v1::Unit {},
                        )),
                        permission(v1::capability_permission::Permission::ExecuteNamedQuery(
                            v1::NamedQueryPermission {
                                contract_lineage: "VectorDocuments".into(),
                                query_module_hash: module.to_vec(),
                                query_name: "SimilarDocuments".into(),
                            },
                        )),
                        permission(
                            v1::capability_permission::Permission::ApplicationRoleIdentity(
                                vec![0x65; 32],
                            ),
                        ),
                    ],
                    field_visibility: vec![v1::EntityFieldVisibility {
                        contract_lineage: "VectorDocuments".into(),
                        entity_type_id: entity.id().get(),
                        field_ids: entity
                            .record()
                            .fields()
                            .iter()
                            .map(|f| f.id().get())
                            .collect(),
                        secret_field_ids: vec![],
                    }],
                    max_scan_rows: 1000,
                    ..Default::default()
                }),
            },
            admin,
        )
        .await
        .unwrap();
    let Some(v1::create_capability_response::Result::Normal(normal)) = response.result else {
        panic!("normal capability")
    };
    let Some(v1::normal_create_capability_result::Result::Created(created)) = normal.result else {
        panic!("created capability")
    };
    CallMetadata::authenticated(BearerCredential::new(&created.token).unwrap())
}

async fn write(
    client: &mut RiffDbClient,
    caller: &CallMetadata,
    id: u8,
    org: u8,
    title: &str,
    components: [f32; 4],
) -> u64 {
    let mut fields = [
        (
            "idempotency_key",
            v1::value::Kind::StringValue(format!("document-{id}")),
        ),
        ("organization_id", v1::value::Kind::UuidValue(vec![org; 16])),
        ("document_id", v1::value::Kind::UuidValue(vec![id; 16])),
        ("title", v1::value::Kind::StringValue(title.into())),
        ("body", v1::value::Kind::StringValue("body".into())),
        (
            "embedding",
            v1::value::Kind::VectorValue(v1::VectorValue {
                components: components.to_vec(),
            }),
        ),
        (
            "submitted_model",
            v1::value::Kind::StringValue("embed-v1".into()),
        ),
        (
            "submitted_version",
            v1::value::Kind::StringValue("2026-08-21".into()),
        ),
    ]
    .into_iter()
    .map(|(name, kind)| v1::ValueField {
        name: name.into(),
        field_id: None,
        value: Some(v1::Value { kind: Some(kind) }),
    })
    .collect::<Vec<_>>();
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    let request = v1::ExecuteCommandRequest {
        request_id: request_id(id),
        command_name: "CreateEmbeddedDocument".into(),
        expected_contract_version: Some(1),
        input: Some(v1::Value {
            kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord { fields })),
        }),
    };
    let response = client.execute(request.clone(), caller).await.unwrap();
    assert_eq!(response.outcome_type, "Created");
    assert_eq!(
        response.status,
        v1::execute_command_response::CompletionStatus::Committed as i32
    );
    assert!(!response.provenance_uri.is_empty());
    let mut retry = request;
    retry.request_id = request_id(id + 100);
    let replay = client.execute(retry, caller).await.unwrap();
    assert_eq!(
        replay.status,
        v1::execute_command_response::CompletionStatus::Replayed as i32
    );
    assert_eq!(replay.commit_sequence, response.commit_sequence);
    assert_eq!(replay.outcome, response.outcome);
    response.commit_sequence
}

async fn nearest(
    client: &mut StableApplicationClient,
    caller: &CallMetadata,
    module: [u8; 32],
    head: u64,
) -> NamedQueryResult {
    let query = NamedQuery::new(
        ApplicationContract::Active,
        "SimilarDocuments",
        Some(module),
        BTreeMap::from([
            (
                "organization_id".into(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes([0x11; 16])),
            ),
            (
                "query_vector".into(),
                ApplicationValue::Vector(CanonicalVector::new(vec![1.0, 0.0, 0.0, 0.0]).unwrap()),
            ),
            ("k".into(), ApplicationValue::U64(10)),
        ]),
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
            match client.execute_named_query(query.clone(), caller).await {
                Ok(result) => break result,
                Err(error) => {
                    assert!(
                        matches!(
                            error.semantic_error().map(|e| e.code()),
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
    .expect("nearest view reaches applied head")
}

async fn scalar(
    client: &mut StableApplicationClient,
    caller: &CallMetadata,
    token: CommitToken,
) -> ProjectedQueryOutcome {
    let query = scalar_request(FreshnessPolicy::Causal {
        token,
        max_wait: Duration::from_secs(5),
    });
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let result = client
                .execute_projected_query(query.clone(), caller)
                .await
                .unwrap();
            match result {
                ProjectedQueryOutcome::Ready { .. } => break result,
                ProjectedQueryOutcome::Building { .. } | ProjectedQueryOutcome::Lagging { .. } => {
                    tokio::task::yield_now().await
                }
                other => panic!("unexpected scalar state: {other:?}"),
            }
        }
    })
    .await
    .expect("scalar view reaches applied head")
}

fn scalar_request(freshness: FreshnessPolicy) -> ProjectedQuery {
    ProjectedQuery::new(
        ApplicationContract::Active,
        "document_board",
        ApplicationValue::Uuid(ApplicationUuid::from_bytes([0x11; 16])),
    )
    .unwrap()
    .select(vec!["title".into()])
    .limit(Some(10))
    .encoding(ProjectedResponseEncoding::Packed)
    .freshness(freshness)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_scalar_and_vector_views_match_primary_after_advance_and_restart() {
    run_scenario(false).await;
}

pub(super) async fn run_scenario(observe_wait: bool) {
    let fixture = Fixture::new();
    let mut initial = fixture.start("primary", None);
    stop(&mut initial);
    let (lineage, admin, _) = seed_primary(&fixture.database("primary"));
    let baseline = super::oracle::baseline(&fixture.database("primary"));
    let mut primary = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    let replication = create_replication_capability(&mut client, &admin).await;
    let module = deploy(&mut client, &admin).await;
    let caller = authority(&mut client, &admin, module).await;
    write(
        &mut client,
        &caller,
        70,
        0x11,
        "closest",
        [1.0, 0.0, 0.0, 0.0],
    )
    .await;
    write(
        &mut client,
        &caller,
        71,
        0x11,
        "farther",
        [0.0, 1.0, 0.0, 0.0],
    )
    .await;
    let mut head = write(
        &mut client,
        &caller,
        72,
        0x22,
        "other tenant",
        [1.0, 0.0, 0.0, 0.0],
    )
    .await;
    stop(&mut primary);
    fixture.configure_document_projection("primary");
    primary = fixture.start("primary", None);
    client = fixture.client("primary").await;
    let mut primary_queries = fixture.application_client().await;
    let token = |head| {
        CommitToken::new_scoped(
            lineage.database_id(),
            lineage.history_incarnation(),
            CommitSequence::new(head).unwrap(),
        )
    };
    scalar(&mut primary_queries, &caller, token(head)).await;
    nearest(&mut primary_queries, &caller, module, head).await;
    let (foreign_database, foreign_token) = independently_issued_token(head).await;
    assert_ne!(foreign_database, lineage.database_id());
    assert_eq!(foreign_token.database_id(), Some(foreign_database));
    assert_eq!(
        foreign_token.history_incarnation(),
        lineage.history_incarnation()
    );
    assert_eq!(foreign_token.commit_sequence().get(), head);
    let proxy = super::proxy::Proxy::start(fixture.channel("primary").await).await;
    fixture.configure_follower_via(lineage, &replication, proxy.endpoint());
    fixture.configure_document_projection("follower");
    assert!(!fixture.projections("follower").exists());
    let mut follower = if observe_wait {
        fixture.start_wait_observed_follower()
    } else {
        fixture.start("follower", Some("follower"))
    };
    super::wait_for_commit(&fixture, &admin, head).await;
    let mut follower_queries = fixture.application_client_for("follower").await;
    for phase in 0..3 {
        if phase == 1 {
            let held = observe_wait.then(|| proxy.hold_after_commit(head));
            head = write(
                &mut client,
                &caller,
                73,
                0x11,
                "newest",
                [0.7, 0.7, 0.0, 0.0],
            )
            .await;
            if let Some((entered, release)) = held {
                tokio::time::timeout(Duration::from_secs(30), entered)
                    .await
                    .unwrap()
                    .unwrap();
                let primary_result = scalar(&mut primary_queries, &caller, token(head)).await;
                let ProjectedQueryOutcome::Ready {
                    commit_token: Some(actual_token),
                    ..
                } = &primary_result
                else {
                    panic!("primary returned no committed frontier token")
                };
                let mut causal_client = fixture.application_client_for("follower").await;
                for freshness in [
                    FreshnessPolicy::Available,
                    FreshnessPolicy::Bounded {
                        max_lag_sequences: 0,
                    },
                ] {
                    let observed = causal_client
                        .execute_projected_query(scalar_request(freshness), &caller)
                        .await
                        .unwrap();
                    let ProjectedQueryOutcome::Ready {
                        frontier,
                        head: local_head,
                        rows,
                        ..
                    } = observed
                    else {
                        panic!("warm view must stay available")
                    };
                    assert_eq!(frontier, local_head);
                    assert!(
                        frontier.position()
                            < FrontierPosition::AppliedThrough(CommitSequence::new(head).unwrap())
                    );
                    assert_eq!(rows.len(), 2);
                }
                let caller = caller.clone();
                let actual_token = actual_token.clone();
                let waiting = tokio::spawn(async move {
                    causal_client
                        .execute_projected_query(
                            scalar_request(FreshnessPolicy::Causal {
                                token: actual_token,
                                max_wait: Duration::from_secs(20),
                            }),
                            &caller,
                        )
                        .await
                        .unwrap()
                });
                follower
                    .wait_for_readiness(
                        "riffdb-columnar-wait-registered-v1",
                        Duration::from_secs(20),
                    )
                    .unwrap();
                assert!(
                    !waiting.is_finished(),
                    "registered read cannot finish below its token"
                );
                let refusal = fixture
                    .client("follower")
                    .await
                    .revoke_capability(
                        v1::RevokeCapabilityRequest {
                            request_id: request_id(95),
                            capability_id: capability_id(1).as_bytes().to_vec(),
                            reason: v1::RevocationReason::Requested as i32,
                        },
                        &admin,
                    )
                    .await
                    .unwrap_err();
                assert_eq!(
                    refusal.public_error().map(|error| error.kind()),
                    Some(riffdb_errors::PublicErrorKind::FollowerMode)
                );
                release.send(()).unwrap();
                let read = tokio::time::timeout(Duration::from_secs(20), waiting)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(read, primary_result);
            }
            super::wait_for_commit(&fixture, &admin, head).await;
        }
        if phase == 2 {
            stop(&mut follower);
            follower = fixture.start("follower", Some("follower"));
            follower_queries = fixture.application_client_for("follower").await;
        }
        let expected = scalar(&mut primary_queries, &caller, token(head)).await;
        let actual = scalar(&mut follower_queries, &caller, token(head)).await;
        assert_eq!(actual, expected);
        for invalid_token in [
            foreign_token.clone(),
            CommitToken::new(
                lineage.history_incarnation(),
                CommitSequence::new(head).unwrap(),
            ),
        ] {
            let foreign_database_error = follower_queries
                .execute_projected_query(
                    scalar_request(FreshnessPolicy::Causal {
                        token: invalid_token,
                        max_wait: Duration::ZERO,
                    }),
                    &caller,
                )
                .await
                .expect_err("foreign and unbound tokens must be refused");
            assert_eq!(
                foreign_database_error
                    .semantic_error()
                    .map(|error| error.code()),
                Some(riffdb_errors::ApplicationErrorCode::HistoryIncarnationMismatch)
            );
        }
        let foreign = follower_queries
            .execute_projected_query(
                scalar_request(FreshnessPolicy::Causal {
                    token: CommitToken::new_scoped(
                        lineage.database_id(),
                        lineage.history_incarnation() + 1,
                        CommitSequence::new(head).unwrap(),
                    ),
                    max_wait: Duration::ZERO,
                }),
                &caller,
            )
            .await
            .expect_err("foreign-incarnation token must be a typed history refusal");
        assert_eq!(
            foreign.semantic_error().map(|error| error.code()),
            Some(riffdb_errors::ApplicationErrorCode::HistoryIncarnationMismatch)
        );
        let ProjectedQueryOutcome::Ready { rows, frontier, .. } = actual else {
            unreachable!()
        };
        assert_eq!(rows.len(), if phase == 0 { 2 } else { 3 });
        assert_eq!(
            frontier.position(),
            FrontierPosition::AppliedThrough(CommitSequence::new(head).unwrap())
        );
        let expected = nearest(&mut primary_queries, &caller, module, head).await;
        let actual = nearest(&mut follower_queries, &caller, module, head).await;
        assert_eq!(actual.fields, expected.fields);
        assert_eq!(actual.identity, expected.identity);
        assert_eq!(actual.application_head, head);
        assert_eq!(expected.application_head, head);
        let rows = &actual.fields["documents"].records;
        assert_eq!(rows.len(), if phase == 0 { 2 } else { 3 });
        assert_eq!(
            rows[0].fields["title"],
            ApplicationValue::String("closest".into())
        );
        assert!(rows.iter().all(|row| row.fields["organization_id"]
            == ApplicationValue::Uuid(ApplicationUuid::from_bytes([0x11; 16]))));
    }
    stop(&mut follower);
    stop(&mut primary);
    proxy.shutdown().await;
    super::oracle::compare(
        baseline,
        &fixture.database("primary"),
        &fixture.database("follower"),
        head,
    );
}

async fn independently_issued_token(head: u64) -> (riffdb_types::DatabaseId, CommitToken) {
    let fixture = Fixture::new();
    let mut initial = fixture.start("primary", None);
    stop(&mut initial);
    let (lineage, admin, _) = seed_primary(&fixture.database("primary"));
    let mut primary = fixture.start("primary", None);
    let mut client = fixture.client("primary").await;
    let module = deploy(&mut client, &admin).await;
    let caller = authority(&mut client, &admin, module).await;
    // Both independent databases have exactly three application commits. Control
    // records do not contribute to this sequence, and incarnation starts at one.
    for id in 70..73 {
        write(
            &mut client,
            &caller,
            id,
            0x11,
            "foreign",
            [1.0, 0.0, 0.0, 0.0],
        )
        .await;
    }
    stop(&mut primary);
    fixture.configure_document_projection("primary");
    primary = fixture.start("primary", None);
    let mut queries = fixture.application_client().await;
    let result = scalar(
        &mut queries,
        &caller,
        CommitToken::new(
            lineage.history_incarnation(),
            CommitSequence::new(head).unwrap(),
        ),
    )
    .await;
    let ProjectedQueryOutcome::Ready {
        commit_token: Some(token),
        ..
    } = result
    else {
        panic!("independent primary must issue a token")
    };
    stop(&mut primary);
    (lineage.database_id(), token)
}
