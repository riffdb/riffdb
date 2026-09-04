//! Rust cell for the shared adapter-shaped operational-query corpus.
// req: OQ-004, OQ-006, OQ-016, OQ-031, OQ-113

#![forbid(unsafe_code)]
#![allow(dead_code, unreachable_pub)]

use std::error::Error;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use riffdb_client_rust::{
    ApplicationCatalogFeature, ApplicationContract, AttemptBudget, CallMetadata, DatabaseAlias,
    GeneratedBatchOptions, QueryOptions, StableApplicationClient, load_protected_bearer_credential,
};
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
};

#[allow(dead_code, unreachable_pub)]
mod generated {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/adapters/operational-conformance/generated/rust/client.rs"
    ));
}

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Rust adapter operational conformance failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> TestResult<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_async())
}

async fn run_async() -> TestResult<()> {
    let endpoint = required("RIFFDB_CONFORMANCE_ENDPOINT")?;
    let tls = TlsClientConfig::new(
        CanonicalHttpsEndpoint::parse(&endpoint)?,
        ProtectedFilePath::new(PathBuf::from(required("RIFFDB_CONFORMANCE_TRUST_ROOT")?))?,
        TlsServerIdentity::parse("127.0.0.1")?,
        Duration::from_secs(5),
        Duration::from_secs(30),
        NonZeroU32::new(4).ok_or("pool bound")?,
        NonZeroU32::new(64).ok_or("stream bound")?,
    )?;
    let credential = load_protected_bearer_credential(&PathBuf::from(required(
        "RIFFDB_CONFORMANCE_CREDENTIAL",
    )?))?;
    let metadata =
        CallMetadata::authenticated(credential).with_database(DatabaseAlias::default_alias());
    let mut transport = StableApplicationClient::connect_verified_tls(&tls).await?;
    let preflight = transport
        .preflight_application_features(
            ApplicationContract::Exact {
                lineage: generated::CONTRACT_LINEAGE.to_owned(),
                version: generated::CONTRACT_VERSION,
                bundle_hash: Some(generated::CONTRACT_BUNDLE_HASH),
            },
            &metadata,
        )
        .await?;
    for feature in [
        ApplicationCatalogFeature::OperationalOptionalPredicates,
        ApplicationCatalogFeature::StableCursorPages,
        ApplicationCatalogFeature::NullExistencePredicates,
        ApplicationCatalogFeature::BinaryTextPrefix,
        ApplicationCatalogFeature::ExactAggregates,
    ] {
        expect(preflight.is_available(feature), "catalog feature preflight")?;
    }
    if std::env::var_os("RIFFDB_WP701_BINARY_INTERVAL").is_none() {
        expect(
            !preflight.is_available(ApplicationCatalogFeature::UnicodeFoldTextPrefixV1),
            "unavailable feature is explicit",
        )?;
    }

    let mut client = generated::AdapterOperationalConformanceClient::new(
        transport,
        metadata,
        AttemptBudget::new(3).ok_or("attempt budget")?,
    );
    seed(&mut client).await?;
    if std::env::var_os("RIFFDB_WP701_BINARY_INTERVAL").is_some() {
        seed_wp701_nullable_transitions(&mut client).await?;
    }
    if std::env::var_os("RIFFDB_CONFORMANCE_SEED_ONLY").is_some() {
        return Ok(());
    }
    verify_queries(&mut client).await?;

    println!(
        "{}",
        serde_json::json!({
            "schema": "riffdb.adapter-operational-observation/v1",
            "language": "rust",
            "catalog_preflight": true,
            "optional_filters": true,
            "stable_cursor": true,
            "null_predicate": true,
            "binary_prefix": true,
            "exact_aggregates": true,
            "exact_text_family": true,
            "exact_predicate_family": true,
            "nullable_exact_order": true,
            "exact_total": true,
            "numeric_offset": true,
            "operator_expansions": true,
            "adapters": ["mlflow", "openfga", "better-auth", "woodpecker"],
            "regression_adapters": ["payload"],
        })
    );
    Ok(())
}

async fn seed_wp701_nullable_transitions(
    client: &mut generated::AdapterOperationalConformanceClient,
) -> TestResult<()> {
    let transitions = vec![
        inventory_transition(9, 71, Some("Omega"), Some(250), 2),
        inventory_transition(10, 72, Some("Aardvark"), Some(50), 5),
        inventory_transition(11, 73, None, None, 3),
    ];
    let transitioned = client
        .change_inventory_record_state_batch(
            transitions,
            GeneratedBatchOptions::new(3).map_err(|error| format!("batch bounds: {error}"))?,
        )
        .await?;
    expect(
        transitioned.items.iter().all(|item| item.result.is_ok()),
        "WP-701 nullable transition seed",
    )
}

async fn seed(client: &mut generated::AdapterOperationalConformanceClient) -> TestResult<()> {
    let mut tuples = vec![
        generated::FgaTuple {
            store_id: id(10),
            tuple_id: id(11),
            object: "document:alpha".to_owned(),
            relation: "viewer".to_owned(),
            subject: "user:agent".to_owned(),
        },
        generated::FgaTuple {
            store_id: id(10),
            tuple_id: id(12),
            object: "document:alpha".to_owned(),
            relation: "editor".to_owned(),
            subject: "team:authors".to_owned(),
        },
    ];
    for suffix in 13..=36 {
        tuples.push(generated::FgaTuple {
            store_id: id(10),
            tuple_id: id(suffix),
            object: format!("document:fixture-{suffix}"),
            relation: "editor".to_owned(),
            subject: format!("user:fixture-{suffix}"),
        });
    }
    client
        .write_tuples(generated::WriteTuplesInput {
            request_id: id(1),
            tuples,
        })
        .await?;
    client
        .log_metrics(generated::LogMetricsInput {
            request_id: id(2),
            metrics: vec![metric(21, 125, 1), metric(22, 175, 2)],
        })
        .await?;
    client
        .create_documents(generated::CreateDocumentsInput {
            request_id: id(3),
            documents: vec![
                generated::Document {
                    site_id: id(30),
                    document_id: id(31),
                    title: "Alpha Draft".to_owned(),
                    published_at: None,
                },
                generated::Document {
                    site_id: id(30),
                    document_id: id(32),
                    title: "Alpha Published".to_owned(),
                    published_at: Some(generated::TimestampValue {
                        seconds: 1_700_000_000,
                        nanos: 0,
                    }),
                },
                generated::Document {
                    site_id: id(30),
                    document_id: id(33),
                    title: "Beta Guide".to_owned(),
                    published_at: Some(generated::TimestampValue {
                        seconds: 1_700_000_100,
                        nanos: 0,
                    }),
                },
                generated::Document {
                    site_id: id(30),
                    document_id: id(34),
                    title: "Gamma Guide".to_owned(),
                    published_at: Some(generated::TimestampValue {
                        seconds: 1_700_000_200,
                        nanos: 0,
                    }),
                },
            ],
        })
        .await?;
    client
        .create_documents(generated::CreateDocumentsInput {
            request_id: id(130),
            documents: vec![
                generated::Document {
                    site_id: id(35),
                    document_id: id(131),
                    title: "a".to_owned(),
                    published_at: None,
                },
                generated::Document {
                    site_id: id(35),
                    document_id: id(132),
                    title: "aa".to_owned(),
                    published_at: None,
                },
                generated::Document {
                    site_id: id(35),
                    document_id: id(133),
                    title: "b".to_owned(),
                    published_at: None,
                },
                generated::Document {
                    site_id: id(35),
                    document_id: id(134),
                    title: "é".to_owned(),
                    published_at: None,
                },
                generated::Document {
                    site_id: id(35),
                    document_id: id(135),
                    title: "😀".to_owned(),
                    published_at: None,
                },
            ],
        })
        .await?;
    client
        .create_directory_users(generated::CreateDirectoryUsersInput {
            request_id: id(6),
            users: vec![
                directory_user(61, "ada@example.test", "active", 40, true),
                directory_user(62, "alan@example.test", "disabled", 30, true),
                directory_user(63, "beta@example.test", "active", 20, false),
                directory_user(64, "álpha@example.test", "archive", 10, true),
            ],
        })
        .await?;
    client
        .create_inventory_record_missing(generated::CreateInventoryRecordMissingInput {
            request_id: id(7),
            organization_id: id(70),
            record_id: id(71),
            tie_rank: 2,
        })
        .await?;
    client
        .create_inventory_records(generated::CreateInventoryRecordsInput {
            request_id: id(8),
            records: vec![
                inventory_record(72, None, None, 5),
                inventory_record(73, Some("Alpha"), Some(100), 3),
                inventory_record(74, Some("Alpha"), Some(200), 1),
                inventory_record(75, Some("Beta"), None, 4),
                inventory_record(76, None, Some(150), 0),
            ],
        })
        .await?;
    client
        .create_pipelines(generated::CreatePipelinesInput {
            request_id: id(4),
            pipelines: vec![
                pipeline(41, "verify", "queued"),
                pipeline(42, "publish", "running"),
            ],
        })
        .await?;
    client
        .create_auth_sessions(generated::CreateAuthSessionsInput {
            request_id: id(5),
            signups: vec![generated::AuthSignupInput {
                organization_id: id(50),
                user_id: id(51),
                session_id: id(52),
                email: "agent@example.test".to_owned(),
                token_digest: "sha256:better-auth-secret".to_owned(),
                expires_at: generated::TimestampValue {
                    seconds: 1_800_000_000,
                    nanos: 0,
                },
            }],
        })
        .await?;
    client
        .seed_tickets(generated::SeedTicketsInput {
            request_id: id(120),
            tickets: vec![
                generated::Ticket {
                    organization_id: id(80),
                    ticket_id: id(81),
                    state: "open".to_owned(),
                    title: "Expansion first".to_owned(),
                },
                generated::Ticket {
                    organization_id: id(80),
                    ticket_id: id(82),
                    state: "open".to_owned(),
                    title: "Expansion second".to_owned(),
                },
            ],
        })
        .await?;
    client
        .seed_ticket_comments(generated::SeedTicketCommentsInput {
            request_id: id(121),
            comments: vec![
                ticket_comment(81, 83, 1, "first comment"),
                ticket_comment(81, 84, 2, "second comment"),
                ticket_comment(82, 85, 1, "other ticket"),
            ],
        })
        .await?;
    client
        .seed_runs(generated::SeedRunsInput {
            request_id: id(122),
            runs: vec![mlflow_run(91, "active"), mlflow_run(92, "active")],
        })
        .await?;
    client
        .seed_run_tags(generated::SeedRunTagsInput {
            request_id: id(123),
            tags: vec![
                mlflow_tag(91, 93, "model", "alpha"),
                mlflow_tag(91, 94, "stage", "test"),
                mlflow_tag(92, 95, "model", "beta"),
            ],
        })
        .await?;
    client
        .seed_objects(generated::SeedObjectsInput {
            request_id: id(124),
            objects: vec![fga_object(101, "document"), fga_object(102, "document")],
        })
        .await?;
    client
        .seed_object_relations(generated::SeedObjectRelationsInput {
            request_id: id(125),
            relations: vec![
                fga_relation(101, 103, "viewer", "user:alice"),
                fga_relation(101, 104, "editor", "team:authors"),
                fga_relation(102, 105, "viewer", "user:bob"),
            ],
        })
        .await?;
    Ok(())
}

async fn verify_queries(
    client: &mut generated::AdapterOperationalConformanceClient,
) -> TestResult<()> {
    let first = client
        .list_fga_tuples_with_options(
            generated::ListFgaTuplesParams {
                store_id: id(10),
                relation: None,
                after: None,
            },
            QueryOptions::new(),
        )
        .await?;
    let first_cursor = first
        .next_cursor
        .ok_or("OpenFGA first-page cursor absent")?;
    let generated::ListFgaTuplesResult::Found(first_page) = first.value;
    expect(first_page.tuples.len() == 25, "OpenFGA bounded first page")?;
    let second = client
        .list_fga_tuples_with_options(
            generated::ListFgaTuplesParams {
                store_id: id(10),
                relation: None,
                after: Some(first_cursor),
            },
            QueryOptions::new(),
        )
        .await?;
    let generated::ListFgaTuplesResult::Found(second_page) = second.value;
    expect(
        second_page.tuples.len() == 1 && second.next_cursor.is_none(),
        "OpenFGA generated cursor continuation",
    )?;
    let viewer = client
        .list_fga_tuples(generated::ListFgaTuplesParams {
            store_id: id(10),
            relation: Some("viewer".to_owned()),
            after: None,
        })
        .await?;
    let generated::ListFgaTuplesResult::Found(viewer) = viewer;
    expect(viewer.tuples.len() == 1, "OpenFGA optional relation")?;

    let dashboard = client
        .metric_dashboard(generated::MetricDashboardParams {
            experiment_id: id(20),
        })
        .await?;
    let generated::MetricDashboardResult::Found(dashboard) = dashboard;
    let summary = dashboard.summary.first().ok_or("MLflow summary absent")?;
    expect(
        summary.sample_count == 2
            && summary.minimum_micros == Some(125)
            && summary.maximum_micros == Some(175),
        "MLflow exact aggregate",
    )?;

    let documents = client
        .search_documents(generated::SearchDocumentsParams {
            site_id: id(30),
            title_prefix: "Alpha".to_owned(),
            after: None,
        })
        .await?;
    let generated::SearchDocumentsResult::Found(documents) = documents;
    expect(documents.documents.len() == 2, "Payload binary prefix")?;
    let drafts = client
        .list_draft_documents(generated::ListDraftDocumentsParams { site_id: id(30) })
        .await?;
    let generated::ListDraftDocumentsResult::Found(drafts) = drafts;
    expect(drafts.documents.len() == 1, "Payload null predicate")?;

    let contains = client
        .exact_documents_contains_asc(generated::ExactDocumentsContainsAscParams {
            site_id: id(30),
            needle: "Alpha".to_owned(),
            document_id: None,
            limit: 1,
            offset: 1,
        })
        .await?;
    let generated::ExactDocumentsContainsAscResult::Found(contains) = contains;
    expect(
        contains.total.value == 2
            && contains.documents.len() == 1
            && contains.documents[0].title == "Alpha Published",
        "generic contains page retains full exact total across numeric offset",
    )?;
    let starts_with = client
        .exact_documents_starts_with_asc(generated::ExactDocumentsStartsWithAscParams {
            site_id: id(30),
            needle: "Alpha".to_owned(),
            document_id: Some(id(31)),
            limit: 25,
            offset: 0,
        })
        .await?;
    let generated::ExactDocumentsStartsWithAscResult::Found(starts_with) = starts_with;
    expect(
        starts_with.total.value == 1
            && starts_with.documents.len() == 1
            && starts_with.documents[0].document_id == id(31),
        "generic starts-with page applies the typed optional filter",
    )?;
    let ends_with = client
        .exact_documents_ends_with_desc(generated::ExactDocumentsEndsWithDescParams {
            site_id: id(30),
            needle: "Guide".to_owned(),
            document_id: None,
            limit: 1,
            offset: 1,
        })
        .await?;
    let generated::ExactDocumentsEndsWithDescResult::Found(ends_with) = ends_with;
    expect(
        ends_with.total.value == 2
            && ends_with.documents.len() == 1
            && ends_with.documents[0].title == "Beta Guide",
        "generic ends-with page uses descending value order and direct ordinal seek",
    )?;

    let rich = client
        .search_directory_users(generated::SearchDirectoryUsersParams {
            organization_id: id(60),
            needle: "example".to_owned(),
            excluded_states: vec!["disabled".to_owned(), "disabled".to_owned()],
            maximum_created_at: None,
            limit: 1,
            offset: 1,
        })
        .await?;
    let generated::SearchDirectoryUsersResult::Found(rich) = rich;
    expect(
        rich.total.value == 3
            && rich.users.len() == 1
            && rich.users[0].email == "beta@example.test",
        "V6 set canonicalization, optional absence, independent order, total, and ordinal",
    )?;
    let reviewed = client
        .reviewed_directory_users(generated::ReviewedDirectoryUsersParams {
            organization_id: id(60),
            states: vec!["active".to_owned(), "archive".to_owned()],
            before_created_at: 35,
            limit: 25,
            offset: 0,
        })
        .await?;
    let generated::ReviewedDirectoryUsersResult::Found(reviewed) = reviewed;
    expect(
        reviewed.total.value == 1
            && reviewed.users.len() == 1
            && reviewed.users[0].email == "álpha@example.test",
        "V6 membership, range, existence, Unicode, and mixed order",
    )?;

    verify_nullable_orders(client, false).await?;
    let transitions = vec![
        inventory_transition(9, 71, Some("Omega"), Some(250), 2),
        inventory_transition(10, 72, Some("Aardvark"), Some(50), 5),
        inventory_transition(11, 73, None, None, 3),
    ];
    let transitioned = client
        .change_inventory_record_state_batch(
            transitions,
            GeneratedBatchOptions::new(3).map_err(|error| format!("batch bounds: {error}"))?,
        )
        .await?;
    expect(
        transitioned.items.iter().all(|item| item.result.is_ok()),
        "concurrent missing/null/value transitions",
    )?;
    wait_for_nullable_orders(client).await?;

    let queued = client
        .list_pipelines(generated::ListPipelinesParams {
            organization_id: id(40),
            state: Some("queued".to_owned()),
            after: None,
        })
        .await?;
    let generated::ListPipelinesResult::Found(queued) = queued;
    expect(queued.pipelines.len() == 1, "Woodpecker optional state")?;

    let auth_session = client
        .get_auth_session(generated::GetAuthSessionParams {
            organization_id: id(50),
            user_id: id(51),
            session_id: id(52),
        })
        .await?;
    let generated::GetAuthSessionResult::Found(auth_session) = auth_session else {
        return Err("Better Auth session was not restored".into());
    };
    expect(
        auth_session.session.state == "AuthActive"
            && auth_session.session.expires_at.seconds == 1_800_000_000,
        "Better Auth typed session graph",
    )?;

    let ticket_page = client
        .ticket_page_with_comments(generated::TicketPageWithCommentsParams {
            organization_id: id(80),
            state: "open".to_owned(),
        })
        .await?;
    let generated::TicketPageWithCommentsResult::Found(ticket_page) = ticket_page;
    expect(
        ticket_page.tickets.len() == 2
            && ticket_page.tickets[0].comments.len() == 2
            && ticket_page.tickets[1].comments.len() == 1,
        "TicketDesk tickets carry bounded comments per ticket",
    )?;

    let runs = client
        .mlflow_runs_with_tags(generated::MlflowRunsWithTagsParams {
            experiment_id: id(90),
            lifecycle: "active".to_owned(),
        })
        .await?;
    let generated::MlflowRunsWithTagsResult::Found(runs) = runs;
    expect(
        runs.runs.len() == 2 && runs.runs[0].tags.len() == 2 && runs.runs[1].tags.len() == 1,
        "MLflow runs carry bounded tags per run",
    )?;

    let objects = client
        .fga_objects_with_relations(generated::FgaObjectsWithRelationsParams {
            store_id: id(100),
            kind: "document".to_owned(),
        })
        .await?;
    let generated::FgaObjectsWithRelationsResult::Found(objects) = objects;
    expect(
        objects.objects.len() == 2
            && objects.objects[0].relations.len() == 2
            && objects.objects[1].relations.len() == 1,
        "OpenFGA objects carry bounded relations per object",
    )?;

    let stale = client
        .list_fga_tuples_with_options(
            generated::ListFgaTuplesParams {
                store_id: id(10),
                relation: None,
                after: Some("not-a-riffdb-cursor".to_owned()),
            },
            QueryOptions::new(),
        )
        .await;
    expect(stale.is_err(), "stale cursor fails closed")?;
    Ok(())
}

async fn wait_for_nullable_orders(
    client: &mut generated::AdapterOperationalConformanceClient,
) -> TestResult<()> {
    for _attempt in 0..100 {
        match verify_nullable_orders(client, true).await {
            Ok(()) => return Ok(()),
            Err(error)
                if error.to_string().contains("RDB-PROJECTION-0103")
                    || error.to_string().contains("RDB-QUERY-0102") =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err("nullable provider did not catch up within the bounded retry budget".into())
}

async fn verify_nullable_orders(
    client: &mut generated::AdapterOperationalConformanceClient,
    transitioned: bool,
) -> TestResult<()> {
    let organization_id = id(70);
    let first = client
        .inventory_by_subtitle_asc_nulls_first(generated::InventoryBySubtitleAscNullsFirstParams {
            organization_id: organization_id.clone(),
            limit: 2,
            offset: 0,
        })
        .await?;
    let generated::InventoryBySubtitleAscNullsFirstResult::Found(first) = first;
    expect(first.total.value == 6, "nullable exact total")?;
    let expected_first = if transitioned {
        vec![id(73), id(76)]
    } else {
        vec![id(72), id(71)]
    };
    expect(
        first
            .records
            .iter()
            .map(|row| row.record_id.clone())
            .collect::<Vec<_>>()
            == expected_first,
        "nulls-first order and later-term tie break",
    )?;

    let end = client
        .inventory_by_subtitle_desc_nulls_last(generated::InventoryBySubtitleDescNullsLastParams {
            organization_id: organization_id.clone(),
            limit: 25,
            offset: 5,
        })
        .await?;
    let generated::InventoryBySubtitleDescNullsLastResult::Found(end) = end;
    expect(
        end.total.value == 6 && end.records.len() == 1,
        "nullable end offset retains exact total",
    )?;

    let beyond = client
        .inventory_by_observed_asc_nulls_last(generated::InventoryByObservedAscNullsLastParams {
            organization_id: organization_id.clone(),
            limit: 25,
            offset: 7,
        })
        .await?;
    let generated::InventoryByObservedAscNullsLastResult::Found(beyond) = beyond;
    expect(
        beyond.total.value == 6 && beyond.records.is_empty(),
        "nullable beyond-end offset",
    )?;

    let maximum = client
        .inventory_by_observed_desc_nulls_first(
            generated::InventoryByObservedDescNullsFirstParams {
                organization_id,
                limit: 499,
                offset: 499,
            },
        )
        .await?;
    let generated::InventoryByObservedDescNullsFirstResult::Found(maximum) = maximum;
    expect(
        maximum.total.value == 6 && maximum.records.is_empty(),
        "nullable maximum offset",
    )?;
    Ok(())
}

fn metric(suffix: u8, value_micros: i64, step: i64) -> generated::Metric {
    generated::Metric {
        experiment_id: id(20),
        metric_id: id(suffix),
        name: "latency".to_owned(),
        value_micros,
        step,
    }
}

fn ticket_comment(
    ticket_suffix: u8,
    comment_suffix: u8,
    created_at: u64,
    body: &str,
) -> generated::TicketComment {
    generated::TicketComment {
        organization_id: id(80),
        ticket_id: id(ticket_suffix),
        comment_id: id(comment_suffix),
        body: body.to_owned(),
        created_at,
    }
}

fn mlflow_run(run_suffix: u8, lifecycle: &str) -> generated::MlflowRun {
    generated::MlflowRun {
        experiment_id: id(90),
        run_id: id(run_suffix),
        lifecycle: lifecycle.to_owned(),
    }
}

fn mlflow_tag(run_suffix: u8, tag_suffix: u8, name: &str, value: &str) -> generated::MlflowRunTag {
    generated::MlflowRunTag {
        experiment_id: id(90),
        run_id: id(run_suffix),
        tag_id: id(tag_suffix),
        name: name.to_owned(),
        value: value.to_owned(),
    }
}

fn fga_object(object_suffix: u8, kind: &str) -> generated::FgaObject {
    generated::FgaObject {
        store_id: id(100),
        object_id: id(object_suffix),
        kind: kind.to_owned(),
    }
}

fn fga_relation(
    object_suffix: u8,
    relation_suffix: u8,
    relation: &str,
    subject: &str,
) -> generated::FgaRelation {
    generated::FgaRelation {
        store_id: id(100),
        object_id: id(object_suffix),
        relation_id: id(relation_suffix),
        relation: relation.to_owned(),
        subject: subject.to_owned(),
    }
}

fn pipeline(suffix: u8, name: &str, state: &str) -> generated::Pipeline {
    generated::Pipeline {
        organization_id: id(40),
        pipeline_id: id(suffix),
        name: name.to_owned(),
        state: state.to_owned(),
    }
}

fn directory_user(
    suffix: u8,
    email: &str,
    state: &str,
    created_at: u64,
    reviewed: bool,
) -> generated::DirectoryUser {
    generated::DirectoryUser {
        organization_id: id(60),
        user_id: id(suffix),
        email: email.to_owned(),
        state: state.to_owned(),
        created_at,
        reviewed_at: reviewed.then_some(generated::TimestampValue {
            seconds: 1_700_000_000 + i64::from(suffix),
            nanos: 0,
        }),
    }
}

fn inventory_record(
    suffix: u8,
    subtitle: Option<&str>,
    observed_seconds: Option<i64>,
    tie_rank: u64,
) -> generated::InventoryRecord {
    generated::InventoryRecord {
        organization_id: id(70),
        record_id: id(suffix),
        subtitle: subtitle.map(str::to_owned),
        observed_at: observed_seconds
            .map(|seconds| generated::TimestampValue { seconds, nanos: 0 }),
        tie_rank,
    }
}

fn inventory_transition(
    request_suffix: u8,
    record_suffix: u8,
    subtitle: Option<&str>,
    observed_seconds: Option<i64>,
    tie_rank: u64,
) -> generated::ChangeInventoryRecordStateInput {
    generated::ChangeInventoryRecordStateInput {
        request_id: id(request_suffix),
        organization_id: id(70),
        record_id: id(record_suffix),
        subtitle: subtitle.map(str::to_owned),
        observed_at: observed_seconds
            .map(|seconds| generated::TimestampValue { seconds, nanos: 0 }),
        tie_rank,
    }
}

fn id(suffix: u8) -> String {
    format!("018f0f8b-7c6d-7e31-8a4f-00000000{suffix:04x}")
}

fn expect(condition: bool, label: &str) -> TestResult<()> {
    condition
        .then_some(())
        .ok_or_else(|| format!("adapter operational assertion failed: {label}").into())
}

fn required(name: &str) -> TestResult<String> {
    std::env::var(name)
        .map_err(|_| format!("required environment variable is absent: {name}").into())
}
