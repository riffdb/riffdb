//! Rust cell for the shared adapter-shaped operational-query corpus.

#![forbid(unsafe_code)]
#![allow(dead_code, unreachable_pub)]

use std::error::Error;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use riffdb_client_rust::{
    ApplicationCatalogFeature, ApplicationContract, AttemptBudget, CallMetadata, DatabaseAlias,
    QueryOptions, StableApplicationClient, load_protected_bearer_credential,
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
    expect(
        !preflight.is_available(ApplicationCatalogFeature::UnicodeFoldTextPrefixV1),
        "unavailable feature is explicit",
    )?;

    let mut client = generated::AdapterOperationalConformanceClient::new(
        transport,
        metadata,
        AttemptBudget::new(3).ok_or("attempt budget")?,
    );
    seed(&mut client).await?;
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
            "adapters": ["mlflow", "openfga", "payload", "woodpecker"],
        })
    );
    Ok(())
}

async fn seed(client: &mut generated::AdapterOperationalConformanceClient) -> TestResult<()> {
    client
        .write_tuples(generated::WriteTuplesInput {
            request_id: id(1),
            tuples: vec![
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
            ],
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
    Ok(())
}

async fn verify_queries(
    client: &mut generated::AdapterOperationalConformanceClient,
) -> TestResult<()> {
    let all = client
        .list_fga_tuples(generated::ListFgaTuplesParams {
            store_id: id(10),
            relation: None,
            after: None,
        })
        .await?;
    let generated::ListFgaTuplesResult::Found(all) = all;
    expect(all.tuples.len() == 2, "OpenFGA unfiltered tuple page")?;
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

    let queued = client
        .list_pipelines(generated::ListPipelinesParams {
            organization_id: id(40),
            state: Some("queued".to_owned()),
            after: None,
        })
        .await?;
    let generated::ListPipelinesResult::Found(queued) = queued;
    expect(queued.pipelines.len() == 1, "Woodpecker optional state")?;

    let stale = client
        .list_fga_tuples_with_options(
            generated::ListFgaTuplesParams {
                store_id: id(10),
                relation: None,
                after: None,
            },
            QueryOptions::new().after("not-a-riffdb-cursor"),
        )
        .await;
    expect(stale.is_err(), "stale cursor fails closed")?;
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

fn pipeline(suffix: u8, name: &str, state: &str) -> generated::Pipeline {
    generated::Pipeline {
        organization_id: id(40),
        pipeline_id: id(suffix),
        name: name.to_owned(),
        state: state.to_owned(),
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
