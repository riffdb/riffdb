//! Rust cell for the shared adapter-shaped bounded-command corpus.

#![forbid(unsafe_code)]
#![allow(dead_code, unreachable_pub)]

use std::error::Error;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use riffdb_client_rust::generated::GeneratedCommand as _;
use riffdb_client_rust::{
    AttemptBudget, CallMetadata, DatabaseAlias, StableApplicationClient,
    load_protected_bearer_credential,
};
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
};

#[allow(dead_code, unreachable_pub)]
mod generated {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/adapters/bulk-conformance/generated/rust/client.rs"
    ));
}

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Rust adapter bulk conformance failed: {error}");
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
    let transport = StableApplicationClient::connect_verified_tls(&tls).await?;
    let mut client = generated::AdapterBulkConformanceClient::new(
        transport,
        metadata,
        AttemptBudget::new(3).ok_or("attempt budget")?,
    );

    let empty = generated::WriteTuplesInput {
        tuples: Vec::new(),
        request_id: id(1),
    };
    if empty.idempotent_command().is_ok() {
        return Err("empty generated collection crossed local preflight".into());
    }

    let store = id(10);
    let tuple_a = tuple(&store, &id(11));
    let tuple_b = tuple(&store, &id(12));
    let first_tuples = generated::WriteTuplesInput {
        tuples: vec![tuple_a.clone()],
        request_id: id(13),
    };
    let first = client.write_tuples(first_tuples.clone()).await?;
    expect(
        matches!(first.outcome, generated::WriteTuplesOutcome::TuplesWritten),
        "OpenFGA tuple create",
    )?;
    expect(
        client.write_tuples(first_tuples).await?.replayed,
        "OpenFGA replay",
    )?;
    let conflict = client
        .write_tuples(generated::WriteTuplesInput {
            tuples: vec![tuple_a, tuple_b.clone()],
            request_id: id(14),
        })
        .await?;
    expect(
        matches!(
            conflict.outcome,
            generated::WriteTuplesOutcome::TupleAlreadyExists
        ),
        "OpenFGA conflict outcome",
    )?;
    let atomic = client
        .write_tuples(generated::WriteTuplesInput {
            tuples: vec![tuple_b],
            request_id: id(15),
        })
        .await?;
    expect(
        matches!(atomic.outcome, generated::WriteTuplesOutcome::TuplesWritten),
        "OpenFGA whole-command atomicity",
    )?;

    let metrics = generated::LogMetricsInput {
        metrics: vec![generated::Metric {
            name: "latency".to_owned(),
            step: 1,
            metric_id: id(21),
            value_micros: 125,
            experiment_id: id(20),
        }],
        request_id: id(22),
    };
    expect(
        matches!(
            client.log_metrics(metrics.clone()).await?.outcome,
            generated::LogMetricsOutcome::MetricsLogged
        ),
        "MLflow metric create",
    )?;
    expect(client.log_metrics(metrics).await?.replayed, "MLflow replay")?;

    let documents = generated::CreateDocumentGraphsInput {
        documents: vec![generated::DocumentGraphInput {
            body: "bounded body".to_owned(),
            title: "Document".to_owned(),
            site_id: id(30),
            document_id: id(31),
            revision_id: id(32),
        }],
        request_id: id(33),
    };
    expect(
        matches!(
            client
                .create_document_graphs(documents.clone())
                .await?
                .outcome,
            generated::CreateDocumentGraphsOutcome::DocumentGraphsCreated
        ),
        "Payload graph create",
    )?;
    expect(
        client.create_document_graphs(documents).await?.replayed,
        "Payload replay",
    )?;

    let pipelines = generated::CreatePipelinesWithStepsInput {
        pipelines: vec![generated::PipelineGraphInput {
            name: "verify".to_owned(),
            step_id: id(42),
            run_text: "cargo test".to_owned(),
            pipeline_id: id(41),
            organization_id: id(40),
        }],
        request_id: id(43),
    };
    expect(
        matches!(
            client
                .create_pipelines_with_steps(pipelines.clone())
                .await?
                .outcome,
            generated::CreatePipelinesWithStepsOutcome::PipelinesCreated
        ),
        "Woodpecker graph create",
    )?;
    expect(
        client
            .create_pipelines_with_steps(pipelines)
            .await?
            .replayed,
        "Woodpecker replay",
    )?;

    let tenant = id(50);
    let parent_a = generated::RestrictParent {
        parent_id: id(51),
        tenant_id: tenant.clone(),
    };
    let parent_b = generated::RestrictParent {
        parent_id: id(52),
        tenant_id: tenant.clone(),
    };
    expect(
        matches!(
            client
                .create_restrict_parents(generated::CreateRestrictParentsInput {
                    parents: vec![parent_a.clone(), parent_b.clone()],
                    request_id: id(53),
                })
                .await?
                .outcome,
            generated::CreateRestrictParentsOutcome::RestrictParentsCreated
        ),
        "restrict parents create",
    )?;
    client
        .create_restrict_children(generated::CreateRestrictChildrenInput {
            children: vec![generated::RestrictChild {
                child_id: id(54),
                parent_id: parent_a.parent_id.clone(),
                tenant_id: tenant.clone(),
            }],
            request_id: id(55),
        })
        .await?;
    let restricted = client
        .delete_restrict_parents(generated::DeleteRestrictParentsInput {
            tenant_id: tenant.clone(),
            parent_ids: vec![parent_a.parent_id.clone()],
            request_id: id(56),
        })
        .await
        .expect_err("inbound child must restrict parent deletion");
    expect(
        restricted.semantic_error().is_some(),
        "delete restrict typed error",
    )?;
    let retained = client
        .create_restrict_parents(generated::CreateRestrictParentsInput {
            parents: vec![parent_a],
            request_id: id(57),
        })
        .await?;
    expect(
        matches!(
            retained.outcome,
            generated::CreateRestrictParentsOutcome::ParentAlreadyExists
        ),
        "restricted parent retained",
    )?;
    let deleted = client
        .delete_restrict_parents(generated::DeleteRestrictParentsInput {
            tenant_id: tenant.clone(),
            parent_ids: vec![parent_b.parent_id.clone()],
            request_id: id(58),
        })
        .await?;
    expect(
        matches!(
            deleted.outcome,
            generated::DeleteRestrictParentsOutcome::RestrictParentsDeleted
        ),
        "unreferenced parent delete",
    )?;
    let recreated = client
        .create_restrict_parents(generated::CreateRestrictParentsInput {
            parents: vec![parent_b],
            request_id: id(59),
        })
        .await?;
    expect(
        matches!(
            recreated.outcome,
            generated::CreateRestrictParentsOutcome::RestrictParentsCreated
        ),
        "deleted parent absent",
    )?;

    println!(
        "{}",
        serde_json::json!({
            "schema": "riffdb.adapter-bulk-observation/v1",
            "language": "rust",
            "bounded_preflight": true,
            "atomic_conflict": true,
            "replayed": true,
            "delete_restrict": true,
            "adapters": ["mlflow", "openfga", "payload", "woodpecker"],
        })
    );
    Ok(())
}

fn tuple(store_id: &str, tuple_id: &str) -> generated::FgaTuple {
    generated::FgaTuple {
        object: "document:roadmap".to_owned(),
        subject: "user:agent".to_owned(),
        relation: "viewer".to_owned(),
        store_id: store_id.to_owned(),
        tuple_id: tuple_id.to_owned(),
    }
}

fn id(suffix: u8) -> String {
    format!("018f0f8b-7c6d-7e31-8a4f-00000000{suffix:04x}")
}

fn expect(condition: bool, label: &str) -> TestResult<()> {
    condition
        .then_some(())
        .ok_or_else(|| format!("adapter bulk assertion failed: {label}").into())
}

fn required(name: &str) -> TestResult<String> {
    std::env::var(name)
        .map_err(|_| format!("required environment variable is absent: {name}").into())
}
