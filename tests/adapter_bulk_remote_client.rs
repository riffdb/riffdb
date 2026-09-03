//! Rust cell for the shared adapter-shaped bounded-command corpus.

// req: AAA-002, AAA-003, AAA-004, AAA-005, BLK-006, BLK-007, BLK-008, BLK-009,
// req: BLK-013, BLK-014, BLK-019, BLK-021, ID-001, ID-004, OUT-001, OUT-002,
// req: STO-002, TXN-001, TXN-010, TXN-013, TXN-040, TXN-041, TXN-042, TXN-043,
// req: TXN-044, BLK-068, BLK-070

#![forbid(unsafe_code)]
#![allow(dead_code, unreachable_pub)]

use std::error::Error;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use riffdb_client_rust::generated::GeneratedCommand as _;
use riffdb_client_rust::generated::{GeneratedCommandError, GeneratedInputBudgetCause};
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

    let experiment_id = id(1_000);
    let metrics = (2_000_u16..3_000)
        .map(|metric_id| generated::Metric {
            name: "latency".to_owned(),
            step: i64::from(metric_id),
            metric_id: id(metric_id),
            value_micros: i64::from(metric_id),
            experiment_id: experiment_id.clone(),
        })
        .collect::<Vec<_>>();
    let high_cardinality = generated::LogMetricsInput {
        metrics: metrics.clone(),
        request_id: id(1_001),
        experiment_id: experiment_id.clone(),
    };
    let first = client.log_metrics(high_cardinality.clone()).await?;
    let generated::LogMetricsOutcome::MetricsLogged { revision } = first.outcome else {
        return Err("MLflow 1,000-entity store did not commit".into());
    };
    expect(revision == 1, "MLflow fixed root mutation")?;
    let replay = client.log_metrics(high_cardinality).await?;
    expect(replay.replayed, "MLflow 1,000-entity replay")?;
    expect(
        matches!(
            replay.outcome,
            generated::LogMetricsOutcome::MetricsLogged { revision: 1 }
        ),
        "MLflow replay retained the root outcome",
    )?;

    let mut forced_failure = vec![metrics[0].clone()];
    forced_failure.extend((3_000_u16..3_999).map(|metric_id| generated::Metric {
        name: "forced-failure".to_owned(),
        step: i64::from(metric_id),
        metric_id: id(metric_id),
        value_micros: i64::from(metric_id),
        experiment_id: experiment_id.clone(),
    }));
    let failed = client
        .log_metrics(generated::LogMetricsInput {
            metrics: forced_failure,
            request_id: id(1_002),
            experiment_id: experiment_id.clone(),
        })
        .await?;
    expect(
        matches!(
            failed.outcome,
            generated::LogMetricsOutcome::MetricAlreadyExists
        ),
        "MLflow forced whole-command failure",
    )?;

    let recovered_metrics = (3_000_u16..4_000)
        .map(|metric_id| generated::Metric {
            name: "after-failure".to_owned(),
            step: i64::from(metric_id),
            metric_id: id(metric_id),
            value_micros: i64::from(metric_id),
            experiment_id: experiment_id.clone(),
        })
        .collect::<Vec<_>>();
    let recovered = client
        .log_metrics(generated::LogMetricsInput {
            metrics: recovered_metrics,
            request_id: id(1_003),
            experiment_id,
        })
        .await?;
    expect(
        matches!(
            recovered.outcome,
            generated::LogMetricsOutcome::MetricsLogged { revision: 2 }
        ),
        "failed store left neither a row prefix nor a root mutation",
    )?;

    let mut too_many = metrics;
    too_many.push(too_many[0].clone());
    if (generated::LogMetricsInput {
        metrics: too_many,
        request_id: id(1_004),
        experiment_id: id(1_000),
    })
    .idempotent_command()
    .is_ok()
    {
        return Err("1,001 generated collection elements crossed Rust preflight".into());
    }

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

    let empty_budget = generated::WritePolicyMutationsInput {
        mutations: vec![],
        request_id: id(60),
    }
    .idempotent_command()
    .expect_err("empty collection refuses locally");
    expect_budget_error(
        empty_budget,
        GeneratedInputBudgetCause::CollectionCount,
        "mutations",
        None,
        None,
    )?;
    let individual = generated::WritePolicyMutationsInput {
        mutations: vec![policy_mutation(&id(60), &id(61), Some(vec![0; 524_289]))],
        request_id: id(62),
    }
    .idempotent_command()
    .expect_err("individual byte overflow refuses locally");
    expect_budget_error(
        individual,
        GeneratedInputBudgetCause::IndividualValueBytes,
        "mutations",
        Some(0),
        Some("context"),
    )?;
    let multibyte_boundary = generated::WritePolicyMutationsInput {
        mutations: vec![generated::PolicyMutation {
            context: None,
            relation: "é".repeat(32),
            mutation_id: id(1201),
            organization_id: id(1200),
        }],
        request_id: id(1202),
    };
    expect(
        matches!(
            client
                .write_policy_mutations(multibyte_boundary)
                .await?
                .outcome,
            generated::WritePolicyMutationsOutcome::PolicyMutationsWritten
        ),
        "64-byte multibyte leaf is accepted",
    )?;
    let multibyte_overflow = generated::WritePolicyMutationsInput {
        mutations: vec![generated::PolicyMutation {
            context: None,
            relation: format!("{}a", "é".repeat(32)),
            mutation_id: id(1204),
            organization_id: id(1203),
        }],
        request_id: id(1205),
    }
    .idempotent_command()
    .expect_err("65-byte multibyte leaf refuses locally");
    expect_budget_error(
        multibyte_overflow,
        GeneratedInputBudgetCause::IndividualValueBytes,
        "mutations",
        Some(0),
        Some("relation"),
    )?;
    let oversized = generated::WritePolicyMutationsInput {
        mutations: vec![
            policy_mutation(&id(60), &id(61), Some(vec![0; 450_000])),
            policy_mutation(&id(60), &id(62), Some(vec![0; 450_000])),
        ],
        request_id: id(63),
    };
    let aggregate = oversized
        .idempotent_command()
        .expect_err("aggregate byte overflow refuses locally");
    expect_budget_error(
        aggregate,
        GeneratedInputBudgetCause::AggregateCanonicalElementBytes,
        "mutations",
        None,
        None,
    )?;
    for (count, start, organization, request) in [
        (1usize, 60u16, 62u16, 63u16),
        (9usize, 70u16, 64u16, 65u16),
        (19, 80, 66, 67),
        (100, 100, 68, 69),
    ] {
        let organization_id = id(organization);
        let mutations = (0..count)
            .map(|offset| {
                policy_mutation(
                    &organization_id,
                    &id(start + u16::try_from(offset).expect("bounded mutation offset")),
                    (count == 100 && offset == 0).then(|| vec![0xa5; 524_288]),
                )
            })
            .collect();
        expect(
            matches!(
                client
                    .write_policy_mutations(generated::WritePolicyMutationsInput {
                        mutations,
                        request_id: id(request),
                    })
                    .await?
                    .outcome,
                generated::WritePolicyMutationsOutcome::PolicyMutationsWritten
            ),
            "neutral aggregate collection",
        )?;
    }

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
        .await?;
    expect(
        matches!(
            restricted.outcome,
            generated::DeleteRestrictParentsOutcome::ParentReferenced
        ),
        "delete restrict declared outcome",
    )?;
    let restricted_replay = client
        .delete_restrict_parents(generated::DeleteRestrictParentsInput {
            tenant_id: tenant.clone(),
            parent_ids: vec![parent_a.parent_id.clone()],
            request_id: id(56),
        })
        .await?;
    expect(
        restricted_replay.replayed
            && matches!(
                restricted_replay.outcome,
                generated::DeleteRestrictParentsOutcome::ParentReferenced
            ),
        "delete restrict outcome replay",
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
            "neutral_aggregate": true,
            "budget_errors": [
                {"cause":"collection_count","collection":"mutations"},
                {"cause":"individual_value_bytes","collection":"mutations","index":0,"leaf":"context"},
                {"cause":"individual_value_bytes","collection":"mutations","index":0,"leaf":"relation"},
                {"cause":"aggregate_canonical_element_bytes","collection":"mutations"},
            ],
            "high_cardinality_atomic": true,
            "adapters": ["mlflow", "openfga", "payload", "woodpecker"],
        })
    );
    Ok(())
}

fn expect_budget_error(
    error: GeneratedCommandError,
    cause: GeneratedInputBudgetCause,
    collection: &str,
    index: Option<usize>,
    leaf: Option<&str>,
) -> TestResult<()> {
    let GeneratedCommandError::InputBudget(error) = error else {
        return Err("generated Rust budget refusal used the wrong error class".into());
    };
    expect(error.cause() == cause, "Rust budget cause")?;
    expect(error.collection() == collection, "Rust budget collection")?;
    expect(error.index() == index, "Rust budget index")?;
    expect(error.leaf() == leaf, "Rust budget leaf")
}

fn policy_mutation(
    organization_id: &str,
    mutation_id: &str,
    context: Option<Vec<u8>>,
) -> generated::PolicyMutation {
    generated::PolicyMutation {
        context,
        relation: "viewer".to_owned(),
        mutation_id: mutation_id.to_owned(),
        organization_id: organization_id.to_owned(),
    }
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

fn id(suffix: u16) -> String {
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
