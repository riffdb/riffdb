//! Rust cell for the shared adapter-shaped row-policy corpus.

#![forbid(unsafe_code)]
#![allow(dead_code, unreachable_pub)]

use std::error::Error;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use riffdb_client_rust::{
    AttemptBudget, CallMetadata, DatabaseAlias, StableApplicationClient,
    load_protected_bearer_credential,
};
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
};

#[allow(dead_code, unreachable_pub, unused_imports)]
mod generated {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/adapters/row-policy-conformance/generated/rust/client.rs"
    ));
}

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Rust adapter row-policy conformance failed: {error}");
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
    let mut client = generated::AdapterRowPolicyConformanceClient::new(
        transport,
        metadata,
        AttemptBudget::new(3).ok_or("attempt budget")?,
    );
    let mode = required("RIFFDB_ROW_POLICY_MODE")?;
    let observation = observe(&mut client, &mode).await?;
    if mode == "owner"
        && let Ok(mutator) = std::env::var("RIFFDB_ROW_POLICY_MUTATOR_CREDENTIAL")
    {
        verify_live_acl_revocation(&mut client, &tls, PathBuf::from(mutator)).await?;
    }
    println!("{}", serde_json::to_string(&observation)?);
    Ok(())
}

async fn verify_live_acl_revocation(
    owner: &mut generated::AdapterRowPolicyConformanceClient,
    tls: &TlsClientConfig,
    mutator_credential: PathBuf,
) -> TestResult<()> {
    let mut stream = owner
        .watch_document_list_watch(
            generated::DocumentListWatchParams {
                organization_id: id(10),
            },
            None,
        )
        .await?;
    let initial = tokio::time::timeout(Duration::from_secs(10), stream.message()).await??;
    let Some(generated::DocumentListWatchUpdate::Snapshot(initial)) = initial else {
        return Err("live row-policy watch omitted its initial snapshot".into());
    };
    let generated::ListDocumentsResult::Found(initial) = initial.result;
    expect(initial.documents.len() == 6, "live initial policy snapshot")?;

    let credential = load_protected_bearer_credential(&mutator_credential)?;
    let metadata =
        CallMetadata::authenticated(credential).with_database(DatabaseAlias::default_alias());
    let transport = StableApplicationClient::connect_verified_tls(tls).await?;
    let mut mutator = generated::AdapterRowPolicyConformanceClient::new(
        transport,
        metadata,
        AttemptBudget::new(3).ok_or("attempt budget")?,
    );
    let narrowed = mutator
        .change_document_access(generated::ChangeDocumentAccessInput {
            group_id: None,
            request_id: id(190),
            visibility: "Private".to_owned(),
            document_id: id(15),
            organization_id: id(10),
        })
        .await?;
    expect(
        matches!(
            narrowed.outcome,
            generated::ChangeDocumentAccessOutcome::DocumentAccessChanged { .. }
        ),
        "ACL narrowing command",
    )?;
    wait_for_reset_document_count(&mut stream, 5).await?;

    let restored = mutator
        .change_document_access(generated::ChangeDocumentAccessInput {
            group_id: Some(id(0x32)),
            request_id: id(191),
            visibility: "Group".to_owned(),
            document_id: id(15),
            organization_id: id(10),
        })
        .await?;
    expect(
        matches!(
            restored.outcome,
            generated::ChangeDocumentAccessOutcome::DocumentAccessChanged { .. }
        ),
        "ACL widening command",
    )?;
    let current = owner
        .list_documents(generated::ListDocumentsParams {
            organization_id: id(10),
        })
        .await?;
    let generated::ListDocumentsResult::Found(current) = current;
    expect(
        current.documents.len() == 6,
        "ACL widening is visible to a fresh authoritative query",
    )?;
    wait_for_reset_document_count(&mut stream, 6).await?;
    Ok(())
}

async fn wait_for_reset_document_count(
    stream: &mut riffdb_client_rust::TypedLiveQueryStream<generated::DocumentListWatchParams>,
    expected: usize,
) -> TestResult<()> {
    for _ in 0..16 {
        let update = tokio::time::timeout(Duration::from_secs(10), stream.message()).await??;
        match update {
            Some(generated::DocumentListWatchUpdate::Reset(reset)) => {
                let generated::ListDocumentsResult::Found(result) = reset.result;
                if result.documents.len() == expected {
                    return Ok(());
                }
            }
            Some(generated::DocumentListWatchUpdate::Checkpoint(_)) => {}
            Some(generated::DocumentListWatchUpdate::Terminal(terminal)) => {
                return Err(
                    format!("live row-policy watch terminated: {}", terminal.reason).into(),
                );
            }
            Some(
                generated::DocumentListWatchUpdate::Snapshot(_)
                | generated::DocumentListWatchUpdate::Patch(_),
            ) => {
                return Err("reset-mode live row-policy watch returned the wrong update".into());
            }
            None => return Err("live row-policy watch closed before ACL update".into()),
        }
    }
    Err(format!("live row-policy watch did not converge to {expected} visible rows").into())
}

async fn observe(
    client: &mut generated::AdapterRowPolicyConformanceClient,
    mode: &str,
) -> TestResult<serde_json::Value> {
    let (principal_id, document_suffix, request_suffix) = match mode {
        "owner" => (id(1), 60, 160),
        "outsider" => (id(3), 61, 161),
        _ => return Err("unknown row-policy mode".into()),
    };
    let created = client
        .create_document(generated::CreateDocumentInput {
            body: "created through every generated language".to_owned(),
            state: "Draft".to_owned(),
            title: format!("Document shared-{mode}"),
            group_id: None,
            owner_id: principal_id.clone(),
            request_id: id(request_suffix),
            visibility: "Private".to_owned(),
            document_id: id(document_suffix),
            organization_id: id(10),
        })
        .await?;
    expect(
        matches!(
            created.outcome,
            generated::CreateDocumentOutcome::DocumentCreated { .. }
        ),
        "typed protected command outcome",
    )?;

    let documents = client
        .list_documents(generated::ListDocumentsParams {
            organization_id: id(10),
        })
        .await?;
    let generated::ListDocumentsResult::Found(documents) = documents;
    let drafts = client
        .list_draft_documents(generated::ListDraftDocumentsParams {
            organization_id: id(10),
        })
        .await?;
    let generated::ListDraftDocumentsResult::Found(drafts) = drafts;
    let search = client
        .search_documents(generated::SearchDocumentsParams {
            organization_id: id(10),
            title_prefix: "Document".to_owned(),
        })
        .await?;
    let generated::SearchDocumentsResult::Found(search) = search;
    let detail = client
        .get_document(generated::GetDocumentParams {
            organization_id: id(10),
            document_id: id(15),
        })
        .await?;
    let document_summary = client
        .document_summary(generated::DocumentSummaryParams {
            organization_id: id(10),
        })
        .await?;
    let generated::DocumentSummaryResult::Found(document_summary) = document_summary;
    let experiments = client
        .list_experiments(generated::ListExperimentsParams {
            organization_id: id(10),
        })
        .await?;
    let generated::ListExperimentsResult::Found(experiments) = experiments;
    let dashboard = client
        .metric_dashboard(generated::MetricDashboardParams {
            organization_id: id(10),
        })
        .await?;
    let generated::MetricDashboardResult::Found(dashboard) = dashboard;
    let summary = dashboard.summary.first();
    let group_run = client
        .run_page(generated::RunPageParams {
            organization_id: id(10),
            experiment_id: id(23),
            run_id: id(31),
        })
        .await?;

    let (document_count, draft_count, experiment_count, metric_count, run_visible) = match mode {
        "owner" => (6, 4, 3, 3, true),
        "outsider" => (3, 2, 1, 1, false),
        _ => unreachable!("mode checked above"),
    };
    expect(
        documents.documents.len() == document_count,
        "policy-filtered Payload document page",
    )?;
    expect(
        experiments.experiments.len() == experiment_count,
        "policy-filtered MLflow experiment page",
    )?;
    expect(
        drafts.documents.len() == draft_count,
        "policy-filtered Payload draft page",
    )?;
    expect(
        search.documents.len() == document_count,
        "policy-filtered Payload text search",
    )?;
    expect(
        document_summary
            .summary
            .iter()
            .map(|group| group.document_count)
            .sum::<u64>()
            == document_count as u64,
        "policy-before-aggregate Payload summary",
    )?;
    expect(
        matches!(detail, generated::GetDocumentResult::Found(_)) == (mode == "owner"),
        "policy-filtered Payload detail",
    )?;
    expect(
        summary.is_some_and(|summary| summary.sample_count == metric_count),
        "policy-before-aggregate MLflow dashboard",
    )?;
    match group_run {
        generated::RunPageResult::Found(page) => {
            expect(run_visible, "hidden group run was disclosed")?;
            expect(
                page.metrics.len() == 1 && page.artifacts.len() == 1,
                "nested row-policy hydration",
            )?;
        }
        generated::RunPageResult::NotFound(_) => {
            expect(!run_visible, "authorized group run was absent")?;
        }
    }

    let transfer = client
        .attempt_document_transfer(generated::AttemptDocumentTransferInput {
            request_id: id(request_suffix + 10),
            document_id: id(document_suffix),
            new_owner_id: id(2),
            organization_id: id(10),
        })
        .await
        .expect_err("successor-row owner escape must be denied");
    expect(
        transfer
            .semantic_error()
            .is_some_and(|error| error.code().as_str() == "RDB-AUTH-0214"),
        "typed successor-row authorization error",
    )?;

    let lifecycle_checked = if mode == "owner" {
        let finished = client
            .finish_run(generated::FinishRunInput {
                run_id: id(33),
                request_id: id(180),
                experiment_id: id(21),
                organization_id: id(10),
                expected_revision: 1,
            })
            .await?;
        expect(
            matches!(
                finished.outcome,
                generated::FinishRunOutcome::RunFinished { .. }
            ),
            "revision-checked MLflow transition",
        )?;
        let stale = client
            .finish_run(generated::FinishRunInput {
                run_id: id(33),
                request_id: id(181),
                experiment_id: id(21),
                organization_id: id(10),
                expected_revision: 1,
            })
            .await?;
        expect(
            matches!(stale.outcome, generated::FinishRunOutcome::FinishStale),
            "stale MLflow transition outcome",
        )?;
        true
    } else {
        false
    };

    Ok(serde_json::json!({
        "schema": "riffdb.adapter-row-policy-observation/v1",
        "language": "rust",
        "mode": mode,
        "documents": document_count,
        "drafts": draft_count,
        "experiments": experiment_count,
        "metrics": metric_count,
        "group_run_visible": run_visible,
        "policy_before_aggregate": true,
        "detail_and_search": true,
        "nested_policy": true,
        "protected_command": true,
        "successor_escape_denied": true,
        "lifecycle_checked": lifecycle_checked,
    }))
}

fn required(name: &str) -> TestResult<String> {
    std::env::var(name).map_err(|_| format!("{name} is required").into())
}

fn id(suffix: u8) -> String {
    format!("018f0f8b-7c6d-7e31-8a4f-0000000000{suffix:02x}")
}

fn expect(condition: bool, label: &str) -> TestResult<()> {
    if condition { Ok(()) } else { Err(label.into()) }
}
