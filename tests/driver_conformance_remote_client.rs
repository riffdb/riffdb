//! Installed Rust client cell for the shared remote driver corpus.

#![forbid(unsafe_code)]
#![allow(dead_code, unreachable_pub)]

use std::collections::BTreeSet;
use std::error::Error;
use std::fs;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use riffdb_client_rust::{
    ApplicationClientError, AttemptBudget, CallMetadata, ClientError, DatabaseAlias, QueryOptions,
    StableApplicationClient, load_protected_bearer_credential,
};
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
};

#[allow(dead_code, unreachable_pub)]
mod generated {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/driver/conformance-app/generated/rust/client.rs"
    ));
}

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Rust remote driver conformance failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> TestResult<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_async())
}

async fn connect_client(
    endpoint: &str,
    trust_root: &Path,
    credential_path: &Path,
    attempts: u32,
) -> TestResult<generated::DriverConformanceClient> {
    let pool_connections = if attempts <= 1 { 1 } else { 4 };
    let tls = TlsClientConfig::new(
        CanonicalHttpsEndpoint::parse(endpoint)?,
        ProtectedFilePath::new(trust_root.to_path_buf())?,
        TlsServerIdentity::parse("127.0.0.1")?,
        Duration::from_secs(5),
        Duration::from_secs(30),
        NonZeroU32::new(pool_connections).ok_or("pool bound")?,
        NonZeroU32::new(64).ok_or("stream bound")?,
    )?;
    let credential = load_protected_bearer_credential(credential_path)?;
    let database = match std::env::var("RIFFDB_CONFORMANCE_DATABASE") {
        Ok(raw) if !raw.is_empty() => DatabaseAlias::new(raw)?,
        _ => DatabaseAlias::default_alias(),
    };
    let metadata = CallMetadata::authenticated(credential).with_database(database);
    let transport = StableApplicationClient::connect_verified_tls(&tls).await?;
    Ok(generated::DriverConformanceClient::new(
        transport,
        metadata,
        AttemptBudget::new(attempts).ok_or("attempt budget")?,
    ))
}

async fn run_async() -> TestResult<()> {
    let endpoint = required("RIFFDB_CONFORMANCE_ENDPOINT")?;
    let trust_root = PathBuf::from(required("RIFFDB_CONFORMANCE_TRUST_ROOT")?);
    let credential_path = PathBuf::from(required("RIFFDB_CONFORMANCE_CREDENTIAL")?);
    let organization_id = "018f0f8b-7c6d-7e31-8a4f-000000000100".to_owned();
    let item_id = "018f0f8b-7c6d-7e31-8a4f-000000000101".to_owned();

    if std::env::var_os("RIFFDB_CONFORMANCE_EXPECT_UNCERTAIN").is_some() {
        let uncertain_endpoint = required("RIFFDB_CONFORMANCE_UNCERTAIN_ENDPOINT")?;
        let arm_file = PathBuf::from(required("RIFFDB_CONFORMANCE_ARM_FILE")?);
        let mut uncertain_client =
            connect_client(&uncertain_endpoint, &trust_root, &credential_path, 1).await?;
        let _ = uncertain_client
            .item_page(generated::ItemPageParams {
                organization_id: organization_id.clone(),
                item_id: item_id.clone(),
            })
            .await?;
        fs::write(&arm_file, b"arm\n")?;
        let uncertain_input = generated::CreateItemInput {
            title: "Uncertain Rust item".to_owned(),
            token_digest: "rust-uncertain-secret".to_owned(),
            item_id: "018f0f8b-7c6d-7e31-8a4f-000000000199".to_owned(),
            idempotency_key: "driver-conformance-rust-uncertain-v1".to_owned(),
            organization_id: organization_id.clone(),
        };
        let err = uncertain_client
            .create_item(uncertain_input.clone())
            .await
            .expect_err("severed post-send Rust stream must return OutcomeUnknown");
        if !matches!(
            err,
            ApplicationClientError::Client(ClientError::OutcomeUnknown(_))
        ) {
            return Err(format!("expected Rust OutcomeUnknown, got {err:?}").into());
        }
        let mut recovery_client =
            connect_client(&endpoint, &trust_root, &credential_path, 3).await?;
        let recovered = recovery_client.create_item(uncertain_input).await?;
        if !recovered.replayed
            || !matches!(recovered.outcome, generated::CreateItemOutcome::Created)
        {
            return Err("Rust uncertain recovery did not replay committed outcome".into());
        }
        println!(
            "{}",
            serde_json::json!({
                "schema": "riffdb.driver-conformance-fault/v1",
                "fault": "outcome_unknown",
                "uncertain": true,
                "replayed": true,
            })
        );
        return Ok(());
    }

    let mut client = connect_client(&endpoint, &trust_root, &credential_path, 3).await?;

    if std::env::var_os("RIFFDB_CONFORMANCE_EXPECT_AUTH_DIAG").is_some() {
        let error = client
            .items_by_ids(generated::ItemsByIdsParams {
                organization_id: organization_id.clone(),
                item_ids: vec![
                    item_id.clone(),
                    "018f0f8b-7c6d-7e31-8a4f-000000000151".to_owned(),
                ],
            })
            .await
            .expect_err("narrowed Rust capability must fail scan budget");
        let semantic = error
            .semantic_error()
            .ok_or("Rust auth diagnostic error lost semantic details")?;
        let code = semantic.code().as_str();
        let diag = semantic
            .context()
            .authorization_diagnostic()
            .map(|d| d.as_str())
            .ok_or("Rust auth error omitted authorization_diagnostic")?;
        if code != "RDB-AUTH-0214" || diag != "scan_budget_exceeded" {
            return Err(format!("unexpected Rust auth diagnostic: {code} / {diag}").into());
        }
        println!(
            "{}",
            serde_json::json!({
                "schema": "riffdb.driver-conformance-fault/v1",
                "fault": "authorization_diagnostic",
                "code": code,
                "diagnostic": diag,
            })
        );
        return Ok(());
    }

    if std::env::var_os("RIFFDB_CONFORMANCE_EXPECT_REVOKED").is_some() {
        let error = client
            .item_secret(generated::ItemSecretParams {
                organization_id: organization_id.clone(),
                item_id,
            })
            .await
            .expect_err("revoked Rust authority must fail");
        let code = error
            .semantic_error()
            .map(|semantic| semantic.code().as_str())
            .ok_or("Rust revocation error lost semantic details")?;
        if code != "RDB-AUTH-0215" {
            return Err(format!("unexpected Rust revocation code: {code}").into());
        }
        println!(
            "{}",
            serde_json::json!({
                "schema": "riffdb.driver-conformance-fault/v1",
                "fault": "revocation",
                "code": code,
            })
        );
        return Ok(());
    }

    let idempotency_key = "driver-conformance-rust-create-v1".to_owned();
    let token_digest = "rust-secret-digest-must-not-log".to_owned();
    let input = generated::CreateItemInput {
        title: "Shared remote Rust".to_owned(),
        token_digest: token_digest.clone(),
        item_id: item_id.clone(),
        idempotency_key: idempotency_key.clone(),
        organization_id: organization_id.clone(),
    };
    let first = client.create_item(input.clone()).await?;
    let mut commit = first
        .commit_sequence
        .ok_or("command omitted commit sequence")?;
    if first.replayed || !matches!(first.outcome, generated::CreateItemOutcome::Created) {
        return Err("first Rust command did not create the item".into());
    }
    let replay = client.create_item(input).await?;
    if !replay.replayed {
        return Err("second Rust command did not replay".into());
    }

    // Extra collation/PK-set items (titles intentionally omit "remote").
    let extra_items = [
        ("018f0f8b-7c6d-7e31-8a4f-000000000151", "zeta-tag"),
        ("018f0f8b-7c6d-7e31-8a4f-000000000152", "Alpha-tag"),
        ("018f0f8b-7c6d-7e31-8a4f-000000000153", "alpha-tag"),
        ("018f0f8b-7c6d-7e31-8a4f-000000000154", "Zeta-tag"),
    ];
    for (index, (extra_id, extra_title)) in extra_items.iter().enumerate() {
        let res = client
            .create_item(generated::CreateItemInput {
                title: (*extra_title).to_owned(),
                token_digest: format!("rust-extra-digest-{index}"),
                item_id: (*extra_id).to_owned(),
                idempotency_key: format!("driver-conformance-rust-extra-{index}"),
                organization_id: organization_id.clone(),
            })
            .await?;
        commit = res.commit_sequence.ok_or("extra item commit sequence")?;
    }

    let token_id = "018f0f8b-7c6d-7e31-8a4f-000000000111".to_owned();
    let token_value = "rust-one-time-secret-must-not-log".to_owned();
    let issued = client
        .issue_token(generated::IssueTokenInput {
            value: token_value.clone(),
            token_id: token_id.clone(),
            expires_at: generated::TimestampValue {
                seconds: 4_102_444_800,
                nanos: 0,
            },
            identifier: "rust-loopback".to_owned(),
            request_id: "018f0f8b-7c6d-7e31-8a4f-000000000112".to_owned(),
            organization_id: organization_id.clone(),
        })
        .await?;
    if !matches!(
        issued.outcome,
        generated::IssueTokenOutcome::TokenIssued { .. }
    ) {
        return Err("Rust token issue did not create the token".into());
    }
    let consume_input = generated::ConsumeTokenInput {
        token_id: token_id.clone(),
        request_id: "018f0f8b-7c6d-7e31-8a4f-000000000113".to_owned(),
        organization_id: organization_id.clone(),
    };
    let consumed = client.consume_token(consume_input.clone()).await?;
    let generated::ConsumeTokenOutcome::TokenConsumed {
        value,
        token_id: returned_token_id,
        identifier,
        ..
    } = &consumed.outcome
    else {
        return Err("Rust token consume did not return the deleted preimage".into());
    };
    if value != &token_value
        || returned_token_id != &token_id
        || identifier != "rust-loopback"
        || format!("{:?}", consumed.outcome).contains(&token_value)
    {
        return Err("Rust deleted preimage or redacted Debug was incorrect".into());
    }
    let consumed_replay = client.consume_token(consume_input).await?;
    if !consumed_replay.replayed || consumed_replay.outcome != consumed.outcome {
        return Err("Rust token consume did not replay the persisted preimage".into());
    }
    let missing = client
        .consume_token(generated::ConsumeTokenInput {
            token_id: token_id.clone(),
            request_id: "018f0f8b-7c6d-7e31-8a4f-000000000114".to_owned(),
            organization_id: organization_id.clone(),
        })
        .await?;
    if !matches!(
        missing.outcome,
        generated::ConsumeTokenOutcome::TokenMissing
    ) {
        return Err("Rust second consumer did not observe the atomic delete".into());
    }
    let page = client
        .item_page_after_commit(
            generated::ItemPageParams {
                organization_id: organization_id.clone(),
                item_id: item_id.clone(),
            },
            commit,
        )
        .await?;
    let generated::ItemPageResult::Found(found) = page.value else {
        return Err("Rust read-after-commit did not find the item".into());
    };
    if found.item.item_id != item_id || found.item.title != "Shared remote Rust" {
        return Err("Rust query returned the wrong item".into());
    }
    let secret = client
        .item_secret_after_commit(
            generated::ItemSecretParams {
                organization_id: organization_id.clone(),
                item_id: item_id.clone(),
            },
            commit,
        )
        .await?;
    let generated::ItemSecretResult::Found(secret_found) = secret.value else {
        return Err("Rust secret query did not find the item".into());
    };
    if secret_found.secret.token_digest != token_digest
        || format!("{secret_found:?}").contains(&token_digest)
    {
        return Err("Rust secret query value or redacted Debug was incorrect".into());
    }
    let mut exact = None;
    for _ in 0..200 {
        match client
            .search_items_after_commit(
                generated::SearchItemsParams {
                    organization_id: organization_id.clone(),
                    needle: "remote Rust".to_owned(),
                    item_id: Some(item_id.clone()),
                    limit: 50,
                    offset: 0,
                },
                commit,
            )
            .await
        {
            Ok(result) => {
                exact = Some(result);
                break;
            }
            Err(error)
                if matches!(
                    error
                        .semantic_error()
                        .map(|semantic| semantic.code().as_str()),
                    Some("RDB-QUERY-0102" | "RDB-PROJECTION-0103")
                ) =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    let exact = exact.ok_or("Rust exact provider did not become ready within the retry bound")?;
    let generated::SearchItemsResult::Found(exact) = exact.value;
    if exact.total.value != 1
        || exact.items.len() != 1
        || exact.items[0].item_id != item_id
        || exact.items[0].organization_id != organization_id
    {
        return Err("Rust exact result page and whole-population total diverged".into());
    }

    // Post-mutation invariant rejection (`RDB-INPUT-0101`, `correct_request`) + non-terminal key.
    let account_id = "018f0f8b-7c6d-7e31-8a4f-000000000161".to_owned();
    let opened = client
        .open_budget_account(generated::OpenBudgetAccountInput {
            request_id: "rust-open-budget-v1".to_owned(),
            organization_id: organization_id.clone(),
            account_id: account_id.clone(),
            credit_limit: 500,
            note: "initial".to_owned(),
        })
        .await?;
    if !matches!(
        opened.outcome,
        generated::OpenBudgetAccountOutcome::AccountOpened { spent: 0 }
    ) {
        return Err("Rust OpenBudgetAccount failed".into());
    }
    let inv_err = client
        .record_spend(generated::RecordSpendInput {
            request_id: "rust-over-budget-v1".to_owned(),
            organization_id: organization_id.clone(),
            account_id: account_id.clone(),
            amount: 600,
        })
        .await
        .expect_err("over-budget spend must fail invariant");
    let inv_sem = inv_err
        .semantic_error()
        .ok_or("Rust invariant error lost semantic details")?;
    let invariant_error = inv_sem.code().as_str().to_owned();
    let invariant_recovery = inv_sem.recovery_action().as_str().to_owned();
    if invariant_error != "RDB-INPUT-0101" || invariant_recovery != "correct_request" {
        return Err(format!(
            "unexpected Rust invariant error: {invariant_error} / {invariant_recovery}"
        )
        .into());
    }
    let valid_spend = client
        .record_spend(generated::RecordSpendInput {
            request_id: "rust-over-budget-v1".to_owned(),
            organization_id: organization_id.clone(),
            account_id: account_id.clone(),
            amount: 200,
        })
        .await?;
    let invariant_non_terminal = !valid_spend.replayed
        && matches!(
            valid_spend.outcome,
            generated::RecordSpendOutcome::SpendRecorded { spent: 200 }
        );
    if !invariant_non_terminal {
        return Err("Rust CommitCheckRejected persisted terminal idempotency state".into());
    }

    // Optimistic workflow revision update + stale-revision conflict.
    let rev_ok = client
        .revise_budget_note(generated::ReviseBudgetNoteInput {
            request_id: "rust-revise-1".to_owned(),
            organization_id: organization_id.clone(),
            account_id: account_id.clone(),
            expected_revision: 2,
            note: "updated-note".to_owned(),
        })
        .await?;
    if !matches!(
        rev_ok.outcome,
        generated::ReviseBudgetNoteOutcome::NoteRevised { .. }
    ) {
        return Err("Rust ReviseBudgetNote at revision 2 did not succeed".into());
    }
    let rev_stale = client
        .revise_budget_note(generated::ReviseBudgetNoteInput {
            request_id: "rust-revise-stale".to_owned(),
            organization_id: organization_id.clone(),
            account_id: account_id.clone(),
            expected_revision: 2,
            note: "stale-note".to_owned(),
        })
        .await?;
    if !matches!(
        rev_stale.outcome,
        generated::ReviseBudgetNoteOutcome::StaleRevision
    ) {
        return Err("Rust stale expected_revision did not return StaleRevision".into());
    }

    // PK-set chunking & ascending UUID order (2-item set bound, 6 scrambled inputs -> 5 unique).
    let raw_ids = [
        "018f0f8b-7c6d-7e31-8a4f-000000000154",
        item_id.as_str(),
        "018f0f8b-7c6d-7e31-8a4f-000000000152",
        "018f0f8b-7c6d-7e31-8a4f-000000000154",
        "018f0f8b-7c6d-7e31-8a4f-000000000151",
        "018f0f8b-7c6d-7e31-8a4f-000000000153",
    ];
    let sorted_ids: Vec<String> = raw_ids
        .iter()
        .map(|s| (*s).to_owned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut merged_by_id = Vec::new();
    for chunk in sorted_ids.chunks(2) {
        let page = client
            .items_by_ids_after_commit(
                generated::ItemsByIdsParams {
                    organization_id: organization_id.clone(),
                    item_ids: chunk.to_vec(),
                },
                commit,
            )
            .await?;
        let generated::ItemsByIdsResult::Found(found_ids) = page.value;
        merged_by_id.extend(found_ids.items.into_iter().map(|row| row.item_id));
    }
    let pk_set_all_ordered = merged_by_id.windows(2).all(|w| w[0] < w[1]);
    if merged_by_id.len() != 5 || !pk_set_all_ordered {
        return Err("Rust PK-set chunking count or ordering failed".into());
    }

    // `binary_utf8_v1` collation set chunking (`"Alpha-tag" < "Zeta-tag" < "alpha-tag" < "zeta-tag"`).
    let raw_titles = [
        "zeta-tag",
        "Alpha-tag",
        "alpha-tag",
        "Zeta-tag",
        "Alpha-tag",
    ];
    let sorted_titles: Vec<String> = raw_titles
        .iter()
        .map(|s| (*s).to_owned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut collation_all_titles = Vec::new();
    for chunk in sorted_titles.chunks(2) {
        let pages = client
            .items_by_titles_pages(
                generated::ItemsByTitlesParams {
                    organization_id: organization_id.clone(),
                    titles: chunk.to_vec(),
                    after: None,
                },
                16,
                QueryOptions::new().read_after_commit(commit),
            )
            .await?;
        for page in pages {
            let generated::ItemsByTitlesResult::Found(found_titles) = page.value;
            collation_all_titles.extend(found_titles.items.into_iter().map(|row| row.title));
        }
    }
    if collation_all_titles != vec!["Alpha-tag", "Zeta-tag", "alpha-tag", "zeta-tag"] {
        return Err(format!("Rust collation order diverged: {collation_all_titles:?}").into());
    }

    // Wide-row + sibling-binding cursor continuation across >= 2 pages (> 4 MiB total).
    let wide_text = "R".repeat(380_000);
    let wide_blob = vec![0x5A_u8; 520_000];
    let mut wide_commit = commit;
    for seq in 1_i64..=6 {
        let written = client
            .write_wide_note(generated::WriteWideNoteInput {
                request_id: format!("rust-wide-note-{seq}"),
                organization_id: organization_id.clone(),
                note_seq: seq,
                note_text: wide_text.clone(),
                note_blob: wide_blob.clone(),
            })
            .await?;
        wide_commit = written.commit_sequence.ok_or("wide note commit sequence")?;
    }
    let wide_pages = client
        .wide_notes_page_pages(
            generated::WideNotesPageParams {
                organization_id: organization_id.clone(),
                anchor_item_id: item_id.clone(),
                limit: 10,
                after: None,
            },
            16,
            QueryOptions::new().read_after_commit(wide_commit),
        )
        .await?;
    let wide_cursor_pages_min_two = wide_pages.len() >= 2;
    let mut observed_seqs = Vec::new();
    for page in &wide_pages {
        let generated::WideNotesPageResult::Found(found_wide) = &page.value else {
            return Err("Rust WideNotesPage missed anchor".into());
        };
        if found_wide.anchor.item_id != item_id {
            return Err("Rust WideNotesPage sibling anchor mismatched".into());
        }
        for note in &found_wide.notes {
            if note.note_text.len() != 380_000 || note.note_blob.len() != 520_000 {
                return Err("Rust WideNotesPage payload truncated".into());
            }
            observed_seqs.push(note.note_seq);
        }
    }
    if !wide_cursor_pages_min_two || observed_seqs != vec![1, 2, 3, 4, 5, 6] {
        return Err(format!(
            "Rust WideNotesPage pagination failed: pages={}, seqs={observed_seqs:?}",
            wide_pages.len()
        )
        .into());
    }

    let reuse = client
        .create_item(generated::CreateItemInput {
            title: "Changed input".to_owned(),
            token_digest,
            item_id,
            idempotency_key,
            organization_id,
        })
        .await
        .expect_err("idempotency-key reuse must fail");
    let code = reuse
        .semantic_error()
        .map(|error| error.code().as_str())
        .ok_or("Rust reuse error lost semantic details")?;
    if code != "RDB-COMMAND-0101" {
        return Err(format!("unexpected Rust reuse code: {code}").into());
    }
    println!(
        "{}",
        serde_json::json!({
            "schema": "riffdb.driver-conformance-observation/v1",
            "language": "rust",
            "created": "Created",
            "replayed": true,
            "query": "Found",
            "read_after_commit": true,
            "reuse_error": code,
            "secret_query": "Found",
            "secret_redacted": true,
            "exact_query": "Found",
            "exact_total": 1,
            "consume": "TokenConsumed",
            "consume_replayed": true,
            "consume_missing": true,
            "delete_preimage_redacted": true,
            "invariant_error": invariant_error,
            "invariant_recovery": invariant_recovery,
            "invariant_non_terminal": invariant_non_terminal,
            "revision_updated": "NoteRevised",
            "revision_stale": "StaleRevision",
            "pk_set_all_count": merged_by_id.len(),
            "pk_set_all_ordered": pk_set_all_ordered,
            "collation_all_titles": collation_all_titles,
            "wide_cursor_pages_min_two": wide_cursor_pages_min_two,
            "wide_cursor_total": observed_seqs.len(),
        })
    );
    Ok(())
}

fn required(name: &str) -> TestResult<String> {
    std::env::var(name)
        .map_err(|_| format!("required environment variable is absent: {name}").into())
}
