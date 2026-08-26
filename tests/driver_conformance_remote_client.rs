//! Installed Rust client cell for the shared remote driver corpus.

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

async fn run_async() -> TestResult<()> {
    let endpoint = required("RIFFDB_CONFORMANCE_ENDPOINT")?;
    let trust_root = PathBuf::from(required("RIFFDB_CONFORMANCE_TRUST_ROOT")?);
    let credential_path = PathBuf::from(required("RIFFDB_CONFORMANCE_CREDENTIAL")?);
    let tls = TlsClientConfig::new(
        CanonicalHttpsEndpoint::parse(&endpoint)?,
        ProtectedFilePath::new(trust_root)?,
        TlsServerIdentity::parse("127.0.0.1")?,
        Duration::from_secs(5),
        Duration::from_secs(30),
        NonZeroU32::new(4).ok_or("pool bound")?,
        NonZeroU32::new(64).ok_or("stream bound")?,
    )?;
    let credential = load_protected_bearer_credential(&credential_path)?;
    let metadata =
        CallMetadata::authenticated(credential).with_database(DatabaseAlias::default_alias());
    let transport = StableApplicationClient::connect_verified_tls(&tls).await?;
    let mut client = generated::DriverConformanceClient::new(
        transport,
        metadata,
        AttemptBudget::new(3).ok_or("attempt budget")?,
    );
    let organization_id = "018f0f8b-7c6d-7e31-8a4f-000000000100".to_owned();
    if std::env::var_os("RIFFDB_CONFORMANCE_EXPECT_REVOKED").is_some() {
        let error = client
            .item_secret(generated::ItemSecretParams {
                organization_id: organization_id.clone(),
                item_id: "018f0f8b-7c6d-7e31-8a4f-000000000101".to_owned(),
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
    let item_id = "018f0f8b-7c6d-7e31-8a4f-000000000101".to_owned();
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
    let commit = first
        .commit_sequence
        .ok_or("command omitted commit sequence")?;
    if first.replayed || !matches!(first.outcome, generated::CreateItemOutcome::Created) {
        return Err("first Rust command did not create the item".into());
    }
    let replay = client.create_item(input).await?;
    if !replay.replayed {
        return Err("second Rust command did not replay".into());
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
        })
    );
    Ok(())
}

fn required(name: &str) -> TestResult<String> {
    std::env::var(name)
        .map_err(|_| format!("required environment variable is absent: {name}").into())
}
