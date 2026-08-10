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
    if std::env::var_os("RIFFDB_CONFORMANCE_EXPECT_REVOKED").is_some() {
        let error = client
            .item_page(generated::ItemPageParams {
                item_id: "018f0f8b-7c6d-7e31-8a4f-000000000101".to_owned(),
            })
            .await
            .expect_err("revoked Rust authority must fail");
        let code = error
            .semantic_error()
            .map(|semantic| semantic.code().as_str())
            .ok_or("Rust revocation error lost semantic details")?;
        if code != "RDB-AUTH-0214" {
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
    let input = generated::CreateItemInput {
        title: "Shared remote Rust".to_owned(),
        item_id: item_id.clone(),
        idempotency_key: idempotency_key.clone(),
    };
    let first = client.create_item(input.clone()).await?;
    let commit = first
        .commit_sequence
        .ok_or("command omitted commit sequence")?;
    if first.replayed || !matches!(first.outcome, generated::CreateItemOutcome::Created { .. }) {
        return Err("first Rust command did not create the item".into());
    }
    let replay = client.create_item(input).await?;
    if !replay.replayed {
        return Err("second Rust command did not replay".into());
    }
    let page = client
        .item_page_after_commit(
            generated::ItemPageParams {
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
    let reuse = client
        .create_item(generated::CreateItemInput {
            title: "Changed input".to_owned(),
            item_id,
            idempotency_key,
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
        })
    );
    Ok(())
}

fn required(name: &str) -> TestResult<String> {
    std::env::var(name)
        .map_err(|_| format!("required environment variable is absent: {name}").into())
}
