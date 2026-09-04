// req: OQ-004, OQ-006, OQ-016, OQ-061
#![forbid(unsafe_code)]
#![allow(dead_code, unreachable_pub)]

use std::error::Error;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::time::Duration;

use riffdb_client_rust::{
    AttemptBudget, CallMetadata, DatabaseAlias, QueryOptions, StableApplicationClient,
    load_protected_bearer_credential,
};
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
};

#[allow(dead_code, unreachable_pub)]
mod generated {
    include!(concat!(env!("WP701_GENERATED_ROOT"), "/rust/client.rs"));
}

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::main]
async fn main() -> TestResult<()> {
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
    let mut client = generated::AdapterOperationalConformanceClient::new(
        transport,
        metadata,
        AttemptBudget::new(3).ok_or("attempt budget")?,
    );

    let first = client
        .documents_in_title_window_with_options(
            generated::DocumentsInTitleWindowParams {
                site_id: id(35),
                after_title: "a".to_owned(),
                horizon_title: "😀".to_owned(),
                after: None,
            },
            QueryOptions::new(),
        )
        .await?;
    let generated::DocumentsInTitleWindowResult::Found(first_page) = first.value;
    let first_titles = first_page
        .documents
        .iter()
        .map(|document| document.title.as_str())
        .collect::<Vec<_>>();
    if first_titles != ["aa", "b"] {
        return Err("binary interval first page diverged".into());
    }
    let first_cursor = first.next_cursor.ok_or("binary interval cursor absent")?;

    let second = client
        .documents_in_title_window_with_options(
            generated::DocumentsInTitleWindowParams {
                site_id: id(35),
                after_title: "a".to_owned(),
                horizon_title: "😀".to_owned(),
                after: Some(first_cursor),
            },
            QueryOptions::new(),
        )
        .await?;
    let generated::DocumentsInTitleWindowResult::Found(second_page) = second.value;
    let second_titles = second_page
        .documents
        .iter()
        .map(|document| document.title.as_str())
        .collect::<Vec<_>>();
    if second_titles != ["é"] || second.next_cursor.is_some() {
        return Err("binary interval continuation diverged".into());
    }

    println!(
        "{}",
        serde_json::json!({
            "surface": "rust",
            "transport": "remote_grpc",
            "first_page": ["aa", "b"],
            "second_page": ["é"],
            "first_cursor": true,
            "second_cursor": false,
        })
    );
    Ok(())
}

fn required(name: &'static str) -> TestResult<String> {
    std::env::var(name).map_err(|_| format!("{name} is required").into())
}

fn id(suffix: u16) -> String {
    format!("018f0f8b-7c6d-7e31-8a4f-00000000{suffix:04x}")
}
