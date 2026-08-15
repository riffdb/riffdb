#![forbid(unsafe_code)]

mod generated;

use std::env;
use std::ffi::OsString;
use std::path::Path;

use generated::{
    CreateItemInput, ItemPageParams, ItemPageResult, {{MODULE_CLIENT}},
};
use riffdb_client_rust::{
    AttemptBudget, CallMetadata, DatabaseAlias, StableApplicationClient,
    load_protected_bearer_credential,
};

const ITEM_ID: &str = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b10";

fn runner_value(name: &'static str, position: usize) -> Result<OsString, Box<dyn std::error::Error>> {
    let environment = env::var_os(name);
    let positional = env::args_os().nth(position);
    match (environment, positional) {
        (Some(left), Some(right)) if left != right => {
            Err(format!("{name} disagrees with the development runner argument").into())
        }
        (Some(value), _) | (_, Some(value)) => Ok(value),
        (None, None) => Err(format!("missing {name}").into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = runner_value("RIFFDB_ENDPOINT", 1)?
        .into_string()
        .map_err(|_| "RIFFDB_ENDPOINT is not UTF-8")?;
    let credential_path = runner_value("RIFFDB_CREDENTIAL_FILE", 2)?;
    let database = runner_value("RIFFDB_DATABASE", 3)?
        .into_string()
        .map_err(|_| "RIFFDB_DATABASE is not UTF-8")?;
    let credential = load_protected_bearer_credential(Path::new(&credential_path))?;
    let client = StableApplicationClient::connect_uri(endpoint).await?;
    let mut application = {{MODULE_CLIENT}}::new(
        client,
        CallMetadata::authenticated(credential).with_database(DatabaseAlias::new(database)?),
        AttemptBudget::new(3).ok_or("invalid attempt budget")?,
    );

    application
        .create_item(CreateItemInput {
            idempotency_key: "{{APPLICATION_NAME}}-generated-client-item".to_owned(),
            item_id: ITEM_ID.to_owned(),
            title: "Generated application client".to_owned(),
        })
        .await?;
    let result = application
        .item_page(ItemPageParams {
            item_id: ITEM_ID.to_owned(),
        })
        .await?;
    let ItemPageResult::Found(found) = result else {
        return Err("generated page query did not find the generated item".into());
    };
    println!("{}", found.item.title);
    Ok(())
}
