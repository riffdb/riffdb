#![forbid(unsafe_code)]

mod generated;

use std::env;
use std::ffi::OsString;
use std::path::Path;

use generated::{
    CreateItemInput, ItemPageParams, {{MODULE_CLIENT}},
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
    let mut application = {{MODULE_CLIENT}}::connect_with_credential_file(
        endpoint,
        Path::new(&credential_path),
        Some(&database),
    )
    .await?;

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
    let found = result.unwrap_found()?;
    println!("{}", found.item.title);
    Ok(())
}
