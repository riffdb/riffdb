#![forbid(unsafe_code)]

mod generated;

use std::env;
use std::path::Path;

use generated::{
    CreateItemInput, ItemPageParams, ItemPageResult, {{MODULE_CLIENT}},
};
use riffdb_client_rust::{
    AttemptBudget, CallMetadata, StableApplicationClient, load_protected_bearer_credential,
};

const ITEM_ID: &str = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b10";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = env::args().nth(1).ok_or("missing RiffDB endpoint")?;
    let credential_path = env::args_os().nth(2).ok_or("missing application credential")?;
    let credential = load_protected_bearer_credential(Path::new(&credential_path))?;
    let client = StableApplicationClient::connect_uri(endpoint).await?;
    let mut application = {{MODULE_CLIENT}}::new(
        client,
        CallMetadata::authenticated(credential),
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
