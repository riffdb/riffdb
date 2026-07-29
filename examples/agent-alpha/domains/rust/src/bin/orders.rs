#![forbid(unsafe_code)]

use std::env;
use std::path::Path;

use agent_alpha_domains_rust::orders::{AgentOrdersClient, OrderPageParams, OrderPageResult};
use riffdb_client_rust::{
    AttemptBudget, CallMetadata, StableApplicationClient, load_protected_bearer_credential,
};

const STORE_ID: &str = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b30";
const ORDER_ID: &str = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b33";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = env::args().nth(1).ok_or("missing RiffDB endpoint")?;
    let credential_path = env::args_os()
        .nth(2)
        .ok_or("missing application credential")?;
    let credential = load_protected_bearer_credential(Path::new(&credential_path))?;
    let client = StableApplicationClient::connect_uri(endpoint).await?;
    let mut application = AgentOrdersClient::new(
        client,
        CallMetadata::authenticated(credential),
        AttemptBudget::new(3).ok_or("invalid attempt budget")?,
    );
    let OrderPageResult::Found(page) = application
        .order_page(OrderPageParams {
            store_id: STORE_ID.to_owned(),
            order_id: ORDER_ID.to_owned(),
        })
        .await?
    else {
        return Err("order page did not return Found".into());
    };
    println!("{}", page.customer.display_name);
    Ok(())
}
