#![forbid(unsafe_code)]

mod generated;

use std::env;
use std::path::Path;

use generated::{
    AgentOrdersClient, CreateStoreInput, CustomerHistoryParams, CustomerHistoryResult,
    InventoryDashboardParams, InventoryDashboardResult, OpenOrdersParams, OpenOrdersResult,
    OrderPageParams, OrderPageResult,
};
use riffdb_client_rust::{
    AttemptBudget, CallMetadata, QueryOptions, StableApplicationClient,
    load_protected_bearer_credential,
};

const STORE_ID: &str = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b30";
const CUSTOMER_ID: &str = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b31";
const ORDER_ID: &str = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b33";

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

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

    let command = application
        .create_store(CreateStoreInput {
            name: "Acme Supply".to_owned(),
            store_id: STORE_ID.to_owned(),
            idempotency_key: "orders-store-acme".to_owned(),
        })
        .await?;
    let commit_sequence = command
        .commit_sequence
        .ok_or("seed replay did not retain its commit sequence")?;
    if !command.replayed {
        return Err("seed replay unexpectedly created another store".into());
    }
    let options = || QueryOptions::new().read_after_commit(commit_sequence);

    let history = application
        .customer_history_with_options(
            CustomerHistoryParams {
                store_id: STORE_ID.to_owned(),
                customer_id: CUSTOMER_ID.to_owned(),
                after: None,
                limit: 25,
            },
            options(),
        )
        .await?;
    if !matches!(history.value, CustomerHistoryResult::Found(_)) {
        return Err("customer history did not return Found".into());
    }
    let dashboard = application
        .inventory_dashboard_with_options(
            InventoryDashboardParams {
                store_id: STORE_ID.to_owned(),
                after: None,
                limit: 50,
            },
            options(),
        )
        .await?;
    if !matches!(dashboard.value, InventoryDashboardResult::Found(_)) {
        return Err("inventory dashboard did not return Found".into());
    }
    let open_orders = application
        .open_orders_with_options(
            OpenOrdersParams {
                store_id: STORE_ID.to_owned(),
                status: "Reserved".to_owned(),
                after: None,
                limit: 25,
            },
            options(),
        )
        .await?;
    if !matches!(open_orders.value, OpenOrdersResult::Found(_)) {
        return Err("open orders did not return Found".into());
    }
    let page = application
        .order_page_with_options(
            OrderPageParams {
                store_id: STORE_ID.to_owned(),
                order_id: ORDER_ID.to_owned(),
            },
            options(),
        )
        .await?;
    if !matches!(page.value, OrderPageResult::Found(_)) {
        return Err("order page did not return Found".into());
    }

    let application_head = [
        history.application_head,
        dashboard.application_head,
        open_orders.application_head,
        page.application_head,
    ]
    .into_iter()
    .max()
    .ok_or("query evidence is empty")?;
    if application_head < commit_sequence {
        return Err("read-after-commit evidence regressed".into());
    }
    println!(
        "riffdb-rehearsal-v2\torders\t{commit_sequence}\t{application_head}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        hex(&command.plan_hash),
        page.identity.contract_lineage,
        page.identity.contract_version,
        hex(&page.identity.contract_bundle_hash),
        hex(&page.identity.module_hash),
        page.identity.query_name,
        hex(&page.identity.plan_hash),
    );
    Ok(())
}
