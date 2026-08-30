//! Concurrent checkout benchmark: one aggregate, coarse conflict key.
//!
//! Every entity lives under one `Tenant` root, so `AtomicCheckout` writes
//! inventory and the order in a single transaction. The price is the
//! aggregate's `conflict_key (tenant_id)`: every checkout in the tenant
//! contends with every other, whatever products it touches.
//!
//! Each concurrent client is given its own disjoint product triple. Under the
//! fine-grained saga contract those clients share no conflict key at all, so
//! any throughput difference between the two runs is the conflict key and not
//! the workload.

#[path = "generated/storefront.rs"]
mod storefront;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use riffdb_client_rust::{
    AttemptBudget, CallMetadata, DatabaseAlias, StableApplicationClient,
    load_protected_bearer_credential,
};

use storefront::{
    AtomicCheckoutInput, AtomicCheckoutOutcome, CheckoutLine, DecimalValue, MoneyValue,
    StorefrontAtomicClient,
};

const TENANT: &str = "018f5f7e-7b0c-7d9a-8f14-77f42f53c851";
const LINES: usize = 3;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}

fn number(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

/// Seeded product identity, matching `gen_seeds.py`.
fn product_id(index: usize) -> String {
    format!("018f0f8b-7c6d-7e31-8a4f-{index:012x}")
}

fn usd(cents: i64) -> MoneyValue {
    let mut bytes = cents.to_be_bytes().to_vec();
    while bytes.len() > 1 {
        let redundant = (bytes[0] == 0x00 && bytes[1] & 0x80 == 0)
            || (bytes[0] == 0xff && bytes[1] & 0x80 != 0);
        if !redundant {
            break;
        }
        bytes.remove(0);
    }
    MoneyValue {
        currency: "USD".to_owned(),
        amount: DecimalValue {
            coefficient_twos_complement: bytes,
            scale: 2,
            precision: Some(38),
        },
    }
}

fn uuid_v4() -> String {
    let mut b = ulid::Ulid::new().to_bytes();
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
    )
}

fn percentile(sorted: &[Duration], fraction: f64) -> f64 {
    let index = ((sorted.len() as f64 - 1.0) * fraction).round() as usize;
    sorted[index].as_secs_f64() * 1e6
}

async fn connect(
    endpoint: &str,
    credential_path: &str,
    database: &str,
) -> Result<StorefrontAtomicClient, Box<dyn std::error::Error>> {
    let credential = load_protected_bearer_credential(Path::new(credential_path))?;
    let client = StableApplicationClient::connect_uri(endpoint.to_owned()).await?;
    Ok(StorefrontAtomicClient::new(
        client,
        CallMetadata::authenticated(credential)
            .with_database(DatabaseAlias::new(database.to_owned())?),
        AttemptBudget::new(3).ok_or("attempt budget")?,
    ))
}

async fn checkout(
    application: &mut StorefrontAtomicClient,
    products: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let order_id = uuid_v4();
    let lines = products
        .iter()
        .map(|product_id| CheckoutLine {
            line_id: uuid_v4(),
            order_id: order_id.clone(),
            tenant_id: TENANT.to_owned(),
            product_id: product_id.clone(),
            quantity: 1,
            line_total: usd(1_999),
        })
        .collect();
    let result = application
        .atomic_checkout(AtomicCheckoutInput {
            request_id: uuid_v4(),
            tenant_id: TENANT.to_owned(),
            order_id,
            customer_id: uuid_v4(),
            order_total: usd(5_997),
            lines,
        })
        .await?;
    match result.outcome {
        AtomicCheckoutOutcome::CheckoutCompleted => Ok(()),
        other => Err(format!("checkout failed: {other:?}").into()),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = Arc::new(env("RIFFDB_ENDPOINT"));
    let credential_path = Arc::new(env("RIFFDB_CREDENTIAL_FILE"));
    let database =
        Arc::new(std::env::var("RIFFDB_DATABASE").unwrap_or_else(|_| "default".to_owned()));
    let rounds = number("BENCH_ROUNDS", 40);

    for clients in [1_usize, 8, 32] {
        let mut tasks = Vec::with_capacity(clients);
        for client in 0..clients {
            let endpoint = Arc::clone(&endpoint);
            let credential_path = Arc::clone(&credential_path);
            let database = Arc::clone(&database);
            // Disjoint products per client: under a fine conflict key these
            // clients would never contend.
            let products: Vec<String> = (0..LINES)
                .map(|line| product_id(client * LINES + line))
                .collect();
            tasks.push(tokio::spawn(async move {
                let mut application = connect(&endpoint, &credential_path, &database)
                    .await
                    .expect("connect");
                checkout(&mut application, &products).await.expect("warm");
                let mut samples = Vec::with_capacity(rounds);
                for _ in 0..rounds {
                    let started = Instant::now();
                    checkout(&mut application, &products).await.expect("checkout");
                    samples.push(started.elapsed());
                }
                samples
            }));
        }

        // Every client is connected and warm before the clock starts.
        let started = Instant::now();
        let mut samples = Vec::new();
        for task in tasks {
            samples.extend(task.await.expect("client"));
        }
        let elapsed = started.elapsed();
        samples.sort_unstable();
        let throughput = samples.len() as f64 / elapsed.as_secs_f64();
        let line = format!(
            "atomic_coarse\tc={clients}\tops={}\tthroughput={throughput:.0}/s\tp50={:.0}us\tp95={:.0}us",
            samples.len(),
            percentile(&samples, 0.50),
            percentile(&samples, 0.95),
        );
        println!("{line}");
        eprintln!("BENCH {line}");
    }
    Ok(())
}
