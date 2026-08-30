//! Postgres control for the concurrent checkout benchmark.
//!
//! One transaction decrements three products and writes the order and its
//! lines — the write RiffDB refuses to express in one command (RDB-C017).
//! Each concurrent client owns its own connection and a disjoint product
//! triple, matching the RiffDB harnesses exactly.
//!
//! Durability is fsync=on, synchronous_commit=on, on the same NVMe device the
//! RiffDB runs use. Statements are prepared once per connection.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_postgres::{Client, NoTls, Statement};

const LINES: usize = 3;
const TENANT: &str = "018f5f7e-7b0c-7d9a-8f14-77f42f53c851";
const CONNECTION: &str = "host=127.0.0.1 port=45432 user=postgres password=bench dbname=shop";

/// Seeded product identity, matching `gen_seeds.py`.
fn product_id(index: usize) -> String {
    format!("018f0f8b-7c6d-7e31-8a4f-{index:012x}")
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

struct Prepared {
    decrement: Statement,
    insert_order: Statement,
    insert_line: Statement,
}

async fn connect() -> Result<(Client, Prepared), Box<dyn std::error::Error + Send + Sync>> {
    let (client, connection) = tokio_postgres::connect(CONNECTION, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let prepared = Prepared {
        decrement: client
            .prepare(
                "UPDATE products SET stock_on_hand = stock_on_hand - 1, revision = revision + 1 \
                 WHERE tenant_id = $1::text::uuid AND product_id = $2::text::uuid \
                 AND stock_on_hand >= 1",
            )
            .await?,
        insert_order: client
            .prepare(
                "INSERT INTO orders (tenant_id, order_id, customer_id, status, placed_at, \
                 order_total, payment_reference, revision) \
                 VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, 'AwaitingPayment', \
                 now(), 59.97, NULL, 1)",
            )
            .await?,
        insert_line: client
            .prepare(
                "INSERT INTO order_lines (tenant_id, order_id, line_id, product_id, quantity, \
                 unit_price, line_total) \
                 VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, 1, \
                 19.99, 19.99)",
            )
            .await?,
    };
    Ok((client, prepared))
}

/// Inventory, order, and lines in ONE transaction.
async fn checkout(
    client: &Client,
    prepared: &Prepared,
    products: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let order_id = uuid_v4();
    let customer_id = uuid_v4();
    client.batch_execute("BEGIN").await?;
    for product in products {
        // The guard is the WHERE clause; a zero rowcount would be an oversell.
        let affected = client
            .execute(&prepared.decrement, &[&TENANT, product])
            .await?;
        if affected != 1 {
            client.batch_execute("ROLLBACK").await?;
            return Err("insufficient stock".into());
        }
    }
    client
        .execute(&prepared.insert_order, &[&TENANT, &order_id, &customer_id])
        .await?;
    for product in products {
        let line_id = uuid_v4();
        client
            .execute(
                &prepared.insert_line,
                &[&TENANT, &order_id, &line_id, product],
            )
            .await?;
    }
    client.batch_execute("COMMIT").await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rounds: usize = std::env::var("BENCH_ROUNDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(40);

    for clients in [1_usize, 8, 32] {
        let mut tasks = Vec::with_capacity(clients);
        for client_index in 0..clients {
            let products: Arc<Vec<String>> = Arc::new(
                (0..LINES)
                    .map(|line| product_id(client_index * LINES + line))
                    .collect(),
            );
            tasks.push(tokio::spawn(async move {
                let (client, prepared) = connect().await.expect("connect");
                checkout(&client, &prepared, &products).await.expect("warm");
                let mut samples = Vec::with_capacity(rounds);
                for _ in 0..rounds {
                    let started = Instant::now();
                    checkout(&client, &prepared, &products)
                        .await
                        .expect("checkout");
                    samples.push(started.elapsed());
                }
                samples
            }));
        }

        let started = Instant::now();
        let mut samples = Vec::new();
        for task in tasks {
            samples.extend(task.await.expect("client"));
        }
        let elapsed = started.elapsed();
        samples.sort_unstable();
        let throughput = samples.len() as f64 / elapsed.as_secs_f64();
        println!(
            "BENCH postgres_one_txn\tc={clients}\tops={}\tthroughput={throughput:.0}/s\tp50={:.0}us\tp95={:.0}us",
            samples.len(),
            percentile(&samples, 0.50),
            percentile(&samples, 0.95),
        );
    }
    Ok(())
}
