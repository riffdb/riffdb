#![forbid(unsafe_code)]

//! Warm named-query latency acceptance over one reused public HTTP/2 channel.

use std::env;
use std::path::Path;
use std::time::Instant;

use riffdb_client_rust::{CallMetadata, RiffDbClient, load_protected_bearer_credential};
use riffdb_ticketdesk::{ListTicketsParams, TicketDeskClient, TicketPageParams};
use tonic::transport::Endpoint;

const WARMUPS: usize = 2;
const SAMPLES: usize = 9;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = env::args().nth(1).ok_or("missing endpoint")?;
    let credential_path = env::args_os().nth(2).ok_or("missing credential")?;
    let credential = load_protected_bearer_credential(Path::new(&credential_path))?;
    let client = RiffDbClient::connect(
        Endpoint::from_shared(endpoint)?
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30)),
    )
    .await?;
    let mut ticketdesk = TicketDeskClient::new(client, CallMetadata::authenticated(credential));

    let mut list = Vec::with_capacity(SAMPLES);
    let mut detail = Vec::with_capacity(SAMPLES);
    for iteration in 0..(WARMUPS + SAMPLES) {
        let started = Instant::now();
        let result = ticketdesk
            .list_tickets(ListTicketsParams {
                organization_id: uuid(1_000),
                project_id: uuid(30_000),
                statuses: vec!["Open".to_owned()],
                after: None,
                limit: 25,
            })
            .await?;
        let elapsed = started.elapsed().as_nanos();
        if result.outcome != "Found"
            || !result
                .fields
                .get("tickets")
                .is_some_and(|field| !field.records.is_empty())
        {
            return Err("list query returned an invalid result".into());
        }
        if iteration >= WARMUPS {
            list.push(elapsed);
        }

        let started = Instant::now();
        let result = ticketdesk
            .ticket_page(TicketPageParams {
                organization_id: uuid(1_000),
                ticket_id: uuid(40_000),
                comments_after: None,
            })
            .await?;
        let elapsed = started.elapsed().as_nanos();
        if result.outcome != "Found"
            || !result
                .fields
                .get("ticket")
                .is_some_and(|field| field.records.len() == 1)
        {
            return Err("detail query returned an invalid result".into());
        }
        if iteration >= WARMUPS {
            detail.push(elapsed);
        }
    }
    list.sort_unstable();
    detail.sort_unstable();
    println!(
        "riffdb-ticketdesk-query-benchmark-v1\t{}\t{}",
        list[SAMPLES / 2],
        detail[SAMPLES / 2]
    );
    Ok(())
}

fn uuid(value: usize) -> String {
    format!("01900000-0000-7000-8000-{value:012x}")
}
