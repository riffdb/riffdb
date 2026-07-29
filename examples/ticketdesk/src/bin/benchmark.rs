#![forbid(unsafe_code)]

//! Warm named-query latency acceptance over one reused public HTTP/2 channel.

use std::env;
use std::path::Path;
use std::time::Instant;

use riffdb_client_rust::{CallMetadata, StableApplicationClient, load_protected_bearer_credential};
use riffdb_ticketdesk::{
    ListTicketsParams, ListTicketsResult, ProjectMembersParams, ProjectMembersResult,
    ProjectSummaryParams, ProjectSummaryResult, TicketDeskClient, TicketPageParams,
    TicketPageResult,
};
use tonic::transport::Endpoint;

const WARMUPS: usize = 2;
const SAMPLES: usize = 9;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = env::args().nth(1).ok_or("missing endpoint")?;
    let credential_path = env::args_os().nth(2).ok_or("missing credential")?;
    let credential = load_protected_bearer_credential(Path::new(&credential_path))?;
    let client = StableApplicationClient::connect(
        Endpoint::from_shared(endpoint)?
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30)),
    )
    .await?;
    let mut ticketdesk = TicketDeskClient::new(client, CallMetadata::authenticated(credential));

    let mut list = Vec::with_capacity(SAMPLES);
    let mut members = Vec::with_capacity(SAMPLES);
    let mut summary = Vec::with_capacity(SAMPLES);
    let mut detail = Vec::with_capacity(SAMPLES);
    for iteration in 0..(WARMUPS + SAMPLES) {
        let started = Instant::now();
        let ListTicketsResult::Found(result) = ticketdesk
            .list_tickets(ListTicketsParams {
                organization_id: uuid(1_000),
                project_id: uuid(30_000),
                statuses: vec!["Open".to_owned()],
                after: None,
                limit: 25,
            })
            .await?;
        let elapsed = started.elapsed().as_nanos();
        if result.tickets.is_empty() || result.tickets.iter().any(|ticket| ticket.status != "Open")
        {
            return Err("list query returned an invalid typed result".into());
        }
        if iteration >= WARMUPS {
            list.push(elapsed);
        }

        let started = Instant::now();
        let ProjectMembersResult::Found(result) = ticketdesk
            .project_members(ProjectMembersParams {
                organization_id: uuid(1_000),
                project_id: uuid(30_000),
                after: None,
            })
            .await?;
        let elapsed = started.elapsed().as_nanos();
        if result.members.len() != 3 {
            return Err("members query returned an invalid typed result".into());
        }
        if iteration >= WARMUPS {
            members.push(elapsed);
        }

        let started = Instant::now();
        let ProjectSummaryResult::Found(result) = ticketdesk
            .project_summary(ProjectSummaryParams {
                organization_id: uuid(1_000),
                project_id: uuid(30_000),
                status: "Open".to_owned(),
            })
            .await?
        else {
            return Err("summary query returned a non-Found outcome".into());
        };
        let elapsed = started.elapsed().as_nanos();
        if result.project.name != "Project 0-0" || result.recent_tickets.is_empty() {
            return Err("summary query returned an invalid typed result".into());
        }
        if iteration >= WARMUPS {
            summary.push(elapsed);
        }

        let started = Instant::now();
        let TicketPageResult::Found(result) = ticketdesk
            .ticket_page(TicketPageParams {
                organization_id: uuid(1_000),
                ticket_id: uuid(40_000),
                comments_after: None,
            })
            .await?
        else {
            return Err("detail query returned a non-Found outcome".into());
        };
        let elapsed = started.elapsed().as_nanos();
        if result.ticket.title != "Ticket 0-0-0"
            || result.ticket.status != "Closed"
            || result.project.name != "Project 0-0"
            || result.organization.name != "Organization 0"
            || result.comments.len() != 3
            || result.labels.len() != 2
        {
            return Err("detail query returned an invalid typed result".into());
        }
        if iteration >= WARMUPS {
            detail.push(elapsed);
        }
    }
    list.sort_unstable();
    members.sort_unstable();
    summary.sort_unstable();
    detail.sort_unstable();
    println!(
        "riffdb-ticketdesk-query-benchmark-v1\t{}\t{}\t{}\t{}",
        list[SAMPLES / 2],
        members[SAMPLES / 2],
        summary[SAMPLES / 2],
        detail[SAMPLES / 2]
    );
    Ok(())
}

fn uuid(value: usize) -> String {
    format!("01900000-0000-7000-8000-{value:012x}")
}
