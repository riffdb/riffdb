#![forbid(unsafe_code)]

//! Bounded deterministic 276-row TicketDesk development seed.

use std::env;
use std::path::Path;
use std::time::Instant;

use riffdb_client_rust::{CallMetadata, RiffDbClient, load_protected_bearer_credential};
use riffdb_ticketdesk::{
    AddProjectMemberInput, AttachLabelInput, CreateCommentInput, CreateLabelInput,
    CreateOrganizationInput, CreateProjectInput, CreateTicketInput, CreateUserInput,
    ListTicketsParams, TicketDeskClient, TicketPageParams,
};
use tonic::transport::Endpoint;

const EXPECTED_ROWS: usize = 276;

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
    let started = Instant::now();
    let mut rows = 0_usize;

    for organization in 0..2 {
        let organization_id = uuid(1_000 + organization);
        ticketdesk
            .create_organization(CreateOrganizationInput {
                name: format!("Organization {organization}"),
                organization_id: organization_id.clone(),
                idempotency_key: key("organization", organization),
            })
            .await?;
        rows += 1;

        let users = (0..5)
            .map(|user| uuid(10_000 + organization * 100 + user))
            .collect::<Vec<_>>();
        for (user, user_id) in users.iter().enumerate() {
            ticketdesk
                .create_user(CreateUserInput {
                    email: format!("user-{organization}-{user}@example.test"),
                    display_name: format!("User {organization}-{user}"),
                    user_id: user_id.clone(),
                    idempotency_key: key("user", organization * 100 + user),
                    organization_id: organization_id.clone(),
                })
                .await?;
            rows += 1;
        }

        let labels = (0..4)
            .map(|label| uuid(20_000 + organization * 100 + label))
            .collect::<Vec<_>>();
        for (label, label_id) in labels.iter().enumerate() {
            ticketdesk
                .create_label(CreateLabelInput {
                    name: format!("Label {label}"),
                    label_id: label_id.clone(),
                    idempotency_key: key("label", organization * 100 + label),
                    organization_id: organization_id.clone(),
                })
                .await?;
            rows += 1;
        }

        for project in 0..2 {
            let project_number = organization * 10 + project;
            let project_id = uuid(30_000 + project_number);
            ticketdesk
                .create_project(CreateProjectInput {
                    name: format!("Project {organization}-{project}"),
                    project_id: project_id.clone(),
                    idempotency_key: key("project", project_number),
                    organization_id: organization_id.clone(),
                })
                .await?;
            rows += 1;

            for (member, user_id) in users.iter().take(3).enumerate() {
                ticketdesk
                    .add_project_member(AddProjectMemberInput {
                        role: if member == 0 { "lead" } else { "member" }.to_owned(),
                        user_id: user_id.clone(),
                        project_id: project_id.clone(),
                        idempotency_key: key("member", project_number * 10 + member),
                        organization_id: organization_id.clone(),
                    })
                    .await?;
                rows += 1;
            }

            for ticket in 0..10 {
                let ticket_number = project_number * 100 + ticket;
                let ticket_id = uuid(40_000 + ticket_number);
                ticketdesk
                    .create_ticket(CreateTicketInput {
                        title: format!("Ticket {organization}-{project}-{ticket}"),
                        status: if ticket % 3 == 0 { "Closed" } else { "Open" }.to_owned(),
                        project_id: project_id.clone(),
                        ticket_id: ticket_id.clone(),
                        assignee_id: users[ticket % users.len()].clone(),
                        reporter_id: users[(ticket + 1) % users.len()].clone(),
                        idempotency_key: key("ticket", ticket_number),
                        organization_id: organization_id.clone(),
                    })
                    .await?;
                rows += 1;

                for comment in 0..3 {
                    let comment_number = ticket_number * 10 + comment;
                    ticketdesk
                        .create_comment(CreateCommentInput {
                            body: format!("Comment {comment}"),
                            author_id: users[comment % users.len()].clone(),
                            ticket_id: ticket_id.clone(),
                            comment_id: uuid(50_000 + comment_number),
                            idempotency_key: key("comment", comment_number),
                            organization_id: organization_id.clone(),
                        })
                        .await?;
                    rows += 1;
                }

                for label in 0..2 {
                    let attachment_number = ticket_number * 10 + label;
                    ticketdesk
                        .attach_label(AttachLabelInput {
                            label_id: labels[label].clone(),
                            ticket_id: ticket_id.clone(),
                            idempotency_key: key("attachment", attachment_number),
                            organization_id: organization_id.clone(),
                        })
                        .await?;
                    rows += 1;
                }
            }
        }
    }

    if rows != EXPECTED_ROWS {
        return Err("seed row count mismatch".into());
    }
    let seed_elapsed = started.elapsed();
    eprintln!("ticketdesk-seed: execute ListTickets");
    let list = ticketdesk
        .list_tickets(ListTicketsParams {
            organization_id: uuid(1_000),
            project_id: uuid(30_000),
            statuses: vec!["Open".to_owned()],
            after: None,
            limit: 25,
        })
        .await?;
    if list.outcome != "Found"
        || !list
            .fields
            .get("tickets")
            .is_some_and(|field| !field.records.is_empty())
    {
        return Err("named list query returned an invalid result".into());
    }
    eprintln!("ticketdesk-seed: execute TicketPage");
    let page = ticketdesk
        .ticket_page(TicketPageParams {
            organization_id: uuid(1_000),
            ticket_id: uuid(40_000),
            comments_after: None,
        })
        .await?;
    if page.outcome != "Found"
        || !page
            .fields
            .get("ticket")
            .is_some_and(|field| field.records.len() == 1)
    {
        eprintln!(
            "ticketdesk-seed: TicketPage outcome={} fields={:?}",
            page.outcome,
            page.fields
                .iter()
                .map(|(name, field)| (name.as_str(), field.records.len()))
                .collect::<Vec<_>>()
        );
        return Err("named detail query returned an invalid result".into());
    }
    println!(
        "riffdb-ticketdesk-seed-v1\t{rows}\t{}",
        seed_elapsed.as_nanos()
    );
    Ok(())
}

fn uuid(value: usize) -> String {
    format!("01900000-0000-7000-8000-{value:012x}")
}

fn key(kind: &str, value: usize) -> String {
    format!("ticketdesk-seed-{kind}-{value}")
}
