#![forbid(unsafe_code)]

//! Bounded deterministic 276-row TicketDesk development seed.

use std::env;
use std::path::Path;
use std::time::Instant;

use riffdb_client_rust::{
    AttemptBudget, CallMetadata, StableApplicationClient, load_protected_bearer_credential,
};
use riffdb_ticketdesk::{
    AddProjectMemberInput, AttachLabelInput, CreateCommentInput, CreateLabelInput,
    CreateOrganizationInput, CreateProjectInput, CreateTicketInput, CreateUserInput,
    ListTicketsParams, ListTicketsResult, ProjectMembersParams, ProjectMembersResult,
    ProjectSummaryParams, ProjectSummaryResult, TicketDeskClient, TicketPageParams,
    TicketPageResult,
};
use tonic::transport::Endpoint;

const EXPECTED_ROWS: usize = 276;
/// Project 0 tickets with `ticket % 3 == 2` are Open → 3 of 10.
const EXPECTED_OPEN_TICKETS: usize = 3;
const EXPECTED_MEMBERS: usize = 3;
const EXPECTED_COMMENTS: usize = 3;

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
    let mut ticketdesk = TicketDeskClient::new(
        client,
        CallMetadata::authenticated(credential),
        AttemptBudget::new(3).expect("positive command attempt budget"),
    );
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
                // Open / InProgress / Closed so enum coverage is complete.
                let status = match ticket % 3 {
                    0 => "Closed",
                    1 => "InProgress",
                    _ => "Open",
                };
                ticketdesk
                    .create_ticket(CreateTicketInput {
                        title: format!("Ticket {organization}-{project}-{ticket}"),
                        status: status.to_owned(),
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

                for (label, label_id) in labels.iter().enumerate().take(2) {
                    let attachment_number = ticket_number * 10 + label;
                    ticketdesk
                        .attach_label(AttachLabelInput {
                            label_id: label_id.clone(),
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
    verify_named_pages(&mut ticketdesk).await?;
    println!(
        "riffdb-ticketdesk-seed-v1\t{rows}\t{}",
        seed_elapsed.as_nanos()
    );
    Ok(())
}

async fn verify_named_pages(
    ticketdesk: &mut TicketDeskClient,
) -> Result<(), Box<dyn std::error::Error>> {
    let organization_id = uuid(1_000);
    let project_id = uuid(30_000);
    let open_ticket_id = uuid(40_002); // ticket 2 → Open
    let closed_ticket_id = uuid(40_000); // ticket 0 → Closed

    eprintln!("ticketdesk-seed: execute ListTickets");
    let ListTicketsResult::Found(list) = ticketdesk
        .list_tickets(ListTicketsParams {
            organization_id: organization_id.clone(),
            project_id: project_id.clone(),
            statuses: vec!["Open".to_owned()],
            after: None,
            limit: 25,
        })
        .await?;
    if list.tickets.len() != EXPECTED_OPEN_TICKETS {
        return Err(format!(
            "ListTickets expected {EXPECTED_OPEN_TICKETS} open tickets, got {}",
            list.tickets.len()
        )
        .into());
    }
    if !list.tickets.iter().all(|ticket| ticket.status == "Open") {
        return Err("ListTickets returned a non-Open status".into());
    }
    if !list
        .tickets
        .iter()
        .any(|ticket| ticket.ticket_id == open_ticket_id && ticket.title == "Ticket 0-0-2")
    {
        return Err("ListTickets missing seeded open ticket 0-0-2".into());
    }

    eprintln!("ticketdesk-seed: execute ProjectMembers");
    let ProjectMembersResult::Found(members) = ticketdesk
        .project_members(ProjectMembersParams {
            organization_id: organization_id.clone(),
            project_id: project_id.clone(),
            after: None,
        })
        .await?;
    if members.members.len() != EXPECTED_MEMBERS {
        return Err(format!(
            "ProjectMembers expected {EXPECTED_MEMBERS} members, got {}",
            members.members.len()
        )
        .into());
    }
    let lead = members
        .members
        .iter()
        .find(|member| member.role == "lead")
        .ok_or("ProjectMembers missing lead")?;
    if lead.user_id != uuid(10_000) {
        return Err("ProjectMembers lead user mismatch".into());
    }

    eprintln!("ticketdesk-seed: execute ProjectSummary");
    let ProjectSummaryResult::Found(summary) = ticketdesk
        .project_summary(ProjectSummaryParams {
            organization_id: organization_id.clone(),
            project_id: project_id.clone(),
            status: "Open".to_owned(),
        })
        .await?
    else {
        return Err("ProjectSummary returned a non-Found outcome".into());
    };
    if summary.project.project_id != project_id || summary.project.name != "Project 0-0" {
        return Err("ProjectSummary project identity mismatch".into());
    }
    if summary.recent_tickets.len() != EXPECTED_OPEN_TICKETS {
        return Err(format!(
            "ProjectSummary expected {EXPECTED_OPEN_TICKETS} recent open tickets, got {}",
            summary.recent_tickets.len()
        )
        .into());
    }
    if !summary
        .recent_tickets
        .iter()
        .all(|ticket| ticket.status == "Open")
    {
        return Err("ProjectSummary returned a non-Open ticket".into());
    }

    eprintln!("ticketdesk-seed: execute TicketPage");
    let TicketPageResult::Found(page) = ticketdesk
        .ticket_page(TicketPageParams {
            organization_id: organization_id.clone(),
            ticket_id: closed_ticket_id.clone(),
            comments_after: None,
        })
        .await?
    else {
        return Err("TicketPage returned a non-Found outcome".into());
    };
    if page.ticket.ticket_id != closed_ticket_id
        || page.ticket.project_id != project_id
        || page.ticket.title != "Ticket 0-0-0"
        || page.ticket.status != "Closed"
    {
        return Err("TicketPage ticket identity mismatch".into());
    }
    if page.comments.len() != EXPECTED_COMMENTS {
        return Err(format!(
            "TicketPage expected {EXPECTED_COMMENTS} comments, got {}",
            page.comments.len()
        )
        .into());
    }
    if page.comments[0].body != "Comment 0" {
        return Err("TicketPage first comment body mismatch".into());
    }
    // ticket 0 assignee = users[0], reporter = users[1]
    let assignee = page
        .assignee
        .as_ref()
        .ok_or("TicketPage missing assignee")?;
    if assignee.user_id != uuid(10_000) || assignee.display_name != "User 0-0" {
        return Err("TicketPage assignee mismatch".into());
    }
    if page.reporter.user_id != uuid(10_001) || page.reporter.display_name != "User 0-1" {
        return Err("TicketPage reporter mismatch".into());
    }
    if page.project.project_id != uuid(30_000) || page.project.name != "Project 0-0" {
        return Err("TicketPage project mismatch".into());
    }
    if page.organization.organization_id != uuid(1_000)
        || page.organization.name != "Organization 0"
    {
        return Err("TicketPage organization mismatch".into());
    }
    if page.labels.len() != 2
        || page.labels[0].label_id != uuid(20_000)
        || page.labels[0].name != "Label 0"
        || page.labels[1].label_id != uuid(20_001)
        || page.labels[1].name != "Label 1"
    {
        return Err("TicketPage labels mismatch".into());
    }

    // Negative path: summary for a missing project is typed NotFound.
    let ProjectSummaryResult::NotFound(_) = ticketdesk
        .project_summary(ProjectSummaryParams {
            organization_id,
            project_id: uuid(39_999),
            status: "Open".to_owned(),
        })
        .await?
    else {
        return Err("ProjectSummary missing project should be NotFound".into());
    };

    Ok(())
}

fn uuid(value: usize) -> String {
    format!("01900000-0000-7000-8000-{value:012x}")
}

fn key(kind: &str, value: usize) -> String {
    format!("ticketdesk-seed-{kind}-{value}")
}
