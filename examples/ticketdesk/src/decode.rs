//! Typed decoding of name-addressed query results into generated shapes.

use riffdb_client_rust::{
    ApplicationCardinality, ApplicationClientError, ApplicationRecord, ApplicationResultField,
    ApplicationValue, NamedQueryResult,
};

use crate::generated::{
    ListTicketsFound, ListTicketsFoundTickets, ListTicketsResult, ProjectMembersFound,
    ProjectMembersFoundMembers, ProjectMembersResult, ProjectSummaryFound,
    ProjectSummaryFoundProject, ProjectSummaryFoundRecentTickets, ProjectSummaryNotFound,
    ProjectSummaryResult, TicketPageFound, TicketPageFoundAssignee, TicketPageFoundComments,
    TicketPageFoundLabels, TicketPageFoundOrganization, TicketPageFoundProject,
    TicketPageFoundReporter, TicketPageFoundTicket, TicketPageIntegrityFailure, TicketPageNotFound,
    TicketPageResult,
};

/// Decodes a `ListTickets` named-query response.
pub(crate) fn list_tickets_result(
    result: NamedQueryResult,
) -> Result<ListTicketsResult, ApplicationClientError> {
    match result.outcome.as_str() {
        "Found" => Ok(ListTicketsResult::Found(Box::new(ListTicketsFound {
            tickets: many_records(&result, "tickets")?
                .into_iter()
                .map(list_ticket)
                .collect::<Result<Vec<_>, _>>()?,
        }))),
        _ => Err(ApplicationClientError::InvalidResponse),
    }
}

/// Decodes a `ProjectMembers` named-query response.
pub(crate) fn project_members_result(
    result: NamedQueryResult,
) -> Result<ProjectMembersResult, ApplicationClientError> {
    match result.outcome.as_str() {
        "Found" => Ok(ProjectMembersResult::Found(Box::new(ProjectMembersFound {
            members: many_records(&result, "members")?
                .into_iter()
                .map(project_member)
                .collect::<Result<Vec<_>, _>>()?,
        }))),
        _ => Err(ApplicationClientError::InvalidResponse),
    }
}

/// Decodes a `ProjectSummary` named-query response.
pub(crate) fn project_summary_result(
    result: NamedQueryResult,
) -> Result<ProjectSummaryResult, ApplicationClientError> {
    match result.outcome.as_str() {
        "Found" => Ok(ProjectSummaryResult::Found(Box::new(ProjectSummaryFound {
            project: one_record(&result, "project").and_then(project_summary_project)?,
            recent_tickets: many_records(&result, "recent_tickets")?
                .into_iter()
                .map(project_summary_ticket)
                .collect::<Result<Vec<_>, _>>()?,
        }))),
        "NotFound" => Ok(ProjectSummaryResult::NotFound(Box::new(
            ProjectSummaryNotFound {},
        ))),
        _ => Err(ApplicationClientError::InvalidResponse),
    }
}

/// Decodes a `TicketPage` named-query response.
pub(crate) fn ticket_page_result(
    result: NamedQueryResult,
) -> Result<TicketPageResult, ApplicationClientError> {
    match result.outcome.as_str() {
        "Found" => Ok(TicketPageResult::Found(Box::new(TicketPageFound {
            ticket: one_record(&result, "ticket").and_then(ticket_page_ticket)?,
            project: one_record(&result, "project").and_then(ticket_page_project)?,
            organization: one_record(&result, "organization").and_then(ticket_page_organization)?,
            reporter: one_record(&result, "reporter").and_then(ticket_page_reporter)?,
            assignee: maybe_record(&result, "assignee")?
                .map(ticket_page_assignee)
                .transpose()?,
            comments: many_records(&result, "comments")?
                .into_iter()
                .map(ticket_page_comment)
                .collect::<Result<Vec<_>, _>>()?,
            labels: many_records(&result, "labels")?
                .into_iter()
                .map(ticket_page_label)
                .collect::<Result<Vec<_>, _>>()?,
        }))),
        "NotFound" => Ok(TicketPageResult::NotFound(Box::new(TicketPageNotFound {}))),
        "IntegrityFailure" => Ok(TicketPageResult::IntegrityFailure(Box::new(
            TicketPageIntegrityFailure {},
        ))),
        _ => Err(ApplicationClientError::InvalidResponse),
    }
}

fn list_ticket(
    record: ApplicationRecord,
) -> Result<ListTicketsFoundTickets, ApplicationClientError> {
    Ok(ListTicketsFoundTickets {
        ticket_id: require_uuid(&record, "ticket_id")?,
        title: require_string(&record, "title")?,
        status: require_enum(&record, "status")?,
        updated_at: require_timestamp_text(&record, "updated_at")?,
        reporter_id: require_uuid(&record, "reporter_id")?,
        assignee_id: require_uuid(&record, "assignee_id")?,
    })
}

fn project_member(
    record: ApplicationRecord,
) -> Result<ProjectMembersFoundMembers, ApplicationClientError> {
    Ok(ProjectMembersFoundMembers {
        user_id: require_uuid(&record, "user_id")?,
        role: require_string(&record, "role")?,
    })
}

fn project_summary_project(
    record: ApplicationRecord,
) -> Result<ProjectSummaryFoundProject, ApplicationClientError> {
    Ok(ProjectSummaryFoundProject {
        project_id: require_uuid(&record, "project_id")?,
        name: require_string(&record, "name")?,
    })
}

fn project_summary_ticket(
    record: ApplicationRecord,
) -> Result<ProjectSummaryFoundRecentTickets, ApplicationClientError> {
    Ok(ProjectSummaryFoundRecentTickets {
        ticket_id: require_uuid(&record, "ticket_id")?,
        title: require_string(&record, "title")?,
        status: require_enum(&record, "status")?,
        updated_at: require_timestamp_text(&record, "updated_at")?,
    })
}

fn ticket_page_ticket(
    record: ApplicationRecord,
) -> Result<TicketPageFoundTicket, ApplicationClientError> {
    Ok(TicketPageFoundTicket {
        ticket_id: require_uuid(&record, "ticket_id")?,
        project_id: require_uuid(&record, "project_id")?,
        title: require_string(&record, "title")?,
        status: require_enum(&record, "status")?,
        created_at: require_timestamp_text(&record, "created_at")?,
        updated_at: require_timestamp_text(&record, "updated_at")?,
    })
}

fn ticket_page_project(
    record: ApplicationRecord,
) -> Result<TicketPageFoundProject, ApplicationClientError> {
    Ok(TicketPageFoundProject {
        project_id: require_uuid(&record, "project_id")?,
        name: require_string(&record, "name")?,
    })
}

fn ticket_page_organization(
    record: ApplicationRecord,
) -> Result<TicketPageFoundOrganization, ApplicationClientError> {
    Ok(TicketPageFoundOrganization {
        organization_id: require_uuid(&record, "organization_id")?,
        name: require_string(&record, "name")?,
    })
}

fn ticket_page_reporter(
    record: ApplicationRecord,
) -> Result<TicketPageFoundReporter, ApplicationClientError> {
    Ok(TicketPageFoundReporter {
        user_id: require_uuid(&record, "user_id")?,
        display_name: require_string(&record, "display_name")?,
    })
}

fn ticket_page_assignee(
    record: ApplicationRecord,
) -> Result<TicketPageFoundAssignee, ApplicationClientError> {
    Ok(TicketPageFoundAssignee {
        user_id: require_uuid(&record, "user_id")?,
        display_name: require_string(&record, "display_name")?,
    })
}

fn ticket_page_comment(
    record: ApplicationRecord,
) -> Result<TicketPageFoundComments, ApplicationClientError> {
    Ok(TicketPageFoundComments {
        comment_id: require_uuid(&record, "comment_id")?,
        body: require_string(&record, "body")?,
        author_id: require_uuid(&record, "author_id")?,
        created_at: require_timestamp_text(&record, "created_at")?,
    })
}

fn ticket_page_label(
    record: ApplicationRecord,
) -> Result<TicketPageFoundLabels, ApplicationClientError> {
    Ok(TicketPageFoundLabels {
        label_id: require_uuid(&record, "label_id")?,
        name: require_string(&record, "name")?,
    })
}

fn one_record(
    result: &NamedQueryResult,
    name: &str,
) -> Result<ApplicationRecord, ApplicationClientError> {
    let field = require_field(result, name, ApplicationCardinality::One)?;
    if field.records.len() != 1 {
        return Err(ApplicationClientError::InvalidResponse);
    }
    field
        .records
        .into_iter()
        .next()
        .ok_or(ApplicationClientError::InvalidResponse)
}

fn maybe_record(
    result: &NamedQueryResult,
    name: &str,
) -> Result<Option<ApplicationRecord>, ApplicationClientError> {
    let field = require_field(result, name, ApplicationCardinality::Maybe)?;
    match field.records.len() {
        0 => Ok(None),
        1 => Ok(field.records.into_iter().next()),
        _ => Err(ApplicationClientError::InvalidResponse),
    }
}

fn many_records(
    result: &NamedQueryResult,
    name: &str,
) -> Result<Vec<ApplicationRecord>, ApplicationClientError> {
    Ok(require_field(result, name, ApplicationCardinality::Many)?.records)
}

fn require_field(
    result: &NamedQueryResult,
    name: &str,
    cardinality: ApplicationCardinality,
) -> Result<ApplicationResultField, ApplicationClientError> {
    let field = result
        .fields
        .get(name)
        .cloned()
        .ok_or(ApplicationClientError::InvalidResponse)?;
    if field.cardinality != cardinality {
        return Err(ApplicationClientError::InvalidResponse);
    }
    Ok(field)
}

fn require_string(
    record: &ApplicationRecord,
    name: &str,
) -> Result<String, ApplicationClientError> {
    match record.fields.get(name) {
        Some(ApplicationValue::String(value)) => Ok(value.clone()),
        _ => Err(ApplicationClientError::InvalidResponse),
    }
}

fn require_uuid(record: &ApplicationRecord, name: &str) -> Result<String, ApplicationClientError> {
    match record.fields.get(name) {
        Some(ApplicationValue::Uuid(value)) => Ok(value.clone()),
        // Some lowerings may surface UUID fields as formatted strings.
        Some(ApplicationValue::String(value)) if value.len() == 36 => Ok(value.clone()),
        _ => Err(ApplicationClientError::InvalidResponse),
    }
}

fn require_enum(record: &ApplicationRecord, name: &str) -> Result<String, ApplicationClientError> {
    match record.fields.get(name) {
        Some(ApplicationValue::Enum(value)) => Ok(value.clone()),
        Some(ApplicationValue::String(value)) => Ok(value.clone()),
        _ => Err(ApplicationClientError::InvalidResponse),
    }
}

fn require_timestamp_text(
    record: &ApplicationRecord,
    name: &str,
) -> Result<String, ApplicationClientError> {
    match record.fields.get(name) {
        Some(ApplicationValue::Timestamp { seconds, nanos }) => Ok(format!("{seconds}.{nanos:09}")),
        Some(ApplicationValue::String(value)) => Ok(value.clone()),
        _ => Err(ApplicationClientError::InvalidResponse),
    }
}
