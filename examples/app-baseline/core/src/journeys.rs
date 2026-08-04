//! Stateful application journeys and exact semantic canaries.

use std::time::Instant;

use crate::{
    AppBackend, CloseTicketWithCommentSeed, CommentRow, CommentSeed, OpenTicketWithLabelsSeed,
    SeedDataset, TicketStatus, uuid_from_ordinal,
};

const NS_JOURNEY: u8 = 0x7d;

/// Runs browser- and agent-shaped causal journeys against one already seeded
/// backend. Every mutation is followed by an application-shaped read and an
/// exact state assertion. Timing is end-to-end per journey, not per RPC.
pub fn run_stateful_journeys<B: AppBackend>(
    backend: &mut B,
    dataset: &SeedDataset,
    ordinal_base: u64,
) -> Result<serde_json::Value, String> {
    let probes = dataset.probes();

    let browser_started = Instant::now();
    let listed = backend
        .list_tickets_by_project_status(
            probes.organization_id,
            probes.project_id,
            TicketStatus::Open,
            50,
        )
        .map_err(|error| format!("browser list: {error}"))?;
    if listed.is_empty() {
        return Err("browser journey list returned no open tickets".to_owned());
    }
    let page = backend
        .ticket_detail_page(probes.organization_id, probes.ticket_id, 50)
        .map_err(|error| format!("browser detail: {error}"))?
        .ok_or_else(|| "browser journey detail returned NotFound".to_owned())?;
    let comment_id = uuid_from_ordinal(NS_JOURNEY, ordinal_base.saturating_add(1));
    let comment = CommentSeed {
        row: CommentRow {
            organization_id: probes.organization_id,
            comment_id,
            ticket_id: page.ticket.ticket_id,
            author_id: probes.write_author_id,
            body: "stateful browser journey comment".to_owned(),
        },
        idempotency_key: format!("journey-browser-{ordinal_base}"),
    };
    backend
        .create_comment(&comment)
        .map_err(|error| format!("browser command: {error}"))?;
    backend
        .replay_comment(&comment)
        .map_err(|error| format!("browser replay: {error}"))?;
    let comments = backend
        .list_comments_for_ticket(probes.organization_id, probes.ticket_id, 50)
        .map_err(|error| format!("browser causal reread: {error}"))?;
    let matching_comment_count = comments
        .iter()
        .filter(|candidate| candidate.comment_id == comment_id)
        .count();
    if matching_comment_count != 1 {
        return Err(format!(
            "browser journey expected exactly one replay-safe comment, observed {matching_comment_count}"
        ));
    }
    let browser_ns = u64::try_from(browser_started.elapsed().as_nanos()).unwrap_or(u64::MAX);

    let agent_started = Instant::now();
    let ticket_id = uuid_from_ordinal(NS_JOURNEY, ordinal_base.saturating_add(2));
    let open = OpenTicketWithLabelsSeed {
        organization_id: probes.organization_id,
        ticket_id,
        project_id: probes.write_project_id,
        reporter_id: probes.write_author_id,
        assignee_id: probes.write_assignee_id,
        title: "stateful agent journey ticket".to_owned(),
        label_a: probes.write_label_a,
        label_b: probes.write_label_b,
        idempotency_key: format!("journey-agent-open-{ordinal_base}"),
    };
    backend
        .open_ticket_with_labels(&open)
        .map_err(|error| format!("agent open command: {error}"))?;
    let opened = backend
        .ticket_detail_page(probes.organization_id, ticket_id, 50)
        .map_err(|error| format!("agent page after open: {error}"))?
        .ok_or_else(|| "agent journey opened ticket was not found".to_owned())?;
    if opened.ticket.status != TicketStatus::Open || opened.labels.len() != 2 {
        return Err(format!(
            "agent journey open shape mismatch: status={} labels={}",
            opened.ticket.status.as_str(),
            opened.labels.len()
        ));
    }
    let close_comment_id = uuid_from_ordinal(NS_JOURNEY, ordinal_base.saturating_add(3));
    backend
        .close_ticket_with_comment(&CloseTicketWithCommentSeed {
            organization_id: probes.organization_id,
            ticket_id,
            author_id: probes.write_author_id,
            comment_id: close_comment_id,
            body: "stateful agent close note".to_owned(),
            idempotency_key: format!("journey-agent-close-{ordinal_base}"),
        })
        .map_err(|error| format!("agent close command: {error}"))?;
    let closed = backend
        .ticket_detail_page(probes.organization_id, ticket_id, 50)
        .map_err(|error| format!("agent causal reread: {error}"))?
        .ok_or_else(|| "agent journey closed ticket was not found".to_owned())?;
    let close_comment_count = closed
        .comments
        .iter()
        .filter(|candidate| candidate.comment_id == close_comment_id)
        .count();
    if closed.ticket.status != TicketStatus::Closed || close_comment_count != 1 {
        return Err(format!(
            "agent journey close mismatch: status={} closing_comments={close_comment_count}",
            closed.ticket.status.as_str()
        ));
    }
    let agent_ns = u64::try_from(agent_started.elapsed().as_nanos()).unwrap_or(u64::MAX);

    Ok(serde_json::json!({
        "schema": "riffdb.app-baseline-journeys/v1",
        "browser": {
            "journey": "list_detail_command_replay_causal_reread",
            "elapsed_ns": browser_ns,
            "listed_ticket_count": listed.len(),
            "matching_durable_effects": matching_comment_count,
            "status": "passed",
        },
        "agent": {
            "journey": "page_open_with_labels_close_declared_outcome_causal_reread",
            "elapsed_ns": agent_ns,
            "label_count": closed.labels.len(),
            "matching_closing_effects": close_comment_count,
            "status": "passed",
        },
        "semantic_reconciliation": {
            "clean": true,
            "missing_effects": 0,
            "duplicate_effects": 0,
            "state_divergences": 0,
            "scope": "stateful_canary_operations",
        }
    }))
}
