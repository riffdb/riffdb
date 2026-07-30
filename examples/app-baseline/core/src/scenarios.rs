//! Timed application scenarios.

use crate::{AppBackend, SampleSet, ScenarioProbes, SeedDataset, time_call};

/// Stable scenario identifiers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScenarioId {
    /// PK ticket get.
    PointGetTicket,
    /// PK user get.
    PointGetUser,
    /// Filter tickets by project+status.
    ListTicketsByProjectStatus,
    /// Filter open tickets by assignee.
    ListOpenTicketsForAssignee,
    /// Comments for one ticket.
    ListCommentsForTicket,
    /// Project members directory.
    ListProjectMembers,
    /// Multi-entity ticket detail page.
    TicketDetailPage,
    /// Post-seed write: create comment.
    CreateComment,
    /// Atomic multi-entity write: close ticket + comment.
    CloseTicketWithComment,
    /// Atomic multi-entity write: swap two member roles.
    SwapMemberRoles,
    /// Multi-command workflow: open ticket + attach two labels.
    OpenTicketWithLabels,
}

impl ScenarioId {
    /// Stable report id.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PointGetTicket => "point_get_ticket",
            Self::PointGetUser => "point_get_user",
            Self::ListTicketsByProjectStatus => "list_tickets_by_project_status",
            Self::ListOpenTicketsForAssignee => "list_open_tickets_for_assignee",
            Self::ListCommentsForTicket => "list_comments_for_ticket",
            Self::ListProjectMembers => "list_project_members",
            Self::TicketDetailPage => "ticket_detail_page",
            Self::CreateComment => "create_comment",
            Self::CloseTicketWithComment => "close_ticket_with_comment",
            Self::SwapMemberRoles => "swap_member_roles",
            Self::OpenTicketWithLabels => "open_ticket_with_labels",
        }
    }

    /// All scenarios in report order.
    #[must_use]
    pub const fn all() -> [Self; 11] {
        [
            Self::PointGetTicket,
            Self::PointGetUser,
            Self::ListTicketsByProjectStatus,
            Self::ListOpenTicketsForAssignee,
            Self::ListCommentsForTicket,
            Self::ListProjectMembers,
            Self::TicketDetailPage,
            Self::CreateComment,
            Self::CloseTicketWithComment,
            Self::SwapMemberRoles,
            Self::OpenTicketWithLabels,
        ]
    }
}

/// One measured scenario result.
#[derive(Clone, Debug)]
pub struct ScenarioResult {
    /// Scenario id.
    pub scenario: ScenarioId,
    /// Timing samples.
    pub samples: SampleSet,
    /// Optional result cardinality check (rows returned on last sample).
    pub last_row_count: usize,
}

/// Runs warmups + measured samples for every scenario against one backend.
pub fn run_scenarios<B: AppBackend>(
    backend: &mut B,
    dataset: &SeedDataset,
    warmups: usize,
    samples: usize,
) -> Result<Vec<ScenarioResult>, B::Error> {
    let probes = dataset.probes();
    for _ in 0..warmups {
        run_once(backend, &probes)?;
    }

    let mut results = ScenarioId::all()
        .into_iter()
        .map(|scenario| ScenarioResult {
            scenario,
            samples: SampleSet::default(),
            last_row_count: 0,
        })
        .collect::<Vec<_>>();

    for _ in 0..samples {
        for result in &mut results {
            let scenario_name = result.scenario.as_str();
            let outcome = match result.scenario {
                ScenarioId::PointGetTicket => {
                    let (value, elapsed) = time_call(|| {
                        backend.point_get_ticket(probes.organization_id, probes.ticket_id)
                    });
                    value.map(|row| (usize::from(row.is_some()), elapsed))
                }
                ScenarioId::PointGetUser => {
                    let (value, elapsed) = time_call(|| {
                        backend.point_get_user(probes.organization_id, probes.user_id)
                    });
                    value.map(|row| (usize::from(row.is_some()), elapsed))
                }
                ScenarioId::ListTicketsByProjectStatus => {
                    let (value, elapsed) = time_call(|| {
                        backend.list_tickets_by_project_status(
                            probes.organization_id,
                            probes.project_id,
                            probes.open_status,
                            50,
                        )
                    });
                    value.map(|rows| (rows.len(), elapsed))
                }
                ScenarioId::ListOpenTicketsForAssignee => {
                    let (value, elapsed) = time_call(|| {
                        backend.list_open_tickets_for_assignee(
                            probes.organization_id,
                            probes.assignee_id,
                            50,
                        )
                    });
                    value.map(|rows| (rows.len(), elapsed))
                }
                ScenarioId::ListCommentsForTicket => {
                    let (value, elapsed) = time_call(|| {
                        backend.list_comments_for_ticket(
                            probes.organization_id,
                            probes.ticket_id,
                            50,
                        )
                    });
                    value.map(|rows| (rows.len(), elapsed))
                }
                ScenarioId::ListProjectMembers => {
                    let (value, elapsed) = time_call(|| {
                        backend.list_project_members(
                            probes.organization_id,
                            probes.project_id,
                            50,
                        )
                    });
                    value.map(|rows| (rows.len(), elapsed))
                }
                ScenarioId::TicketDetailPage => {
                    let (value, elapsed) = time_call(|| {
                        backend.ticket_detail_page(probes.organization_id, probes.ticket_id, 50)
                    });
                    value.map(|page| {
                        let count = page.as_ref().map_or(0, |page| {
                            1 + page.comments.len()
                                + page.labels.len()
                                + usize::from(page.assignee.is_some())
                        });
                        (count, elapsed)
                    })
                }
                ScenarioId::CreateComment => {
                    // Idempotent create: first sample inserts, later samples replay.
                    let (value, elapsed) =
                        time_call(|| backend.create_comment(&probes.write_comment));
                    value.map(|()| (1, elapsed))
                }
                ScenarioId::CloseTicketWithComment => {
                    let (value, elapsed) = time_call(|| {
                        backend.close_ticket_with_comment(&probes.close_ticket_with_comment)
                    });
                    // Two entity mutations: ticket + comment.
                    value.map(|()| (2, elapsed))
                }
                ScenarioId::SwapMemberRoles => {
                    let (value, elapsed) =
                        time_call(|| backend.swap_member_roles(&probes.swap_member_roles));
                    value.map(|()| (2, elapsed))
                }
                ScenarioId::OpenTicketWithLabels => {
                    let (value, elapsed) = time_call(|| {
                        backend.open_ticket_with_labels(&probes.open_ticket_with_labels)
                    });
                    // Ticket + two label links.
                    value.map(|()| (3, elapsed))
                }
            };
            let (row_count, elapsed) = match outcome {
                Ok(pair) => pair,
                Err(error) => {
                    eprintln!("scenario {scenario_name} failed: {error}");
                    return Err(error);
                }
            };
            result.samples.record(elapsed);
            result.last_row_count = row_count;
        }
    }
    Ok(results)
}

fn run_once<B: AppBackend>(
    backend: &mut B,
    probes: &ScenarioProbes,
) -> Result<(), B::Error> {
    let _ = backend.point_get_ticket(probes.organization_id, probes.ticket_id)?;
    let _ = backend.point_get_user(probes.organization_id, probes.user_id)?;
    let _ = backend.list_tickets_by_project_status(
        probes.organization_id,
        probes.project_id,
        probes.open_status,
        50,
    )?;
    let _ = backend.list_open_tickets_for_assignee(
        probes.organization_id,
        probes.assignee_id,
        50,
    )?;
    let _ = backend.list_comments_for_ticket(probes.organization_id, probes.ticket_id, 50)?;
    let _ = backend.list_project_members(probes.organization_id, probes.project_id, 50)?;
    let _ = backend.ticket_detail_page(probes.organization_id, probes.ticket_id, 50)?;
    Ok(())
}
