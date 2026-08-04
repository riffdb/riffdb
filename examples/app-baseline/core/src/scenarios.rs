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
    /// Board-scale wide page: 50 open tickets in the dense cell.
    BoardPage50,
    /// Board-scale wide page: 200 open tickets in the dense cell.
    BoardPage200,
    /// Board-scale wide page: 450 open tickets in the dense cell.
    BoardPage450,
    /// Projected columnar board page (RiffDB only): 50 open tickets.
    BoardPageProjected50,
    /// Projected columnar board page (RiffDB only): 200 open tickets.
    BoardPageProjected200,
    /// Projected columnar board page (RiffDB only): 450 open tickets.
    BoardPageProjected450,
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
            Self::BoardPage50 => "board_page_50",
            Self::BoardPage200 => "board_page_200",
            Self::BoardPage450 => "board_page_450",
            Self::BoardPageProjected50 => "board_page_projected_50",
            Self::BoardPageProjected200 => "board_page_projected_200",
            Self::BoardPageProjected450 => "board_page_projected_450",
            Self::CreateComment => "create_comment",
            Self::CloseTicketWithComment => "close_ticket_with_comment",
            Self::SwapMemberRoles => "swap_member_roles",
            Self::OpenTicketWithLabels => "open_ticket_with_labels",
        }
    }

    /// Board page size for board scenarios; `None` for non-board scenarios.
    #[must_use]
    pub const fn board_page_limit(self) -> Option<u32> {
        match self {
            Self::BoardPage50 | Self::BoardPageProjected50 => Some(50),
            Self::BoardPage200 | Self::BoardPageProjected200 => Some(200),
            Self::BoardPage450 | Self::BoardPageProjected450 => Some(450),
            _ => None,
        }
    }

    /// Whether this scenario is the projected columnar board path (RiffDB only).
    #[must_use]
    pub const fn is_projected_board(self) -> bool {
        matches!(
            self,
            Self::BoardPageProjected50 | Self::BoardPageProjected200 | Self::BoardPageProjected450
        )
    }

    /// Whether this scenario is the compiled named-query board path.
    #[must_use]
    pub const fn is_compiled_board(self) -> bool {
        matches!(
            self,
            Self::BoardPage50 | Self::BoardPage200 | Self::BoardPage450
        )
    }

    /// All scenarios in report order (including board; may be filtered by scale).
    #[must_use]
    pub const fn all() -> [Self; 17] {
        [
            Self::PointGetTicket,
            Self::PointGetUser,
            Self::ListTicketsByProjectStatus,
            Self::ListOpenTicketsForAssignee,
            Self::ListCommentsForTicket,
            Self::ListProjectMembers,
            Self::TicketDetailPage,
            Self::BoardPage50,
            Self::BoardPage200,
            Self::BoardPage450,
            Self::BoardPageProjected50,
            Self::BoardPageProjected200,
            Self::BoardPageProjected450,
            Self::CreateComment,
            Self::CloseTicketWithComment,
            Self::SwapMemberRoles,
            Self::OpenTicketWithLabels,
        ]
    }

    /// Scenarios measured for `dataset` (compiled board only; no projected).
    ///
    /// Board scenarios require a dense open cell large enough to fill the page.
    /// Smoke (`board_dense_open == 0`) skips all board scenarios to stay fast.
    /// Use [`Self::for_dataset_with_projected`] for the RiffDB projected path.
    #[must_use]
    pub fn for_dataset(dataset: &SeedDataset) -> Vec<Self> {
        Self::for_dataset_filtered(dataset, false)
    }

    /// Scenarios measured for `dataset`, optionally including projected board pages.
    ///
    /// Projected variants are RiffDB-only (PG has no columnar path).
    #[must_use]
    pub fn for_dataset_with_projected(dataset: &SeedDataset) -> Vec<Self> {
        Self::for_dataset_filtered(dataset, true)
    }

    fn for_dataset_filtered(dataset: &SeedDataset, include_projected: bool) -> Vec<Self> {
        let dense = dataset.board_dense_open_count();
        Self::all()
            .into_iter()
            .filter(|scenario| {
                if scenario.is_projected_board() && !include_projected {
                    return false;
                }
                match scenario.board_page_limit() {
                    Some(limit) => dense >= limit as usize,
                    None => true,
                }
            })
            .collect()
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

/// Per-row marginal cost from the board size curve: `(p50_450 − p50_50) / 400`.
///
/// Returns `None` when either sample is missing or `p50_450 < p50_50`.
#[must_use]
pub fn board_marginal_ns_per_row(p50_50_ns: u64, p50_450_ns: u64) -> Option<u64> {
    p50_450_ns.checked_sub(p50_50_ns).map(|delta| delta / 400)
}

/// Extracts compiled board p50s from measured results and computes marginal cost.
#[must_use]
pub fn board_marginal_from_results(results: &[ScenarioResult]) -> Option<u64> {
    let p50 = |id: ScenarioId| -> Option<u64> {
        results
            .iter()
            .find(|row| row.scenario == id)
            .map(|row| row.samples.summary().p50_ns)
    };
    board_marginal_ns_per_row(
        p50(ScenarioId::BoardPage50)?,
        p50(ScenarioId::BoardPage450)?,
    )
}

/// Extracts projected board p50s and computes marginal cost (same formula).
#[must_use]
pub fn board_projected_marginal_from_results(results: &[ScenarioResult]) -> Option<u64> {
    let p50 = |id: ScenarioId| -> Option<u64> {
        results
            .iter()
            .find(|row| row.scenario == id)
            .map(|row| row.samples.summary().p50_ns)
    };
    board_marginal_ns_per_row(
        p50(ScenarioId::BoardPageProjected50)?,
        p50(ScenarioId::BoardPageProjected450)?,
    )
}

/// Runs warmups + measured samples for every scenario against one backend.
pub fn run_scenarios<B: AppBackend>(
    backend: &mut B,
    dataset: &SeedDataset,
    warmups: usize,
    samples: usize,
) -> Result<Vec<ScenarioResult>, B::Error> {
    run_scenarios_with_options(backend, dataset, warmups, samples, false)
}

/// Like [`run_scenarios`], optionally including RiffDB projected board scenarios.
pub fn run_scenarios_with_options<B: AppBackend>(
    backend: &mut B,
    dataset: &SeedDataset,
    warmups: usize,
    samples: usize,
    include_projected: bool,
) -> Result<Vec<ScenarioResult>, B::Error> {
    let probes = dataset.probes();
    let scenario_ids = if include_projected {
        ScenarioId::for_dataset_with_projected(dataset)
    } else {
        ScenarioId::for_dataset(dataset)
    };
    for _ in 0..warmups {
        run_once(backend, &probes, &scenario_ids)?;
    }

    let mut results = scenario_ids
        .into_iter()
        .map(|scenario| ScenarioResult {
            scenario,
            samples: SampleSet::default(),
            last_row_count: 0,
        })
        .collect::<Vec<_>>();

    for sample in 0..samples {
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
                        backend.list_project_members(probes.organization_id, probes.project_id, 50)
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
                ScenarioId::BoardPage50 | ScenarioId::BoardPage200 | ScenarioId::BoardPage450 => {
                    let limit = result
                        .scenario
                        .board_page_limit()
                        .expect("board scenario has limit");
                    let (value, elapsed) = time_call(|| {
                        backend.board_page(
                            probes.board_organization_id,
                            probes.board_project_id,
                            probes.open_status,
                            limit,
                        )
                    });
                    value.map(|rows| (rows.len(), elapsed))
                }
                ScenarioId::BoardPageProjected50
                | ScenarioId::BoardPageProjected200
                | ScenarioId::BoardPageProjected450 => {
                    let limit = result
                        .scenario
                        .board_page_limit()
                        .expect("projected board scenario has limit");
                    let (value, elapsed) = time_call(|| {
                        backend.board_page_projected(
                            probes.board_organization_id,
                            probes.board_project_id,
                            probes.open_status,
                            limit,
                        )
                    });
                    value.map(|rows| (rows.len(), elapsed))
                }
                ScenarioId::CreateComment => {
                    // Each sample inserts a distinct comment (new idempotency
                    // key + comment id) so RiffDB never takes the replay path
                    // and PostgreSQL never no-ops on conflict.
                    let input = probes.write_comment(sample);
                    let (value, elapsed) = time_call(|| backend.create_comment(&input));
                    value.map(|()| (1, elapsed))
                }
                ScenarioId::CloseTicketWithComment => {
                    let input = probes.close_ticket_with_comment(sample);
                    let (value, elapsed) = time_call(|| backend.close_ticket_with_comment(&input));
                    // Two entity mutations: ticket + comment.
                    value.map(|()| (2, elapsed))
                }
                ScenarioId::SwapMemberRoles => {
                    let input = probes.swap_member_roles(sample);
                    let (value, elapsed) = time_call(|| backend.swap_member_roles(&input));
                    value.map(|()| (2, elapsed))
                }
                ScenarioId::OpenTicketWithLabels => {
                    let input = probes.open_ticket_with_labels(sample);
                    let (value, elapsed) = time_call(|| backend.open_ticket_with_labels(&input));
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
    scenario_ids: &[ScenarioId],
) -> Result<(), B::Error> {
    for scenario in scenario_ids {
        match scenario {
            ScenarioId::PointGetTicket => {
                let _ = backend.point_get_ticket(probes.organization_id, probes.ticket_id)?;
            }
            ScenarioId::PointGetUser => {
                let _ = backend.point_get_user(probes.organization_id, probes.user_id)?;
            }
            ScenarioId::ListTicketsByProjectStatus => {
                let _ = backend.list_tickets_by_project_status(
                    probes.organization_id,
                    probes.project_id,
                    probes.open_status,
                    50,
                )?;
            }
            ScenarioId::ListOpenTicketsForAssignee => {
                let _ = backend.list_open_tickets_for_assignee(
                    probes.organization_id,
                    probes.assignee_id,
                    50,
                )?;
            }
            ScenarioId::ListCommentsForTicket => {
                let _ = backend.list_comments_for_ticket(
                    probes.organization_id,
                    probes.ticket_id,
                    50,
                )?;
            }
            ScenarioId::ListProjectMembers => {
                let _ =
                    backend.list_project_members(probes.organization_id, probes.project_id, 50)?;
            }
            ScenarioId::TicketDetailPage => {
                let _ = backend.ticket_detail_page(probes.organization_id, probes.ticket_id, 50)?;
            }
            ScenarioId::BoardPage50 | ScenarioId::BoardPage200 | ScenarioId::BoardPage450 => {
                let limit = scenario.board_page_limit().expect("board limit");
                let _ = backend.board_page(
                    probes.board_organization_id,
                    probes.board_project_id,
                    probes.open_status,
                    limit,
                )?;
            }
            ScenarioId::BoardPageProjected50
            | ScenarioId::BoardPageProjected200
            | ScenarioId::BoardPageProjected450 => {
                let limit = scenario.board_page_limit().expect("projected board limit");
                let _ = backend.board_page_projected(
                    probes.board_organization_id,
                    probes.board_project_id,
                    probes.open_status,
                    limit,
                )?;
            }
            // Write scenarios are not warmed: each measured sample must be a
            // genuinely new durable write with a distinct idempotency key.
            ScenarioId::CreateComment
            | ScenarioId::CloseTicketWithComment
            | ScenarioId::SwapMemberRoles
            | ScenarioId::OpenTicketWithLabels => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ScenarioId, ScenarioResult, board_marginal_from_results, board_marginal_ns_per_row,
        board_projected_marginal_from_results,
    };
    use crate::{
        AppBackend, CloseTicketWithCommentSeed, CommentRow, CommentSeed, LoadErrorClass,
        OpenTicketWithLabelsSeed, ProjectMemberRow, SampleSet, Scale, SeedDataset,
        SwapMemberRolesSeed, TicketDetailPage, TicketRow, TicketStatus, UserRow, UuidBytes,
    };

    /// Seed-backed backend that implements board_page by filtering the dataset.
    ///
    /// Used for row-count and order-sensitive equivalence without live engines.
    struct SeedBackedBackend {
        dataset: SeedDataset,
        last_board_ids: Vec<UuidBytes>,
    }

    impl AppBackend for SeedBackedBackend {
        type Error = String;

        fn reset(&mut self) -> Result<(), Self::Error> {
            Ok(())
        }
        fn seed(&mut self, _: &SeedDataset) -> Result<(), Self::Error> {
            Ok(())
        }
        fn load_error_class(_: &Self::Error) -> LoadErrorClass {
            LoadErrorClass::Other
        }
        fn load_error_code(_: &Self::Error) -> Option<&str> {
            None
        }
        fn point_get_ticket(
            &mut self,
            _: UuidBytes,
            _: UuidBytes,
        ) -> Result<Option<TicketRow>, Self::Error> {
            Ok(None)
        }
        fn point_get_user(
            &mut self,
            _: UuidBytes,
            _: UuidBytes,
        ) -> Result<Option<UserRow>, Self::Error> {
            Ok(None)
        }
        fn list_tickets_by_project_status(
            &mut self,
            _: UuidBytes,
            _: UuidBytes,
            _: TicketStatus,
            _: u32,
        ) -> Result<Vec<TicketRow>, Self::Error> {
            Ok(Vec::new())
        }
        fn list_open_tickets_for_assignee(
            &mut self,
            _: UuidBytes,
            _: UuidBytes,
            _: u32,
        ) -> Result<Vec<TicketRow>, Self::Error> {
            Ok(Vec::new())
        }
        fn list_comments_for_ticket(
            &mut self,
            _: UuidBytes,
            _: UuidBytes,
            _: u32,
        ) -> Result<Vec<CommentRow>, Self::Error> {
            Ok(Vec::new())
        }
        fn list_project_members(
            &mut self,
            _: UuidBytes,
            _: UuidBytes,
            _: u32,
        ) -> Result<Vec<ProjectMemberRow>, Self::Error> {
            Ok(Vec::new())
        }
        fn ticket_detail_page(
            &mut self,
            _: UuidBytes,
            _: UuidBytes,
            _: u32,
        ) -> Result<Option<TicketDetailPage>, Self::Error> {
            Ok(None)
        }
        fn board_page(
            &mut self,
            organization_id: UuidBytes,
            project_id: UuidBytes,
            status: TicketStatus,
            limit: u32,
        ) -> Result<Vec<TicketRow>, Self::Error> {
            let mut rows = self
                .dataset
                .tickets
                .iter()
                .filter(|ticket| {
                    ticket.organization_id == organization_id
                        && ticket.project_id == project_id
                        && ticket.status == status
                })
                .cloned()
                .collect::<Vec<_>>();
            rows.sort_by_key(|row| row.ticket_id);
            rows.truncate(limit as usize);
            self.last_board_ids = rows.iter().map(|row| row.ticket_id).collect();
            Ok(rows)
        }
        fn board_page_projected(
            &mut self,
            organization_id: UuidBytes,
            project_id: UuidBytes,
            status: TicketStatus,
            limit: u32,
        ) -> Result<Vec<TicketRow>, Self::Error> {
            // Seed-backed stand-in uses the same filter as the compiled path so
            // unit tests can exercise equivalence without a live engine.
            self.board_page(organization_id, project_id, status, limit)
        }
        fn create_comment(&mut self, _: &CommentSeed) -> Result<(), Self::Error> {
            Ok(())
        }
        fn replay_comment(&mut self, _: &CommentSeed) -> Result<(), Self::Error> {
            Ok(())
        }
        fn close_ticket_with_comment(
            &mut self,
            _: &CloseTicketWithCommentSeed,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
        fn swap_member_roles(&mut self, _: &SwapMemberRolesSeed) -> Result<(), Self::Error> {
            Ok(())
        }
        fn open_ticket_with_labels(
            &mut self,
            _: &OpenTicketWithLabelsSeed,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn board_marginal_ns_per_row_divides_delta_by_400() {
        assert_eq!(board_marginal_ns_per_row(1_000, 41_000), Some(100));
        assert_eq!(board_marginal_ns_per_row(10, 10), Some(0));
        assert_eq!(board_marginal_ns_per_row(100, 50), None);
        // (p50_450 - p50_50) / 400 with non-multiple remainder floors.
        assert_eq!(board_marginal_ns_per_row(0, 399), Some(0));
        assert_eq!(board_marginal_ns_per_row(0, 400), Some(1));
    }

    #[test]
    fn board_marginal_from_results_reads_board_page_p50s() {
        let mut s50 = SampleSet::default();
        let mut s450 = SampleSet::default();
        for _ in 0..3 {
            s50.record(std::time::Duration::from_nanos(1_000));
            s450.record(std::time::Duration::from_nanos(41_000));
        }
        let results = vec![
            ScenarioResult {
                scenario: ScenarioId::BoardPage50,
                samples: s50,
                last_row_count: 50,
            },
            ScenarioResult {
                scenario: ScenarioId::BoardPage450,
                samples: s450,
                last_row_count: 450,
            },
        ];
        assert_eq!(board_marginal_from_results(&results), Some(100));
    }

    #[test]
    fn board_projected_marginal_uses_projected_scenario_ids() {
        let mut s50 = SampleSet::default();
        let mut s450 = SampleSet::default();
        for _ in 0..3 {
            s50.record(std::time::Duration::from_nanos(2_000));
            s450.record(std::time::Duration::from_nanos(42_000));
        }
        let results = vec![
            ScenarioResult {
                scenario: ScenarioId::BoardPageProjected50,
                samples: s50,
                last_row_count: 50,
            },
            ScenarioResult {
                scenario: ScenarioId::BoardPageProjected450,
                samples: s450,
                last_row_count: 450,
            },
        ];
        assert_eq!(board_projected_marginal_from_results(&results), Some(100));
    }

    /// Falsifiability (c): swapping 50/450 in the marginal formula fails the shape.
    #[test]
    fn board_marginal_rejects_swapped_50_450_order() {
        // Correct order: (450 - 50) / 400. Swapped yields None (negative delta).
        assert_eq!(board_marginal_ns_per_row(41_000, 1_000), None);
        assert_eq!(board_marginal_ns_per_row(1_000, 41_000), Some(100));
    }

    #[test]
    fn smoke_skips_board_scenarios() {
        let dataset = SeedDataset::generate(Scale::smoke());
        let ids = ScenarioId::for_dataset(&dataset);
        assert!(!ids.iter().any(|id| id.board_page_limit().is_some()));
        assert_eq!(ids.len(), 11);
        let with_projected = ScenarioId::for_dataset_with_projected(&dataset);
        assert!(
            !with_projected
                .iter()
                .any(|id| id.is_projected_board() || id.is_compiled_board())
        );
    }

    #[test]
    fn full_includes_all_board_page_sizes() {
        let dataset = SeedDataset::generate(Scale::full());
        let ids = ScenarioId::for_dataset(&dataset);
        assert!(ids.contains(&ScenarioId::BoardPage50));
        assert!(ids.contains(&ScenarioId::BoardPage200));
        assert!(ids.contains(&ScenarioId::BoardPage450));
        assert!(!ids.iter().any(|id| id.is_projected_board()));
        assert_eq!(ids.len(), 14);
        let with_projected = ScenarioId::for_dataset_with_projected(&dataset);
        assert!(with_projected.contains(&ScenarioId::BoardPageProjected50));
        assert!(with_projected.contains(&ScenarioId::BoardPageProjected200));
        assert!(with_projected.contains(&ScenarioId::BoardPageProjected450));
        assert_eq!(with_projected.len(), 17);
    }

    #[test]
    fn board_scenarios_return_exact_page_sizes_matching_seed_order() {
        // Seed-filter unit check: the AppBackend-shaped filter agrees with
        // SeedDataset::board_page_ticket_ids. Live PG vs RiffDB sequence
        // equivalence is enforced once-per-run in the binary harness
        // (`assert_board_ticket_sequences_equal`).
        let dataset = SeedDataset::generate(Scale::full());
        let mut backend = SeedBackedBackend {
            dataset: dataset.clone(),
            last_board_ids: Vec::new(),
        };
        for limit in [50_u32, 200, 450] {
            let expected = dataset.board_page_ticket_ids(limit);
            assert_eq!(expected.len(), limit as usize);
            let rows = backend
                .board_page(
                    dataset.board_cell().0,
                    dataset.board_cell().1,
                    TicketStatus::Open,
                    limit,
                )
                .expect("board page");
            let ids: Vec<_> = rows.iter().map(|row| row.ticket_id).collect();
            assert_eq!(ids, expected, "board page limit={limit}");
            assert_eq!(rows.len(), limit as usize);
        }
        assert_eq!(backend.last_board_ids.len(), 450);
    }
}
