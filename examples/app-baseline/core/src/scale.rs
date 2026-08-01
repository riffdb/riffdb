//! Dataset scale profiles.

/// Default open-ticket density for the board-scale cell under the full profile.
///
/// One `(project, status=open)` cell holds this many tickets so
/// `board_page_500` returns a real 500-row page.
pub const FULL_BOARD_DENSE_OPEN: u32 = 600;

/// Row-count knobs for the TicketDesk seed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Scale {
    /// Number of organizations.
    pub organizations: u32,
    /// Users per organization.
    pub users_per_org: u32,
    /// Projects per organization.
    pub projects_per_org: u32,
    /// Members per project.
    pub members_per_project: u32,
    /// Tickets per project (non-board projects; board project may override).
    pub tickets_per_project: u32,
    /// Comments per ticket.
    pub comments_per_ticket: u32,
    /// Labels per organization.
    pub labels_per_org: u32,
    /// Labels attached per ticket.
    pub labels_per_ticket: u32,
    /// Open tickets forced into org-0/project-0 for board-scale reads.
    ///
    /// `0` disables board density (smoke). Full uses [`FULL_BOARD_DENSE_OPEN`].
    pub board_dense_open: u32,
}

impl Scale {
    /// Fast correctness/smoke profile.
    ///
    /// Board scenarios are skipped: `board_dense_open == 0` keeps smoke seed
    /// small and fast.
    #[must_use]
    pub const fn smoke() -> Self {
        Self {
            organizations: 2,
            users_per_org: 5,
            projects_per_org: 2,
            members_per_project: 3,
            tickets_per_project: 10,
            comments_per_ticket: 3,
            labels_per_org: 4,
            labels_per_ticket: 2,
            board_dense_open: 0,
        }
    }

    /// Default baseline with thousands of rows plus a dense board cell.
    #[must_use]
    pub const fn full() -> Self {
        Self {
            organizations: 10,
            users_per_org: 50,
            projects_per_org: 10,
            members_per_project: 5,
            tickets_per_project: 20,
            comments_per_ticket: 4,
            labels_per_org: 5,
            labels_per_ticket: 2,
            board_dense_open: FULL_BOARD_DENSE_OPEN,
        }
    }

    /// Approximate total authoritative rows (excluding pure link-free counts).
    #[must_use]
    pub fn approximate_row_count(self) -> u64 {
        let orgs = u64::from(self.organizations);
        let users = orgs * u64::from(self.users_per_org);
        let projects = orgs * u64::from(self.projects_per_org);
        let members = projects * u64::from(self.members_per_project);
        let base_tickets = projects * u64::from(self.tickets_per_project);
        // Board density replaces project-0/org-0's ordinary ticket count when denser.
        let board_extra = if self.board_dense_open > self.tickets_per_project {
            u64::from(
                self.board_dense_open
                    .saturating_sub(self.tickets_per_project),
            )
        } else {
            0
        };
        let tickets = base_tickets + board_extra;
        let comments = tickets * u64::from(self.comments_per_ticket);
        let labels = orgs * u64::from(self.labels_per_org);
        let ticket_labels = tickets * u64::from(self.labels_per_ticket);
        orgs + users + projects + members + tickets + comments + labels + ticket_labels
    }

    /// Profile name for reports.
    #[must_use]
    pub const fn name(self) -> &'static str {
        if self.organizations == Self::smoke().organizations
            && self.tickets_per_project == Self::smoke().tickets_per_project
            && self.board_dense_open == Self::smoke().board_dense_open
        {
            "smoke"
        } else if self.organizations == Self::full().organizations
            && self.tickets_per_project == Self::full().tickets_per_project
            && self.board_dense_open == Self::full().board_dense_open
        {
            "full"
        } else {
            "custom"
        }
    }
}
