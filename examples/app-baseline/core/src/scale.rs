//! Dataset scale profiles.

/// Default open-ticket density for the board-scale cell under the full profile.
///
/// One `(project, status=open)` cell holds this many tickets so
/// `board_page_450` returns a real 450-row page under the scan ceiling.
pub const FULL_BOARD_DENSE_OPEN: u32 = 600;

/// Contract maximum for `comment.body` (`string<256>` in ticketdesk.riff).
///
/// Seeding above it fails the command with `RDB-INPUT-0101`. Real help desk
/// comments are longer than this; carrying that would require widening the
/// contract, which also defines the frozen comparator dataset, so it is out of
/// scope here and noted as a known limitation of the profile.
pub const MAX_COMMENT_BODY_BYTES: u32 = 256;

/// Open-ticket density for the board cell under the production profile.
///
/// Held at the `full` value on purpose: `board_page_450` reads one bounded
/// page, and the point of the production profile is the size of the data
/// *around* that page, not a wider page.
pub const PRODUCTION_BOARD_DENSE_OPEN: u32 = FULL_BOARD_DENSE_OPEN;

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
    /// Target UTF-8 bytes for generated comment bodies. Ticket titles use the
    /// same target capped at the contract's 128-byte maximum. Zero retains the
    /// compact human-readable seed strings.
    pub payload_bytes: u32,
}

impl Scale {
    /// Fast correctness/smoke profile.
    ///
    /// Board scenarios are skipped: `board_dense_open == 0` keeps smoke seed
    /// small and fast. `projects_per_org` is at least 3 so write probes can
    /// land outside both the ordinary read-probe project and the board-cell
    /// project (first project).
    #[must_use]
    pub const fn smoke() -> Self {
        Self {
            organizations: 2,
            users_per_org: 5,
            projects_per_org: 3,
            members_per_project: 3,
            tickets_per_project: 10,
            comments_per_ticket: 3,
            labels_per_org: 4,
            labels_per_ticket: 2,
            board_dense_open: 0,
            payload_bytes: 0,
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
            payload_bytes: 0,
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

    /// Production-shaped profile: a help desk with real history and real text.
    ///
    /// `smoke` and `full` are deliberately small. `full` seeds roughly 14,600
    /// rows and 2,000 tickets with `payload_bytes: 0`, so every index is
    /// shallow, the whole set is resident, and comment bodies are short
    /// generated labels. That measures protocol and CPU cost rather than a
    /// database, and it under-measures any write path whose cost scales with
    /// bytes.
    ///
    /// This profile seeds roughly 120,000 tickets and 600,000 comments across
    /// 200 tenants, with comment bodies at a realistic length. It is NOT the
    /// frozen `PERF-018` comparator dataset and must never replace it: those
    /// gates, their banked receipts, and the seed ceiling are all stated
    /// against `full`. Use this to learn where the two engines actually differ
    /// at size, and report it as its own profile.
    #[must_use]
    pub const fn production() -> Self {
        Self {
            organizations: 200,
            users_per_org: 40,
            projects_per_org: 12,
            members_per_project: 6,
            tickets_per_project: 50,
            comments_per_ticket: 5,
            labels_per_org: 12,
            labels_per_ticket: 3,
            board_dense_open: PRODUCTION_BOARD_DENSE_OPEN,
            payload_bytes: MAX_COMMENT_BODY_BYTES,
        }
    }

    /// Profile name for reports.
    #[must_use]
    pub const fn name(self) -> &'static str {
        if self.organizations == Self::smoke().organizations
            && self.tickets_per_project == Self::smoke().tickets_per_project
            && self.board_dense_open == Self::smoke().board_dense_open
            && self.projects_per_org == Self::smoke().projects_per_org
            && self.comments_per_ticket == Self::smoke().comments_per_ticket
            && self.labels_per_ticket == Self::smoke().labels_per_ticket
            && self.payload_bytes == Self::smoke().payload_bytes
        {
            "smoke"
        } else if self.organizations == Self::full().organizations
            && self.tickets_per_project == Self::full().tickets_per_project
            && self.board_dense_open == Self::full().board_dense_open
            && self.projects_per_org == Self::full().projects_per_org
            && self.comments_per_ticket == Self::full().comments_per_ticket
            && self.labels_per_ticket == Self::full().labels_per_ticket
            && self.payload_bytes == Self::full().payload_bytes
        {
            "full"
        } else if self.organizations == Self::production().organizations
            && self.tickets_per_project == Self::production().tickets_per_project
            && self.board_dense_open == Self::production().board_dense_open
            && self.projects_per_org == Self::production().projects_per_org
            && self.comments_per_ticket == Self::production().comments_per_ticket
            && self.labels_per_ticket == Self::production().labels_per_ticket
            && self.payload_bytes == Self::production().payload_bytes
        {
            "production"
        } else {
            "custom"
        }
    }
}

/// Seed layout generation number.
///
/// Generation 2 is the board-density layout (dense org-0/project-0 open cell,
/// write probes outside the board cell). Pre-B1 generation-1 full baselines
/// (≈2000 tickets / ≈15160 rows) are not comparable.
pub const SEED_GENERATION: u32 = 2;
