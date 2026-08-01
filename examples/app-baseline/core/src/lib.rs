//! Shared TicketDesk app-baseline workload: seed, scenarios, timing, report.
//!
//! This is comparison evidence only. It is not a RiffDB product surface.

#![forbid(unsafe_code)]

mod histogram;
mod ids;
mod load;
mod report;
mod scale;
mod scenarios;
mod seed;
mod timing;

pub use histogram::*;
pub use ids::*;
pub use load::*;
pub use report::*;
pub use scale::*;
pub use scenarios::*;
pub use seed::*;
pub use timing::*;

/// Stable load-driver classification of a backend error.
///
/// Backends must map from typed codes (RiffDB `RDB-*` application codes or
/// PostgreSQL SQLSTATE), never from free-form prose.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoadErrorClass {
    /// Durable uniqueness / concurrency conflict (e.g. SQLSTATE 23505).
    Conflict,
    /// Same idempotency identity was reused with unequal input.
    IdempotencyMismatch,
    /// Temporary unavailability (e.g. RDB-STORAGE-0101).
    Unavailable,
    /// Typed capacity rejection (RDB-CAPACITY-0101); certain-not-executed.
    Overloaded,
    /// Observed history predates a restore (RDB-HISTORY-0101).
    HistoryIncarnationMismatch,
    /// Any other failure.
    Other,
}

/// Backend-neutral operations exercised by the app baseline.
pub trait AppBackend {
    /// Backend-specific failure.
    type Error: std::fmt::Display;

    /// Installs empty schema / resets state for a dedicated database.
    fn reset(&mut self) -> Result<(), Self::Error>;

    /// Loads the deterministic seed dataset.
    fn seed(&mut self, dataset: &SeedDataset) -> Result<(), Self::Error>;

    /// Prepares every statement/path used by timed load/parity operations.
    ///
    /// Default is a no-op. PostgreSQL must prepare its full statement set so
    /// Parse/Describe is never paid inside a timed sample.
    fn prewarm(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Classifies `error` for load-driver outcome metrics.
    fn load_error_class(error: &Self::Error) -> LoadErrorClass;

    /// Stable machine code for load reports (`RDB-*` or SQLSTATE), when known.
    fn load_error_code(error: &Self::Error) -> Option<&str>;

    /// Primary-key ticket lookup.
    fn point_get_ticket(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
    ) -> Result<Option<TicketRow>, Self::Error>;

    /// Primary-key user lookup.
    fn point_get_user(
        &mut self,
        organization_id: UuidBytes,
        user_id: UuidBytes,
    ) -> Result<Option<UserRow>, Self::Error>;

    /// Filtered ticket list for one project and status (bounded).
    fn list_tickets_by_project_status(
        &mut self,
        organization_id: UuidBytes,
        project_id: UuidBytes,
        status: TicketStatus,
        limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error>;

    /// Open tickets assigned to one user (bounded).
    fn list_open_tickets_for_assignee(
        &mut self,
        organization_id: UuidBytes,
        assignee_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error>;

    /// Comments for one ticket ordered by creation (bounded).
    fn list_comments_for_ticket(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<CommentRow>, Self::Error>;

    /// Project membership directory.
    fn list_project_members(
        &mut self,
        organization_id: UuidBytes,
        project_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<ProjectMemberRow>, Self::Error>;

    /// Application "ticket detail page": ticket + project + org + assignee + comments + labels.
    fn ticket_detail_page(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
        comment_limit: u32,
    ) -> Result<Option<TicketDetailPage>, Self::Error>;

    /// Board-scale wide ticket page: full row fields for one (project, status), ordered.
    ///
    /// Both backends must use the same predicate, column set, `ORDER BY ticket_id ASC`,
    /// and `LIMIT n` (see `BOARD_PAGE_SQL` / `queries/ticketdesk/board_page.riffq`).
    fn board_page(
        &mut self,
        organization_id: UuidBytes,
        project_id: UuidBytes,
        status: TicketStatus,
        limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error>;

    /// Append one comment (post-seed write path).
    fn create_comment(&mut self, comment: &CommentSeed) -> Result<(), Self::Error>;

    /// Repeat an already-successful comment creation with the same identity and input.
    ///
    /// This is a distinct benchmark operation so the PostgreSQL adapter can
    /// exercise its explicit idempotency policy without weakening ordinary
    /// create semantics.
    fn replay_comment(&mut self, comment: &CommentSeed) -> Result<(), Self::Error>;

    /// Atomic multi-entity write: close ticket + create comment.
    fn close_ticket_with_comment(
        &mut self,
        input: &CloseTicketWithCommentSeed,
    ) -> Result<(), Self::Error>;

    /// Atomic multi-entity write: swap two membership roles.
    fn swap_member_roles(&mut self, input: &SwapMemberRolesSeed) -> Result<(), Self::Error>;

    /// Multi-command workflow: create ticket + attach two labels.
    ///
    /// RiffDB issues three symbolic commands; Postgres commits one SQL
    /// transaction covering the same three inserts.
    fn open_ticket_with_labels(
        &mut self,
        input: &OpenTicketWithLabelsSeed,
    ) -> Result<(), Self::Error>;
}
