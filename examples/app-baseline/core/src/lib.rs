//! Shared TicketDesk app-baseline workload: seed, scenarios, timing, report.
//!
//! This is comparison evidence only. It is not a RiffDB product surface.

#![forbid(unsafe_code)]

mod ids;
mod report;
mod scale;
mod scenarios;
mod seed;
mod timing;

pub use ids::*;
pub use report::*;
pub use scale::*;
pub use scenarios::*;
pub use seed::*;
pub use timing::*;

/// Backend-neutral operations exercised by the app baseline.
pub trait AppBackend {
    /// Backend-specific failure.
    type Error: std::fmt::Display;

    /// Installs empty schema / resets state for a dedicated database.
    fn reset(&mut self) -> Result<(), Self::Error>;

    /// Loads the deterministic seed dataset.
    fn seed(&mut self, dataset: &SeedDataset) -> Result<(), Self::Error>;

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

    /// Append one comment (post-seed write path).
    fn create_comment(&mut self, comment: &CommentSeed) -> Result<(), Self::Error>;
}
