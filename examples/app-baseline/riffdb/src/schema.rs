//! Frozen TicketDesk contract identity constants.
//!
//! IDs come from compiling `contracts/ticketdesk.riff` with the workspace
//! compiler. Re-verify after any contract change.

/// Exact contract source deployed to `riffdbd`.
pub(crate) const TICKETDESK_CONTRACT: &str = include_str!("../../contracts/ticketdesk.riff");

/// Contract lineage.
pub(crate) const CONTRACT_LINEAGE: &str = "TicketDesk";
/// Contract version.
pub(crate) const CONTRACT_VERSION: u64 = 1;

/// Entity type ids (alphabetical allocation by the compiler).
pub(crate) mod entity {
    pub(crate) const LABEL: u32 = 1;
    pub(crate) const TICKET: u32 = 2;
    pub(crate) const APP_USER: u32 = 3;
    pub(crate) const COMMENT: u32 = 4;
    pub(crate) const PROJECT: u32 = 5;
    pub(crate) const TICKET_LABEL: u32 = 6;
    pub(crate) const ORGANIZATION: u32 = 7;
    pub(crate) const PROJECT_MEMBER: u32 = 8;
}

/// Global index ids.
pub(crate) mod index {
    pub(crate) const TICKET_BY_PROJECT_STATUS: u32 = 1;
    pub(crate) const TICKET_BY_ASSIGNEE_STATUS: u32 = 2;
    pub(crate) const COMMENT_BY_TICKET: u32 = 4;
    pub(crate) const TICKET_LABEL_BY_TICKET: u32 = 7;
    pub(crate) const PROJECT_MEMBER_BY_PROJECT: u32 = 9;
}

/// Ticket entity field ids.
pub(crate) mod ticket_field {
    pub(crate) const TITLE: u32 = 1;
    pub(crate) const STATUS: u32 = 2;
    pub(crate) const PROJECT_ID: u32 = 5;
    pub(crate) const ASSIGNEE_ID: u32 = 7;
    pub(crate) const REPORTER_ID: u32 = 8;
}

/// AppUser field ids.
pub(crate) mod user_field {
    pub(crate) const EMAIL: u32 = 1;
    pub(crate) const DISPLAY_NAME: u32 = 4;
}

/// Project field ids.
pub(crate) mod project_field {
    pub(crate) const NAME: u32 = 1;
}

/// Organization field ids.
pub(crate) mod org_field {
    pub(crate) const NAME: u32 = 1;
}

/// Comment field ids.
pub(crate) mod comment_field {
    pub(crate) const BODY: u32 = 1;
    pub(crate) const AUTHOR_ID: u32 = 2;
}

/// Label field ids.
pub(crate) mod label_field {
    pub(crate) const NAME: u32 = 1;
}

/// ProjectMember field ids.
pub(crate) mod member_field {
    pub(crate) const ROLE: u32 = 1;
}

/// TicketLabel field ids.
pub(crate) mod ticket_label_field {
    /// created_at is the only non-key field on TicketLabel.
    pub(crate) const CREATED_AT: u32 = 3;
}
