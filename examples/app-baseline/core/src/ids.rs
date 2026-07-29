//! Stable workload identifiers and row shapes.

/// Network-order UUID bytes.
pub type UuidBytes = [u8; 16];

/// Ticket lifecycle status shared by both backends.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum TicketStatus {
    /// Newly opened work item.
    Open = 1,
    /// Ticket is closed.
    Closed = 2,
    /// Actively being worked.
    InProgress = 3,
}

impl TicketStatus {
    /// Stable wire / SQL text spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closed => "closed",
            Self::InProgress => "in_progress",
        }
    }

    /// RiffDB enum variant id from the compiled TicketDesk contract.
    #[must_use]
    pub const fn riffdb_variant_id(self) -> u32 {
        self as u32
    }

    /// Parses the SQL spelling.
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "open" => Some(Self::Open),
            "closed" => Some(Self::Closed),
            "in_progress" => Some(Self::InProgress),
            _ => None,
        }
    }
}

/// Organization row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrganizationRow {
    /// Organization id.
    pub organization_id: UuidBytes,
    /// Display name.
    pub name: String,
}

/// Application user row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserRow {
    /// Organization partition.
    pub organization_id: UuidBytes,
    /// User id.
    pub user_id: UuidBytes,
    /// Email.
    pub email: String,
    /// Display name.
    pub display_name: String,
}

/// Project row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectRow {
    /// Organization partition.
    pub organization_id: UuidBytes,
    /// Project id.
    pub project_id: UuidBytes,
    /// Name.
    pub name: String,
}

/// Project membership row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectMemberRow {
    /// Organization partition.
    pub organization_id: UuidBytes,
    /// Project id.
    pub project_id: UuidBytes,
    /// User id.
    pub user_id: UuidBytes,
    /// Role label.
    pub role: String,
}

/// Ticket row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TicketRow {
    /// Organization partition.
    pub organization_id: UuidBytes,
    /// Ticket id.
    pub ticket_id: UuidBytes,
    /// Owning project.
    pub project_id: UuidBytes,
    /// Reporter.
    pub reporter_id: UuidBytes,
    /// Assignee.
    pub assignee_id: UuidBytes,
    /// Status.
    pub status: TicketStatus,
    /// Title.
    pub title: String,
}

/// Comment row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommentRow {
    /// Organization partition.
    pub organization_id: UuidBytes,
    /// Comment id.
    pub comment_id: UuidBytes,
    /// Parent ticket.
    pub ticket_id: UuidBytes,
    /// Author.
    pub author_id: UuidBytes,
    /// Body text.
    pub body: String,
}

/// Label row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelRow {
    /// Organization partition.
    pub organization_id: UuidBytes,
    /// Label id.
    pub label_id: UuidBytes,
    /// Name.
    pub name: String,
}

/// Ticket↔label link.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TicketLabelRow {
    /// Organization partition.
    pub organization_id: UuidBytes,
    /// Ticket id.
    pub ticket_id: UuidBytes,
    /// Label id.
    pub label_id: UuidBytes,
}

/// Application detail page aggregate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TicketDetailPage {
    /// Ticket.
    pub ticket: TicketRow,
    /// Project.
    pub project: ProjectRow,
    /// Organization.
    pub organization: OrganizationRow,
    /// Assignee user when present.
    pub assignee: Option<UserRow>,
    /// Bounded comments.
    pub comments: Vec<CommentRow>,
    /// Attached labels.
    pub labels: Vec<LabelRow>,
}

/// Deterministic UUIDv7-shaped bytes from a namespace and ordinal.
#[must_use]
pub fn uuid_from_ordinal(namespace: u8, ordinal: u64) -> UuidBytes {
    let mut bytes = [namespace; 16];
    bytes[6] = 0x70 | (namespace & 0x0f);
    bytes[8..].copy_from_slice(&ordinal.to_be_bytes());
    bytes[8] = 0x80 | (bytes[8] & 0x3f);
    bytes
}

/// Formats UUID bytes as lowercase hyphenated text.
#[must_use]
pub fn format_uuid(bytes: UuidBytes) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}
