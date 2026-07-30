//! Live PostgreSQL TicketDesk adapter for the app baseline.

#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt;
use std::time::Duration;

use postgres::{Client, Config, NoTls, Row};
use riffdb_app_baseline_core::{
    AppBackend, CloseTicketWithCommentSeed, CommentRow, CommentSeed, LabelRow, OpenTicketWithLabelsSeed,
    OrganizationRow, ProjectMemberRow, ProjectRow, SeedDataset, SwapMemberRolesSeed,
    TicketDetailPage, TicketRow, TicketStatus, UserRow, UuidBytes, format_uuid,
};

/// Digest-pinned image used by the baseline runner.
pub const POSTGRES_IMAGE: &str =
    "postgres:18.4-bookworm@sha256:d9c83446333daec3f0588cc709adb80c26090b7f9f0f7ec8d43c243385d79818";

const SCHEMA_SQL: &str = r#"
DROP TABLE IF EXISTS ticket_label CASCADE;
DROP TABLE IF EXISTS comment CASCADE;
DROP TABLE IF EXISTS ticket CASCADE;
DROP TABLE IF EXISTS project_member CASCADE;
DROP TABLE IF EXISTS label CASCADE;
DROP TABLE IF EXISTS project CASCADE;
DROP TABLE IF EXISTS app_user CASCADE;
DROP TABLE IF EXISTS organization CASCADE;

CREATE TABLE organization (
    organization_id UUID PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE app_user (
    organization_id UUID NOT NULL REFERENCES organization(organization_id),
    user_id UUID NOT NULL,
    email TEXT NOT NULL,
    display_name TEXT NOT NULL,
    PRIMARY KEY (organization_id, user_id)
);

CREATE TABLE project (
    organization_id UUID NOT NULL REFERENCES organization(organization_id),
    project_id UUID NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (organization_id, project_id)
);

CREATE TABLE project_member (
    organization_id UUID NOT NULL,
    project_id UUID NOT NULL,
    user_id UUID NOT NULL,
    role TEXT NOT NULL,
    PRIMARY KEY (organization_id, project_id, user_id),
    FOREIGN KEY (organization_id, project_id) REFERENCES project(organization_id, project_id),
    FOREIGN KEY (organization_id, user_id) REFERENCES app_user(organization_id, user_id)
);

CREATE TABLE ticket (
    organization_id UUID NOT NULL,
    ticket_id UUID NOT NULL,
    project_id UUID NOT NULL,
    reporter_id UUID NOT NULL,
    assignee_id UUID NOT NULL,
    status TEXT NOT NULL,
    title TEXT NOT NULL,
    PRIMARY KEY (organization_id, ticket_id),
    FOREIGN KEY (organization_id, project_id) REFERENCES project(organization_id, project_id)
);

CREATE INDEX ticket_by_project_status
    ON ticket (organization_id, project_id, status, ticket_id);
CREATE INDEX ticket_by_assignee_status
    ON ticket (organization_id, assignee_id, status, ticket_id);

CREATE TABLE comment (
    organization_id UUID NOT NULL,
    comment_id UUID NOT NULL,
    ticket_id UUID NOT NULL,
    author_id UUID NOT NULL,
    body TEXT NOT NULL,
    PRIMARY KEY (organization_id, comment_id),
    FOREIGN KEY (organization_id, ticket_id) REFERENCES ticket(organization_id, ticket_id)
);

CREATE INDEX comment_by_ticket
    ON comment (organization_id, ticket_id, comment_id);

CREATE TABLE label (
    organization_id UUID NOT NULL,
    label_id UUID NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (organization_id, label_id),
    FOREIGN KEY (organization_id) REFERENCES organization(organization_id)
);

CREATE TABLE ticket_label (
    organization_id UUID NOT NULL,
    ticket_id UUID NOT NULL,
    label_id UUID NOT NULL,
    PRIMARY KEY (organization_id, ticket_id, label_id),
    FOREIGN KEY (organization_id, ticket_id) REFERENCES ticket(organization_id, ticket_id),
    FOREIGN KEY (organization_id, label_id) REFERENCES label(organization_id, label_id)
);
"#;

/// PostgreSQL comparison adapter.
pub struct PostgresAppBackend {
    config: Config,
    client: Option<Client>,
}

impl PostgresAppBackend {
    /// Creates an adapter from a database URL.
    pub fn new(database_url: impl Into<String>) -> Result<Self, PostgresError> {
        let database_url = database_url.into();
        if database_url.is_empty() || database_url.len() > 4_096 {
            return Err(PostgresError::InvalidConfiguration);
        }
        let mut config = database_url
            .parse::<Config>()
            .map_err(|_| PostgresError::InvalidConfiguration)?;
        config.connect_timeout(Duration::from_secs(5));
        Ok(Self {
            config,
            client: None,
        })
    }

    /// Returns the persistent connection, opening it on first use.
    ///
    /// One warm connection for the whole benchmark run mirrors how the
    /// RiffDB side reuses one HTTP/2 channel; connection setup must not be
    /// paid inside timed scenario samples.
    fn client(&mut self) -> Result<&mut Client, PostgresError> {
        if self.client.is_none() {
            let client = self.config.connect(NoTls).map_err(db_err)?;
            self.client = Some(client);
        }
        self.client
            .as_mut()
            .ok_or(PostgresError::InvalidConfiguration)
    }
}

impl AppBackend for PostgresAppBackend {
    type Error = PostgresError;

    fn reset(&mut self) -> Result<(), Self::Error> {
        let client = self.client()?;
        client
            .batch_execute(SCHEMA_SQL)
            .map_err(db_err)
    }

    fn seed(&mut self, dataset: &SeedDataset) -> Result<(), Self::Error> {
        let client = self.client()?;
        let mut tx = client
            .transaction()
            .map_err(db_err)?;
        for org in &dataset.organizations {
            tx.execute(
                "INSERT INTO organization(organization_id, name) VALUES ($1::text::uuid, $2)",
                &[&format_uuid(org.organization_id), &org.name],
            )
            .map_err(db_err)?;
        }
        for user in &dataset.users {
            tx.execute(
                "INSERT INTO app_user(organization_id, user_id, email, display_name)
                 VALUES ($1::text::uuid, $2::text::uuid, $3, $4)",
                &[
                    &format_uuid(user.organization_id),
                    &format_uuid(user.user_id),
                    &user.email,
                    &user.display_name,
                ],
            )
            .map_err(db_err)?;
        }
        for project in &dataset.projects {
            tx.execute(
                "INSERT INTO project(organization_id, project_id, name)
                 VALUES ($1::text::uuid, $2::text::uuid, $3)",
                &[
                    &format_uuid(project.organization_id),
                    &format_uuid(project.project_id),
                    &project.name,
                ],
            )
            .map_err(db_err)?;
        }
        for member in &dataset.members {
            tx.execute(
                "INSERT INTO project_member(organization_id, project_id, user_id, role)
                 VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4)",
                &[
                    &format_uuid(member.organization_id),
                    &format_uuid(member.project_id),
                    &format_uuid(member.user_id),
                    &member.role,
                ],
            )
            .map_err(db_err)?;
        }
        for label in &dataset.labels {
            tx.execute(
                "INSERT INTO label(organization_id, label_id, name)
                 VALUES ($1::text::uuid, $2::text::uuid, $3)",
                &[
                    &format_uuid(label.organization_id),
                    &format_uuid(label.label_id),
                    &label.name,
                ],
            )
            .map_err(db_err)?;
        }
        for ticket in &dataset.tickets {
            tx.execute(
                "INSERT INTO ticket(
                    organization_id, ticket_id, project_id, reporter_id, assignee_id, status, title
                 ) VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5::text::uuid, $6, $7)",
                &[
                    &format_uuid(ticket.organization_id),
                    &format_uuid(ticket.ticket_id),
                    &format_uuid(ticket.project_id),
                    &format_uuid(ticket.reporter_id),
                    &format_uuid(ticket.assignee_id),
                    &ticket.status.as_str().to_owned(),
                    &ticket.title,
                ],
            )
            .map_err(db_err)?;
        }
        for comment in &dataset.comments {
            tx.execute(
                "INSERT INTO comment(organization_id, comment_id, ticket_id, author_id, body)
                 VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5)",
                &[
                    &format_uuid(comment.organization_id),
                    &format_uuid(comment.comment_id),
                    &format_uuid(comment.ticket_id),
                    &format_uuid(comment.author_id),
                    &comment.body,
                ],
            )
            .map_err(db_err)?;
        }
        for link in &dataset.ticket_labels {
            tx.execute(
                "INSERT INTO ticket_label(organization_id, ticket_id, label_id)
                 VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid)",
                &[
                    &format_uuid(link.organization_id),
                    &format_uuid(link.ticket_id),
                    &format_uuid(link.label_id),
                ],
            )
            .map_err(db_err)?;
        }
        tx.commit().map_err(db_err)
    }

    fn point_get_ticket(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
    ) -> Result<Option<TicketRow>, Self::Error> {
        let client = self.client()?;
        let row = client
            .query_opt(
                "SELECT organization_id::text, ticket_id::text, project_id::text,
                        reporter_id::text, assignee_id::text, status, title
                 FROM ticket
                 WHERE organization_id = $1::text::uuid AND ticket_id = $2::text::uuid",
                &[&format_uuid(organization_id), &format_uuid(ticket_id)],
            )
            .map_err(db_err)?;
        row.map(|row| decode_ticket(&row)).transpose()
    }

    fn point_get_user(
        &mut self,
        organization_id: UuidBytes,
        user_id: UuidBytes,
    ) -> Result<Option<UserRow>, Self::Error> {
        let client = self.client()?;
        let row = client
            .query_opt(
                "SELECT organization_id::text, user_id::text, email, display_name
                 FROM app_user
                 WHERE organization_id = $1::text::uuid AND user_id = $2::text::uuid",
                &[&format_uuid(organization_id), &format_uuid(user_id)],
            )
            .map_err(db_err)?;
        row.map(|row| decode_user(&row)).transpose()
    }

    fn list_tickets_by_project_status(
        &mut self,
        organization_id: UuidBytes,
        project_id: UuidBytes,
        status: TicketStatus,
        limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error> {
        let client = self.client()?;
        let rows = client
            .query(
                "SELECT organization_id::text, ticket_id::text, project_id::text,
                        reporter_id::text, assignee_id::text, status, title
                 FROM ticket
                 WHERE organization_id = $1::text::uuid
                   AND project_id = $2::text::uuid
                   AND status = $3
                 ORDER BY ticket_id
                 LIMIT $4",
                &[
                    &format_uuid(organization_id),
                    &format_uuid(project_id),
                    &status.as_str().to_owned(),
                    &(i64::from(limit)),
                ],
            )
            .map_err(db_err)?;
        rows.iter().map(decode_ticket).collect()
    }

    fn list_open_tickets_for_assignee(
        &mut self,
        organization_id: UuidBytes,
        assignee_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error> {
        let client = self.client()?;
        let rows = client
            .query(
                "SELECT organization_id::text, ticket_id::text, project_id::text,
                        reporter_id::text, assignee_id::text, status, title
                 FROM ticket
                 WHERE organization_id = $1::text::uuid
                   AND assignee_id = $2::text::uuid
                   AND status = 'open'
                 ORDER BY ticket_id
                 LIMIT $3",
                &[
                    &format_uuid(organization_id),
                    &format_uuid(assignee_id),
                    &(i64::from(limit)),
                ],
            )
            .map_err(db_err)?;
        rows.iter().map(decode_ticket).collect()
    }

    fn list_comments_for_ticket(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<CommentRow>, Self::Error> {
        let client = self.client()?;
        let rows = client
            .query(
                "SELECT organization_id::text, comment_id::text, ticket_id::text,
                        author_id::text, body
                 FROM comment
                 WHERE organization_id = $1::text::uuid AND ticket_id = $2::text::uuid
                 ORDER BY comment_id
                 LIMIT $3",
                &[
                    &format_uuid(organization_id),
                    &format_uuid(ticket_id),
                    &(i64::from(limit)),
                ],
            )
            .map_err(db_err)?;
        rows.iter().map(decode_comment).collect()
    }

    fn list_project_members(
        &mut self,
        organization_id: UuidBytes,
        project_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<ProjectMemberRow>, Self::Error> {
        let client = self.client()?;
        let rows = client
            .query(
                "SELECT organization_id::text, project_id::text, user_id::text, role
                 FROM project_member
                 WHERE organization_id = $1::text::uuid AND project_id = $2::text::uuid
                 ORDER BY user_id
                 LIMIT $3",
                &[
                    &format_uuid(organization_id),
                    &format_uuid(project_id),
                    &(i64::from(limit)),
                ],
            )
            .map_err(db_err)?;
        rows.iter().map(decode_member).collect()
    }

    fn ticket_detail_page(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
        comment_limit: u32,
    ) -> Result<Option<TicketDetailPage>, Self::Error> {
        let client = self.client()?;
        let Some(ticket_row) = client
            .query_opt(
                "SELECT t.organization_id::text, t.ticket_id::text, t.project_id::text,
                        t.reporter_id::text, t.assignee_id::text, t.status, t.title,
                        p.name AS project_name,
                        o.name AS organization_name,
                        u.email AS assignee_email,
                        u.display_name AS assignee_display_name
                 FROM ticket t
                 JOIN project p
                   ON p.organization_id = t.organization_id AND p.project_id = t.project_id
                 JOIN organization o
                   ON o.organization_id = t.organization_id
                 LEFT JOIN app_user u
                   ON u.organization_id = t.organization_id AND u.user_id = t.assignee_id
                 WHERE t.organization_id = $1::text::uuid AND t.ticket_id = $2::text::uuid",
                &[&format_uuid(organization_id), &format_uuid(ticket_id)],
            )
            .map_err(db_err)?
        else {
            return Ok(None);
        };

        let ticket = TicketRow {
            organization_id: parse_uuid(ticket_row.get(0))?,
            ticket_id: parse_uuid(ticket_row.get(1))?,
            project_id: parse_uuid(ticket_row.get(2))?,
            reporter_id: parse_uuid(ticket_row.get(3))?,
            assignee_id: parse_uuid(ticket_row.get(4))?,
            status: TicketStatus::parse(ticket_row.get::<_, &str>(5))
                .ok_or(PostgresError::Decode)?,
            title: ticket_row.get(6),
        };
        let project = ProjectRow {
            organization_id: ticket.organization_id,
            project_id: ticket.project_id,
            name: ticket_row.get(7),
        };
        let organization = OrganizationRow {
            organization_id: ticket.organization_id,
            name: ticket_row.get(8),
        };
        let assignee = match (
            ticket_row.get::<_, Option<&str>>(9),
            ticket_row.get::<_, Option<&str>>(10),
        ) {
            (Some(email), Some(display_name)) => Some(UserRow {
                organization_id: ticket.organization_id,
                user_id: ticket.assignee_id,
                email: email.to_owned(),
                display_name: display_name.to_owned(),
            }),
            _ => None,
        };

        let comment_rows = client
            .query(
                "SELECT organization_id::text, comment_id::text, ticket_id::text,
                        author_id::text, body
                 FROM comment
                 WHERE organization_id = $1::text::uuid AND ticket_id = $2::text::uuid
                 ORDER BY comment_id
                 LIMIT $3",
                &[
                    &format_uuid(organization_id),
                    &format_uuid(ticket_id),
                    &(i64::from(comment_limit)),
                ],
            )
            .map_err(db_err)?;
        let comments = comment_rows.iter().map(decode_comment).collect::<Result<Vec<_>, _>>()?;

        let label_rows = client
            .query(
                "SELECT l.organization_id::text, l.label_id::text, l.name
                 FROM ticket_label tl
                 JOIN label l
                   ON l.organization_id = tl.organization_id AND l.label_id = tl.label_id
                 WHERE tl.organization_id = $1::text::uuid AND tl.ticket_id = $2::text::uuid
                 ORDER BY l.label_id",
                &[&format_uuid(organization_id), &format_uuid(ticket_id)],
            )
            .map_err(db_err)?;
        let labels = label_rows
            .iter()
            .map(|row| {
                Ok(LabelRow {
                    organization_id: parse_uuid(row.get(0))?,
                    label_id: parse_uuid(row.get(1))?,
                    name: row.get(2),
                })
            })
            .collect::<Result<Vec<_>, PostgresError>>()?;

        Ok(Some(TicketDetailPage {
            ticket,
            project,
            organization,
            assignee,
            comments,
            labels,
        }))
    }

    fn create_comment(&mut self, comment: &CommentSeed) -> Result<(), Self::Error> {
        let client = self.client()?;
        // Each sample inserts a distinct comment; a conflict is a harness bug.
        client
            .execute(
                "INSERT INTO comment(organization_id, comment_id, ticket_id, author_id, body)
                 VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5)",
                &[
                    &format_uuid(comment.row.organization_id),
                    &format_uuid(comment.row.comment_id),
                    &format_uuid(comment.row.ticket_id),
                    &format_uuid(comment.row.author_id),
                    &comment.row.body,
                ],
            )
            .map_err(db_err)?;
        Ok(())
    }

    fn close_ticket_with_comment(
        &mut self,
        input: &CloseTicketWithCommentSeed,
    ) -> Result<(), Self::Error> {
        let client = self.client()?;
        let mut tx = client.transaction().map_err(db_err)?;
        // Existence checks mirror RiffDB relationship validation before mutate/create.
        let ticket_ok: i64 = tx
            .query_one(
                "SELECT COUNT(*)::bigint FROM ticket
                 WHERE organization_id = $1::text::uuid AND ticket_id = $2::text::uuid",
                &[
                    &format_uuid(input.organization_id),
                    &format_uuid(input.ticket_id),
                ],
            )
            .map_err(db_err)?
            .get(0);
        if ticket_ok == 0 {
            return Err(PostgresError::Decode);
        }
        let author_ok: i64 = tx
            .query_one(
                "SELECT COUNT(*)::bigint FROM app_user
                 WHERE organization_id = $1::text::uuid AND user_id = $2::text::uuid",
                &[
                    &format_uuid(input.organization_id),
                    &format_uuid(input.author_id),
                ],
            )
            .map_err(db_err)?
            .get(0);
        if author_ok == 0 {
            return Err(PostgresError::Decode);
        }
        tx.execute(
            "UPDATE ticket SET status = 'closed'
             WHERE organization_id = $1::text::uuid AND ticket_id = $2::text::uuid",
            &[
                &format_uuid(input.organization_id),
                &format_uuid(input.ticket_id),
            ],
        )
        .map_err(db_err)?;
        tx.execute(
            "INSERT INTO comment(organization_id, comment_id, ticket_id, author_id, body)
             VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5)",
            &[
                &format_uuid(input.organization_id),
                &format_uuid(input.comment_id),
                &format_uuid(input.ticket_id),
                &format_uuid(input.author_id),
                &input.body,
            ],
        )
        .map_err(db_err)?;
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    fn swap_member_roles(&mut self, input: &SwapMemberRolesSeed) -> Result<(), Self::Error> {
        let client = self.client()?;
        let mut tx = client.transaction().map_err(db_err)?;
        let updated_a = tx
            .execute(
                "UPDATE project_member SET role = $4
                 WHERE organization_id = $1::text::uuid
                   AND project_id = $2::text::uuid
                   AND user_id = $3::text::uuid",
                &[
                    &format_uuid(input.organization_id),
                    &format_uuid(input.project_id),
                    &format_uuid(input.user_a),
                    &input.role_a,
                ],
            )
            .map_err(db_err)?;
        let updated_b = tx
            .execute(
                "UPDATE project_member SET role = $4
                 WHERE organization_id = $1::text::uuid
                   AND project_id = $2::text::uuid
                   AND user_id = $3::text::uuid",
                &[
                    &format_uuid(input.organization_id),
                    &format_uuid(input.project_id),
                    &format_uuid(input.user_b),
                    &input.role_b,
                ],
            )
            .map_err(db_err)?;
        if updated_a == 0 || updated_b == 0 {
            return Err(PostgresError::Decode);
        }
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    fn open_ticket_with_labels(
        &mut self,
        input: &OpenTicketWithLabelsSeed,
    ) -> Result<(), Self::Error> {
        let client = self.client()?;
        let mut tx = client.transaction().map_err(db_err)?;
        tx.execute(
            "INSERT INTO ticket(
                 organization_id, ticket_id, project_id, reporter_id, assignee_id, status, title
             ) VALUES (
                 $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5::text::uuid,
                 'open', $6
             )",
            &[
                &format_uuid(input.organization_id),
                &format_uuid(input.ticket_id),
                &format_uuid(input.project_id),
                &format_uuid(input.reporter_id),
                &format_uuid(input.assignee_id),
                &input.title,
            ],
        )
        .map_err(db_err)?;
        tx.execute(
            "INSERT INTO ticket_label(organization_id, ticket_id, label_id)
             VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid)",
            &[
                &format_uuid(input.organization_id),
                &format_uuid(input.ticket_id),
                &format_uuid(input.label_a),
            ],
        )
        .map_err(db_err)?;
        tx.execute(
            "INSERT INTO ticket_label(organization_id, ticket_id, label_id)
             VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid)",
            &[
                &format_uuid(input.organization_id),
                &format_uuid(input.ticket_id),
                &format_uuid(input.label_b),
            ],
        )
        .map_err(db_err)?;
        tx.commit().map_err(db_err)?;
        Ok(())
    }
}

fn decode_ticket(row: &Row) -> Result<TicketRow, PostgresError> {
    Ok(TicketRow {
        organization_id: parse_uuid(row.get(0))?,
        ticket_id: parse_uuid(row.get(1))?,
        project_id: parse_uuid(row.get(2))?,
        reporter_id: parse_uuid(row.get(3))?,
        assignee_id: parse_uuid(row.get(4))?,
        status: TicketStatus::parse(row.get::<_, &str>(5)).ok_or(PostgresError::Decode)?,
        title: row.get(6),
    })
}

fn decode_user(row: &Row) -> Result<UserRow, PostgresError> {
    Ok(UserRow {
        organization_id: parse_uuid(row.get(0))?,
        user_id: parse_uuid(row.get(1))?,
        email: row.get(2),
        display_name: row.get(3),
    })
}

fn decode_comment(row: &Row) -> Result<CommentRow, PostgresError> {
    Ok(CommentRow {
        organization_id: parse_uuid(row.get(0))?,
        comment_id: parse_uuid(row.get(1))?,
        ticket_id: parse_uuid(row.get(2))?,
        author_id: parse_uuid(row.get(3))?,
        body: row.get(4),
    })
}

fn decode_member(row: &Row) -> Result<ProjectMemberRow, PostgresError> {
    Ok(ProjectMemberRow {
        organization_id: parse_uuid(row.get(0))?,
        project_id: parse_uuid(row.get(1))?,
        user_id: parse_uuid(row.get(2))?,
        role: row.get(3),
    })
}

fn parse_uuid(text: String) -> Result<UuidBytes, PostgresError> {
    parse_uuid_str(&text)
}

fn parse_uuid_str(text: &str) -> Result<UuidBytes, PostgresError> {
    let compact: String = text.chars().filter(|ch| *ch != '-').collect();
    if compact.len() != 32 {
        return Err(PostgresError::Decode);
    }
    let mut bytes = [0_u8; 16];
    for (index, chunk) in compact.as_bytes().chunks(2).enumerate() {
        let hex = std::str::from_utf8(chunk).map_err(|_| PostgresError::Decode)?;
        bytes[index] = u8::from_str_radix(hex, 16).map_err(|_| PostgresError::Decode)?;
    }
    Ok(bytes)
}

/// PostgreSQL adapter errors.
#[derive(Clone, Debug)]
pub enum PostgresError {
    /// URL/config invalid.
    InvalidConfiguration,
    /// Database operation failed.
    Database(String),
    /// Row decode failed.
    Decode,
}

impl fmt::Display for PostgresError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => formatter.write_str("invalid PostgreSQL configuration"),
            Self::Database(detail) => write!(formatter, "PostgreSQL database error: {detail}"),
            Self::Decode => formatter.write_str("PostgreSQL row decode error"),
        }
    }
}

impl Error for PostgresError {}

fn db_err(error: impl fmt::Display) -> PostgresError {
    PostgresError::Database(error.to_string())
}
