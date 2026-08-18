//! Live PostgreSQL TicketDesk adapter for the app baseline.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::time::{Duration, Instant};

use postgres::error::SqlState;
use postgres::{Client, Config, NoTls, Row, Statement, Transaction};
use riffdb_app_baseline_core::{
    AppBackend, BOARD_PAGE_SQL, CloseTicketWithCommentSeed, CommentRow, CommentSeed, LabelRow,
    OpenTicketWithLabelsSeed, OrganizationRow, ProjectMemberRow, ProjectRow, SeedDataset,
    SwapMemberRolesSeed, TicketDetailPage, TicketRow, TicketStatus, UserRow, UuidBytes,
    format_uuid,
};
use sha2::{Digest, Sha256};

/// Digest-pinned image used by the baseline runner.
pub const POSTGRES_IMAGE: &str = "postgres:18.4-bookworm@sha256:d9c83446333daec3f0588cc709adb80c26090b7f9f0f7ec8d43c243385d79818";

/// Low-cardinality server resource counters sampled outside timed operations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresResourceSnapshot {
    /// Committed transactions observed for the benchmark database.
    pub committed_transactions: u64,
    /// Physical blocks read for the benchmark database.
    pub blocks_read: u64,
    /// Buffer hits for the benchmark database.
    pub blocks_hit: u64,
    /// Temporary bytes written for the benchmark database.
    pub temporary_bytes: u64,
    /// Current logical database size.
    pub database_bytes: u64,
    /// Cluster WAL bytes since statistics reset.
    pub wal_bytes: u64,
}

const SCHEMA_SQL: &str = r#"
DROP TABLE IF EXISTS app_outbox_intent CASCADE;
DROP TABLE IF EXISTS app_domain_event CASCADE;
DROP TABLE IF EXISTS app_audit CASCADE;
DROP TABLE IF EXISTS app_idempotency CASCADE;
DROP TABLE IF EXISTS app_permission CASCADE;
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

CREATE TABLE app_permission (
    principal TEXT NOT NULL,
    operation TEXT NOT NULL,
    PRIMARY KEY (principal, operation)
);

CREATE TABLE app_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    organization_id UUID NOT NULL,
    input_fingerprint TEXT NOT NULL,
    outcome TEXT NOT NULL
);

CREATE TABLE app_audit (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    operation TEXT NOT NULL,
    organization_id UUID NOT NULL,
    result TEXT NOT NULL
);

CREATE TABLE app_domain_event (
    event_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    organization_id UUID NOT NULL
);

CREATE TABLE app_outbox_intent (
    event_id TEXT PRIMARY KEY REFERENCES app_domain_event(event_id),
    delivery_state TEXT NOT NULL CHECK (delivery_state = 'pending')
);

INSERT INTO app_permission(principal, operation) VALUES
    ('app-baseline', 'point_get_ticket'),
    ('app-baseline', 'point_get_user'),
    ('app-baseline', 'list_tickets_by_project_status'),
    ('app-baseline', 'list_open_tickets_for_assignee'),
    ('app-baseline', 'list_comments_for_ticket'),
    ('app-baseline', 'list_project_members'),
    ('app-baseline', 'ticket_detail_page'),
    ('app-baseline', 'board_page'),
    ('app-baseline', 'create_comment'),
    ('app-baseline', 'close_ticket_with_comment'),
    ('app-baseline', 'swap_member_roles'),
    ('app-baseline', 'open_ticket_with_labels');
"#;

// Every statement executed against the live server is a named constant so it
// can be prepared exactly once and reused from the statement cache. Statement
// preparation (Parse/Describe/Sync plus the server-side parse and plan) must
// never be paid inside a timed scenario sample.

const INSERT_ORGANIZATION_SQL: &str =
    "INSERT INTO organization(organization_id, name) VALUES ($1::text::uuid, $2)";

const INSERT_USER_SQL: &str = "INSERT INTO app_user(organization_id, user_id, email, display_name)
     VALUES ($1::text::uuid, $2::text::uuid, $3, $4)";

const INSERT_PROJECT_SQL: &str = "INSERT INTO project(organization_id, project_id, name)
     VALUES ($1::text::uuid, $2::text::uuid, $3)";

const INSERT_MEMBER_SQL: &str =
    "INSERT INTO project_member(organization_id, project_id, user_id, role)
     VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4)";

const INSERT_LABEL_SQL: &str = "INSERT INTO label(organization_id, label_id, name)
     VALUES ($1::text::uuid, $2::text::uuid, $3)";

const INSERT_TICKET_SQL: &str = "INSERT INTO ticket(
         organization_id, ticket_id, project_id, reporter_id, assignee_id, status, title
     ) VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5::text::uuid, $6, $7)";

const INSERT_COMMENT_SQL: &str =
    "INSERT INTO comment(organization_id, comment_id, ticket_id, author_id, body)
     VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5)";

/// Idempotent comment insert matching RiffDB same-key replay (success, no second row).
const INSERT_COMMENT_IDEMPOTENT_SQL: &str =
    "INSERT INTO comment(organization_id, comment_id, ticket_id, author_id, body)
     VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5)
     ON CONFLICT (organization_id, comment_id) DO NOTHING";

const INSERT_TICKET_LABEL_SQL: &str =
    "INSERT INTO ticket_label(organization_id, ticket_id, label_id)
     VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid)";

const SELECT_TICKET_SQL: &str = "SELECT organization_id::text, ticket_id::text, project_id::text,
            reporter_id::text, assignee_id::text, status, title
     FROM ticket
     WHERE organization_id = $1::text::uuid AND ticket_id = $2::text::uuid";

const SELECT_USER_SQL: &str = "SELECT organization_id::text, user_id::text, email, display_name
     FROM app_user
     WHERE organization_id = $1::text::uuid AND user_id = $2::text::uuid";

const LIST_TICKETS_BY_PROJECT_STATUS_SQL: &str =
    "SELECT organization_id::text, ticket_id::text, project_id::text,
            reporter_id::text, assignee_id::text, status, title
     FROM ticket
     WHERE organization_id = $1::text::uuid
       AND project_id = $2::text::uuid
       AND status = $3
     ORDER BY ticket_id
     LIMIT $4";

const LIST_OPEN_TICKETS_FOR_ASSIGNEE_SQL: &str =
    "SELECT organization_id::text, ticket_id::text, project_id::text,
            reporter_id::text, assignee_id::text, status, title
     FROM ticket
     WHERE organization_id = $1::text::uuid
       AND assignee_id = $2::text::uuid
       AND status = 'open'
     ORDER BY ticket_id
     LIMIT $3";

const LIST_COMMENTS_SQL: &str = "SELECT organization_id::text, comment_id::text, ticket_id::text,
            author_id::text, body
     FROM comment
     WHERE organization_id = $1::text::uuid AND ticket_id = $2::text::uuid
     ORDER BY comment_id
     LIMIT $3";

const LIST_PROJECT_MEMBERS_SQL: &str =
    "SELECT organization_id::text, project_id::text, user_id::text, role
     FROM project_member
     WHERE organization_id = $1::text::uuid AND project_id = $2::text::uuid
     ORDER BY user_id
     LIMIT $3";

const TICKET_DETAIL_SQL: &str =
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
     WHERE t.organization_id = $1::text::uuid AND t.ticket_id = $2::text::uuid";

const TICKET_DETAIL_LABELS_SQL: &str = "SELECT l.organization_id::text, l.label_id::text, l.name
     FROM ticket_label tl
     JOIN label l
       ON l.organization_id = tl.organization_id AND l.label_id = tl.label_id
     WHERE tl.organization_id = $1::text::uuid AND tl.ticket_id = $2::text::uuid
     ORDER BY l.label_id";

const COUNT_TICKET_SQL: &str = "SELECT COUNT(*)::bigint FROM ticket
     WHERE organization_id = $1::text::uuid AND ticket_id = $2::text::uuid";

const COUNT_USER_SQL: &str = "SELECT COUNT(*)::bigint FROM app_user
     WHERE organization_id = $1::text::uuid AND user_id = $2::text::uuid";

const CLOSE_TICKET_SQL: &str = "UPDATE ticket SET status = 'closed'
     WHERE organization_id = $1::text::uuid AND ticket_id = $2::text::uuid";

const UPDATE_MEMBER_ROLE_SQL: &str = "UPDATE project_member SET role = $4
     WHERE organization_id = $1::text::uuid
       AND project_id = $2::text::uuid
       AND user_id = $3::text::uuid";

const OPEN_TICKET_SQL: &str = "INSERT INTO ticket(
         organization_id, ticket_id, project_id, reporter_id, assignee_id, status, title
     ) VALUES (
         $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5::text::uuid,
         'open', $6
     )";

fn safe_input_fingerprint(fields: &[&[u8]]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"riffdb-app-baseline-safe-input-v1\0");
    for field in fields {
        digest.update(u64::try_from(field.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(field);
    }
    let bytes = digest.finalize();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn safe_admit(
    tx: &mut Transaction<'_>,
    idempotency_key: &str,
    operation: &'static str,
    organization_id: UuidBytes,
    input_fingerprint: &str,
) -> Result<bool, PostgresError> {
    let authorized = tx
        .query_opt(
            "SELECT 1 FROM app_permission WHERE principal = 'app-baseline' AND operation = $1",
            &[&operation],
        )
        .map_err(db_err)?;
    if authorized.is_none() {
        return Err(PostgresError::Decode);
    }
    if let Some(row) = tx
        .query_opt(
            "SELECT operation, organization_id::text, input_fingerprint
             FROM app_idempotency WHERE idempotency_key = $1 FOR UPDATE",
            &[&idempotency_key],
        )
        .map_err(db_err)?
    {
        let stored_operation: &str = row.get(0);
        let stored_organization: &str = row.get(1);
        let stored_fingerprint: &str = row.get(2);
        if stored_operation != operation
            || stored_organization != format_uuid(organization_id)
            || stored_fingerprint != input_fingerprint
        {
            return Err(PostgresError::Decode);
        }
        tx.execute(
            "INSERT INTO app_audit(idempotency_key, operation, organization_id, result)
             VALUES ($1, $2, $3::text::uuid, 'replayed')",
            &[&idempotency_key, &operation, &format_uuid(organization_id)],
        )
        .map_err(db_err)?;
        return Ok(true);
    }
    tx.execute(
        "INSERT INTO app_idempotency(
             idempotency_key, operation, organization_id, input_fingerprint, outcome
         ) VALUES ($1, $2, $3::text::uuid, $4, 'committed')",
        &[
            &idempotency_key,
            &operation,
            &format_uuid(organization_id),
            &input_fingerprint,
        ],
    )
    .map_err(db_err)?;
    Ok(false)
}

fn safe_complete(
    tx: &mut Transaction<'_>,
    idempotency_key: &str,
    operation: &'static str,
    organization_id: UuidBytes,
    event_type: &'static str,
) -> Result<(), PostgresError> {
    let event_id = format!("{operation}/{idempotency_key}");
    tx.execute(
        "INSERT INTO app_audit(idempotency_key, operation, organization_id, result)
         VALUES ($1, $2, $3::text::uuid, 'committed')",
        &[&idempotency_key, &operation, &format_uuid(organization_id)],
    )
    .map_err(db_err)?;
    tx.execute(
        "INSERT INTO app_domain_event(event_id, idempotency_key, event_type, organization_id)
         VALUES ($1, $2, $3, $4::text::uuid)",
        &[
            &event_id,
            &idempotency_key,
            &event_type,
            &format_uuid(organization_id),
        ],
    )
    .map_err(db_err)?;
    tx.execute(
        "INSERT INTO app_outbox_intent(event_id, delivery_state) VALUES ($1, 'pending')",
        &[&event_id],
    )
    .map_err(db_err)?;
    Ok(())
}

/// PostgreSQL comparison adapter.
pub struct PostgresAppBackend {
    config: Config,
    client: Option<Client>,
    statements: HashMap<&'static str, Statement>,
    profile: PostgresComparisonProfile,
    /// Whether the post-seed `CHECKPOINT` was accepted; a deployment without
    /// the privilege still settles statistics and dead tuples.
    seed_checkpoint_issued: bool,
}

/// Explicit PostgreSQL comparison semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PostgresComparisonProfile {
    /// Optimized conventional schema and application transaction floor.
    Minimal,
    /// Adds application authorization, idempotency, audit/provenance,
    /// domain-event, and outbox-intent obligations atomically.
    SafeApp,
}

impl PostgresComparisonProfile {
    /// Stable report identity.
    #[must_use]
    pub const fn backend_id(self) -> &'static str {
        match self {
            Self::Minimal => "postgres_minimal",
            Self::SafeApp => "postgres_safe_app",
        }
    }

    /// Closed application obligations included by this comparator profile.
    #[must_use]
    pub const fn obligations(self) -> &'static [&'static str] {
        match self {
            Self::Minimal => &["transactional_domain_mutation"],
            Self::SafeApp => &[
                "symbolic_operation_authorization",
                "idempotency_admission_and_equal_input_replay",
                "domain_mutation",
                "audit_and_provenance",
                "domain_event",
                "outbox_intent",
                "one_atomic_transaction",
            ],
        }
    }

    /// Parses a CLI profile name.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "minimal" => Some(Self::Minimal),
            "safe-app" => Some(Self::SafeApp),
            _ => None,
        }
    }
}

impl PostgresAppBackend {
    /// Creates an adapter from a database URL.
    pub fn new(database_url: impl Into<String>) -> Result<Self, PostgresError> {
        Self::new_with_profile(database_url, PostgresComparisonProfile::Minimal)
    }

    /// Creates an adapter under an explicit comparison profile.
    pub fn new_with_profile(
        database_url: impl Into<String>,
        profile: PostgresComparisonProfile,
    ) -> Result<Self, PostgresError> {
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
            statements: HashMap::new(),
            profile,
            seed_checkpoint_issued: false,
        })
    }

    /// Whether the post-seed `CHECKPOINT` was accepted by this deployment.
    ///
    /// `false` means the connected role lacked `pg_checkpoint`, so the seed's
    /// dirty buffers were not forced out before measurement and a checkpoint
    /// may still land inside a measured window.
    #[must_use]
    pub const fn seed_checkpoint_issued(&self) -> bool {
        self.seed_checkpoint_issued
    }

    /// Selected comparison profile.
    #[must_use]
    pub const fn comparison_profile(&self) -> PostgresComparisonProfile {
        self.profile
    }

    fn authorize_read(&mut self, operation: &'static str) -> Result<(), PostgresError> {
        if self.profile == PostgresComparisonProfile::Minimal {
            return Ok(());
        }
        let row = self
            .client()?
            .query_opt(
                "SELECT 1 FROM app_permission WHERE principal = 'app-baseline' AND operation = $1",
                &[&operation],
            )
            .map_err(db_err)?;
        if row.is_none() {
            return Err(PostgresError::Decode);
        }
        Ok(())
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

    /// Settles the freshly seeded database so the measured window observes
    /// steady state rather than bulk-load aftermath.
    ///
    /// The seed drops, recreates, and bulk-loads every table. Without this the
    /// measured window starts with no planner statistics, autovacuum pending on
    /// every seeded relation, and the seed's dirty buffers unwritten, so an
    /// autoanalyze or checkpoint can land inside it. That is the comparator's
    /// dominant variance source: same-run repetitions have differed by 1.9x
    /// while the RiffDB backend, which restarts into a bounded checkpoint
    /// cadence, differed by 1.05x.
    ///
    /// `VACUUM (ANALYZE)` cannot run inside a transaction block, so it is
    /// issued on the plain connection. `CHECKPOINT` then moves the seed's
    /// dirty pages out of the measured window.
    fn settle_after_seed(&mut self) -> Result<(), PostgresError> {
        let client = self.client()?;
        client.batch_execute("VACUUM (ANALYZE)").map_err(db_err)?;
        // CHECKPOINT needs superuser or pg_checkpoint. A deployment that
        // withholds it still gets the statistics and dead-tuple settling
        // above, so an insufficient-privilege refusal is reported by the
        // durability receipt rather than failing the comparator outright.
        match client.batch_execute("CHECKPOINT") {
            Ok(()) => {
                self.seed_checkpoint_issued = true;
                Ok(())
            }
            Err(error) if error.code().map(SqlState::code) == Some("42501") => {
                self.seed_checkpoint_issued = false;
                Ok(())
            }
            Err(error) => Err(db_err(error)),
        }
    }

    /// Returns the cached prepared statement for `sql`, preparing it once.
    ///
    /// Statement preparation must not be paid inside timed scenario samples;
    /// `Statement` is a cheap connection-tied handle, so cloning it out of
    /// the cache is free.
    fn statement(&mut self, sql: &'static str) -> Result<Statement, PostgresError> {
        if let Some(statement) = self.statements.get(sql) {
            return Ok(statement.clone());
        }
        let statement = self.client()?.prepare(sql).map_err(db_err)?;
        self.statements.insert(sql, statement.clone());
        Ok(statement)
    }

    /// Prepares every statement used by timed scenarios/load workers.
    fn prepare_all_timed_statements(&mut self) -> Result<(), PostgresError> {
        const TIMED: &[&str] = &[
            SELECT_TICKET_SQL,
            SELECT_USER_SQL,
            LIST_TICKETS_BY_PROJECT_STATUS_SQL,
            BOARD_PAGE_SQL,
            LIST_OPEN_TICKETS_FOR_ASSIGNEE_SQL,
            LIST_COMMENTS_SQL,
            LIST_PROJECT_MEMBERS_SQL,
            TICKET_DETAIL_SQL,
            TICKET_DETAIL_LABELS_SQL,
            COUNT_TICKET_SQL,
            COUNT_USER_SQL,
            CLOSE_TICKET_SQL,
            UPDATE_MEMBER_ROLE_SQL,
            INSERT_COMMENT_SQL,
            INSERT_COMMENT_IDEMPOTENT_SQL,
            OPEN_TICKET_SQL,
            INSERT_TICKET_LABEL_SQL,
        ];
        for sql in TIMED {
            let _ = self.statement(sql)?;
        }
        Ok(())
    }

    /// Returns a conservative load-session capacity from the live server.
    ///
    /// The bound leaves configured superuser-reserved slots plus two ordinary
    /// connections for the harness/control plane. Callers reject, rather than
    /// silently clamp, an invalid comparison.
    pub fn load_session_capacity(&mut self) -> Result<usize, PostgresError> {
        let max_connections = self
            .client()?
            .query_one("SHOW max_connections", &[])
            .map_err(db_err)?
            .get::<_, String>(0)
            .parse::<usize>()
            .map_err(|_| PostgresError::Decode)?;
        let reserved = self
            .client()?
            .query_one("SHOW superuser_reserved_connections", &[])
            .map_err(db_err)?
            .get::<_, String>(0)
            .parse::<usize>()
            .map_err(|_| PostgresError::Decode)?;
        Ok(max_connections.saturating_sub(reserved).saturating_sub(2))
    }

    /// Queries durability-relevant GUC values for fairness reporting and gates.
    pub fn durability_settings(&mut self) -> Result<PostgresDurabilitySettings, PostgresError> {
        let row = self
            .client()?
            .query_one(
                "SELECT current_setting('server_version_num'), \
                        current_setting('synchronous_commit'), \
                        current_setting('fsync'), \
                        current_setting('full_page_writes'), \
                        current_setting('wal_sync_method'), \
                        current_setting('data_directory')",
                &[],
            )
            .map_err(db_err)?;
        Ok(PostgresDurabilitySettings {
            server_version_num: row.get(0),
            synchronous_commit: row.get(1),
            fsync: row.get(2),
            full_page_writes: row.get(3),
            wal_sync_method: row.get(4),
            data_directory: row.get(5),
        })
    }

    /// Captures PostgreSQL resource counters for before/after load attribution.
    pub fn resource_snapshot(&mut self) -> Result<PostgresResourceSnapshot, PostgresError> {
        let row = self
            .client()?
            .query_one(
                "SELECT xact_commit::bigint, blks_read::bigint, blks_hit::bigint, \
                        temp_bytes::bigint, pg_database_size(current_database())::bigint, \
                        (SELECT wal_bytes::text FROM pg_stat_wal)
                 FROM pg_stat_database
                 WHERE datname = current_database()",
                &[],
            )
            .map_err(db_err)?;
        let nonnegative = |value: i64| u64::try_from(value).map_err(|_| PostgresError::Decode);
        Ok(PostgresResourceSnapshot {
            committed_transactions: nonnegative(row.get(0))?,
            blocks_read: nonnegative(row.get(1))?,
            blocks_hit: nonnegative(row.get(2))?,
            temporary_bytes: nonnegative(row.get(3))?,
            database_bytes: nonnegative(row.get(4))?,
            wal_bytes: row
                .get::<_, String>(5)
                .parse::<u64>()
                .map_err(|_| PostgresError::Decode)?,
        })
    }

    /// Counts other client backends on this database (excludes this control session).
    ///
    /// Used after each load point to prove worker connections closed and no
    /// INSERT/UPDATE is still in flight before we claim measurement complete.
    pub fn foreign_client_session_counts(
        &mut self,
    ) -> Result<PostgresClientSessionCounts, PostgresError> {
        let row = self
            .client()?
            .query_one(
                "SELECT \
                    COUNT(*)::bigint AS client_backends, \
                    COUNT(*) FILTER ( \
                        WHERE state IS DISTINCT FROM 'idle' \
                    )::bigint AS non_idle_client_backends \
                 FROM pg_stat_activity \
                 WHERE datname = current_database() \
                   AND pid <> pg_backend_pid() \
                   AND backend_type = 'client backend'",
                &[],
            )
            .map_err(db_err)?;
        let client_backends = i64_to_usize(row.get::<_, i64>(0))?;
        let non_idle_client_backends = i64_to_usize(row.get::<_, i64>(1))?;
        Ok(PostgresClientSessionCounts {
            client_backends,
            non_idle_client_backends,
        })
    }

    /// Bounded snapshot of other client backends for failure diagnostics.
    pub fn foreign_client_session_snapshot(
        &mut self,
        limit: i64,
    ) -> Result<Vec<String>, PostgresError> {
        let limit = limit.clamp(1, 64);
        let rows = self
            .client()?
            .query(
                "SELECT pid::text, coalesce(state, '?'), \
                        left(coalesce(query, ''), 120) \
                 FROM pg_stat_activity \
                 WHERE datname = current_database() \
                   AND pid <> pg_backend_pid() \
                   AND backend_type = 'client backend' \
                 ORDER BY pid \
                 LIMIT $1",
                &[&limit],
            )
            .map_err(db_err)?;
        Ok(rows
            .iter()
            .map(|row| {
                format!(
                    "pid={} state={} query={}",
                    row.get::<_, String>(0),
                    row.get::<_, String>(1),
                    row.get::<_, String>(2)
                )
            })
            .collect())
    }

    /// Blocks until no other client backends remain on this database.
    ///
    /// Call after every Postgres load point (and after the full PG phase) so a
    /// "phase complete" claim is server-verified, not just "worker threads joined".
    pub fn wait_until_load_clients_gone(&mut self, timeout: Duration) -> Result<(), PostgresError> {
        let deadline = Instant::now() + timeout;
        loop {
            let counts = self.foreign_client_session_counts()?;
            if counts.client_backends == 0 && counts.non_idle_client_backends == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                let snapshot = self.foreign_client_session_snapshot(16).unwrap_or_default();
                return Err(PostgresError::LoadNotQuiesced {
                    client_backends: counts.client_backends,
                    non_idle_client_backends: counts.non_idle_client_backends,
                    sample: snapshot,
                });
            }
            // Short poll: load workers should drop connections immediately on join.
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}

fn i64_to_usize(value: i64) -> Result<usize, PostgresError> {
    usize::try_from(value).map_err(|_| PostgresError::Decode)
}

/// Live client-backend counts excluding the observing control session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PostgresClientSessionCounts {
    /// Other `client backend` rows on this database.
    pub client_backends: usize,
    /// Subset of [`Self::client_backends`] whose `state` is not `idle`.
    pub non_idle_client_backends: usize,
}

/// Durability-relevant PostgreSQL settings observed from a live connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresDurabilitySettings {
    /// `server_version_num` GUC.
    pub server_version_num: String,
    /// `synchronous_commit` GUC (must be `on` for parity).
    pub synchronous_commit: String,
    /// `fsync` GUC (must be `on` for parity).
    pub fsync: String,
    /// `full_page_writes` GUC (must be `on` for parity).
    pub full_page_writes: String,
    /// `wal_sync_method` GUC.
    pub wal_sync_method: String,
    /// `data_directory` GUC (for same-device reporting).
    pub data_directory: String,
}

impl PostgresDurabilitySettings {
    /// Refuses comparison when durability GUCs are not fully on.
    pub fn assert_durable_for_parity(&self) -> Result<(), String> {
        for (name, value) in [
            ("synchronous_commit", self.synchronous_commit.as_str()),
            ("fsync", self.fsync.as_str()),
            ("full_page_writes", self.full_page_writes.as_str()),
        ] {
            if value != "on" {
                return Err(format!(
                    "PostgreSQL durability gate refused: {name}={value:?} (must be on); \
                     server_version_num={} data_directory={}",
                    self.server_version_num, self.data_directory
                ));
            }
        }
        Ok(())
    }
}

impl AppBackend for PostgresAppBackend {
    type Error = PostgresError;

    fn reset(&mut self) -> Result<(), Self::Error> {
        // Reset drops and recreates every table, so every cached plan is
        // invalidated; drop the handles before the schema they reference.
        self.statements.clear();
        let client = self.client()?;
        client.batch_execute(SCHEMA_SQL).map_err(db_err)
    }

    fn prewarm(&mut self) -> Result<(), Self::Error> {
        // Open the connection and prepare every timed statement once.
        let _ = self.client()?;
        self.prepare_all_timed_statements()
    }

    fn load_error_class(error: &Self::Error) -> riffdb_app_baseline_core::LoadErrorClass {
        use riffdb_app_baseline_core::LoadErrorClass;
        match error {
            PostgresError::Database {
                sqlstate: Some(state),
                ..
            } => match state.as_str() {
                // unique_violation / exclusion_violation
                "23505" | "23P01" => LoadErrorClass::Conflict,
                // cannot_connect_now / too_many_connections / admin_shutdown
                "57P03" | "53300" | "57P01" | "57P02" | "08006" | "08001" | "08004" => {
                    LoadErrorClass::Unavailable
                }
                _ => LoadErrorClass::Other,
            },
            PostgresError::Database { sqlstate: None, .. }
            | PostgresError::InvalidConfiguration
            | PostgresError::Decode
            | PostgresError::LoadNotQuiesced { .. } => LoadErrorClass::Other,
        }
    }

    fn load_error_code(error: &Self::Error) -> Option<&str> {
        match error {
            PostgresError::Database {
                sqlstate: Some(state),
                ..
            } => Some(state.as_str()),
            _ => None,
        }
    }

    fn seed(&mut self, dataset: &SeedDataset) -> Result<(), Self::Error> {
        // Prepare every seed statement once, before the client/transaction
        // borrow: `statement` and `client` both borrow `self` mutably.
        let insert_organization = self.statement(INSERT_ORGANIZATION_SQL)?;
        let insert_user = self.statement(INSERT_USER_SQL)?;
        let insert_project = self.statement(INSERT_PROJECT_SQL)?;
        let insert_member = self.statement(INSERT_MEMBER_SQL)?;
        let insert_label = self.statement(INSERT_LABEL_SQL)?;
        let insert_ticket = self.statement(INSERT_TICKET_SQL)?;
        let insert_comment = self.statement(INSERT_COMMENT_SQL)?;
        let insert_ticket_label = self.statement(INSERT_TICKET_LABEL_SQL)?;

        let client = self.client()?;
        let mut tx = client.transaction().map_err(db_err)?;
        for org in &dataset.organizations {
            tx.execute(
                &insert_organization,
                &[&format_uuid(org.organization_id), &org.name],
            )
            .map_err(db_err)?;
        }
        for user in &dataset.users {
            tx.execute(
                &insert_user,
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
                &insert_project,
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
                &insert_member,
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
                &insert_label,
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
                &insert_ticket,
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
                &insert_comment,
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
                &insert_ticket_label,
                &[
                    &format_uuid(link.organization_id),
                    &format_uuid(link.ticket_id),
                    &format_uuid(link.label_id),
                ],
            )
            .map_err(db_err)?;
        }
        tx.commit().map_err(db_err)?;
        self.settle_after_seed()
    }

    fn point_get_ticket(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
    ) -> Result<Option<TicketRow>, Self::Error> {
        self.authorize_read("point_get_ticket")?;
        let statement = self.statement(SELECT_TICKET_SQL)?;
        let client = self.client()?;
        let row = client
            .query_opt(
                &statement,
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
        self.authorize_read("point_get_user")?;
        let statement = self.statement(SELECT_USER_SQL)?;
        let client = self.client()?;
        let row = client
            .query_opt(
                &statement,
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
        self.authorize_read("list_tickets_by_project_status")?;
        let statement = self.statement(LIST_TICKETS_BY_PROJECT_STATUS_SQL)?;
        let client = self.client()?;
        let rows = client
            .query(
                &statement,
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

    fn board_page(
        &mut self,
        organization_id: UuidBytes,
        project_id: UuidBytes,
        status: TicketStatus,
        limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error> {
        self.authorize_read("board_page")?;
        // BOARD_PAGE_SQL is the single shared constant also cited in the report.
        let statement = self.statement(BOARD_PAGE_SQL)?;
        let client = self.client()?;
        let rows = client
            .query(
                &statement,
                &[
                    &format_uuid(organization_id),
                    &format_uuid(project_id),
                    &status.as_str().to_owned(),
                    &(i64::from(limit)),
                ],
            )
            .map_err(db_err)?;
        // organization_id is not selected (mirrors RiffDB filling org from the
        // request parameter rather than re-encoding it per row).
        rows.iter()
            .map(|row| decode_board_ticket(row, organization_id))
            .collect()
    }

    fn board_page_projected(
        &mut self,
        _organization_id: UuidBytes,
        _project_id: UuidBytes,
        _status: TicketStatus,
        _limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error> {
        // PG has no projected columnar path; harness never schedules these scenarios.
        Err(PostgresError::InvalidConfiguration)
    }

    fn list_open_tickets_for_assignee(
        &mut self,
        organization_id: UuidBytes,
        assignee_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error> {
        self.authorize_read("list_open_tickets_for_assignee")?;
        let statement = self.statement(LIST_OPEN_TICKETS_FOR_ASSIGNEE_SQL)?;
        let client = self.client()?;
        let rows = client
            .query(
                &statement,
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
        self.authorize_read("list_comments_for_ticket")?;
        let statement = self.statement(LIST_COMMENTS_SQL)?;
        let client = self.client()?;
        let rows = client
            .query(
                &statement,
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
        self.authorize_read("list_project_members")?;
        let statement = self.statement(LIST_PROJECT_MEMBERS_SQL)?;
        let client = self.client()?;
        let rows = client
            .query(
                &statement,
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
        self.authorize_read("ticket_detail_page")?;
        let detail_statement = self.statement(TICKET_DETAIL_SQL)?;
        let comments_statement = self.statement(LIST_COMMENTS_SQL)?;
        let labels_statement = self.statement(TICKET_DETAIL_LABELS_SQL)?;
        let client = self.client()?;
        let Some(ticket_row) = client
            .query_opt(
                &detail_statement,
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
                &comments_statement,
                &[
                    &format_uuid(organization_id),
                    &format_uuid(ticket_id),
                    &(i64::from(comment_limit)),
                ],
            )
            .map_err(db_err)?;
        let comments = comment_rows
            .iter()
            .map(decode_comment)
            .collect::<Result<Vec<_>, _>>()?;

        let label_rows = client
            .query(
                &labels_statement,
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
        let statement = self.statement(INSERT_COMMENT_SQL)?;
        if self.profile == PostgresComparisonProfile::SafeApp {
            let fingerprint = safe_input_fingerprint(&[
                &comment.row.comment_id,
                &comment.row.ticket_id,
                &comment.row.author_id,
                comment.row.body.as_bytes(),
            ]);
            let client = self.client()?;
            let mut tx = client.transaction().map_err(db_err)?;
            if safe_admit(
                &mut tx,
                &comment.idempotency_key,
                "create_comment",
                comment.row.organization_id,
                &fingerprint,
            )? {
                tx.commit().map_err(db_err)?;
                return Ok(());
            }
            tx.execute(
                &statement,
                &[
                    &format_uuid(comment.row.organization_id),
                    &format_uuid(comment.row.comment_id),
                    &format_uuid(comment.row.ticket_id),
                    &format_uuid(comment.row.author_id),
                    &comment.row.body,
                ],
            )
            .map_err(db_err)?;
            safe_complete(
                &mut tx,
                &comment.idempotency_key,
                "create_comment",
                comment.row.organization_id,
                "CommentCreated",
            )?;
            tx.commit().map_err(db_err)?;
            return Ok(());
        }
        let client = self.client()?;
        client
            .execute(
                &statement,
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

    fn replay_comment(&mut self, comment: &CommentSeed) -> Result<(), Self::Error> {
        if self.profile == PostgresComparisonProfile::SafeApp {
            return self.create_comment(comment);
        }
        // This explicit application policy mirrors RiffDB's same-key,
        // equal-input replay: the existing row makes the repeated write a
        // successful no-op. Ordinary creates continue to use strict INSERT.
        let statement = self.statement(INSERT_COMMENT_IDEMPOTENT_SQL)?;
        let client = self.client()?;
        client
            .execute(
                &statement,
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
        let count_ticket = self.statement(COUNT_TICKET_SQL)?;
        let count_user = self.statement(COUNT_USER_SQL)?;
        let close_ticket = self.statement(CLOSE_TICKET_SQL)?;
        let insert_comment = self.statement(INSERT_COMMENT_SQL)?;
        let safe_profile = self.profile == PostgresComparisonProfile::SafeApp;
        let client = self.client()?;
        let mut tx = client.transaction().map_err(db_err)?;
        if safe_profile {
            let fingerprint = safe_input_fingerprint(&[
                &input.ticket_id,
                &input.comment_id,
                &input.author_id,
                input.body.as_bytes(),
            ]);
            if safe_admit(
                &mut tx,
                &input.idempotency_key,
                "close_ticket_with_comment",
                input.organization_id,
                &fingerprint,
            )? {
                tx.commit().map_err(db_err)?;
                return Ok(());
            }
        }
        // Existence checks mirror RiffDB relationship validation before mutate/create.
        let ticket_ok: i64 = tx
            .query_one(
                &count_ticket,
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
                &count_user,
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
            &close_ticket,
            &[
                &format_uuid(input.organization_id),
                &format_uuid(input.ticket_id),
            ],
        )
        .map_err(db_err)?;
        tx.execute(
            &insert_comment,
            &[
                &format_uuid(input.organization_id),
                &format_uuid(input.comment_id),
                &format_uuid(input.ticket_id),
                &format_uuid(input.author_id),
                &input.body,
            ],
        )
        .map_err(db_err)?;
        if safe_profile {
            safe_complete(
                &mut tx,
                &input.idempotency_key,
                "close_ticket_with_comment",
                input.organization_id,
                "TicketClosedWithComment",
            )?;
        }
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    fn swap_member_roles(&mut self, input: &SwapMemberRolesSeed) -> Result<(), Self::Error> {
        let update_member_role = self.statement(UPDATE_MEMBER_ROLE_SQL)?;
        let safe_profile = self.profile == PostgresComparisonProfile::SafeApp;
        let client = self.client()?;
        let mut tx = client.transaction().map_err(db_err)?;
        if safe_profile {
            let fingerprint = safe_input_fingerprint(&[
                &input.project_id,
                &input.user_a,
                &input.user_b,
                input.role_a.as_bytes(),
                input.role_b.as_bytes(),
            ]);
            if safe_admit(
                &mut tx,
                &input.idempotency_key,
                "swap_member_roles",
                input.organization_id,
                &fingerprint,
            )? {
                tx.commit().map_err(db_err)?;
                return Ok(());
            }
        }
        let updated_a = tx
            .execute(
                &update_member_role,
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
                &update_member_role,
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
        if safe_profile {
            safe_complete(
                &mut tx,
                &input.idempotency_key,
                "swap_member_roles",
                input.organization_id,
                "MemberRolesSwapped",
            )?;
        }
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    fn open_ticket_with_labels(
        &mut self,
        input: &OpenTicketWithLabelsSeed,
    ) -> Result<(), Self::Error> {
        let open_ticket = self.statement(OPEN_TICKET_SQL)?;
        let insert_ticket_label = self.statement(INSERT_TICKET_LABEL_SQL)?;
        let safe_profile = self.profile == PostgresComparisonProfile::SafeApp;
        let client = self.client()?;
        let mut tx = client.transaction().map_err(db_err)?;
        if safe_profile {
            let fingerprint = safe_input_fingerprint(&[
                &input.ticket_id,
                &input.project_id,
                &input.reporter_id,
                &input.assignee_id,
                input.title.as_bytes(),
                &input.label_a,
                &input.label_b,
            ]);
            if safe_admit(
                &mut tx,
                &input.idempotency_key,
                "open_ticket_with_labels",
                input.organization_id,
                &fingerprint,
            )? {
                tx.commit().map_err(db_err)?;
                return Ok(());
            }
        }
        tx.execute(
            &open_ticket,
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
            &insert_ticket_label,
            &[
                &format_uuid(input.organization_id),
                &format_uuid(input.ticket_id),
                &format_uuid(input.label_a),
            ],
        )
        .map_err(db_err)?;
        tx.execute(
            &insert_ticket_label,
            &[
                &format_uuid(input.organization_id),
                &format_uuid(input.ticket_id),
                &format_uuid(input.label_b),
            ],
        )
        .map_err(db_err)?;
        if safe_profile {
            safe_complete(
                &mut tx,
                &input.idempotency_key,
                "open_ticket_with_labels",
                input.organization_id,
                "TicketOpenedWithLabels",
            )?;
        }
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

/// Board page decode: six selected columns; org filled from the bind parameter.
fn decode_board_ticket(row: &Row, organization_id: UuidBytes) -> Result<TicketRow, PostgresError> {
    Ok(TicketRow {
        organization_id,
        ticket_id: parse_uuid(row.get(0))?,
        project_id: parse_uuid(row.get(1))?,
        reporter_id: parse_uuid(row.get(2))?,
        assignee_id: parse_uuid(row.get(3))?,
        status: TicketStatus::parse(row.get::<_, &str>(4)).ok_or(PostgresError::Decode)?,
        title: row.get(5),
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
    /// Database operation failed, with SQLSTATE when the driver supplied one.
    Database {
        /// Five-character SQLSTATE (e.g. `23505`), when known.
        sqlstate: Option<String>,
        /// Driver display text (not used for load classification).
        message: String,
    },
    /// Row decode failed.
    Decode,
    /// Load workers still have sessions after the harness claimed the point done.
    LoadNotQuiesced {
        /// Other client backends still connected.
        client_backends: usize,
        /// Subset not in `idle` state (active INSERT/UPDATE/SELECT/etc.).
        non_idle_client_backends: usize,
        /// Bounded `pid/state/query` sample for diagnosis.
        sample: Vec<String>,
    },
}

impl fmt::Display for PostgresError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => formatter.write_str("invalid PostgreSQL configuration"),
            Self::Database {
                sqlstate: Some(state),
                message,
            } => write!(
                formatter,
                "PostgreSQL database error sqlstate={state}: {message}"
            ),
            Self::Database {
                sqlstate: None,
                message,
            } => write!(formatter, "PostgreSQL database error: {message}"),
            Self::Decode => formatter.write_str("PostgreSQL row decode error"),
            Self::LoadNotQuiesced {
                client_backends,
                non_idle_client_backends,
                sample,
            } => write!(
                formatter,
                "PostgreSQL load not quiesced: client_backends={client_backends} \
                 non_idle={non_idle_client_backends} sample={sample:?}"
            ),
        }
    }
}

impl Error for PostgresError {}

fn db_err(error: postgres::Error) -> PostgresError {
    PostgresError::Database {
        sqlstate: error.code().map(|code| code.code().to_owned()),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use riffdb_app_baseline_core::{AppBackend, LoadErrorClass};

    use super::{
        PostgresAppBackend, PostgresComparisonProfile, PostgresError, SCHEMA_SQL,
        safe_input_fingerprint,
    };

    fn database_error(sqlstate: &str) -> PostgresError {
        PostgresError::Database {
            sqlstate: Some(sqlstate.to_owned()),
            message: "prose is deliberately ignored".to_owned(),
        }
    }

    #[test]
    fn load_classification_uses_sqlstate_not_message_text() {
        assert_eq!(
            PostgresAppBackend::load_error_class(&database_error("23505")),
            LoadErrorClass::Conflict
        );
        assert_eq!(
            PostgresAppBackend::load_error_class(&database_error("53300")),
            LoadErrorClass::Unavailable
        );
        assert_eq!(
            PostgresAppBackend::load_error_class(&database_error("22000")),
            LoadErrorClass::Other
        );
        assert_eq!(
            PostgresAppBackend::load_error_code(&database_error("23505")),
            Some("23505")
        );
    }

    #[test]
    fn durability_assert_requires_on_settings() {
        use super::PostgresDurabilitySettings;
        let ok = PostgresDurabilitySettings {
            server_version_num: "180004".to_owned(),
            synchronous_commit: "on".to_owned(),
            fsync: "on".to_owned(),
            full_page_writes: "on".to_owned(),
            wal_sync_method: "fdatasync".to_owned(),
            data_directory: "/var/lib/postgresql/data".to_owned(),
        };
        assert!(ok.assert_durable_for_parity().is_ok());
        let mut bad = ok.clone();
        bad.synchronous_commit = "off".to_owned();
        assert!(bad.assert_durable_for_parity().is_err());
    }

    #[test]
    fn comparator_profiles_have_unambiguous_report_identities() {
        assert_eq!(
            PostgresComparisonProfile::parse("minimal").map(|profile| profile.backend_id()),
            Some("postgres_minimal")
        );
        assert_eq!(
            PostgresComparisonProfile::parse("safe-app").map(|profile| profile.backend_id()),
            Some("postgres_safe_app")
        );
        assert!(PostgresComparisonProfile::parse("sql-is-unsafe").is_none());
        assert_eq!(
            PostgresComparisonProfile::SafeApp.obligations(),
            [
                "symbolic_operation_authorization",
                "idempotency_admission_and_equal_input_replay",
                "domain_mutation",
                "audit_and_provenance",
                "domain_event",
                "outbox_intent",
                "one_atomic_transaction",
            ]
        );
    }

    #[test]
    fn safety_comparator_schema_contains_every_claimed_obligation() {
        for table in [
            "app_permission",
            "app_idempotency",
            "app_audit",
            "app_domain_event",
            "app_outbox_intent",
        ] {
            assert!(SCHEMA_SQL.contains(&format!("CREATE TABLE {table}")));
        }
    }

    #[test]
    fn safe_input_fingerprint_is_canonical_and_does_not_retain_values() {
        let first = safe_input_fingerprint(&[b"a/b", b"c"]);
        let second = safe_input_fingerprint(&[b"a", b"b/c"]);
        let repeated = safe_input_fingerprint(&[b"a/b", b"c"]);

        assert_eq!(first.len(), 64);
        assert_eq!(first, repeated);
        assert_ne!(first, second);
        assert!(!first.contains("a/b"));
    }
}
