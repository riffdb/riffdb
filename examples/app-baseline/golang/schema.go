package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

var obligations = []string{
	"symbolic_operation_authorization",
	"idempotency_admission_and_equal_input_replay",
	"domain_mutation",
	"audit_and_provenance",
	"domain_event",
	"outbox_intent",
	"one_atomic_transaction",
}

const postgresBackendID = "postgres_safe_app"

const schemaSQL = `
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
`

const insertOrganizationSQL = "INSERT INTO organization(organization_id, name) VALUES ($1::text::uuid, $2)"
const insertUserSQL = `INSERT INTO app_user(organization_id, user_id, email, display_name)
     VALUES ($1::text::uuid, $2::text::uuid, $3, $4)`
const insertProjectSQL = `INSERT INTO project(organization_id, project_id, name)
     VALUES ($1::text::uuid, $2::text::uuid, $3)`
const insertMemberSQL = `INSERT INTO project_member(organization_id, project_id, user_id, role)
     VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4)`
const insertLabelSQL = `INSERT INTO label(organization_id, label_id, name)
     VALUES ($1::text::uuid, $2::text::uuid, $3)`
const insertTicketSQL = `INSERT INTO ticket(
         organization_id, ticket_id, project_id, reporter_id, assignee_id, status, title
     ) VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5::text::uuid, $6, $7)`
const insertCommentSQL = `INSERT INTO comment(organization_id, comment_id, ticket_id, author_id, body)
     VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5)`
const insertTicketLabelSQL = `INSERT INTO ticket_label(organization_id, ticket_id, label_id)
     VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid)`
const selectTicketSQL = `SELECT organization_id::text, ticket_id::text, project_id::text,
            reporter_id::text, assignee_id::text, status, title
     FROM ticket
     WHERE organization_id = $1::text::uuid AND ticket_id = $2::text::uuid`
const selectUserSQL = `SELECT organization_id::text, user_id::text, email, display_name
     FROM app_user
     WHERE organization_id = $1::text::uuid AND user_id = $2::text::uuid`
const listTicketsByProjectStatusSQL = `SELECT organization_id::text, ticket_id::text, project_id::text,
            reporter_id::text, assignee_id::text, status, title
     FROM ticket
     WHERE organization_id = $1::text::uuid
       AND project_id = $2::text::uuid
       AND status = $3
     ORDER BY ticket_id
     LIMIT $4`
const listOpenTicketsForAssigneeSQL = `SELECT organization_id::text, ticket_id::text, project_id::text,
            reporter_id::text, assignee_id::text, status, title
     FROM ticket
     WHERE organization_id = $1::text::uuid
       AND assignee_id = $2::text::uuid
       AND status = 'open'
     ORDER BY ticket_id
     LIMIT $3`
const listCommentsSQL = `SELECT organization_id::text, comment_id::text, ticket_id::text,
            author_id::text, body
     FROM comment
     WHERE organization_id = $1::text::uuid AND ticket_id = $2::text::uuid
     ORDER BY comment_id
     LIMIT $3`
const listProjectMembersSQL = `SELECT organization_id::text, project_id::text, user_id::text, role
     FROM project_member
     WHERE organization_id = $1::text::uuid AND project_id = $2::text::uuid
     ORDER BY user_id
     LIMIT $3`
const ticketDetailSQL = `SELECT t.organization_id::text, t.ticket_id::text, t.project_id::text,
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
     WHERE t.organization_id = $1::text::uuid AND t.ticket_id = $2::text::uuid`
const ticketDetailLabelsSQL = `SELECT l.organization_id::text, l.label_id::text, l.name
     FROM ticket_label tl
     JOIN label l
       ON l.organization_id = tl.organization_id AND l.label_id = tl.label_id
     WHERE tl.organization_id = $1::text::uuid AND tl.ticket_id = $2::text::uuid
     ORDER BY l.label_id`
const countTicketSQL = `SELECT COUNT(*)::bigint FROM ticket
     WHERE organization_id = $1::text::uuid AND ticket_id = $2::text::uuid`
const countUserSQL = `SELECT COUNT(*)::bigint FROM app_user
     WHERE organization_id = $1::text::uuid AND user_id = $2::text::uuid`
const closeTicketSQL = `UPDATE ticket SET status = 'closed'
     WHERE organization_id = $1::text::uuid AND ticket_id = $2::text::uuid`
const openTicketSQL = `INSERT INTO ticket(
         organization_id, ticket_id, project_id, reporter_id, assignee_id, status, title
     ) VALUES (
         $1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5::text::uuid,
         'open', $6
     )`
const permissionSQL = "SELECT 1 FROM app_permission WHERE principal = 'app-baseline' AND operation = $1"
const idempotencyLockSQL = `SELECT operation, organization_id::text, input_fingerprint
             FROM app_idempotency WHERE idempotency_key = $1 FOR UPDATE`
const insertIdempotencySQL = `INSERT INTO app_idempotency(
             idempotency_key, operation, organization_id, input_fingerprint, outcome
         ) VALUES ($1, $2, $3::text::uuid, $4, 'committed')`
const insertAuditSQL = `INSERT INTO app_audit(idempotency_key, operation, organization_id, result)
             VALUES ($1, $2, $3::text::uuid, $4)`
const insertEventSQL = `INSERT INTO app_domain_event(event_id, idempotency_key, event_type, organization_id)
         VALUES ($1, $2, $3, $4::text::uuid)`
const insertOutboxSQL = "INSERT INTO app_outbox_intent(event_id, delivery_state) VALUES ($1, 'pending')"

var fingerprintPrefix = []byte("riffdb-app-baseline-safe-input-v1\x00")

func safeInputFingerprint(fields [][]byte) string {
	digest := sha256.New()
	digest.Write(fingerprintPrefix)
	var length [8]byte
	for _, field := range fields {
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		digest.Write(length[:])
		digest.Write(field)
	}
	return hex.EncodeToString(digest.Sum(nil))
}
