package main

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var timedStatements = []string{
	selectTicketSQL,
	selectUserSQL,
	listTicketsByProjectStatusSQL,
	listOpenTicketsForAssigneeSQL,
	listCommentsSQL,
	listProjectMembersSQL,
	ticketDetailSQL,
	ticketDetailLabelsSQL,
	countTicketSQL,
	countUserSQL,
	insertCommentSQL,
	closeTicketSQL,
	openTicketSQL,
	insertTicketLabelSQL,
	permissionSQL,
	idempotencyLockSQL,
	insertIdempotencySQL,
	insertAuditSQL,
	insertEventSQL,
	insertOutboxSQL,
}

type safeAppError struct {
	msg      string
	sqlstate string
}

func (e *safeAppError) Error() string { return e.msg }

func classifyPostgres(err error) string {
	var app *safeAppError
	if errors.As(err, &app) {
		switch app.sqlstate {
		case "23505", "23P01":
			return "conflict"
		case "57P03", "53300", "57P01", "57P02", "08006", "08001", "08004":
			return "unavailable"
		}
	}
	return "error"
}

func dbError(err error) error {
	if err == nil {
		return nil
	}
	var app *safeAppError
	if errors.As(err, &app) {
		return app
	}
	sqlstate := ""
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		sqlstate = pgErr.Code
	}
	return &safeAppError{msg: err.Error(), sqlstate: sqlstate}
}

type querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type safeAppSession struct {
	conn *pgx.Conn
	q    querier
}

func connectPostgres(ctx context.Context, url string) (*safeAppSession, error) {
	var last error
	for attempt := 0; attempt < 40; attempt++ {
		conn, err := pgx.Connect(ctx, url)
		if err != nil {
			last = err
			time.Sleep(250 * time.Millisecond)
			continue
		}
		if _, err := conn.Exec(ctx, "SET synchronous_commit = on"); err != nil {
			_ = conn.Close(ctx)
			last = err
			time.Sleep(250 * time.Millisecond)
			continue
		}
		return &safeAppSession{conn: conn, q: conn}, nil
	}
	if last == nil {
		last = errors.New("postgres connect failed")
	}
	return nil, last
}

func (s *safeAppSession) Close(ctx context.Context) error {
	return s.conn.Close(ctx)
}

func (s *safeAppSession) reset(ctx context.Context) error {
	_, err := s.conn.Exec(ctx, schemaSQL)
	return dbError(err)
}

func (s *safeAppSession) Prewarm(ctx context.Context, organizationID, ticketID UUID) error {
	org := formatUUID(organizationID)
	ticket := formatUUID(ticketID)
	for _, statement := range timedStatements {
		if _, err := s.q.Exec(ctx, statement, prewarmParams(statement, org, ticket)...); err != nil {
			_, _ = s.conn.Exec(ctx, "ROLLBACK")
		}
	}
	return nil
}

func (s *safeAppSession) Seed(ctx context.Context, dataset seedDataset) error {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return dbError(err)
	}
	prev := s.q
	s.q = tx
	defer func() { s.q = prev }()
	if err := s.seedTx(ctx, dataset); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return dbError(err)
	}
	if _, err := s.conn.Exec(ctx, "VACUUM (ANALYZE)"); err != nil {
		_, _ = s.conn.Exec(ctx, "ROLLBACK")
	}
	if _, err := s.conn.Exec(ctx, "CHECKPOINT"); err != nil {
		_, _ = s.conn.Exec(ctx, "ROLLBACK")
	}
	return nil
}

func (s *safeAppSession) seedTx(ctx context.Context, dataset seedDataset) error {
	for _, org := range dataset.organizations {
		if _, err := s.exec(ctx, insertOrganizationSQL, formatUUID(org.id), org.name); err != nil {
			return err
		}
	}
	for _, user := range dataset.users {
		if _, err := s.exec(ctx, insertUserSQL, formatUUID(user.organizationID), formatUUID(user.userID), user.email, user.display); err != nil {
			return err
		}
	}
	for _, project := range dataset.projects {
		if _, err := s.exec(ctx, insertProjectSQL, formatUUID(project.organizationID), formatUUID(project.projectID), project.name); err != nil {
			return err
		}
	}
	for _, member := range dataset.members {
		if _, err := s.exec(ctx, insertMemberSQL, formatUUID(member.organizationID), formatUUID(member.projectID), formatUUID(member.userID), member.role); err != nil {
			return err
		}
	}
	for _, label := range dataset.labels {
		if _, err := s.exec(ctx, insertLabelSQL, formatUUID(label.organizationID), formatUUID(label.labelID), label.name); err != nil {
			return err
		}
	}
	for _, ticket := range dataset.tickets {
		if _, err := s.exec(ctx, insertTicketSQL,
			formatUUID(ticket.organizationID), formatUUID(ticket.ticketID), formatUUID(ticket.projectID),
			formatUUID(ticket.reporterID), formatUUID(ticket.assigneeID), ticket.status, ticket.title); err != nil {
			return err
		}
	}
	for _, comment := range dataset.comments {
		if _, err := s.exec(ctx, insertCommentSQL,
			formatUUID(comment.organizationID), formatUUID(comment.commentID), formatUUID(comment.ticketID),
			formatUUID(comment.authorID), comment.body); err != nil {
			return err
		}
	}
	for _, link := range dataset.ticketLabels {
		if _, err := s.exec(ctx, insertTicketLabelSQL, formatUUID(link.organizationID), formatUUID(link.ticketID), formatUUID(link.labelID)); err != nil {
			return err
		}
	}
	return nil
}

func (s *safeAppSession) PointGetTicket(ctx context.Context, organizationID, ticketID UUID) (bool, error) {
	return s.exists(ctx, selectTicketSQL, formatUUID(organizationID), formatUUID(ticketID))
}

func (s *safeAppSession) PointGetUser(ctx context.Context, organizationID, userID UUID) (bool, error) {
	return s.exists(ctx, selectUserSQL, formatUUID(organizationID), formatUUID(userID))
}

func (s *safeAppSession) ListTicketsByProjectStatus(ctx context.Context, organizationID, projectID UUID, status string, limit int) error {
	_, err := s.exec(ctx, listTicketsByProjectStatusSQL, formatUUID(organizationID), formatUUID(projectID), status, limit)
	return err
}

func (s *safeAppSession) ListOpenTicketsForAssignee(ctx context.Context, organizationID, assigneeID UUID, limit int) error {
	_, err := s.exec(ctx, listOpenTicketsForAssigneeSQL, formatUUID(organizationID), formatUUID(assigneeID), limit)
	return err
}

func (s *safeAppSession) ListCommentsForTicket(ctx context.Context, organizationID, ticketID UUID, limit int) error {
	_, err := s.exec(ctx, listCommentsSQL, formatUUID(organizationID), formatUUID(ticketID), limit)
	return err
}

func (s *safeAppSession) ListProjectMembers(ctx context.Context, organizationID, projectID UUID, limit int) error {
	_, err := s.exec(ctx, listProjectMembersSQL, formatUUID(organizationID), formatUUID(projectID), limit)
	return err
}

func (s *safeAppSession) TicketDetailPage(ctx context.Context, organizationID, ticketID UUID) (bool, error) {
	found, err := s.exists(ctx, ticketDetailSQL, formatUUID(organizationID), formatUUID(ticketID))
	if err != nil {
		return false, err
	}
	if _, err := s.exec(ctx, ticketDetailLabelsSQL, formatUUID(organizationID), formatUUID(ticketID)); err != nil {
		return false, err
	}
	return found, nil
}

func (s *safeAppSession) CreateComment(ctx context.Context, comment commentSeed) error {
	fingerprint := safeInputFingerprint([][]byte{
		comment.row.commentID[:],
		comment.row.ticketID[:],
		comment.row.authorID[:],
		[]byte(comment.row.body),
	})
	return s.transact(ctx, func(ctx context.Context) error {
		replayed, err := s.safeAdmit(ctx, comment.idempotencyKey, "create_comment", comment.row.organizationID, fingerprint)
		if err != nil || replayed {
			return err
		}
		if _, err := s.exec(ctx, insertCommentSQL,
			formatUUID(comment.row.organizationID), formatUUID(comment.row.commentID),
			formatUUID(comment.row.ticketID), formatUUID(comment.row.authorID), comment.row.body); err != nil {
			return err
		}
		return s.safeComplete(ctx, comment.idempotencyKey, "create_comment", comment.row.organizationID, "CommentCreated")
	})
}

func (s *safeAppSession) CloseTicketWithComment(ctx context.Context, input closeTicketWithCommentSeed) error {
	fingerprint := safeInputFingerprint([][]byte{
		input.ticketID[:],
		input.commentID[:],
		input.authorID[:],
		[]byte(input.body),
	})
	return s.transact(ctx, func(ctx context.Context) error {
		replayed, err := s.safeAdmit(ctx, input.idempotencyKey, "close_ticket_with_comment", input.organizationID, fingerprint)
		if err != nil || replayed {
			return err
		}
		ticketOK, err := s.count(ctx, countTicketSQL, formatUUID(input.organizationID), formatUUID(input.ticketID))
		if err != nil {
			return err
		}
		authorOK, err := s.count(ctx, countUserSQL, formatUUID(input.organizationID), formatUUID(input.authorID))
		if err != nil {
			return err
		}
		if ticketOK == 0 || authorOK == 0 {
			return &safeAppError{msg: "decode"}
		}
		if _, err := s.exec(ctx, closeTicketSQL, formatUUID(input.organizationID), formatUUID(input.ticketID)); err != nil {
			return err
		}
		if _, err := s.exec(ctx, insertCommentSQL,
			formatUUID(input.organizationID), formatUUID(input.commentID),
			formatUUID(input.ticketID), formatUUID(input.authorID), input.body); err != nil {
			return err
		}
		return s.safeComplete(ctx, input.idempotencyKey, "close_ticket_with_comment", input.organizationID, "TicketClosedWithComment")
	})
}

func (s *safeAppSession) OpenTicketWithLabels(ctx context.Context, input openTicketWithLabelsSeed) error {
	fingerprint := safeInputFingerprint([][]byte{
		input.ticketID[:],
		input.projectID[:],
		input.reporterID[:],
		input.assigneeID[:],
		[]byte(input.title),
		input.labelA[:],
		input.labelB[:],
	})
	return s.transact(ctx, func(ctx context.Context) error {
		replayed, err := s.safeAdmit(ctx, input.idempotencyKey, "open_ticket_with_labels", input.organizationID, fingerprint)
		if err != nil || replayed {
			return err
		}
		if _, err := s.exec(ctx, openTicketSQL,
			formatUUID(input.organizationID), formatUUID(input.ticketID), formatUUID(input.projectID),
			formatUUID(input.reporterID), formatUUID(input.assigneeID), input.title); err != nil {
			return err
		}
		if _, err := s.exec(ctx, insertTicketLabelSQL, formatUUID(input.organizationID), formatUUID(input.ticketID), formatUUID(input.labelA)); err != nil {
			return err
		}
		if _, err := s.exec(ctx, insertTicketLabelSQL, formatUUID(input.organizationID), formatUUID(input.ticketID), formatUUID(input.labelB)); err != nil {
			return err
		}
		return s.safeComplete(ctx, input.idempotencyKey, "open_ticket_with_labels", input.organizationID, "TicketOpenedWithLabels")
	})
}

func (s *safeAppSession) exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := s.q.Exec(ctx, sql, args...)
	if err != nil {
		return 0, dbError(err)
	}
	return tag.RowsAffected(), nil
}

func (s *safeAppSession) exists(ctx context.Context, sql string, args ...any) (bool, error) {
	rows, err := s.q.Query(ctx, sql, args...)
	if err != nil {
		return false, dbError(err)
	}
	defer rows.Close()
	found := rows.Next()
	if err := rows.Err(); err != nil {
		return false, dbError(err)
	}
	return found, nil
}

func (s *safeAppSession) count(ctx context.Context, sql string, args ...any) (int64, error) {
	var n int64
	if err := s.q.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		return 0, dbError(err)
	}
	return n, nil
}

func (s *safeAppSession) transact(ctx context.Context, body func(context.Context) error) error {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return dbError(err)
	}
	prev := s.q
	s.q = tx
	defer func() { s.q = prev }()
	if err := body(ctx); err != nil {
		_ = tx.Rollback(ctx)
		return dbError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return dbError(err)
	}
	return nil
}

func (s *safeAppSession) safeAdmit(ctx context.Context, idempotencyKey, operation string, organizationID UUID, fingerprint string) (bool, error) {
	authorized, err := s.exists(ctx, permissionSQL, operation)
	if err != nil {
		return false, err
	}
	if !authorized {
		return false, &safeAppError{msg: "decode"}
	}
	orgText := formatUUID(organizationID)
	var existingOp, existingOrg, existingFP string
	scanErr := s.q.QueryRow(ctx, idempotencyLockSQL, idempotencyKey).Scan(&existingOp, &existingOrg, &existingFP)
	if scanErr == nil {
		if existingOp != operation || existingOrg != orgText || existingFP != fingerprint {
			return false, &safeAppError{msg: "decode"}
		}
		if _, err := s.exec(ctx, insertAuditSQL, idempotencyKey, operation, orgText, "replayed"); err != nil {
			return false, err
		}
		return true, nil
	}
	if !errors.Is(scanErr, pgx.ErrNoRows) {
		return false, dbError(scanErr)
	}
	if _, err := s.exec(ctx, insertIdempotencySQL, idempotencyKey, operation, orgText, fingerprint); err != nil {
		return false, err
	}
	return false, nil
}

func (s *safeAppSession) safeComplete(ctx context.Context, idempotencyKey, operation string, organizationID UUID, eventType string) error {
	orgText := formatUUID(organizationID)
	eventID := operation + "/" + idempotencyKey
	if _, err := s.exec(ctx, insertAuditSQL, idempotencyKey, operation, orgText, "committed"); err != nil {
		return err
	}
	if _, err := s.exec(ctx, insertEventSQL, eventID, idempotencyKey, eventType, orgText); err != nil {
		return err
	}
	_, err := s.exec(ctx, insertOutboxSQL, eventID)
	return err
}

type postgresDriver struct {
	url string
}

func (d postgresDriver) BackendID() string { return postgresBackendID }

func (d postgresDriver) Seed(ctx context.Context, dataset seedDataset) error {
	session, err := connectPostgres(ctx, d.url)
	if err != nil {
		return err
	}
	defer session.Close(ctx)
	if err := session.reset(ctx); err != nil {
		return err
	}
	return session.Seed(ctx, dataset)
}

func (d postgresDriver) OpenSession(ctx context.Context) (loadSession, error) {
	return connectPostgres(ctx, d.url)
}

func prewarmParams(statement, org, ticket string) []any {
	placeholders := strings.Count(statement, "$")
	if placeholders == 0 {
		return nil
	}
	if strings.Contains(statement, "LIMIT $4") {
		return []any{org, ticket, "open", 1}
	}
	if strings.Contains(statement, "LIMIT $3") {
		return []any{org, ticket, 1}
	}
	if strings.Contains(statement, "principal = 'app-baseline'") {
		return []any{"point_get_ticket"}
	}
	if strings.Contains(statement, "idempotency_key = $1") {
		return []any{"prewarm"}
	}
	if placeholders == 1 {
		return []any{org}
	}
	filled := []any{org, ticket}
	for len(filled) < placeholders {
		filled = append(filled, org)
	}
	return filled[:placeholders]
}
