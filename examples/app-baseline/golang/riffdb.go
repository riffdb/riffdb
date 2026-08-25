// RiffDB load session: generated TicketDesk client only. No Go safety code.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"

	application "riffdb.dev/application"
	"riffdb.dev/ticketdesk"
)

const riffdbBackendID = "riffdb_public_grpc"

type loadKindError struct {
	kind string
	msg  string
}

func (e *loadKindError) Error() string { return e.msg }

func classifyRiffdb(err error) string {
	var app *application.ApplicationError
	if errors.As(err, &app) {
		switch app.Details.Code {
		case "RDB-COMMAND-0102":
			return "conflict"
		case "RDB-STORAGE-0101", "RDB-RESOURCE-0101", "RDB-CAPACITY-0101":
			return "unavailable"
		default:
			return "error"
		}
	}
	var kind *loadKindError
	if errors.As(err, &kind) {
		if kind.kind == "conflict" {
			return "conflict"
		}
		return "error"
	}
	return "error"
}

type driverIdentityFile struct {
	SocketPath string
	Identity   application.Identity
}

func readDriverIdentity(path string) (driverIdentityFile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return driverIdentityFile{}, err
	}
	var parsed struct {
		SocketPath string `json:"socketPath"`
		Identity   struct {
			ApplicationManifestHash string          `json:"applicationManifestHash"`
			OperationCatalogHash    string          `json:"operationCatalogHash"`
			ContractLineage         string          `json:"contractLineage"`
			ContractVersion         json.RawMessage `json:"contractVersion"`
			ContractBundleHash      string          `json:"contractBundleHash"`
			Database                string          `json:"database"`
			Role                    string          `json:"role"`
			RoleDefinitionHash      string          `json:"roleDefinitionHash"`
			RemoteIdentityHash      string          `json:"remoteIdentityHash"`
		} `json:"identity"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return driverIdentityFile{}, err
	}
	version, err := parseFlexUint64(parsed.Identity.ContractVersion)
	if err != nil {
		return driverIdentityFile{}, err
	}
	return driverIdentityFile{
		SocketPath: parsed.SocketPath,
		Identity: application.Identity{
			ApplicationManifestHash: parsed.Identity.ApplicationManifestHash,
			OperationCatalogHash:    parsed.Identity.OperationCatalogHash,
			ContractLineage:         parsed.Identity.ContractLineage,
			ContractVersion:         version,
			ContractBundleHash:      parsed.Identity.ContractBundleHash,
			Database:                parsed.Identity.Database,
			Role:                    parsed.Identity.Role,
			RoleDefinitionHash:      parsed.Identity.RoleDefinitionHash,
			RemoteIdentityHash:      parsed.Identity.RemoteIdentityHash,
		},
	}, nil
}

func parseFlexUint64(raw json.RawMessage) (uint64, error) {
	if len(raw) == 0 {
		return 0, errors.New("missing contractVersion")
	}
	if raw[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return 0, err
		}
		return strconv.ParseUint(text, 10, 64)
	}
	var value uint64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, err
	}
	return value, nil
}

func namedOutcome(value any) string {
	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	if rv.Kind() == reflect.Struct {
		field := rv.FieldByName("Outcome")
		if field.IsValid() && field.Kind() == reflect.String {
			return field.String()
		}
	}
	return fmt.Sprintf("%T", value)
}

func requireOutcome(actual, expected string) error {
	if actual == expected {
		return nil
	}
	switch actual {
	case "CommentExists", "TicketExists", "LinkExists":
		return &loadKindError{kind: "conflict", msg: fmt.Sprintf("unexpected outcome %s, wanted %s", actual, expected)}
	default:
		return &loadKindError{kind: "error", msg: fmt.Sprintf("unexpected outcome %s, wanted %s", actual, expected)}
	}
}

func u32(value uint32) *uint32 { return &value }

type riffDbSession struct {
	session *application.Session
	client  *ticketdesk.Client
}

func connectRiffdb(ctx context.Context, identity driverIdentityFile) (*riffDbSession, error) {
	session, err := application.Connect(ctx, identity.SocketPath, identity.Identity)
	if err != nil {
		return nil, err
	}
	client, err := ticketdesk.NewClient(session, 1)
	if err != nil {
		_ = session.Close()
		return nil, err
	}
	return &riffDbSession{session: session, client: client}, nil
}

func (s *riffDbSession) Close(ctx context.Context) error {
	_ = ctx
	return s.session.Close()
}

func (s *riffDbSession) Prewarm(ctx context.Context, organizationID, ticketID UUID) error {
	_, err := s.PointGetTicket(ctx, organizationID, ticketID)
	return err
}

func (s *riffDbSession) PointGetTicket(ctx context.Context, organizationID, ticketID UUID) (bool, error) {
	result, err := s.client.GetTicket(ctx, ticketdesk.GetTicketParams{
		OrganizationId: formatUUID(organizationID),
		TicketId:       formatUUID(ticketID),
	}, ticketdesk.QueryOptions{})
	if err != nil {
		return false, err
	}
	_, found := result.Value.(ticketdesk.GetTicketFound)
	return found, nil
}

func (s *riffDbSession) PointGetUser(ctx context.Context, organizationID, userID UUID) (bool, error) {
	result, err := s.client.GetUser(ctx, ticketdesk.GetUserParams{
		OrganizationId: formatUUID(organizationID),
		UserId:         formatUUID(userID),
	}, ticketdesk.QueryOptions{})
	if err != nil {
		return false, err
	}
	_, found := result.Value.(ticketdesk.GetUserFound)
	return found, nil
}

func (s *riffDbSession) ListTicketsByProjectStatus(ctx context.Context, organizationID, projectID UUID, status string, limit int) error {
	_, err := s.client.ListTickets(ctx, ticketdesk.ListTicketsParams{
		OrganizationId: formatUUID(organizationID),
		ProjectId:      formatUUID(projectID),
		Statuses:       []ticketdesk.TicketStatus{ticketdesk.TicketStatus(sqlStatusToRiff(status))},
		Limit:          u32(uint32(limit)),
	}, ticketdesk.QueryOptions{})
	return err
}

func (s *riffDbSession) ListOpenTicketsForAssignee(ctx context.Context, organizationID, assigneeID UUID, limit int) error {
	_, err := s.client.ListTicketsByAssignee(ctx, ticketdesk.ListTicketsByAssigneeParams{
		OrganizationId: formatUUID(organizationID),
		AssigneeId:     formatUUID(assigneeID),
		Statuses:       []ticketdesk.TicketStatus{ticketdesk.TicketStatusOpen},
		Limit:          u32(uint32(limit)),
	}, ticketdesk.QueryOptions{})
	return err
}

func (s *riffDbSession) ListCommentsForTicket(ctx context.Context, organizationID, ticketID UUID, limit int) error {
	_, err := s.client.ListComments(ctx, ticketdesk.ListCommentsParams{
		OrganizationId: formatUUID(organizationID),
		TicketId:       formatUUID(ticketID),
		Limit:          u32(uint32(limit)),
	}, ticketdesk.QueryOptions{})
	return err
}

func (s *riffDbSession) ListProjectMembers(ctx context.Context, organizationID, projectID UUID, _ int) error {
	_, err := s.client.ProjectMembers(ctx, ticketdesk.ProjectMembersParams{
		OrganizationId: formatUUID(organizationID),
		ProjectId:      formatUUID(projectID),
	}, ticketdesk.QueryOptions{})
	return err
}

func (s *riffDbSession) TicketDetailPage(ctx context.Context, organizationID, ticketID UUID) (bool, error) {
	result, err := s.client.TicketPage(ctx, ticketdesk.TicketPageParams{
		OrganizationId: formatUUID(organizationID),
		TicketId:       formatUUID(ticketID),
	}, ticketdesk.QueryOptions{})
	if err != nil {
		return false, err
	}
	_, found := result.Value.(ticketdesk.TicketPageFound)
	return found, nil
}

func (s *riffDbSession) CreateComment(ctx context.Context, comment commentSeed) error {
	result, err := s.client.CreateComment(ctx, ticketdesk.CreateCommentInput{
		Body:           comment.row.body,
		AuthorId:       formatUUID(comment.row.authorID),
		TicketId:       formatUUID(comment.row.ticketID),
		CommentId:      formatUUID(comment.row.commentID),
		IdempotencyKey: comment.idempotencyKey,
		OrganizationId: formatUUID(comment.row.organizationID),
	})
	if err != nil {
		return err
	}
	return requireOutcome(namedOutcome(result.Outcome), "Created")
}

func (s *riffDbSession) CloseTicketWithComment(ctx context.Context, input closeTicketWithCommentSeed) error {
	result, err := s.client.CloseTicketWithComment(ctx, ticketdesk.CloseTicketWithCommentInput{
		Body:           input.body,
		AuthorId:       formatUUID(input.authorID),
		TicketId:       formatUUID(input.ticketID),
		CommentId:      formatUUID(input.commentID),
		IdempotencyKey: input.idempotencyKey,
		OrganizationId: formatUUID(input.organizationID),
	})
	if err != nil {
		return err
	}
	return requireOutcome(namedOutcome(result.Outcome), "Closed")
}

func (s *riffDbSession) OpenTicketWithLabels(ctx context.Context, input openTicketWithLabelsSeed) error {
	result, err := s.client.OpenTicketWithLabels(ctx, ticketdesk.OpenTicketWithLabelsInput{
		Title:          input.title,
		LabelA:         formatUUID(input.labelA),
		LabelB:         formatUUID(input.labelB),
		TicketId:       formatUUID(input.ticketID),
		ProjectId:      formatUUID(input.projectID),
		AssigneeId:     formatUUID(input.assigneeID),
		ReporterId:     formatUUID(input.reporterID),
		IdempotencyKey: input.idempotencyKey,
		OrganizationId: formatUUID(input.organizationID),
	})
	if err != nil {
		return err
	}
	return requireOutcome(namedOutcome(result.Outcome), "Created")
}

func (s *riffDbSession) Seed(ctx context.Context, dataset seedDataset) error {
	for _, org := range dataset.organizations {
		result, err := s.client.CreateOrganization(ctx, ticketdesk.CreateOrganizationInput{
			Name:           org.name,
			OrganizationId: formatUUID(org.id),
			IdempotencyKey: "seed-org-" + encodeShort(org.id),
		})
		if err != nil {
			return err
		}
		if err := requireOutcome(namedOutcome(result.Outcome), "Created"); err != nil {
			return err
		}
	}
	for _, user := range dataset.users {
		result, err := s.client.CreateUser(ctx, ticketdesk.CreateUserInput{
			Email:          user.email,
			UserId:         formatUUID(user.userID),
			DisplayName:    user.display,
			OrganizationId: formatUUID(user.organizationID),
			IdempotencyKey: "seed-user-" + encodeShort(user.userID),
		})
		if err != nil {
			return err
		}
		if err := requireOutcome(namedOutcome(result.Outcome), "Created"); err != nil {
			return err
		}
	}
	for _, project := range dataset.projects {
		result, err := s.client.CreateProject(ctx, ticketdesk.CreateProjectInput{
			Name:           project.name,
			ProjectId:      formatUUID(project.projectID),
			OrganizationId: formatUUID(project.organizationID),
			IdempotencyKey: "seed-project-" + encodeShort(project.projectID),
		})
		if err != nil {
			return err
		}
		if err := requireOutcome(namedOutcome(result.Outcome), "Created"); err != nil {
			return err
		}
	}
	for _, member := range dataset.members {
		result, err := s.client.AddProjectMember(ctx, ticketdesk.AddProjectMemberInput{
			Role:           member.role,
			UserId:         formatUUID(member.userID),
			ProjectId:      formatUUID(member.projectID),
			OrganizationId: formatUUID(member.organizationID),
			IdempotencyKey: "seed-member-" + encodeShort(member.projectID) + "-" + encodeShort(member.userID),
		})
		if err != nil {
			return err
		}
		if err := requireOutcome(namedOutcome(result.Outcome), "Created"); err != nil {
			return err
		}
	}
	for _, label := range dataset.labels {
		result, err := s.client.CreateLabel(ctx, ticketdesk.CreateLabelInput{
			Name:           label.name,
			LabelId:        formatUUID(label.labelID),
			OrganizationId: formatUUID(label.organizationID),
			IdempotencyKey: "seed-label-" + encodeShort(label.labelID),
		})
		if err != nil {
			return err
		}
		if err := requireOutcome(namedOutcome(result.Outcome), "Created"); err != nil {
			return err
		}
	}
	for _, ticket := range dataset.tickets {
		result, err := s.client.CreateTicket(ctx, ticketdesk.CreateTicketInput{
			Title:          ticket.title,
			Status:         ticketdesk.TicketStatus(sqlStatusToRiff(ticket.status)),
			TicketId:       formatUUID(ticket.ticketID),
			ProjectId:      formatUUID(ticket.projectID),
			AssigneeId:     formatUUID(ticket.assigneeID),
			ReporterId:     formatUUID(ticket.reporterID),
			OrganizationId: formatUUID(ticket.organizationID),
			IdempotencyKey: "seed-ticket-" + encodeShort(ticket.ticketID),
		})
		if err != nil {
			return err
		}
		if err := requireOutcome(namedOutcome(result.Outcome), "Created"); err != nil {
			return err
		}
	}
	for _, comment := range dataset.comments {
		result, err := s.client.CreateComment(ctx, ticketdesk.CreateCommentInput{
			Body:           comment.body,
			AuthorId:       formatUUID(comment.authorID),
			TicketId:       formatUUID(comment.ticketID),
			CommentId:      formatUUID(comment.commentID),
			OrganizationId: formatUUID(comment.organizationID),
			IdempotencyKey: "seed-comment-" + encodeShort(comment.commentID),
		})
		if err != nil {
			return err
		}
		if err := requireOutcome(namedOutcome(result.Outcome), "Created"); err != nil {
			return err
		}
	}
	for _, link := range dataset.ticketLabels {
		result, err := s.client.AttachLabel(ctx, ticketdesk.AttachLabelInput{
			LabelId:        formatUUID(link.labelID),
			TicketId:       formatUUID(link.ticketID),
			OrganizationId: formatUUID(link.organizationID),
			IdempotencyKey: "seed-link-" + encodeShort(link.ticketID) + "-" + encodeShort(link.labelID),
		})
		if err != nil {
			return err
		}
		if err := requireOutcome(namedOutcome(result.Outcome), "Created"); err != nil {
			return err
		}
	}
	return nil
}

type riffDbDriver struct {
	identity driverIdentityFile
}

func (d riffDbDriver) BackendID() string { return riffdbBackendID }

func (d riffDbDriver) Seed(ctx context.Context, dataset seedDataset) error {
	session, err := connectRiffdb(ctx, d.identity)
	if err != nil {
		return err
	}
	defer session.Close(ctx)
	return session.Seed(ctx, dataset)
}

func (d riffDbDriver) OpenSession(ctx context.Context) (loadSession, error) {
	return connectRiffdb(ctx, d.identity)
}
