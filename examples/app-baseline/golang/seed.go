package main

import "strings"

const fullBoardDenseOpen = 600

type scale struct {
	organizations     int
	usersPerOrg       int
	projectsPerOrg    int
	membersPerProject int
	ticketsPerProject int
	commentsPerTicket int
	labelsPerOrg      int
	labelsPerTicket   int
	boardDenseOpen    int
	payloadBytes      int
}

func smokeScale() scale {
	return scale{
		organizations:     2,
		usersPerOrg:       5,
		projectsPerOrg:    3,
		membersPerProject: 3,
		ticketsPerProject: 10,
		commentsPerTicket: 3,
		labelsPerOrg:      4,
		labelsPerTicket:   2,
	}
}

// productionScale is a help desk with real history and real text.
//
// fullScale seeds roughly 14,600 rows and 2,000 tickets with no payload bytes,
// so every index is shallow, the whole set is resident, and comment bodies are
// short generated labels. That measures protocol and CPU cost rather than a
// database. This tier seeds roughly 120,000 tickets and 600,000 comments across
// 200 tenants with realistic body length.
//
// It is NOT the frozen PERF-018 comparator dataset and must not replace it.
// Keep these numbers identical to Scale::production in the Rust core, the
// TypeScript harness, and the Python harness; every harness carries its own copy.
// maxCommentBodyBytes is the contract maximum for comment.body
// (string<256> in ticketdesk.riff). Seeding above it fails RDB-INPUT-0101.
const maxCommentBodyBytes = 256

func productionScale() scale {
	return scale{
		organizations:     200,
		usersPerOrg:       40,
		projectsPerOrg:    12,
		membersPerProject: 6,
		ticketsPerProject: 50,
		commentsPerTicket: 5,
		labelsPerOrg:      12,
		labelsPerTicket:   3,
		boardDenseOpen:    fullBoardDenseOpen,
		payloadBytes:      maxCommentBodyBytes,
	}
}

func fullScale() scale {
	return scale{
		organizations:     10,
		usersPerOrg:       50,
		projectsPerOrg:    10,
		membersPerProject: 5,
		ticketsPerProject: 20,
		commentsPerTicket: 4,
		labelsPerOrg:      5,
		labelsPerTicket:   2,
		boardDenseOpen:    fullBoardDenseOpen,
	}
}

type ticketRow struct {
	organizationID UUID
	ticketID       UUID
	projectID      UUID
	reporterID     UUID
	assigneeID     UUID
	status         string
	title          string
}

type commentRow struct {
	organizationID UUID
	commentID      UUID
	ticketID       UUID
	authorID       UUID
	body           string
}

type commentSeed struct {
	row            commentRow
	idempotencyKey string
}

type closeTicketWithCommentSeed struct {
	organizationID UUID
	ticketID       UUID
	authorID       UUID
	commentID      UUID
	body           string
	idempotencyKey string
}

type openTicketWithLabelsSeed struct {
	organizationID UUID
	ticketID       UUID
	projectID      UUID
	reporterID     UUID
	assigneeID     UUID
	title          string
	labelA         UUID
	labelB         UUID
	idempotencyKey string
}

type scenarioProbes struct {
	organizationID      UUID
	projectID           UUID
	ticketID            UUID
	userID              UUID
	assigneeID          UUID
	boardOrganizationID UUID
	boardProjectID      UUID
	writeTicketID       UUID
	writeAuthorID       UUID
	writeProjectID      UUID
	writeAssigneeID     UUID
	writeLabelA         UUID
	writeLabelB         UUID
}

type orgName struct {
	id   UUID
	name string
}

type userRow struct {
	organizationID UUID
	userID         UUID
	email          string
	display        string
}

type projectRow struct {
	organizationID UUID
	projectID      UUID
	name           string
}

type memberRow struct {
	organizationID UUID
	projectID      UUID
	userID         UUID
	role           string
}

type labelRow struct {
	organizationID UUID
	labelID        UUID
	name           string
}

type ticketLabelRow struct {
	organizationID UUID
	ticketID       UUID
	labelID        UUID
}

type seedDataset struct {
	scale         scale
	organizations []orgName
	users         []userRow
	projects      []projectRow
	members       []memberRow
	tickets       []ticketRow
	comments      []commentRow
	labels        []labelRow
	ticketLabels  []ticketLabelRow
}

func sizedText(prefix string, targetBytes int) string {
	if targetBytes == 0 || len(prefix) >= targetBytes {
		return prefix
	}
	return prefix + strings.Repeat("x", targetBytes-len(prefix))
}

func generateSeed(s scale) seedDataset {
	dataset := seedDataset{scale: s}
	for orgI := 0; orgI < s.organizations; orgI++ {
		organizationID := uuidFromOrdinal(nsOrg, uint64(orgI))
		dataset.organizations = append(dataset.organizations, orgName{organizationID, "org-" + itoa(orgI)})
		var orgUsers []userRow
		for userI := 0; userI < s.usersPerOrg; userI++ {
			userID := uuidFromOrdinal(nsUser, uint64(orgI*1_000_000+userI))
			user := userRow{
				organizationID: organizationID,
				userID:         userID,
				email:          "user-" + itoa(orgI) + "-" + itoa(userI) + "@example.test",
				display:        "User " + itoa(orgI) + "/" + itoa(userI),
			}
			orgUsers = append(orgUsers, user)
			dataset.users = append(dataset.users, user)
		}
		var orgLabels []labelRow
		for labelI := 0; labelI < s.labelsPerOrg; labelI++ {
			label := labelRow{
				organizationID: organizationID,
				labelID:        uuidFromOrdinal(nsLabel, uint64(orgI*1_000+labelI)),
				name:           "label-" + itoa(orgI) + "-" + itoa(labelI),
			}
			orgLabels = append(orgLabels, label)
			dataset.labels = append(dataset.labels, label)
		}
		for projectI := 0; projectI < s.projectsPerOrg; projectI++ {
			projectOrdinal := orgI*10_000 + projectI
			projectID := uuidFromOrdinal(nsProject, uint64(projectOrdinal))
			dataset.projects = append(dataset.projects, projectRow{organizationID, projectID, "project-" + itoa(orgI) + "-" + itoa(projectI)})
			memberCount := s.membersPerProject
			if memberCount < 1 {
				memberCount = 1
			}
			if memberCount > s.usersPerOrg {
				memberCount = s.usersPerOrg
			}
			for memberI := 0; memberI < memberCount; memberI++ {
				user := orgUsers[memberI%len(orgUsers)]
				role := "member"
				if memberI == 0 {
					role = "owner"
				}
				dataset.members = append(dataset.members, memberRow{organizationID, projectID, user.userID, role})
			}
			isBoard := orgI == 0 && projectI == 0 && s.boardDenseOpen > 0
			ticketCount := s.ticketsPerProject
			if isBoard && s.boardDenseOpen > ticketCount {
				ticketCount = s.boardDenseOpen
			}
			statuses := []string{statusOpen, statusInProgress, statusClosed}
			for ticketI := 0; ticketI < ticketCount; ticketI++ {
				ticketOrdinal := projectOrdinal*1_000 + ticketI
				ticketID := uuidFromOrdinal(nsTicket, uint64(ticketOrdinal))
				reporter := orgUsers[ticketI%len(orgUsers)]
				assignee := orgUsers[(ticketI+1)%len(orgUsers)]
				status := statuses[ticketI%3]
				if isBoard {
					status = statusOpen
				}
				dataset.tickets = append(dataset.tickets, ticketRow{
					organizationID: organizationID,
					ticketID:       ticketID,
					projectID:      projectID,
					reporterID:     reporter.userID,
					assigneeID:     assignee.userID,
					status:         status,
					title:          sizedText("ticket-"+itoa(orgI)+"-"+itoa(projectI)+"-"+itoa(ticketI), minInt(s.payloadBytes, 128)),
				})
				for commentI := 0; commentI < s.commentsPerTicket; commentI++ {
					author := orgUsers[commentI%len(orgUsers)]
					dataset.comments = append(dataset.comments, commentRow{
						organizationID: organizationID,
						commentID:      uuidFromOrdinal(nsComment, uint64(ticketOrdinal*100+commentI)),
						ticketID:       ticketID,
						authorID:       author.userID,
						body:           sizedText("comment body "+itoa(orgI)+"/"+itoa(projectI)+"/"+itoa(ticketI)+"/"+itoa(commentI), s.payloadBytes),
					})
				}
				labelCount := s.labelsPerTicket
				if labelCount > s.labelsPerOrg {
					labelCount = s.labelsPerOrg
				}
				for labelI := 0; labelI < labelCount; labelI++ {
					label := orgLabels[labelI%len(orgLabels)]
					dataset.ticketLabels = append(dataset.ticketLabels, ticketLabelRow{organizationID, ticketID, label.labelID})
				}
			}
		}
	}
	return dataset
}

func boardCell(dataset seedDataset) (UUID, UUID) {
	if len(dataset.projects) == 0 {
		panic("seed has no projects")
	}
	return dataset.projects[0].organizationID, dataset.projects[0].projectID
}

func boardDenseOpenCount(dataset seedDataset) int {
	organizationID, projectID := boardCell(dataset)
	count := 0
	for _, ticket := range dataset.tickets {
		if ticket.organizationID == organizationID && ticket.projectID == projectID && ticket.status == statusOpen {
			count++
		}
	}
	return count
}

func probesFor(dataset seedDataset) scenarioProbes {
	boardOrg, boardProject := boardCell(dataset)
	var ticket *ticketRow
	for i := range dataset.tickets {
		row := &dataset.tickets[i]
		if row.status == statusOpen && !(row.organizationID == boardOrg && row.projectID == boardProject) {
			ticket = row
			break
		}
	}
	if ticket == nil {
		for i := range dataset.tickets {
			if dataset.tickets[i].status == statusOpen {
				ticket = &dataset.tickets[i]
				break
			}
		}
	}
	if ticket == nil {
		panic("seed has no open ticket")
	}
	var user *userRow
	for i := range dataset.users {
		if dataset.users[i].organizationID == ticket.organizationID {
			user = &dataset.users[i]
			break
		}
	}
	if user == nil {
		panic("seed has no user for ticket org")
	}
	otherOpen := func(row ticketRow) bool {
		return row.organizationID == ticket.organizationID && row.ticketID != ticket.ticketID && row.status == statusOpen
	}
	closeTicket := *ticket
	pick := func(pred func(ticketRow) bool) bool {
		for _, row := range dataset.tickets {
			if pred(row) {
				closeTicket = row
				return true
			}
		}
		return false
	}
	_ = pick(func(row ticketRow) bool {
		return otherOpen(row) && row.projectID != ticket.projectID && row.projectID != boardProject && row.assigneeID != ticket.assigneeID
	}) || pick(func(row ticketRow) bool {
		return otherOpen(row) && row.projectID != ticket.projectID && row.projectID != boardProject
	}) || pick(func(row ticketRow) bool {
		return otherOpen(row) && row.projectID != boardProject
	}) || pick(otherOpen)
	var orgLabels []labelRow
	for _, label := range dataset.labels {
		if label.organizationID == ticket.organizationID {
			orgLabels = append(orgLabels, label)
		}
	}
	if len(orgLabels) == 0 {
		panic("seed has no labels")
	}
	labelA := orgLabels[0].labelID
	labelB := labelA
	if len(orgLabels) > 1 {
		labelB = orgLabels[1].labelID
	}
	writeProjectID := ticket.projectID
	foundWrite := false
	for _, project := range dataset.projects {
		if project.organizationID == ticket.organizationID && project.projectID != ticket.projectID && project.projectID != boardProject {
			writeProjectID = project.projectID
			foundWrite = true
			break
		}
	}
	if !foundWrite {
		for _, project := range dataset.projects {
			if project.organizationID == ticket.organizationID && project.projectID != boardProject {
				writeProjectID = project.projectID
				break
			}
		}
	}
	writeAssigneeID := ticket.assigneeID
	for _, candidate := range dataset.users {
		if candidate.organizationID == ticket.organizationID && candidate.userID != ticket.assigneeID {
			writeAssigneeID = candidate.userID
			break
		}
	}
	return scenarioProbes{
		organizationID:      ticket.organizationID,
		projectID:           ticket.projectID,
		ticketID:            ticket.ticketID,
		userID:              user.userID,
		assigneeID:          ticket.assigneeID,
		boardOrganizationID: boardOrg,
		boardProjectID:      boardProject,
		writeTicketID:       closeTicket.ticketID,
		writeAuthorID:       user.userID,
		writeProjectID:      writeProjectID,
		writeAssigneeID:     writeAssigneeID,
		writeLabelA:         labelA,
		writeLabelB:         labelB,
	}
}

func tenantProbes(dataset seedDataset, count int) []scenarioProbes {
	if count < 1 {
		count = 1
	}
	if count > len(dataset.organizations) {
		count = len(dataset.organizations)
	}
	out := make([]scenarioProbes, 0, count)
	for _, org := range dataset.organizations[:count] {
		subset := seedDataset{
			scale:         dataset.scale,
			organizations: []orgName{org},
		}
		subset.scale.organizations = 1
		subset.scale.boardDenseOpen = 0
		for _, row := range dataset.users {
			if row.organizationID == org.id {
				subset.users = append(subset.users, row)
			}
		}
		for _, row := range dataset.projects {
			if row.organizationID == org.id {
				subset.projects = append(subset.projects, row)
			}
		}
		for _, row := range dataset.members {
			if row.organizationID == org.id {
				subset.members = append(subset.members, row)
			}
		}
		for _, row := range dataset.tickets {
			if row.organizationID == org.id {
				subset.tickets = append(subset.tickets, row)
			}
		}
		for _, row := range dataset.comments {
			if row.organizationID == org.id {
				subset.comments = append(subset.comments, row)
			}
		}
		for _, row := range dataset.labels {
			if row.organizationID == org.id {
				subset.labels = append(subset.labels, row)
			}
		}
		for _, row := range dataset.ticketLabels {
			if row.organizationID == org.id {
				subset.ticketLabels = append(subset.ticketLabels, row)
			}
		}
		out = append(out, probesFor(subset))
	}
	return out
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	var digits [20]byte
	i := len(digits)
	for value > 0 {
		i--
		digits[i] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		i--
		digits[i] = '-'
	}
	return string(digits[i:])
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
