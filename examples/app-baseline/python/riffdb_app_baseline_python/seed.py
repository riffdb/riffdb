"""Deterministic TicketDesk seed matching the Rust dataset."""

from __future__ import annotations

from dataclasses import dataclass, replace

from .ids import (
    NS_COMMENT,
    NS_LABEL,
    NS_ORG,
    NS_PROJECT,
    NS_TICKET,
    NS_USER,
    STATUS_CLOSED,
    STATUS_IN_PROGRESS,
    STATUS_OPEN,
    UuidBytes,
    uuid_from_ordinal,
)

FULL_BOARD_DENSE_OPEN = 600
# Contract maximum for comment.body (string<256> in ticketdesk.riff).
MAX_COMMENT_BODY_BYTES = 256


def _sized_text(prefix: str, target_bytes: int) -> str:
    if target_bytes == 0 or len(prefix) >= target_bytes:
        return prefix
    return prefix + ("x" * (target_bytes - len(prefix)))


@dataclass(frozen=True, slots=True)
class Scale:
    organizations: int
    users_per_org: int
    projects_per_org: int
    members_per_project: int
    tickets_per_project: int
    comments_per_ticket: int
    labels_per_org: int
    labels_per_ticket: int
    board_dense_open: int
    payload_bytes: int = 0

    @classmethod
    def smoke(cls) -> Scale:
        return cls(2, 5, 3, 3, 10, 3, 4, 2, 0, 0)

    @classmethod
    def full(cls) -> Scale:
        return cls(10, 50, 10, 5, 20, 4, 5, 2, FULL_BOARD_DENSE_OPEN, 0)

    @classmethod
    def production(cls) -> Scale:
        """A help desk with real history and real text.

        ``full`` seeds roughly 14,600 rows and 2,000 tickets with no payload
        bytes, so every index is shallow, the whole set is resident, and comment
        bodies are short generated labels. That measures protocol and CPU cost
        rather than a database. This tier seeds roughly 120,000 tickets and
        600,000 comments across 200 tenants with realistic body length.

        It is NOT the frozen PERF-018 comparator dataset and must not replace
        it. Keep these numbers identical to ``Scale::production`` in the Rust
        core, the TypeScript harness, and the Go harness; every harness carries
        its own copy.
        """
        return cls(200, 40, 12, 6, 50, 5, 12, 3, FULL_BOARD_DENSE_OPEN, MAX_COMMENT_BODY_BYTES)


@dataclass(frozen=True, slots=True)
class TicketRow:
    organization_id: UuidBytes
    ticket_id: UuidBytes
    project_id: UuidBytes
    reporter_id: UuidBytes
    assignee_id: UuidBytes
    status: str
    title: str


@dataclass(frozen=True, slots=True)
class CommentRow:
    organization_id: UuidBytes
    comment_id: UuidBytes
    ticket_id: UuidBytes
    author_id: UuidBytes
    body: str


@dataclass(frozen=True, slots=True)
class CommentSeed:
    row: CommentRow
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class CloseTicketWithCommentSeed:
    organization_id: UuidBytes
    ticket_id: UuidBytes
    author_id: UuidBytes
    comment_id: UuidBytes
    body: str
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class OpenTicketWithLabelsSeed:
    organization_id: UuidBytes
    ticket_id: UuidBytes
    project_id: UuidBytes
    reporter_id: UuidBytes
    assignee_id: UuidBytes
    title: str
    label_a: UuidBytes
    label_b: UuidBytes
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class ScenarioProbes:
    organization_id: UuidBytes
    project_id: UuidBytes
    ticket_id: UuidBytes
    user_id: UuidBytes
    assignee_id: UuidBytes
    board_organization_id: UuidBytes
    board_project_id: UuidBytes
    write_ticket_id: UuidBytes
    write_author_id: UuidBytes
    write_project_id: UuidBytes
    write_assignee_id: UuidBytes
    write_label_a: UuidBytes
    write_label_b: UuidBytes


@dataclass
class SeedDataset:
    scale: Scale
    organizations: list[tuple[UuidBytes, str]]
    users: list[tuple[UuidBytes, UuidBytes, str, str]]
    projects: list[tuple[UuidBytes, UuidBytes, str]]
    members: list[tuple[UuidBytes, UuidBytes, UuidBytes, str]]
    tickets: list[TicketRow]
    comments: list[CommentRow]
    labels: list[tuple[UuidBytes, UuidBytes, str]]
    ticket_labels: list[tuple[UuidBytes, UuidBytes, UuidBytes]]

    @classmethod
    def generate(cls, scale: Scale) -> SeedDataset:
        organizations: list[tuple[UuidBytes, str]] = []
        users: list[tuple[UuidBytes, UuidBytes, str, str]] = []
        projects: list[tuple[UuidBytes, UuidBytes, str]] = []
        members: list[tuple[UuidBytes, UuidBytes, UuidBytes, str]] = []
        tickets: list[TicketRow] = []
        comments: list[CommentRow] = []
        labels: list[tuple[UuidBytes, UuidBytes, str]] = []
        ticket_labels: list[tuple[UuidBytes, UuidBytes, UuidBytes]] = []

        for org_i in range(scale.organizations):
            organization_id = uuid_from_ordinal(NS_ORG, org_i)
            organizations.append((organization_id, f"org-{org_i}"))
            org_users: list[tuple[UuidBytes, UuidBytes, str, str]] = []
            for user_i in range(scale.users_per_org):
                ordinal = org_i * 1_000_000 + user_i
                user_id = uuid_from_ordinal(NS_USER, ordinal)
                user = (
                    organization_id,
                    user_id,
                    f"user-{org_i}-{user_i}@example.test",
                    f"User {org_i}/{user_i}",
                )
                org_users.append(user)
                users.append(user)
            org_labels: list[tuple[UuidBytes, UuidBytes, str]] = []
            for label_i in range(scale.labels_per_org):
                ordinal = org_i * 1_000 + label_i
                label = (
                    organization_id,
                    uuid_from_ordinal(NS_LABEL, ordinal),
                    f"label-{org_i}-{label_i}",
                )
                org_labels.append(label)
                labels.append(label)
            for project_i in range(scale.projects_per_org):
                project_ordinal = org_i * 10_000 + project_i
                project_id = uuid_from_ordinal(NS_PROJECT, project_ordinal)
                projects.append((organization_id, project_id, f"project-{org_i}-{project_i}"))
                member_count = max(1, min(scale.members_per_project, scale.users_per_org))
                for member_i in range(member_count):
                    user = org_users[member_i % len(org_users)]
                    role = "owner" if member_i == 0 else "member"
                    members.append((organization_id, project_id, user[1], role))
                is_board_project = org_i == 0 and project_i == 0 and scale.board_dense_open > 0
                ticket_count = (
                    max(scale.board_dense_open, scale.tickets_per_project)
                    if is_board_project
                    else scale.tickets_per_project
                )
                for ticket_i in range(ticket_count):
                    ticket_ordinal = project_ordinal * 1_000 + ticket_i
                    ticket_id = uuid_from_ordinal(NS_TICKET, ticket_ordinal)
                    reporter = org_users[ticket_i % len(org_users)]
                    assignee = org_users[(ticket_i + 1) % len(org_users)]
                    if is_board_project:
                        status = STATUS_OPEN
                    else:
                        status = (STATUS_OPEN, STATUS_IN_PROGRESS, STATUS_CLOSED)[ticket_i % 3]
                    tickets.append(
                        TicketRow(
                            organization_id=organization_id,
                            ticket_id=ticket_id,
                            project_id=project_id,
                            reporter_id=reporter[1],
                            assignee_id=assignee[1],
                            status=status,
                            title=_sized_text(
                                f"ticket-{org_i}-{project_i}-{ticket_i}",
                                min(scale.payload_bytes, 128),
                            ),
                        )
                    )
                    for comment_i in range(scale.comments_per_ticket):
                        comment_ordinal = ticket_ordinal * 100 + comment_i
                        author = org_users[comment_i % len(org_users)]
                        comments.append(
                            CommentRow(
                                organization_id=organization_id,
                                comment_id=uuid_from_ordinal(NS_COMMENT, comment_ordinal),
                                ticket_id=ticket_id,
                                author_id=author[1],
                                body=_sized_text(
                                    f"comment body {org_i}/{project_i}/{ticket_i}/{comment_i}",
                                    scale.payload_bytes,
                                ),
                            )
                        )
                    label_count = min(scale.labels_per_ticket, scale.labels_per_org)
                    for label_i in range(label_count):
                        label = org_labels[label_i % len(org_labels)]
                        ticket_labels.append((organization_id, ticket_id, label[1]))

        return cls(
            scale=scale,
            organizations=organizations,
            users=users,
            projects=projects,
            members=members,
            tickets=tickets,
            comments=comments,
            labels=labels,
            ticket_labels=ticket_labels,
        )

    def board_cell(self) -> tuple[UuidBytes, UuidBytes]:
        organization_id, project_id, _ = self.projects[0]
        return organization_id, project_id

    def board_dense_open_count(self) -> int:
        organization_id, project_id = self.board_cell()
        return sum(
            1
            for ticket in self.tickets
            if ticket.organization_id == organization_id
            and ticket.project_id == project_id
            and ticket.status == STATUS_OPEN
        )

    def probes(self) -> ScenarioProbes:
        board_org, board_project = self.board_cell()
        ticket = next(
            (
                row
                for row in self.tickets
                if row.status == STATUS_OPEN
                and not (row.organization_id == board_org and row.project_id == board_project)
            ),
            None,
        )
        if ticket is None:
            ticket = next(row for row in self.tickets if row.status == STATUS_OPEN)
        user = next(row for row in self.users if row[0] == ticket.organization_id)
        project_id = ticket.project_id
        organization_id = ticket.organization_id
        assignee_id = ticket.assignee_id

        def other_open(row: TicketRow) -> bool:
            return (
                row.organization_id == organization_id
                and row.ticket_id != ticket.ticket_id
                and row.status == STATUS_OPEN
            )

        close_ticket = next(
            (
                row
                for row in self.tickets
                if other_open(row)
                and row.project_id != project_id
                and row.project_id != board_project
                and row.assignee_id != assignee_id
            ),
            None,
        )
        if close_ticket is None:
            close_ticket = next(
                (
                    row
                    for row in self.tickets
                    if other_open(row)
                    and row.project_id != project_id
                    and row.project_id != board_project
                ),
                None,
            )
        if close_ticket is None:
            close_ticket = next(
                (
                    row
                    for row in self.tickets
                    if other_open(row) and row.project_id != board_project
                ),
                None,
            )
        if close_ticket is None:
            close_ticket = next((row for row in self.tickets if other_open(row)), ticket)

        org_labels = [label for label in self.labels if label[0] == organization_id]
        label_a = org_labels[0][1]
        label_b = org_labels[1][1] if len(org_labels) > 1 else label_a
        write_project_id = next(
            (
                project[1]
                for project in self.projects
                if project[0] == organization_id
                and project[1] != project_id
                and project[1] != board_project
            ),
            None,
        )
        if write_project_id is None:
            write_project_id = next(
                (
                    project[1]
                    for project in self.projects
                    if project[0] == organization_id and project[1] != board_project
                ),
                project_id,
            )
        write_assignee_id = next(
            (
                candidate[1]
                for candidate in self.users
                if candidate[0] == organization_id and candidate[1] != assignee_id
            ),
            assignee_id,
        )
        return ScenarioProbes(
            organization_id=organization_id,
            project_id=project_id,
            ticket_id=ticket.ticket_id,
            user_id=user[1],
            assignee_id=assignee_id,
            board_organization_id=board_org,
            board_project_id=board_project,
            write_ticket_id=close_ticket.ticket_id,
            write_author_id=user[1],
            write_project_id=write_project_id,
            write_assignee_id=write_assignee_id,
            write_label_a=label_a,
            write_label_b=label_b,
        )

    def tenant_probes(self, count: int) -> list[ScenarioProbes]:
        probes: list[ScenarioProbes] = []
        for organization_id, name in self.organizations[: max(count, 1)]:
            tenant = SeedDataset(
                scale=replace(self.scale, organizations=1, board_dense_open=0),
                organizations=[(organization_id, name)],
                users=[row for row in self.users if row[0] == organization_id],
                projects=[row for row in self.projects if row[0] == organization_id],
                members=[row for row in self.members if row[0] == organization_id],
                tickets=[row for row in self.tickets if row.organization_id == organization_id],
                comments=[row for row in self.comments if row.organization_id == organization_id],
                labels=[row for row in self.labels if row[0] == organization_id],
                ticket_labels=[row for row in self.ticket_labels if row[0] == organization_id],
            )
            probes.append(tenant.probes())
        return probes
