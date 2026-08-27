"""RiffDB load session: generated TicketDesk client only. No Python safety code."""

from __future__ import annotations

import sys
import time
from pathlib import Path

from riffdb_application import CommandBatchOptions

from .ids import as_uuid, encode_short, sql_status_to_riff
from .seed import CloseTicketWithCommentSeed, CommentSeed, OpenTicketWithLabelsSeed, SeedDataset

#: Bounded batch fan-out for seeding, matching the Rust harness.
SEED_CONCURRENCY = 128
#: Generated batch row ceiling.
SEED_BATCH_ROWS = 4096

RIFFDB_BACKEND_ID = "riffdb_public_grpc"


def _load_generated():
    root = Path(__file__).resolve().parents[3] / "ticketdesk" / "generated" / "python"
    import sys

    path = str(root)
    if path not in sys.path:
        sys.path.insert(0, path)
    import client as generated

    return generated


class RiffDbError(Exception):
    def __init__(self, message: str, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code


def classify_riffdb(error: Exception) -> str:
    try:
        from riffdb_application import ConnectionFailure, RiffDbApplicationError
    except ImportError:
        return "error"
    if isinstance(error, ConnectionFailure):
        return "unavailable"
    if isinstance(error, RiffDbApplicationError):
        code = error.details.code.value
        if code in {"RDB-COMMAND-0102"}:
            return "conflict"
        if code in {"RDB-STORAGE-0101", "RDB-RESOURCE-0101", "RDB-CAPACITY-0101"}:
            return "unavailable"
        return "error"
    if isinstance(error, RiffDbError):
        if error.code in {"RDB-COMMAND-0102"}:
            return "conflict"
        return "error"
    return "error"


class RiffDbSession:
    """One HTTP/2 session of the generated TicketDesk application client."""

    def __init__(self, endpoint: str, token: str) -> None:
        from riffdb_application import (
            AttemptBudget,
            BearerCredential,
            CallMetadata,
            SyncApplicationTransport,
        )

        generated = _load_generated()
        self._generated = generated
        self._transport = SyncApplicationTransport.connect_uri(
            endpoint, CallMetadata.authenticated(BearerCredential(token))
        )
        self._client = generated.TicketDeskClient(self._transport, AttemptBudget(3))

    def close(self) -> None:
        self._transport.close()

    def prewarm(self, organization_id: bytes, ticket_id: bytes) -> None:
        self.point_get_ticket(organization_id, ticket_id)

    def point_get_ticket(self, organization_id: bytes, ticket_id: bytes) -> bool:
        result = self._client.get_ticket(
            self._generated.GetTicketParams(
                organization_id=as_uuid(organization_id), ticket_id=as_uuid(ticket_id)
            )
        )
        return isinstance(result.value, self._generated.GetTicketFound)

    def point_get_user(self, organization_id: bytes, user_id: bytes) -> bool:
        result = self._client.get_user(
            self._generated.GetUserParams(
                organization_id=as_uuid(organization_id), user_id=as_uuid(user_id)
            )
        )
        return isinstance(result.value, self._generated.GetUserFound)

    def list_tickets_by_project_status(
        self, organization_id: bytes, project_id: bytes, status: str, limit: int
    ) -> None:
        status_enum = self._generated.TicketStatus(sql_status_to_riff(status))
        self._client.list_tickets(
            self._generated.ListTicketsParams(
                organization_id=as_uuid(organization_id),
                project_id=as_uuid(project_id),
                statuses=(status_enum,),
                limit=limit,
            )
        )

    def list_open_tickets_for_assignee(
        self, organization_id: bytes, assignee_id: bytes, limit: int
    ) -> None:
        self._client.list_tickets_by_assignee(
            self._generated.ListTicketsByAssigneeParams(
                organization_id=as_uuid(organization_id),
                assignee_id=as_uuid(assignee_id),
                statuses=(self._generated.TicketStatus.OPEN,),
                limit=limit,
            )
        )

    def list_comments_for_ticket(
        self, organization_id: bytes, ticket_id: bytes, limit: int
    ) -> None:
        self._client.list_comments(
            self._generated.ListCommentsParams(
                organization_id=as_uuid(organization_id),
                ticket_id=as_uuid(ticket_id),
                limit=limit,
            )
        )

    def list_project_members(self, organization_id: bytes, project_id: bytes, limit: int) -> None:
        self._client.project_members(
            self._generated.ProjectMembersParams(
                organization_id=as_uuid(organization_id), project_id=as_uuid(project_id)
            )
        )

    def ticket_detail_page(self, organization_id: bytes, ticket_id: bytes) -> bool:
        result = self._client.ticket_page(
            self._generated.TicketPageParams(
                organization_id=as_uuid(organization_id), ticket_id=as_uuid(ticket_id)
            )
        )
        return isinstance(result.value, self._generated.TicketPageFound)

    def create_comment(self, comment: CommentSeed) -> None:
        result = self._client.create_comment(
            self._generated.CreateCommentInput(
                body=comment.row.body,
                author_id=as_uuid(comment.row.author_id),
                ticket_id=as_uuid(comment.row.ticket_id),
                comment_id=as_uuid(comment.row.comment_id),
                idempotency_key=comment.idempotency_key,
                organization_id=as_uuid(comment.row.organization_id),
            )
        )
        _require_outcome(result.outcome, "Created")

    def close_ticket_with_comment(self, input: CloseTicketWithCommentSeed) -> None:
        result = self._client.close_ticket_with_comment(
            self._generated.CloseTicketWithCommentInput(
                body=input.body,
                author_id=as_uuid(input.author_id),
                ticket_id=as_uuid(input.ticket_id),
                comment_id=as_uuid(input.comment_id),
                idempotency_key=input.idempotency_key,
                organization_id=as_uuid(input.organization_id),
            )
        )
        _require_outcome(result.outcome, "Closed")

    def open_ticket_with_labels(self, input: OpenTicketWithLabelsSeed) -> None:
        result = self._client.open_ticket_with_labels(
            self._generated.OpenTicketWithLabelsInput(
                title=input.title,
                label_a=as_uuid(input.label_a),
                label_b=as_uuid(input.label_b),
                ticket_id=as_uuid(input.ticket_id),
                project_id=as_uuid(input.project_id),
                assignee_id=as_uuid(input.assignee_id),
                reporter_id=as_uuid(input.reporter_id),
                idempotency_key=input.idempotency_key,
                organization_id=as_uuid(input.organization_id),
            )
        )
        _require_outcome(result.outcome, "Created")


class RiffDbDriver:
    """Seeds and opens generated-client sessions. Safety stays in riffdbd."""

    backend_id = RIFFDB_BACKEND_ID

    def __init__(self, endpoint: str, token: str) -> None:
        self.endpoint = endpoint
        self.token = token

    def seed(self, dataset: SeedDataset) -> None:
        # Seed through the generated bounded batch envelope, as the Rust harness
        # does. A sequential request-per-row loop runs at single-client rate,
        # which is invisible at `full` scale (19,220 commands) and unusable at
        # `production` scale (~1.1M). Every item remains an ordinary independent
        # generated command with its own durable lifecycle; only the transport is
        # batched, and the runtime's sync batch releases the GIL for each native
        # round trip.
        generated = _load_generated()
        session = RiffDbSession(self.endpoint, self.token)
        options = CommandBatchOptions(concurrency=SEED_CONCURRENCY)
        total = (
            len(dataset.organizations)
            + len(dataset.users)
            + len(dataset.projects)
            + len(dataset.members)
            + len(dataset.labels)
            + len(dataset.tickets)
            + len(dataset.comments)
            + len(dataset.ticket_labels)
        )
        completed = 0
        started = time.monotonic()
        print(
            f"riffdb-seed-start\ttotal={total}\tconcurrency={SEED_CONCURRENCY}",
            file=sys.stderr,
            flush=True,
        )

        def run_phase(phase: str, inputs: list[object], call: object) -> None:
            nonlocal completed
            for offset in range(0, len(inputs), SEED_BATCH_ROWS):
                chunk = inputs[offset : offset + SEED_BATCH_ROWS]
                result = call(chunk, options)
                if len(result.items) != len(chunk):
                    raise RiffDbError(
                        f"seed phase {phase} returned {len(result.items)} of {len(chunk)}"
                    )
                for item in result.items:
                    # A batch reports a per-item failure in `error` rather than
                    # raising, so a phase that ignored it would seed short.
                    if item.error is not None:
                        raise item.error
                    if item.result is None:
                        raise RiffDbError(
                            f"seed phase {phase} item {item.index} carried no result"
                        )
                    _require_outcome(item.result.outcome, "Created")
                completed += len(chunk)
            print(
                f"riffdb-seed-progress\tphase={phase}\tcompleted={completed}/{total}"
                f"\toverall_ms={int((time.monotonic() - started) * 1000)}",
                file=sys.stderr,
                flush=True,
            )

        try:
            client = session._client

            # Phases respect foreign-key order.
            run_phase(
                "organization",
                [
                    generated.CreateOrganizationInput(
                        name=name,
                        organization_id=as_uuid(org_id),
                        idempotency_key=f"seed-org-{encode_short(org_id)}",
                    )
                    for org_id, name in dataset.organizations
                ],
                client.create_organization_batch,
            )
            run_phase(
                "user",
                [
                    generated.CreateUserInput(
                        email=email,
                        user_id=as_uuid(user_id),
                        display_name=display,
                        organization_id=as_uuid(org_id),
                        idempotency_key=f"seed-user-{encode_short(user_id)}",
                    )
                    for org_id, user_id, email, display in dataset.users
                ],
                client.create_user_batch,
            )
            run_phase(
                "project",
                [
                    generated.CreateProjectInput(
                        name=name,
                        project_id=as_uuid(project_id),
                        organization_id=as_uuid(org_id),
                        idempotency_key=f"seed-project-{encode_short(project_id)}",
                    )
                    for org_id, project_id, name in dataset.projects
                ],
                client.create_project_batch,
            )
            run_phase(
                "member",
                [
                    generated.AddProjectMemberInput(
                        role=role,
                        user_id=as_uuid(user_id),
                        project_id=as_uuid(project_id),
                        organization_id=as_uuid(org_id),
                        idempotency_key=(
                            f"seed-member-{encode_short(project_id)}-{encode_short(user_id)}"
                        ),
                    )
                    for org_id, project_id, user_id, role in dataset.members
                ],
                client.add_project_member_batch,
            )
            run_phase(
                "label",
                [
                    generated.CreateLabelInput(
                        name=name,
                        label_id=as_uuid(label_id),
                        organization_id=as_uuid(org_id),
                        idempotency_key=f"seed-label-{encode_short(label_id)}",
                    )
                    for org_id, label_id, name in dataset.labels
                ],
                client.create_label_batch,
            )
            run_phase(
                "ticket",
                [
                    generated.CreateTicketInput(
                        title=ticket.title,
                        status=generated.TicketStatus(sql_status_to_riff(ticket.status)),
                        ticket_id=as_uuid(ticket.ticket_id),
                        project_id=as_uuid(ticket.project_id),
                        assignee_id=as_uuid(ticket.assignee_id),
                        reporter_id=as_uuid(ticket.reporter_id),
                        organization_id=as_uuid(ticket.organization_id),
                        idempotency_key=f"seed-ticket-{encode_short(ticket.ticket_id)}",
                    )
                    for ticket in dataset.tickets
                ],
                client.create_ticket_batch,
            )
            run_phase(
                "comment",
                [
                    generated.CreateCommentInput(
                        body=comment.body,
                        author_id=as_uuid(comment.author_id),
                        ticket_id=as_uuid(comment.ticket_id),
                        comment_id=as_uuid(comment.comment_id),
                        organization_id=as_uuid(comment.organization_id),
                        idempotency_key=f"seed-comment-{encode_short(comment.comment_id)}",
                    )
                    for comment in dataset.comments
                ],
                client.create_comment_batch,
            )
            run_phase(
                "ticket_label",
                [
                    generated.AttachLabelInput(
                        label_id=as_uuid(label_id),
                        ticket_id=as_uuid(ticket_id),
                        organization_id=as_uuid(org_id),
                        idempotency_key=(
                            f"seed-link-{encode_short(ticket_id)}-{encode_short(label_id)}"
                        ),
                    )
                    for org_id, ticket_id, label_id in dataset.ticket_labels
                ],
                client.attach_label_batch,
            )
            print(
                f"riffdb-seed-progress\tphase=done\tcompleted={completed}/{total}"
                f"\toverall_ms={int((time.monotonic() - started) * 1000)}",
                file=sys.stderr,
                flush=True,
            )
        finally:
            session.close()

    def open_session(self) -> RiffDbSession:
        return RiffDbSession(self.endpoint, self.token)


# Business-outcome collisions the Zipf workload produces on purpose: two
# clients target the same hot row and the loser observes the existing entity.
# They are contention, not failures, and the TypeScript harness already counts
# them as conflicts. Classifying them as errors here made the same events look
# like defects in one harness and normal load in the other.
_COLLISION_OUTCOMES = frozenset({"CommentExists", "TicketExists", "LinkExists"})


def _require_outcome(outcome: object, expected: str) -> None:
    actual = getattr(outcome, "outcome", None)
    if actual == expected:
        return
    code = "RDB-COMMAND-0102" if actual in _COLLISION_OUTCOMES else None
    raise RiffDbError(f"unexpected outcome {actual!r}, wanted {expected}", code)
