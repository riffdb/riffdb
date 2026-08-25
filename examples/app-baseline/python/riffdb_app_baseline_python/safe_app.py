"""PostgreSQL postgres_safe_app adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

try:
    import psycopg
except ImportError:  # pragma: no cover - unit tests import schema/seed without Postgres
    psycopg = None  # type: ignore[assignment]

from .ids import format_uuid
from .schema import (
    CLOSE_TICKET_SQL,
    COUNT_TICKET_SQL,
    COUNT_USER_SQL,
    IDEMPOTENCY_LOCK_SQL,
    INSERT_AUDIT_SQL,
    INSERT_COMMENT_SQL,
    INSERT_EVENT_SQL,
    INSERT_IDEMPOTENCY_SQL,
    INSERT_LABEL_SQL,
    INSERT_MEMBER_SQL,
    INSERT_ORGANIZATION_SQL,
    INSERT_OUTBOX_SQL,
    INSERT_PROJECT_SQL,
    INSERT_TICKET_LABEL_SQL,
    INSERT_TICKET_SQL,
    INSERT_USER_SQL,
    LIST_COMMENTS_SQL,
    LIST_OPEN_TICKETS_FOR_ASSIGNEE_SQL,
    LIST_PROJECT_MEMBERS_SQL,
    LIST_TICKETS_BY_PROJECT_STATUS_SQL,
    OPEN_TICKET_SQL,
    PERMISSION_SQL,
    SCHEMA_SQL,
    SELECT_TICKET_SQL,
    SELECT_USER_SQL,
    TICKET_DETAIL_LABELS_SQL,
    TICKET_DETAIL_SQL,
    safe_input_fingerprint,
    to_pyformat,
)
from .seed import (
    CloseTicketWithCommentSeed,
    CommentSeed,
    OpenTicketWithLabelsSeed,
    SeedDataset,
)

CONFLICT_STATES = {"23505", "23P01"}
UNAVAILABLE_STATES = {"57P03", "53300", "57P01", "57P02", "08006", "08001", "08004"}

TIMED_STATEMENTS = (
    SELECT_TICKET_SQL,
    SELECT_USER_SQL,
    LIST_TICKETS_BY_PROJECT_STATUS_SQL,
    LIST_OPEN_TICKETS_FOR_ASSIGNEE_SQL,
    LIST_COMMENTS_SQL,
    LIST_PROJECT_MEMBERS_SQL,
    TICKET_DETAIL_SQL,
    TICKET_DETAIL_LABELS_SQL,
    COUNT_TICKET_SQL,
    COUNT_USER_SQL,
    INSERT_COMMENT_SQL,
    CLOSE_TICKET_SQL,
    OPEN_TICKET_SQL,
    INSERT_TICKET_LABEL_SQL,
    PERMISSION_SQL,
    IDEMPOTENCY_LOCK_SQL,
    INSERT_IDEMPOTENCY_SQL,
    INSERT_AUDIT_SQL,
    INSERT_EVENT_SQL,
    INSERT_OUTBOX_SQL,
)


class SafeAppError(Exception):
    def __init__(self, message: str, sqlstate: str | None = None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


def classify_error(error: SafeAppError) -> str:
    state = error.sqlstate
    if state in CONFLICT_STATES:
        return "conflict"
    if state in UNAVAILABLE_STATES:
        return "unavailable"
    return "error"


def _db_error(exc: BaseException) -> SafeAppError:
    sqlstate = getattr(exc, "sqlstate", None)
    return SafeAppError(str(exc), sqlstate=sqlstate)


@dataclass
class SafeAppBackend:
    conn: Any

    @classmethod
    def connect(cls, url: str) -> SafeAppBackend:
        if psycopg is None:
            raise RuntimeError("psycopg is required to run the Python postgres_safe_app load")
        conn = psycopg.connect(url, autocommit=True, prepare_threshold=1)
        conn.execute("SET synchronous_commit = on")
        return cls(conn)

    def close(self) -> None:
        self.conn.close()

    def reset(self) -> None:
        self.conn.execute(SCHEMA_SQL)

    def prewarm(self, organization_id: bytes, ticket_id: bytes) -> None:
        probes_org = format_uuid(organization_id)
        probes_ticket = format_uuid(ticket_id)
        for statement in TIMED_STATEMENTS:
            try:
                self._exec(statement, _prewarm_params(statement, probes_org, probes_ticket))
            except Exception:
                if psycopg is not None:
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass

    def seed(self, dataset: SeedDataset) -> None:
        with self.conn.transaction():
            self._executemany(
                INSERT_ORGANIZATION_SQL,
                [(format_uuid(org_id), name) for org_id, name in dataset.organizations],
            )
            self._executemany(
                INSERT_USER_SQL,
                [
                    (format_uuid(org_id), format_uuid(user_id), email, display)
                    for org_id, user_id, email, display in dataset.users
                ],
            )
            self._executemany(
                INSERT_PROJECT_SQL,
                [
                    (format_uuid(org_id), format_uuid(project_id), name)
                    for org_id, project_id, name in dataset.projects
                ],
            )
            self._executemany(
                INSERT_MEMBER_SQL,
                [
                    (format_uuid(org_id), format_uuid(project_id), format_uuid(user_id), role)
                    for org_id, project_id, user_id, role in dataset.members
                ],
            )
            self._executemany(
                INSERT_LABEL_SQL,
                [
                    (format_uuid(org_id), format_uuid(label_id), name)
                    for org_id, label_id, name in dataset.labels
                ],
            )
            self._executemany(
                INSERT_TICKET_SQL,
                [
                    (
                        format_uuid(ticket.organization_id),
                        format_uuid(ticket.ticket_id),
                        format_uuid(ticket.project_id),
                        format_uuid(ticket.reporter_id),
                        format_uuid(ticket.assignee_id),
                        ticket.status,
                        ticket.title,
                    )
                    for ticket in dataset.tickets
                ],
            )
            self._executemany(
                INSERT_COMMENT_SQL,
                [
                    (
                        format_uuid(comment.organization_id),
                        format_uuid(comment.comment_id),
                        format_uuid(comment.ticket_id),
                        format_uuid(comment.author_id),
                        comment.body,
                    )
                    for comment in dataset.comments
                ],
            )
            self._executemany(
                INSERT_TICKET_LABEL_SQL,
                [
                    (format_uuid(org_id), format_uuid(ticket_id), format_uuid(label_id))
                    for org_id, ticket_id, label_id in dataset.ticket_labels
                ],
            )
        # Settle the freshly bulk-loaded database before measurement. Without
        # ANALYZE the measured window starts with no planner statistics and an
        # autoanalyze can fire inside it, which is the comparator's dominant
        # run-to-run variance source; CHECKPOINT then moves the seed's dirty
        # pages out of that window. This mirrors the Rust harness.
        try:
            self.conn.execute("VACUUM (ANALYZE)")
            self.conn.execute("CHECKPOINT")
        except Exception:
            try:
                self.conn.rollback()
            except Exception:
                pass

    def _exec(self, statement: str, params: tuple[object, ...] = ()) -> Any:
        try:
            return self.conn.execute(to_pyformat(statement), params)
        except Exception as exc:
            if psycopg is not None and isinstance(exc, psycopg.Error):
                raise _db_error(exc) from exc
            raise

    def _executemany(self, statement: str, rows: list[tuple[object, ...]]) -> None:
        if not rows:
            return
        with self.conn.cursor() as cursor:
            cursor.executemany(to_pyformat(statement), rows)

    def point_get_ticket(self, organization_id: bytes, ticket_id: bytes) -> bool:
        row = self._exec(
            SELECT_TICKET_SQL, (format_uuid(organization_id), format_uuid(ticket_id))
        ).fetchone()
        return row is not None

    def point_get_user(self, organization_id: bytes, user_id: bytes) -> bool:
        row = self._exec(
            SELECT_USER_SQL, (format_uuid(organization_id), format_uuid(user_id))
        ).fetchone()
        return row is not None

    def list_tickets_by_project_status(
        self, organization_id: bytes, project_id: bytes, status: str, limit: int
    ) -> None:
        self._exec(
            LIST_TICKETS_BY_PROJECT_STATUS_SQL,
            (format_uuid(organization_id), format_uuid(project_id), status, limit),
        ).fetchall()

    def list_open_tickets_for_assignee(
        self, organization_id: bytes, assignee_id: bytes, limit: int
    ) -> None:
        self._exec(
            LIST_OPEN_TICKETS_FOR_ASSIGNEE_SQL,
            (format_uuid(organization_id), format_uuid(assignee_id), limit),
        ).fetchall()

    def list_comments_for_ticket(
        self, organization_id: bytes, ticket_id: bytes, limit: int
    ) -> None:
        self._exec(
            LIST_COMMENTS_SQL, (format_uuid(organization_id), format_uuid(ticket_id), limit)
        ).fetchall()

    def list_project_members(self, organization_id: bytes, project_id: bytes, limit: int) -> None:
        self._exec(
            LIST_PROJECT_MEMBERS_SQL,
            (format_uuid(organization_id), format_uuid(project_id), limit),
        ).fetchall()

    def ticket_detail_page(self, organization_id: bytes, ticket_id: bytes) -> bool:
        row = self._exec(
            TICKET_DETAIL_SQL, (format_uuid(organization_id), format_uuid(ticket_id))
        ).fetchone()
        self._exec(
            TICKET_DETAIL_LABELS_SQL, (format_uuid(organization_id), format_uuid(ticket_id))
        ).fetchall()
        return row is not None

    def create_comment(self, comment: CommentSeed) -> None:
        fingerprint = safe_input_fingerprint(
            [
                comment.row.comment_id,
                comment.row.ticket_id,
                comment.row.author_id,
                comment.row.body.encode(),
            ]
        )
        try:
            with self.conn.transaction():
                if self._safe_admit(
                    comment.idempotency_key,
                    "create_comment",
                    comment.row.organization_id,
                    fingerprint,
                ):
                    return
                self._exec(
                    INSERT_COMMENT_SQL,
                    (
                        format_uuid(comment.row.organization_id),
                        format_uuid(comment.row.comment_id),
                        format_uuid(comment.row.ticket_id),
                        format_uuid(comment.row.author_id),
                        comment.row.body,
                    ),
                )
                self._safe_complete(
                    comment.idempotency_key,
                    "create_comment",
                    comment.row.organization_id,
                    "CommentCreated",
                )
        except Exception as exc:
            if isinstance(exc, SafeAppError):
                raise
            if psycopg is not None and isinstance(exc, psycopg.Error):
                raise _db_error(exc) from exc
            raise

    def close_ticket_with_comment(self, input: CloseTicketWithCommentSeed) -> None:
        fingerprint = safe_input_fingerprint(
            [input.ticket_id, input.comment_id, input.author_id, input.body.encode()]
        )
        try:
            with self.conn.transaction():
                if self._safe_admit(
                    input.idempotency_key,
                    "close_ticket_with_comment",
                    input.organization_id,
                    fingerprint,
                ):
                    return
                ticket_ok = self._exec(
                    COUNT_TICKET_SQL,
                    (format_uuid(input.organization_id), format_uuid(input.ticket_id)),
                ).fetchone()
                author_ok = self._exec(
                    COUNT_USER_SQL,
                    (format_uuid(input.organization_id), format_uuid(input.author_id)),
                ).fetchone()
                if not ticket_ok or ticket_ok[0] == 0 or not author_ok or author_ok[0] == 0:
                    raise SafeAppError("decode")
                self._exec(
                    CLOSE_TICKET_SQL,
                    (format_uuid(input.organization_id), format_uuid(input.ticket_id)),
                )
                self._exec(
                    INSERT_COMMENT_SQL,
                    (
                        format_uuid(input.organization_id),
                        format_uuid(input.comment_id),
                        format_uuid(input.ticket_id),
                        format_uuid(input.author_id),
                        input.body,
                    ),
                )
                self._safe_complete(
                    input.idempotency_key,
                    "close_ticket_with_comment",
                    input.organization_id,
                    "TicketClosedWithComment",
                )
        except Exception as exc:
            if isinstance(exc, SafeAppError):
                raise
            if psycopg is not None and isinstance(exc, psycopg.Error):
                raise _db_error(exc) from exc
            raise

    def open_ticket_with_labels(self, input: OpenTicketWithLabelsSeed) -> None:
        fingerprint = safe_input_fingerprint(
            [
                input.ticket_id,
                input.project_id,
                input.reporter_id,
                input.assignee_id,
                input.title.encode(),
                input.label_a,
                input.label_b,
            ]
        )
        try:
            with self.conn.transaction():
                if self._safe_admit(
                    input.idempotency_key,
                    "open_ticket_with_labels",
                    input.organization_id,
                    fingerprint,
                ):
                    return
                self._exec(
                    OPEN_TICKET_SQL,
                    (
                        format_uuid(input.organization_id),
                        format_uuid(input.ticket_id),
                        format_uuid(input.project_id),
                        format_uuid(input.reporter_id),
                        format_uuid(input.assignee_id),
                        input.title,
                    ),
                )
                self._exec(
                    INSERT_TICKET_LABEL_SQL,
                    (
                        format_uuid(input.organization_id),
                        format_uuid(input.ticket_id),
                        format_uuid(input.label_a),
                    ),
                )
                self._exec(
                    INSERT_TICKET_LABEL_SQL,
                    (
                        format_uuid(input.organization_id),
                        format_uuid(input.ticket_id),
                        format_uuid(input.label_b),
                    ),
                )
                self._safe_complete(
                    input.idempotency_key,
                    "open_ticket_with_labels",
                    input.organization_id,
                    "TicketOpenedWithLabels",
                )
        except Exception as exc:
            if isinstance(exc, SafeAppError):
                raise
            if psycopg is not None and isinstance(exc, psycopg.Error):
                raise _db_error(exc) from exc
            raise

    def _safe_admit(
        self, idempotency_key: str, operation: str, organization_id: bytes, fingerprint: str
    ) -> bool:
        authorized = self._exec(PERMISSION_SQL, (operation,)).fetchone()
        if authorized is None:
            raise SafeAppError("decode")
        existing = self._exec(IDEMPOTENCY_LOCK_SQL, (idempotency_key,)).fetchone()
        org_text = format_uuid(organization_id)
        if existing is not None:
            stored_operation, stored_organization, stored_fingerprint = existing
            if (
                stored_operation != operation
                or stored_organization != org_text
                or stored_fingerprint != fingerprint
            ):
                raise SafeAppError("decode")
            self._exec(INSERT_AUDIT_SQL, (idempotency_key, operation, org_text, "replayed"))
            return True
        self._exec(INSERT_IDEMPOTENCY_SQL, (idempotency_key, operation, org_text, fingerprint))
        return False

    def _safe_complete(
        self, idempotency_key: str, operation: str, organization_id: bytes, event_type: str
    ) -> None:
        org_text = format_uuid(organization_id)
        event_id = f"{operation}/{idempotency_key}"
        self._exec(INSERT_AUDIT_SQL, (idempotency_key, operation, org_text, "committed"))
        self._exec(INSERT_EVENT_SQL, (event_id, idempotency_key, event_type, org_text))
        self._exec(INSERT_OUTBOX_SQL, (event_id,))


class PostgresDriver:
    """Seeds and opens postgres_safe_app sessions. Safety lives in Python SQL."""

    backend_id = "postgres_safe_app"

    def __init__(self, url: str) -> None:
        self.url = url

    def seed(self, dataset: SeedDataset) -> None:
        backend = SafeAppBackend.connect(self.url)
        try:
            backend.reset()
            backend.seed(dataset)
        finally:
            backend.close()

    def open_session(self) -> SafeAppBackend:
        return SafeAppBackend.connect(self.url)


def _prewarm_params(statement: str, org: str, ticket: str) -> tuple[object, ...]:
    placeholders = statement.count("$")
    if placeholders == 0:
        return ()
    if "LIMIT $4" in statement:
        return (org, ticket, "open", 1)
    if "LIMIT $3" in statement:
        return (org, ticket, 1)
    if "principal = 'app-baseline'" in statement:
        return ("point_get_ticket",)
    if "idempotency_key = $1" in statement:
        return ("prewarm",)
    if placeholders == 1:
        return (org,)
    if placeholders >= 2:
        filled = [org, ticket] + [org] * (placeholders - 2)
        return tuple(filled[:placeholders])
    return ()
