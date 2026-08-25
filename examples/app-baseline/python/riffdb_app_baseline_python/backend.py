"""Shared load-session surface for Postgres (safety in SQL) and RiffDB (safety in Rust)."""

from __future__ import annotations

from typing import Protocol

from .seed import CloseTicketWithCommentSeed, CommentSeed, OpenTicketWithLabelsSeed, SeedDataset


class LoadSession(Protocol):
    def prewarm(self, organization_id: bytes, ticket_id: bytes) -> None: ...

    def point_get_ticket(self, organization_id: bytes, ticket_id: bytes) -> bool: ...

    def point_get_user(self, organization_id: bytes, user_id: bytes) -> bool: ...

    def list_tickets_by_project_status(
        self, organization_id: bytes, project_id: bytes, status: str, limit: int
    ) -> None: ...

    def list_open_tickets_for_assignee(
        self, organization_id: bytes, assignee_id: bytes, limit: int
    ) -> None: ...

    def list_comments_for_ticket(
        self, organization_id: bytes, ticket_id: bytes, limit: int
    ) -> None: ...

    def list_project_members(
        self, organization_id: bytes, project_id: bytes, limit: int
    ) -> None: ...

    def ticket_detail_page(self, organization_id: bytes, ticket_id: bytes) -> bool: ...

    def create_comment(self, comment: CommentSeed) -> None: ...

    def close_ticket_with_comment(self, input: CloseTicketWithCommentSeed) -> None: ...

    def open_ticket_with_labels(self, input: OpenTicketWithLabelsSeed) -> None: ...

    def close(self) -> None: ...


class LoadDriver(Protocol):
    backend_id: str

    def seed(self, dataset: SeedDataset) -> None: ...

    def open_session(self) -> LoadSession: ...
