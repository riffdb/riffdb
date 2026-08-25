"""Python TicketDesk load: postgres_safe_app vs generated RiffDB client.

The same Python interactive mix runs against two backends:

- PostgreSQL: authorization, idempotency, audit, event, and outbox live in
  Python SQL (`postgres_safe_app`).
- RiffDB: the generated TicketDesk client has no safety code; riffdbd (Rust)
  enforces those obligations.

The Rust evidentiary harness measures both sides in Rust. This package exists
so the language-runtime cost of implementing safety in Python is visible.
Reports are not evidentiary substitutes for `benchmarks/run-app-baseline`.
"""

from .ids import format_uuid, uuid_from_ordinal
from .schema import OBLIGATIONS, SCHEMA_SQL, safe_input_fingerprint
from .seed import Scale, SeedDataset

__all__ = [
    "OBLIGATIONS",
    "SCHEMA_SQL",
    "Scale",
    "SeedDataset",
    "format_uuid",
    "safe_input_fingerprint",
    "uuid_from_ordinal",
]
