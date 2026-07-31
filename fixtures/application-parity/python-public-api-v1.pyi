from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Final, Generic, Literal, Self, TypeVar
from uuid import UUID

T = TypeVar("T")

@dataclass(frozen=True, slots=True)
class RiffDate:
    days_since_unix_epoch: int
    @classmethod
    def from_date(cls, value: date) -> Self: ...
    def to_date(self) -> date: ...

@dataclass(frozen=True, slots=True)
class Timestamp:
    seconds: int
    nanos: int

@dataclass(frozen=True, slots=True)
class Money:
    currency: str
    amount: Decimal

class BearerCredential:
    def __init__(self, token: str) -> None: ...
    @classmethod
    def from_protected_file(cls, path: str) -> Self: ...
    def has_same_presentation(self, other: BearerCredential) -> bool: ...

@dataclass(frozen=True, slots=True)
class DatabaseAlias:
    value: str

@dataclass(frozen=True, slots=True)
class TraceParent:
    value: str

@dataclass(frozen=True, slots=True)
class CallMetadata:
    credential: BearerCredential | None = ...
    database: DatabaseAlias | None = ...
    trace_parent: TraceParent | None = ...
    @classmethod
    def authenticated(cls, credential: BearerCredential) -> Self: ...
    def with_database(self, database: DatabaseAlias) -> Self: ...
    def with_trace_parent(self, trace_parent: TraceParent) -> Self: ...

@dataclass(frozen=True, slots=True)
class AttemptBudget:
    maximum_submissions: int

@dataclass(frozen=True, slots=True)
class QueryOptions:
    cursor: str | None = ...
    read_after_commit: int | None = ...

@dataclass(frozen=True, slots=True)
class CommandBatchOptions:
    concurrency: int
    checkpoint: int = ...

@dataclass(frozen=True, slots=True)
class CommandBatchProgress:
    completed: int
    total: int
    checkpoint: int

@dataclass(frozen=True, slots=True)
class CommandBatchItem(Generic[T]):
    index: int
    result: TypedCommandResult[T] | None = ...
    error: Exception | None = ...

@dataclass(frozen=True, slots=True)
class CommandBatchResult(Generic[T]):
    items: tuple[CommandBatchItem[T], ...]
    checkpoint: int

@dataclass(frozen=True, slots=True)
class QueryResponseIdentity:
    contract_lineage: str
    contract_version: int
    contract_bundle_hash: str
    module_hash: str
    query_name: str
    plan_hash: str

@dataclass(frozen=True, slots=True)
class TypedQueryResult(Generic[T]):
    identity: QueryResponseIdentity
    value: T
    application_head: int
    next_cursor: str | None = ...

@dataclass(frozen=True, slots=True)
class TypedCommandResult(Generic[T]):
    outcome: T
    commit_sequence: int | None
    contract_version: int
    plan_hash: str
    replayed: bool
    outcome_uri: str | None = ...

class ApplicationErrorCode(StrEnum):
    INVALID_REQUEST = "RDB-APP-0001"
    INPUT_INVALID = "RDB-INPUT-0101"
    AUTHORIZATION_DENIED = "RDB-AUTH-0214"
    CONTRACT_MISMATCH = "RDB-CONTRACT-0101"
    QUERY_INVALID = "RDB-QUERY-0101"
    QUERY_UNAVAILABLE = "RDB-QUERY-0102"
    MODULE_UNAVAILABLE = "RDB-MODULE-0101"
    CURSOR_INVALID = "RDB-CURSOR-0101"
    RESPONSE_TOO_LARGE = "RDB-RESOURCE-0101"
    STORAGE_UNAVAILABLE = "RDB-STORAGE-0101"
    OUTCOME_UNKNOWN = "RDB-UNCERTAIN-0101"
    OPERATION_CANCELLED = "RDB-APP-0002"
    DEADLINE_EXCEEDED = "RDB-APP-0003"
    INTERNAL_DEFECT = "RDB-INTERNAL-0001"
    IDEMPOTENCY_KEY_REUSE = "RDB-COMMAND-0101"
    COMMAND_EXECUTION_FAILED = "RDB-COMMAND-0102"
    CAPABILITY_REVOKED = "RDB-AUTH-0215"
    PROTOCOL_INVALID = "RDB-PROTOCOL-0101"
    HISTORY_INCARNATION_MISMATCH = "RDB-HISTORY-0101"
    OVERLOADED = "RDB-CAPACITY-0101"

@dataclass(frozen=True, slots=True)
class SourceSpan:
    start: int
    end: int

@dataclass(frozen=True, slots=True)
class ApplicationErrorDetails:
    code: ApplicationErrorCode
    message: str
    category: str
    recovery_action: str
    operation: str
    contract_lineage: str | None
    contract_version: int | None
    operation_symbol: str | None
    symbol_path: tuple[str, ...]
    source_span: SourceSpan | None
    fixes: tuple[str, ...]
    trace_id: UUID | None
    incident_id: UUID | None

class RiffDbApplicationError(Exception):
    details: ApplicationErrorDetails
class InvalidInput(Exception): ...
class ProtocolError(Exception): ...
class ConnectionFailure(Exception): ...
class OutcomeUnknown(Exception): ...

class SyncApplicationTransport:
    @classmethod
    def connect_uri(cls, endpoint: str, metadata: CallMetadata) -> Self: ...
    def close(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None: ...

class AsyncApplicationTransport:
    @classmethod
    async def connect_uri(cls, endpoint: str, metadata: CallMetadata) -> Self: ...
    async def close(self) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None: ...

# Representative generated-module surface. Concrete names and fields are
# compiler-owned and frozen by per-application golden fixtures.
CONTRACT_LINEAGE: Final[str]
CONTRACT_VERSION: Final[int]
CONTRACT_BUNDLE_HASH: Final[str]
QUERY_MODULE_HASH: Final[str]

@dataclass(frozen=True, slots=True)
class ExampleCommandInput:
    idempotency_key: str
    entity_id: UUID
    amount: Decimal

@dataclass(frozen=True, slots=True)
class ExampleCommitted:
    outcome: Literal["Committed"]
    entity_id: UUID

ExampleCommandOutcome = ExampleCommitted

class ExampleModuleClient:
    def __init__(self, transport: SyncApplicationTransport, command_attempts: AttemptBudget) -> None: ...
    def example_command(self, input: ExampleCommandInput) -> TypedCommandResult[ExampleCommandOutcome]: ...
    def example_command_batch(
        self,
        inputs: Sequence[ExampleCommandInput],
        options: CommandBatchOptions,
        progress: Callable[[CommandBatchProgress], None] | None = ...,
    ) -> CommandBatchResult[ExampleCommandOutcome]: ...

class AsyncExampleModuleClient:
    def __init__(self, transport: AsyncApplicationTransport, command_attempts: AttemptBudget) -> None: ...
    async def example_command(self, input: ExampleCommandInput) -> TypedCommandResult[ExampleCommandOutcome]: ...
    async def example_command_batch(
        self,
        inputs: Sequence[ExampleCommandInput],
        options: CommandBatchOptions,
        progress: Callable[[CommandBatchProgress], None] | None = ...,
    ) -> CommandBatchResult[ExampleCommandOutcome]: ...
