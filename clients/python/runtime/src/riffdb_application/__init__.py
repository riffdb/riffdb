from __future__ import annotations

import json
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Generic, Never, Self, TypeVar
from uuid import UUID

from . import _native

T = TypeVar("T")
U = TypeVar("U")


@dataclass(frozen=True, slots=True)
class RiffDate:
    days_since_unix_epoch: int

    def __post_init__(self) -> None:
        if type(self.days_since_unix_epoch) is not int or not (
            -(2**31) <= self.days_since_unix_epoch < 2**31
        ):
            raise InvalidInput("RiffDB date is outside the signed epoch-day range")

    @classmethod
    def from_date(cls, value: date) -> Self:
        return cls((value - date(1970, 1, 1)).days)

    def to_date(self) -> date:
        try:
            return date.fromordinal(date(1970, 1, 1).toordinal() + self.days_since_unix_epoch)
        except (OverflowError, ValueError):
            raise InvalidInput("RiffDB date is outside Python's calendar range") from None


@dataclass(frozen=True, slots=True)
class Timestamp:
    seconds: int
    nanos: int

    def __post_init__(self) -> None:
        if (
            type(self.seconds) is not int
            or type(self.nanos) is not int
            or not -(2**63) <= self.seconds < 2**63
            or not 0 <= self.nanos < 1_000_000_000
        ):
            raise InvalidInput("RiffDB timestamp is outside its exact range")


@dataclass(frozen=True, slots=True)
class Money:
    currency: str
    amount: Decimal

    def __post_init__(self) -> None:
        if (
            not isinstance(self.currency, str)
            or len(self.currency) != 3
            or not self.currency.isascii()
            or not self.currency.isupper()
        ):
            raise InvalidInput("RiffDB money currency must be three uppercase ASCII letters")
        if not isinstance(self.amount, Decimal) or not self.amount.is_finite():
            raise InvalidInput("RiffDB money amount must be finite")


class BearerCredential:
    __slots__ = ("__native",)

    def __init__(self, token: str) -> None:
        try:
            self.__native = _native._BearerCredential(token)
        except Exception as error:
            raise _translate_native(error) from None

    @classmethod
    def from_protected_file(cls, path: str) -> Self:
        try:
            value = cls.__new__(cls)
            value.__native = _native._BearerCredential.from_protected_file(path)
            return value
        except Exception as error:
            raise _translate_native(error) from None

    def has_same_presentation(self, other: BearerCredential) -> bool:
        return bool(self.__native.has_same_presentation(other.__native))

    def _native_value(self) -> _native._BearerCredential:
        return self.__native

    def __repr__(self) -> str:
        return "BearerCredential([REDACTED])"

    def __reduce__(self) -> Never:
        raise TypeError("bearer credentials cannot be serialized")


@dataclass(frozen=True, slots=True)
class DatabaseAlias:
    value: str


@dataclass(frozen=True, slots=True)
class TraceParent:
    value: str


@dataclass(frozen=True, slots=True)
class CallMetadata:
    credential: BearerCredential | None = None
    database: DatabaseAlias | None = None
    trace_parent: TraceParent | None = None

    @classmethod
    def authenticated(cls, credential: BearerCredential) -> Self:
        return cls(credential=credential)

    def with_database(self, database: DatabaseAlias) -> Self:
        return replace(self, database=database)

    def with_trace_parent(self, trace_parent: TraceParent) -> Self:
        return replace(self, trace_parent=trace_parent)

    def _native_value(self) -> _native._CallMetadata:
        return _native._CallMetadata(
            None if self.credential is None else self.credential._native_value(),
            None if self.database is None else self.database.value,
            None if self.trace_parent is None else self.trace_parent.value,
        )


@dataclass(frozen=True, slots=True)
class AttemptBudget:
    maximum_submissions: int

    def __post_init__(self) -> None:
        if type(self.maximum_submissions) is not int or not (
            1 <= self.maximum_submissions <= 2**32 - 1
        ):
            raise InvalidInput("attempt budget must be a positive u32")


@dataclass(frozen=True, slots=True)
class QueryOptions:
    cursor: str | None = None
    read_after_commit: int | None = None


@dataclass(frozen=True, slots=True)
class CommandBatchOptions:
    concurrency: int
    checkpoint: int = 0


@dataclass(frozen=True, slots=True)
class CommandBatchProgress:
    completed: int
    total: int
    checkpoint: int


@dataclass(frozen=True, slots=True)
class CommandBatchItem(Generic[T]):
    index: int
    result: TypedCommandResult[T] | None = None
    error: Exception | None = None


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
    next_cursor: str | None = None

    def _map_value(self, transform: Callable[[T], U]) -> TypedQueryResult[U]:
        return TypedQueryResult(
            self.identity, transform(self.value), self.application_head, self.next_cursor
        )


@dataclass(frozen=True, slots=True)
class TypedCommandResult(Generic[T]):
    outcome: T
    commit_sequence: int | None
    contract_version: int
    plan_hash: str
    replayed: bool
    outcome_uri: str | None = None

    def _map_outcome(self, transform: Callable[[T], U]) -> TypedCommandResult[U]:
        return TypedCommandResult(
            transform(self.outcome),
            self.commit_sequence,
            self.contract_version,
            self.plan_hash,
            self.replayed,
            self.outcome_uri,
        )


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
    def __init__(self, details: ApplicationErrorDetails) -> None:
        self.details = details
        super().__init__(f"{details.code.value}: {details.message} [{details.operation}]")


class InvalidInput(Exception):
    pass


class ProtocolError(Exception):
    pass


class ConnectionFailure(Exception):
    pass


class OutcomeUnknown(Exception):
    pass


def _translate_native(error: Exception) -> Exception:
    if not isinstance(error, _native.NativeError) or len(error.args) != 2:
        return ProtocolError("the native RiffDB client failed")
    kind, encoded = error.args
    if kind == "invalid_input":
        return InvalidInput("application input is invalid")
    if kind == "protocol_error":
        return ProtocolError("the RiffDB peer returned an invalid application response")
    if kind == "connection_failure":
        return ConnectionFailure("the RiffDB transport is unavailable")
    if kind == "outcome_unknown":
        return OutcomeUnknown("the command outcome remains unknown")
    if kind == "application":
        try:
            value = json.loads(encoded)
            span = value.get("source_span")
            details = ApplicationErrorDetails(
                code=ApplicationErrorCode(value["code"]),
                message=value["message"],
                category=value["category"],
                recovery_action=value["recovery_action"],
                operation=value["operation"],
                contract_lineage=value.get("contract_lineage"),
                contract_version=value.get("contract_version"),
                operation_symbol=value.get("operation_symbol"),
                symbol_path=tuple(value["symbol_path"]),
                source_span=None if span is None else SourceSpan(span["start"], span["end"]),
                fixes=tuple(value["fixes"]),
                trace_id=None if value.get("trace_id") is None else UUID(value["trace_id"]),
                incident_id=(
                    None if value.get("incident_id") is None else UUID(value["incident_id"])
                ),
            )
            return RiffDbApplicationError(details)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return ProtocolError("the native RiffDB error response was invalid")
    return ProtocolError("the native RiffDB error classification was invalid")


class SyncApplicationTransport:
    __slots__ = ("_client", "_closed")

    def __init__(self, client: _native._SyncClient) -> None:
        self._client = client
        self._closed = False

    @classmethod
    def connect_uri(cls, endpoint: str, metadata: CallMetadata) -> Self:
        try:
            return cls(_native._SyncClient.connect_uri(endpoint, metadata._native_value()))
        except Exception as error:
            raise _translate_native(error) from None

    def close(self) -> None:
        if not self._closed:
            try:
                self._client.close()
                self._closed = True
            except Exception as error:
                raise _translate_native(error) from None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def _execute_named_query(self, **request: object) -> TypedQueryResult[dict[str, object]]:
        self._require_open()
        options = request.pop("options")
        assert isinstance(options, QueryOptions)
        request["cursor"] = options.cursor
        request["read_after_commit"] = options.read_after_commit
        try:
            encoded = json.dumps(request, separators=(",", ":"))
            return _query_result(json.loads(self._client.execute_named_query(encoded)))
        except (
            ProtocolError,
            InvalidInput,
            RiffDbApplicationError,
            ConnectionFailure,
            OutcomeUnknown,
        ):
            raise
        except Exception as error:
            raise _translate_native(error) from None

    def _execute_command(self, **request: object) -> TypedCommandResult[dict[str, object]]:
        self._require_open()
        attempts = request.pop("attempts")
        assert isinstance(attempts, AttemptBudget)
        request["maximum_submissions"] = attempts.maximum_submissions
        expected_plan = request["plan_hash"]
        expected_version = request["contract_version"]
        try:
            encoded = json.dumps(request, separators=(",", ":"))
            result = _command_result(json.loads(self._client.execute_command(encoded)))
            if result.plan_hash != expected_plan or result.contract_version != expected_version:
                raise ProtocolError("RiffDB application identity mismatch")
            return result
        except (
            ProtocolError,
            InvalidInput,
            RiffDbApplicationError,
            ConnectionFailure,
            OutcomeUnknown,
        ):
            raise
        except Exception as error:
            raise _translate_native(error) from None

    def _command_batch(
        self,
        inputs: Sequence[T],
        options: CommandBatchOptions,
        operation: Callable[[T], TypedCommandResult[U]],
        progress: Callable[[CommandBatchProgress], None] | None,
    ) -> CommandBatchResult[U]:
        return _sync_batch(inputs, options, operation, progress)

    def _require_open(self) -> None:
        if self._closed:
            raise ConnectionFailure("transport is closed")


class AsyncApplicationTransport:
    __slots__ = ("_client", "_closed")

    def __init__(self, client: _native._AsyncClient) -> None:
        self._client = client
        self._closed = False

    @classmethod
    async def connect_uri(cls, endpoint: str, metadata: CallMetadata) -> Self:
        try:
            return cls(await _native.connect_async(endpoint, metadata._native_value()))
        except Exception as error:
            raise _translate_native(error) from None

    async def close(self) -> None:
        if not self._closed:
            try:
                await self._client.close()
                self._closed = True
            except Exception as error:
                raise _translate_native(error) from None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        await self.close()

    async def _execute_named_query(self, **request: object) -> TypedQueryResult[dict[str, object]]:
        self._require_open()
        options = request.pop("options")
        assert isinstance(options, QueryOptions)
        request["cursor"] = options.cursor
        request["read_after_commit"] = options.read_after_commit
        try:
            encoded = json.dumps(request, separators=(",", ":"))
            return _query_result(json.loads(await self._client.execute_named_query(encoded)))
        except (
            ProtocolError,
            InvalidInput,
            RiffDbApplicationError,
            ConnectionFailure,
            OutcomeUnknown,
        ):
            raise
        except Exception as error:
            raise _translate_native(error) from None

    async def _execute_command(self, **request: object) -> TypedCommandResult[dict[str, object]]:
        self._require_open()
        attempts = request.pop("attempts")
        assert isinstance(attempts, AttemptBudget)
        request["maximum_submissions"] = attempts.maximum_submissions
        expected_plan = request["plan_hash"]
        expected_version = request["contract_version"]
        try:
            encoded = json.dumps(request, separators=(",", ":"))
            result = _command_result(json.loads(await self._client.execute_command(encoded)))
            if result.plan_hash != expected_plan or result.contract_version != expected_version:
                raise ProtocolError("RiffDB application identity mismatch")
            return result
        except (
            ProtocolError,
            InvalidInput,
            RiffDbApplicationError,
            ConnectionFailure,
            OutcomeUnknown,
        ):
            raise
        except Exception as error:
            raise _translate_native(error) from None

    async def _consume_event_stream(self, **request: object) -> AsyncIterator[dict[str, object]]:
        self._require_open()
        request.setdefault("batch_limit", 1)
        request.setdefault("in_flight_limit", 16)
        request.setdefault("lease_seconds", 60)
        request.setdefault("maximum_wait_nanos", 30_000_000_000)
        while not self._closed:
            try:
                encoded = json.dumps(request, separators=(",", ":"))
                batch = json.loads(await self._client.consume_event_stream(encoded))
                if not isinstance(batch, dict) or not isinstance(batch.get("events"), list):
                    raise ProtocolError("the native RiffDB event response was invalid")
                for raw in batch["events"]:
                    if not isinstance(raw, dict) or not isinstance(raw.get("fields"), dict):
                        raise ProtocolError("the native RiffDB event delivery was invalid")
                    yield {
                        "type": raw.get("type"),
                        **raw["fields"],
                        "_delivery": {
                            key: value for key, value in raw.items() if key != "fields"
                        },
                    }
            except (
                ProtocolError,
                InvalidInput,
                RiffDbApplicationError,
                ConnectionFailure,
                OutcomeUnknown,
            ):
                raise
            except Exception as error:
                raise _translate_native(error) from None

    async def _consume_contextual_subscription(
        self, **request: object
    ) -> dict[str, object] | None:
        self._require_open()
        request.setdefault("maximum_wait_nanos", 30_000_000_000)
        try:
            encoded = json.dumps(request, separators=(",", ":"))
            batch = json.loads(await self._client.consume_contextual_subscription(encoded))
            if not isinstance(batch, dict) or not isinstance(batch.get("items"), list):
                raise ProtocolError("the native RiffDB contextual response was invalid")
            items = batch["items"]
            if len(items) > 1 or any(not isinstance(item, dict) for item in items):
                raise ProtocolError("the native RiffDB contextual work item was invalid")
            return items[0] if items else None
        except (
            ProtocolError,
            InvalidInput,
            RiffDbApplicationError,
            ConnectionFailure,
            OutcomeUnknown,
        ):
            raise
        except Exception as error:
            raise _translate_native(error) from None

    async def _acknowledge_contextual_item(self, **request: object) -> str:
        return await self._mutate_contextual_item(request, "ack", 0)

    async def _negative_acknowledge_contextual_item(self, **request: object) -> str:
        retry_delay = request.pop("retry_delay_nanos", 0)
        if type(retry_delay) is not int:
            raise InvalidInput("contextual retry delay is invalid")
        return await self._mutate_contextual_item(request, "nack", retry_delay)

    async def _mutate_contextual_item(
        self, request: dict[str, object], action: str, retry_delay_nanos: int
    ) -> str:
        self._require_open()
        item = request.pop("item", None)
        try:
            request.update(
                action=action,
                event_id=getattr(item, "event_id"),
                lease_token=getattr(item, "lease_token"),
                history_incarnation=getattr(item, "history_incarnation"),
                retry_delay_nanos=retry_delay_nanos,
            )
            encoded = json.dumps(request, separators=(",", ":"))
            result = json.loads(await self._client.mutate_contextual_subscription(encoded))
            if not isinstance(result, dict) or not isinstance(result.get("result"), str):
                raise ProtocolError("the native RiffDB contextual mutation was invalid")
            return str(result["result"])
        except (
            ProtocolError,
            InvalidInput,
            RiffDbApplicationError,
            ConnectionFailure,
            OutcomeUnknown,
        ):
            raise
        except (AttributeError, TypeError):
            raise InvalidInput("contextual work item evidence is invalid") from None
        except Exception as error:
            raise _translate_native(error) from None

    async def _contextual_subscription_status(
        self, **request: object
    ) -> dict[str, object] | None:
        self._require_open()
        try:
            encoded = json.dumps(request, separators=(",", ":"))
            result = json.loads(await self._client.contextual_subscription_status(encoded))
            if result is not None and not isinstance(result, dict):
                raise ProtocolError("the native RiffDB contextual status was invalid")
            return result
        except (
            ProtocolError,
            InvalidInput,
            RiffDbApplicationError,
            ConnectionFailure,
            OutcomeUnknown,
        ):
            raise
        except Exception as error:
            raise _translate_native(error) from None

    async def _execute_contextual_reaction(
        self, **request: object
    ) -> TypedCommandResult[dict[str, object]]:
        self._require_open()
        expected_plan = request["plan_hash"]
        expected_version = request["contract_version"]
        try:
            encoded = json.dumps(request, separators=(",", ":"))
            result = _command_result(
                json.loads(await self._client.execute_contextual_reaction(encoded))
            )
            if result.plan_hash != expected_plan or result.contract_version != expected_version:
                raise ProtocolError("RiffDB contextual reaction identity mismatch")
            return result
        except (
            ProtocolError,
            InvalidInput,
            RiffDbApplicationError,
            ConnectionFailure,
            OutcomeUnknown,
        ):
            raise
        except Exception as error:
            raise _translate_native(error) from None

    async def _acknowledge_event(self, **request: object) -> str:
        request["action"] = "ack"
        request["retry_delay_nanos"] = None
        return await self._mutate_event_consumer(request)

    async def _negative_acknowledge_event(self, **request: object) -> str:
        request["action"] = "nack"
        return await self._mutate_event_consumer(request)

    async def _mutate_event_consumer(self, request: dict[str, object]) -> str:
        self._require_open()
        try:
            encoded = json.dumps(request, separators=(",", ":"))
            result = json.loads(await self._client.mutate_event_consumer(encoded))
            if not isinstance(result, dict) or not isinstance(result.get("result"), str):
                raise ProtocolError("the native RiffDB consumer mutation was invalid")
            return str(result["result"])
        except (
            ProtocolError,
            InvalidInput,
            RiffDbApplicationError,
            ConnectionFailure,
            OutcomeUnknown,
        ):
            raise
        except Exception as error:
            raise _translate_native(error) from None

    async def _seek_event_consumer(self, **request: object) -> str:
        self._require_open()
        try:
            encoded = json.dumps(request, separators=(",", ":"))
            result = json.loads(await self._client.seek_event_consumer(encoded))
            if not isinstance(result, dict) or not isinstance(result.get("result"), str):
                raise ProtocolError("the native RiffDB consumer seek was invalid")
            return str(result["result"])
        except (
            ProtocolError,
            InvalidInput,
            RiffDbApplicationError,
            ConnectionFailure,
            OutcomeUnknown,
        ):
            raise
        except Exception as error:
            raise _translate_native(error) from None

    async def _event_consumer_status(self, **request: object) -> dict[str, object] | None:
        self._require_open()
        try:
            encoded = json.dumps(request, separators=(",", ":"))
            result = json.loads(await self._client.event_consumer_status(encoded))
            if result is not None and not isinstance(result, dict):
                raise ProtocolError("the native RiffDB consumer status was invalid")
            return result
        except (
            ProtocolError,
            InvalidInput,
            RiffDbApplicationError,
            ConnectionFailure,
            OutcomeUnknown,
        ):
            raise
        except Exception as error:
            raise _translate_native(error) from None

    async def _watch_named_query(self, **request: object) -> AsyncIterator[dict[str, object]]:
        self._require_open()
        cursor = request.get("cursor")
        while not self._closed:
            request["cursor"] = cursor
            try:
                encoded = json.dumps(request, separators=(",", ":"))
                update = json.loads(await self._client.watch_named_query(encoded))
                if not isinstance(update, dict) or not isinstance(update.get("type"), str):
                    raise ProtocolError("the native RiffDB live update was invalid")
                yield update
                if update["type"] == "terminal":
                    return
                next_cursor = update.get("cursor")
                if not isinstance(next_cursor, str):
                    raise ProtocolError("the native RiffDB live cursor was invalid")
                cursor = next_cursor
            except (
                ProtocolError,
                InvalidInput,
                RiffDbApplicationError,
                ConnectionFailure,
                OutcomeUnknown,
            ):
                raise
            except Exception as error:
                raise _translate_native(error) from None

    async def _command_batch(
        self,
        inputs: Sequence[T],
        options: CommandBatchOptions,
        operation: Callable[[T], Awaitable[TypedCommandResult[U]]],
        progress: Callable[[CommandBatchProgress], None] | None,
    ) -> CommandBatchResult[U]:
        import asyncio

        _validate_batch(inputs, options)
        semaphore = asyncio.Semaphore(options.concurrency)

        async def run(index: int, item: T) -> CommandBatchItem[U]:
            async with semaphore:
                try:
                    return CommandBatchItem(index=index, result=await operation(item))
                except Exception as error:
                    return CommandBatchItem(index=index, error=error)

        items = list(
            await asyncio.gather(
                *(run(index, inputs[index]) for index in range(options.checkpoint, len(inputs)))
            )
        )
        checkpoint = _report_batch(items, options.checkpoint, len(inputs), progress)
        return CommandBatchResult(tuple(items), checkpoint)

    def _require_open(self) -> None:
        if self._closed:
            raise ConnectionFailure("transport is closed")


def _query_result(value: object) -> TypedQueryResult[dict[str, object]]:
    if not isinstance(value, dict) or not isinstance(value.get("identity"), dict):
        raise ProtocolError("invalid RiffDB query response")
    identity = QueryResponseIdentity(**value["identity"])
    result = value.get("value")
    if not isinstance(result, dict):
        raise ProtocolError("invalid RiffDB query response")
    return TypedQueryResult(
        identity, result, int(value["application_head"]), value.get("next_cursor")
    )


def _command_result(value: object) -> TypedCommandResult[dict[str, object]]:
    if not isinstance(value, dict) or not isinstance(value.get("outcome"), dict):
        raise ProtocolError("invalid RiffDB command response")
    return TypedCommandResult(
        value["outcome"],
        value.get("commit_sequence"),
        int(value["contract_version"]),
        str(value["plan_hash"]),
        bool(value["replayed"]),
        value.get("outcome_uri"),
    )


def _validate_batch(inputs: Sequence[object], options: CommandBatchOptions) -> None:
    if (
        not 1 <= len(inputs) <= 4096
        or type(options.concurrency) is not int
        or not 1 <= options.concurrency <= 128
    ):
        raise InvalidInput("command batch bounds are invalid")
    if type(options.checkpoint) is not int or not 0 <= options.checkpoint <= len(inputs):
        raise InvalidInput("command batch checkpoint is invalid")


def _report_batch[T](
    items: Sequence[CommandBatchItem[T]],
    checkpoint: int,
    total: int,
    progress: Callable[[CommandBatchProgress], None] | None,
) -> int:
    completed = checkpoint
    contiguous = checkpoint
    completed_indexes: set[int] = set()
    for item in sorted(items, key=lambda value: value.index):
        completed += 1
        completed_indexes.add(item.index)
        while contiguous in completed_indexes:
            contiguous += 1
        if progress is not None:
            progress(CommandBatchProgress(completed, total, contiguous))
    return contiguous


def _sync_batch(
    inputs: Sequence[T],
    options: CommandBatchOptions,
    operation: Callable[[T], TypedCommandResult[U]],
    progress: Callable[[CommandBatchProgress], None] | None,
) -> CommandBatchResult[U]:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    _validate_batch(inputs, options)
    items: list[CommandBatchItem[U]] = []
    with ThreadPoolExecutor(
        max_workers=options.concurrency, thread_name_prefix="riffdb-command"
    ) as pool:
        futures = {
            pool.submit(operation, inputs[index]): index
            for index in range(options.checkpoint, len(inputs))
        }
        for future in as_completed(futures):
            index = futures[future]
            try:
                items.append(CommandBatchItem(index=index, result=future.result()))
            except Exception as error:
                items.append(CommandBatchItem(index=index, error=error))
    items.sort(key=lambda value: value.index)
    checkpoint = _report_batch(items, options.checkpoint, len(inputs), progress)
    return CommandBatchResult(tuple(items), checkpoint)


__all__ = [
    "ApplicationErrorCode",
    "ApplicationErrorDetails",
    "AsyncApplicationTransport",
    "AttemptBudget",
    "BearerCredential",
    "CallMetadata",
    "CommandBatchItem",
    "CommandBatchOptions",
    "CommandBatchProgress",
    "CommandBatchResult",
    "ConnectionFailure",
    "DatabaseAlias",
    "InvalidInput",
    "Money",
    "OutcomeUnknown",
    "ProtocolError",
    "QueryOptions",
    "QueryResponseIdentity",
    "RiffDate",
    "RiffDbApplicationError",
    "SourceSpan",
    "SyncApplicationTransport",
    "Timestamp",
    "TraceParent",
    "TypedCommandResult",
    "TypedQueryResult",
]
