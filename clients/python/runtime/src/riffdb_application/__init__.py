from __future__ import annotations

import base64
import json
import os
import re
import socket
import struct
import threading
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Any, Generic, Never, Self, TypeVar
from uuid import UUID, uuid4

from . import _native

T = TypeVar("T")
U = TypeVar("U")


def _validate_tenant(value: str) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 128
        or any(ord(ch) <= 0x20 or ord(ch) > 0x7E for ch in value)
    ):
        raise InvalidInput("RiffDB tenant must be 1..128 visible ASCII characters")
    return value


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
        except BaseException as error:
            raise _translate_native(error) from None

    @classmethod
    def from_protected_file(cls, path: str) -> Self:
        try:
            value = cls.__new__(cls)
            value.__native = _native._BearerCredential.from_protected_file(path)
            return value
        except BaseException as error:
            raise _translate_native(error) from None

    def has_same_presentation(self, other: BearerCredential) -> bool:
        try:
            return bool(self.__native.has_same_presentation(other.__native))
        except BaseException as error:
            raise _translate_native(error) from None

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
    tenant: str | None = None

    def __post_init__(self) -> None:
        if self.tenant is not None:
            _validate_tenant(self.tenant)

    @classmethod
    def authenticated(cls, credential: BearerCredential, *, tenant: str | None = None) -> Self:
        resolved_tenant = tenant if tenant is not None else os.environ.get("RIFFDB_TENANT")
        return cls(credential=credential, tenant=resolved_tenant)

    def with_database(self, database: DatabaseAlias) -> Self:
        return replace(self, database=database)

    def with_trace_parent(self, trace_parent: TraceParent) -> Self:
        return replace(self, trace_parent=trace_parent)

    def with_tenant(self, tenant: str) -> Self:
        return replace(self, tenant=_validate_tenant(tenant))

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


class QueryConsistency(StrEnum):
    ADMISSION_HEAD = "admission_head"


@dataclass(frozen=True, slots=True)
class QueryOptions:
    cursor: str | None = None
    read_after_commit: int | None = None
    consistency: QueryConsistency | None = None


@dataclass(frozen=True, slots=True)
class VectorInspectionOptions:
    cursor: bytes | None = None


@dataclass(frozen=True, slots=True)
class VectorStalenessItem:
    entity_key: bytes
    newest_source_write: int
    embedding_write: int | None


@dataclass(frozen=True, slots=True)
class VectorStalenessSummary:
    total_entities: int
    stale_count: int
    stale_entity_count_threshold: int
    slo_breached: bool


@dataclass(frozen=True, slots=True)
class VectorStaleEntities:
    items: tuple[VectorStalenessItem, ...]
    observed_frontier: int


@dataclass(frozen=True, slots=True)
class VectorModelVersionItem:
    entity_key: bytes
    model: str
    model_version: str
    embedding_write: int


@dataclass(frozen=True, slots=True)
class VectorModelVersionSummary:
    current_count: int
    outdated_count: int


@dataclass(frozen=True, slots=True)
class VectorOutdatedModelEntities:
    items: tuple[VectorModelVersionItem, ...]
    observed_frontier: int


VectorStalenessResult = VectorStalenessSummary | VectorStaleEntities
VectorModelVersionResult = VectorModelVersionSummary | VectorOutdatedModelEntities


@dataclass(frozen=True, slots=True)
class TypedVectorInspectionResult(Generic[T]):
    value: T
    next_cursor: bytes | None = None


@dataclass(frozen=True, slots=True)
class VerifiedTlsConfig:
    endpoint: str
    trust_root: str = ""
    server_name: str = ""
    pool_connections: int = 4
    streams_per_connection: int = 64

    def __post_init__(self) -> None:
        if (
            not isinstance(self.endpoint, str)
            or not isinstance(self.trust_root, str)
            or not isinstance(self.server_name, str)
            or type(self.pool_connections) is not int
            or type(self.streams_per_connection) is not int
            or not 1 <= self.pool_connections <= 16
            or not 1 <= self.streams_per_connection <= 256
        ):
            raise InvalidInput("verified TLS configuration is invalid")


_ENV_PREFIX_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _resolve_client_env(
    prefix: str | None = None,
    *,
    driver_socket: str | None = None,
    endpoint: str | None = None,
    credential: BearerCredential | None = None,
    credential_file: str | None = None,
    database: DatabaseAlias | str | None = None,
    trust_root: str | None = None,
    server_name: str | None = None,
    tenant: str | None = None,
    pool_connections: int = 4,
    streams_per_connection: int = 64,
) -> tuple[str, VerifiedTlsConfig | None, CallMetadata]:
    if prefix is None or prefix == "":
        candidate_prefixes = ["RIFFDB_"]
    else:
        if not isinstance(prefix, str) or not _ENV_PREFIX_RE.match(prefix):
            raise InvalidInput("invalid RiffDB environment prefix")
        normalized = prefix if prefix.endswith("_") else f"{prefix}_"
        if normalized == "RIFFDB_" or normalized.endswith("RIFFDB_"):
            candidate_prefixes = [normalized]
        else:
            candidate_prefixes = [f"{normalized}RIFFDB_", normalized]

    def read_env(*suffixes: str) -> str | None:
        for candidate_prefix in candidate_prefixes:
            for suffix in suffixes:
                raw = os.environ.get(f"{candidate_prefix}{suffix}")
                if raw is not None and raw.strip():
                    return raw.strip()
        return None

    resolved_socket = (
        driver_socket if driver_socket is not None else read_env("DRIVER_SOCKET")
    )
    resolved_endpoint = endpoint if endpoint is not None else read_env("ENDPOINT")
    if resolved_socket and endpoint is None:
        resolved_endpoint = (
            resolved_socket
            if resolved_socket.startswith("unix://")
            else f"unix://{resolved_socket}"
        )
    if not resolved_endpoint:
        raise InvalidInput("missing RiffDB endpoint or driver socket in environment")

    is_unix_socket = resolved_endpoint.startswith("unix://") or resolved_endpoint.startswith("/")

    resolved_credential = credential
    if resolved_credential is None and not is_unix_socket:
        resolved_cred_file = (
            credential_file
            if credential_file is not None
            else read_env("CREDENTIAL_FILE")
        )
        if resolved_cred_file is not None:
            resolved_credential = BearerCredential.from_protected_file(resolved_cred_file)
        elif (raw_cred := read_env("CREDENTIAL")) is not None:
            resolved_credential = BearerCredential(raw_cred)

    resolved_db: DatabaseAlias | None
    if isinstance(database, DatabaseAlias):
        resolved_db = database
    elif isinstance(database, str):
        resolved_db = DatabaseAlias(database)
    elif (raw_db := read_env("DATABASE")) is not None:
        resolved_db = DatabaseAlias(raw_db)
    else:
        resolved_db = None

    resolved_tenant = tenant if tenant is not None else read_env("TENANT")
    metadata = CallMetadata(
        credential=resolved_credential,
        database=resolved_db,
        tenant=resolved_tenant,
    )
    if is_unix_socket:
        return (resolved_endpoint, None, metadata)

    resolved_trust_root = (
        trust_root
        if trust_root is not None
        else read_env("TLS_TRUST_ROOT", "TRUST_ROOT_FILE", "CA_CERT_FILE")
    )
    resolved_server_name = (
        server_name
        if server_name is not None
        else read_env("TLS_SERVER_NAME", "SERVER_NAME")
    )
    if (
        resolved_endpoint.startswith("https://")
        or resolved_trust_root is not None
        or resolved_server_name is not None
    ):
        tls_config = VerifiedTlsConfig(
            endpoint=resolved_endpoint,
            trust_root=resolved_trust_root or "",
            server_name=resolved_server_name or "",
            pool_connections=pool_connections,
            streams_per_connection=streams_per_connection,
        )
        return (resolved_endpoint, tls_config, metadata)
    return (resolved_endpoint, None, metadata)


MAX_COMMAND_BATCH_CONCURRENCY = 384


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

    @property
    def outcome(self) -> str:
        return str(getattr(self.value, "outcome"))

    def is_outcome(self, expected: str) -> bool:
        return self.outcome == expected

    def unwrap_found(self) -> T:
        if self.outcome.lower() == "found":
            return self.value
        raise ValueError(
            f"expected query outcome 'found' for {self.identity.query_name}, got '{self.outcome}'"
        )

    def _map_value(self, transform: Callable[[T], U]) -> TypedQueryResult[U]:
        return TypedQueryResult(
            self.identity, transform(self.value), self.application_head, self.next_cursor
        )


@dataclass(frozen=True, slots=True)
class WorkflowSuccessorRevision:
    binding: str
    revision: int


@dataclass(frozen=True, slots=True)
class TypedCommandResult(Generic[T]):
    outcome: T
    commit_sequence: int | None
    contract_version: int
    plan_hash: str
    replayed: bool
    outcome_uri: str | None = None
    workflow_revisions: tuple[WorkflowSuccessorRevision, ...] = ()

    @property
    def outcome_name(self) -> str:
        return str(getattr(self.outcome, "outcome"))

    def is_outcome(self, expected: str) -> bool:
        return self.outcome_name == expected

    def _map_outcome(self, transform: Callable[[T], U]) -> TypedCommandResult[U]:
        return TypedCommandResult(
            transform(self.outcome),
            self.commit_sequence,
            self.contract_version,
            self.plan_hash,
            self.replayed,
            self.outcome_uri,
            self.workflow_revisions,
        )

    def _with_workflow_revisions(
        self, revisions: tuple[WorkflowSuccessorRevision, ...]
    ) -> TypedCommandResult[T]:
        return TypedCommandResult(
            self.outcome,
            self.commit_sequence,
            self.contract_version,
            self.plan_hash,
            self.replayed,
            self.outcome_uri,
            revisions,
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
    HISTORY_PRUNED = "RDB-HISTORY-0102"
    OVERLOADED = "RDB-CAPACITY-0101"
    PROJECTION_DIVERGED = "RDB-PROJECTION-0101"
    SNAPSHOT_RETIRED = "RDB-PROJECTION-0102"
    FRESHNESS_UNSATISFIED = "RDB-PROJECTION-0103"


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


def _translate_native(error: BaseException) -> Exception:
    error_type = type(error)
    if not isinstance(error, Exception):
        if (
            error_type.__module__ == "pyo3_runtime"
            and error_type.__name__ == "PanicException"
        ):
            return ProtocolError("the native RiffDB client failed")
        raise error
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


_DRIVER_PROTOCOL_VERSION = 4
_DRIVER_VALUE_REGISTRY_HASH = (
    "8e1681ddf5e6a82e7fa646f9737128ad7e36f54f8b5846ac6e33e732125407e5"
)
_DRIVER_ERROR_REGISTRY_HASH = (
    "b94d685ecbc18f2369a2bfa1a53139d06100699c4ee41b31c86d6a7e17039850"
)
_MAX_DRIVER_FRAME_BYTES = 16 * 1024 * 1024


def _normalize_driver_socket_path(socket_path: str) -> str:
    if not isinstance(socket_path, str):
        raise InvalidInput("driver socket path must be a string")
    raw = socket_path[7:] if socket_path.startswith("unix://") else socket_path
    if not raw.startswith("/") or len(raw.encode("utf-8")) > 4096:
        raise InvalidInput("driver socket path must be an absolute path")
    return raw


def _python_value_to_driver_value(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or not isinstance(value.get("kind"), str):
        raise InvalidInput("application input is invalid")
    kind = value["kind"]
    if kind == "null" and len(value) == 1:
        return {"type": "null"}
    if kind == "bool" and len(value) == 2 and type(value.get("value")) is bool:
        return {"type": "bool", "value": value["value"]}
    if kind == "i64" and len(value) == 2 and type(value.get("value")) is int:
        return {"type": "i64", "value": str(value["value"])}
    if kind == "u64" and len(value) == 2 and type(value.get("value")) is int:
        return {"type": "u64", "value": str(value["value"])}
    if kind in {"string", "uuid", "enum"} and len(value) == 2 and isinstance(value.get("value"), str):
        return {"type": kind, "value": value["value"]}
    if kind == "bytes" and len(value) == 2 and isinstance(value.get("value"), str):
        try:
            raw_bytes = bytes.fromhex(value["value"])
        except ValueError:
            raise InvalidInput("application input is invalid") from None
        return {"type": "bytes", "value": base64.b64encode(raw_bytes).decode("ascii")}
    if kind == "date" and len(value) == 2 and type(value.get("value")) is int:
        return {"type": "date", "value": str(value["value"])}
    if (
        kind == "timestamp"
        and len(value) == 3
        and type(value.get("seconds")) is int
        and type(value.get("nanos")) is int
    ):
        return {
            "type": "timestamp",
            "value": {"seconds": str(value["seconds"]), "nanos": value["nanos"]},
        }
    if kind == "decimal" and len(value) in {3, 4} and isinstance(value.get("coefficient"), str) and type(value.get("scale")) is int:
        try:
            raw_coeff = bytes.fromhex(value["coefficient"])
        except ValueError:
            raise InvalidInput("application input is invalid") from None
        return {
            "type": "decimal",
            "value": {
                "coefficient": base64.b64encode(raw_coeff).decode("ascii"),
                "scale": value["scale"],
                "precision": value.get("precision"),
            },
        }
    if kind == "money" and len(value) == 3 and isinstance(value.get("currency"), str):
        amount = _python_value_to_driver_value(value.get("amount"))
        if amount.get("type") != "decimal":
            raise InvalidInput("application input is invalid")
        return {
            "type": "money",
            "value": {"currency": value["currency"], "amount": amount["value"]},
        }
    if kind == "vector" and len(value) == 2 and isinstance(value.get("components"), list):
        bits = [
            struct.unpack(">I", struct.pack(">f", float(c)))[0]
            for c in value["components"]
        ]
        return {"type": "vector", "value": {"component_bits": bits}}
    if kind == "list" and len(value) == 2 and isinstance(value.get("value"), list):
        return {
            "type": "list",
            "value": [_python_value_to_driver_value(item) for item in value["value"]],
        }
    if kind == "record" and len(value) == 2 and isinstance(value.get("value"), dict):
        return {
            "type": "record",
            "value": {
                str(k): _python_value_to_driver_value(v)
                for k, v in value["value"].items()
            },
        }
    raise InvalidInput("application input is invalid")


def _driver_decimal_to_python(dec: object) -> dict[str, Any]:
    if not isinstance(dec, dict) or not isinstance(dec.get("coefficient"), str) or type(dec.get("scale")) is not int:
        raise ProtocolError("the RiffDB peer returned an invalid application response")
    try:
        coeff_hex = base64.b64decode(dec["coefficient"]).hex()
    except Exception:
        raise ProtocolError("the RiffDB peer returned an invalid application response") from None
    return {
        "$riffdb": "decimal",
        "coefficient": coeff_hex,
        "scale": dec["scale"],
        "precision": dec.get("precision"),
    }


def _driver_value_to_python(value: object) -> Any:
    if not isinstance(value, dict) or not isinstance(value.get("type"), str):
        raise ProtocolError("the RiffDB peer returned an invalid application response")
    vtype = value["type"]
    if vtype == "null":
        return None
    inner = value.get("value")
    if vtype == "bool" and type(inner) is bool:
        return inner
    if vtype in {"i64", "u64"} and isinstance(inner, str):
        return int(inner)
    if vtype == "string" and isinstance(inner, str):
        return inner
    if vtype in {"uuid", "enum"} and isinstance(inner, str):
        return {"$riffdb": vtype, "value": inner}
    if vtype == "bytes" and isinstance(inner, str):
        return {"$riffdb": "bytes", "value": base64.b64decode(inner).hex()}
    if vtype == "date" and isinstance(inner, str):
        return {"$riffdb": "date", "value": int(inner)}
    if vtype == "timestamp" and isinstance(inner, dict):
        return {
            "$riffdb": "timestamp",
            "seconds": int(inner["seconds"]),
            "nanos": int(inner["nanos"]),
        }
    if vtype == "decimal":
        return _driver_decimal_to_python(inner)
    if vtype == "money" and isinstance(inner, dict) and isinstance(inner.get("currency"), str):
        return {
            "$riffdb": "money",
            "currency": inner["currency"],
            "amount": _driver_decimal_to_python(inner.get("amount")),
        }
    if vtype == "vector" and isinstance(inner, dict) and isinstance(inner.get("component_bits"), list):
        return {
            "$riffdb": "vector",
            "components": [
                struct.unpack(">f", struct.pack(">I", int(b)))[0]
                for b in inner["component_bits"]
            ],
        }
    if vtype == "list" and isinstance(inner, list):
        return [_driver_value_to_python(item) for item in inner]
    if vtype == "record" and isinstance(inner, dict):
        return {str(k): _driver_value_to_python(v) for k, v in inner.items()}
    raise ProtocolError("the RiffDB peer returned an invalid application response")


def _driver_record_with_outcome_to_python(value: object) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or value.get("type") != "record"
        or not isinstance(value.get("value"), dict)
    ):
        raise ProtocolError("the RiffDB peer returned an invalid application response")
    fields = value["value"]
    out: dict[str, Any] = {}
    for k, v in fields.items():
        if k == "outcome" and isinstance(v, dict) and v.get("type") == "enum" and isinstance(v.get("value"), str):
            out["outcome"] = v["value"]
        else:
            out[str(k)] = _driver_value_to_python(v)
    return out


def _translate_driver_error(response: dict[str, Any]) -> Exception:
    code_str = str(response.get("code", ""))
    message = str(response.get("message", "driver request failed"))
    category = str(response.get("category", "protocol"))
    recovery_action = str(response.get("recovery_action", "contact_operator"))
    operation = response.get("operation")
    if response.get("outcome_uncertain") is True or code_str == "RDB-UNCERTAIN-0101":
        return OutcomeUnknown("the command outcome remains unknown")
    if code_str in {"RDB-DRIVER-0003", "RDB-DRIVER-0005"}:
        return InvalidInput(message)
    if code_str in {"RDB-DRIVER-0001", "RDB-DRIVER-0002", "RDB-DRIVER-0004"}:
        return ProtocolError(message)
    try:
        app_code = ApplicationErrorCode(code_str)
    except ValueError:
        if category == "transport":
            return ConnectionFailure("the RiffDB transport is unavailable")
        return ProtocolError(message)
    raw_trace = response.get("trace_id")
    raw_incident = response.get("incident_id")
    details = ApplicationErrorDetails(
        code=app_code,
        message=message,
        category=category,
        recovery_action=recovery_action,
        operation=str(operation) if isinstance(operation, str) and operation else "ApplicationRequest",
        contract_lineage=response.get("contract_lineage") if isinstance(response.get("contract_lineage"), str) else None,
        contract_version=response.get("contract_version") if type(response.get("contract_version")) is int else None,
        operation_symbol=str(operation) if isinstance(operation, str) else None,
        symbol_path=tuple(str(s) for s in response.get("symbol_path", ()) if isinstance(s, str)),
        source_span=None,
        fixes=(),
        trace_id=UUID(raw_trace) if isinstance(raw_trace, str) else None,
        incident_id=UUID(raw_incident) if isinstance(raw_incident, str) else None,
    )
    return RiffDbApplicationError(details)


def _encode_driver_frame(payload: dict[str, Any]) -> bytes:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    if not 1 <= len(body) <= _MAX_DRIVER_FRAME_BYTES:
        raise InvalidInput("driver frame length is invalid")
    return struct.pack(">I", len(body)) + body


class _DriverSocketSyncClient:
    def __init__(self, socket_path: str, database: str | None) -> None:
        self._socket_path = _normalize_driver_socket_path(socket_path)
        self._database = database or ""
        self._lock = threading.Lock()
        self._sock: socket.socket | None = None
        self._handshaken_lineage: str | None = None
        self._handshaken_version: int | None = None
        self._handshaken_bundle_hash: str | None = None
        # Probe connectivity eagerly so invalid/dead sockets fail at connect time.
        probe = self._open_socket()
        self._sock = probe

    def _open_socket(self) -> socket.socket:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(self._socket_path)
        except OSError:
            sock.close()
            raise ConnectionFailure("the RiffDB transport is unavailable") from None
        return sock

    def _recv_exact(self, sock: socket.socket, count: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < count:
            try:
                chunk = sock.recv(count - len(chunks))
            except OSError:
                raise ConnectionFailure("the RiffDB transport is unavailable") from None
            if not chunk:
                raise ConnectionFailure("the RiffDB transport is unavailable")
            chunks.extend(chunk)
        return bytes(chunks)

    def _send_and_recv(self, sock: socket.socket, request: dict[str, Any]) -> dict[str, Any]:
        frame = _encode_driver_frame(request)
        try:
            sock.sendall(frame)
        except OSError:
            raise ConnectionFailure("the RiffDB transport is unavailable") from None
        prefix = self._recv_exact(sock, 4)
        (length,) = struct.unpack(">I", prefix)
        if not 1 <= length <= _MAX_DRIVER_FRAME_BYTES:
            raise ProtocolError("the RiffDB peer returned an invalid application response")
        body = self._recv_exact(sock, length)
        try:
            decoded = json.loads(body.decode("utf-8"))
        except Exception:
            raise ProtocolError("the RiffDB peer returned an invalid application response") from None
        if not isinstance(decoded, dict) or not isinstance(decoded.get("type"), str):
            raise ProtocolError("the RiffDB peer returned an invalid application response")
        if decoded["type"] == "error":
            raise _translate_driver_error(decoded)
        return decoded

    def _ensure_session(
        self, contract_lineage: str, contract_version: int, contract_bundle_hash: str = ""
    ) -> socket.socket:
        if self._sock is None:
            self._sock = self._open_socket()
            self._handshaken_lineage = None
        if self._handshaken_lineage == contract_lineage and self._handshaken_version == contract_version:
            return self._sock
        req = {
            "type": "handshake",
            "request_id": f"hs-{uuid4()}",
            "protocol_version": _DRIVER_PROTOCOL_VERSION,
            "application_manifest_hash": "",
            "operation_catalog_hash": "",
            "contract_lineage": contract_lineage,
            "contract_version": contract_version,
            "contract_bundle_hash": contract_bundle_hash,
            "value_registry_hash": _DRIVER_VALUE_REGISTRY_HASH,
            "error_registry_hash": _DRIVER_ERROR_REGISTRY_HASH,
            "database": self._database,
            "role": "",
            "role_definition_hash": "",
            "remote_identity_hash": "",
        }
        try:
            resp = self._send_and_recv(self._sock, req)
        except ConnectionFailure:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = self._open_socket()
            resp = self._send_and_recv(self._sock, req)
        if resp.get("type") != "handshake":
            raise ProtocolError("the RiffDB peer returned an invalid application response")
        self._handshaken_lineage = str(resp.get("contract_lineage", contract_lineage))
        self._handshaken_version = int(resp.get("contract_version", contract_version))
        self._handshaken_bundle_hash = str(resp.get("contract_bundle_hash", contract_bundle_hash))
        return self._sock

    def execute_named_query(self, encoded: str) -> str:
        req = json.loads(encoded)
        parameters = {
            str(k): _python_value_to_driver_value(v)
            for k, v in req["parameters"].items()
        }
        invoke_req = {
            "type": "invoke",
            "request_id": f"q-{uuid4()}",
            "operation": req["query_name"],
            "input_schema_hash": req["plan_hash"],
            "input": parameters,
            "options": {
                "deadline_millis": 30000,
                "maximum_attempts": 1,
                "read_after_commit": req.get("read_after_commit"),
                "cursor": req.get("cursor"),
                "query_consistency": req.get("query_consistency"),
                "accept_compact_result": bool(req.get("accept_compact_result", False)),
                "accept_packed_result": bool(req.get("accept_packed_result", False)),
            },
        }
        with self._lock:
            sock = self._ensure_session(
                req["contract_lineage"],
                int(req["contract_version"]),
                str(req.get("contract_bundle_hash", "")),
            )
            resp = self._send_and_recv(sock, invoke_req)
        identity = {
            "contract_lineage": req["contract_lineage"],
            "contract_version": int(req["contract_version"]),
            "contract_bundle_hash": req["contract_bundle_hash"],
            "module_hash": req["module_hash"],
            "query_name": req["query_name"],
            "plan_hash": req["plan_hash"],
        }
        rtype = resp["type"]
        if rtype == "packed_query_result":
            value: dict[str, Any] = {
                "$riffdb_packed": {
                    "outcome": resp["outcome"],
                    "result_name": resp["result_name"],
                    "entity": resp["entity"],
                    "fields": resp["fields"],
                    "row_count": resp["row_count"],
                    "columns": resp["columns"],
                }
            }
            head = int(resp["application_head"])
        elif rtype == "compact_query_result":
            rows = [
                [_driver_value_to_python(cell) for cell in row]
                for row in resp["rows"]
            ]
            value = {
                "$riffdb_compact": {
                    "outcome": resp["outcome"],
                    "result_name": resp["result_name"],
                    "entity": resp["entity"],
                    "fields": resp["fields"],
                    "rows": rows,
                }
            }
            head = int(resp["application_head"])
        elif rtype == "result":
            value = _driver_record_with_outcome_to_python(resp["value"])
            head = int(resp.get("application_head") or 0)
        else:
            raise ProtocolError("the RiffDB peer returned an invalid application response")
        return json.dumps(
            {
                "value": value,
                "identity": identity,
                "application_head": head,
                "next_cursor": resp.get("cursor"),
            },
            separators=(",", ":"),
        )

    def execute_command(self, encoded: str) -> str:
        req = json.loads(encoded)
        inputs = {
            str(k): _python_value_to_driver_value(v)
            for k, v in req["input"].items()
        }
        invoke_req = {
            "type": "invoke",
            "request_id": f"c-{uuid4()}",
            "operation": req["command_name"],
            "input_schema_hash": req["plan_hash"],
            "input": inputs,
            "options": {
                "deadline_millis": 30000,
                "maximum_attempts": int(req["maximum_submissions"]),
                "read_after_commit": None,
                "cursor": None,
                "query_consistency": None,
                "accept_compact_result": False,
                "accept_packed_result": False,
            },
        }
        with self._lock:
            sock = self._ensure_session(
                req["contract_lineage"],
                int(req["contract_version"]),
            )
            resp = self._send_and_recv(sock, invoke_req)
        if resp.get("type") != "result":
            raise ProtocolError("the RiffDB peer returned an invalid application response")
        outcome = _driver_record_with_outcome_to_python(resp["value"])
        return json.dumps(
            {
                "outcome": outcome,
                "commit_sequence": resp.get("application_head"),
                "contract_version": int(req["contract_version"]),
                "plan_hash": req["plan_hash"],
                "replayed": bool(resp.get("replayed", False)),
                "outcome_uri": resp.get("cursor"),
            },
            separators=(",", ":"),
        )

    def inspect_vector_state(self, encoded: str) -> str:
        req = json.loads(encoded)
        cursor_hex = req.get("cursor")
        cursor_b64 = (
            base64.b64encode(bytes.fromhex(cursor_hex)).decode("ascii")
            if isinstance(cursor_hex, str)
            else None
        )
        invoke_req = {
            "type": "invoke",
            "request_id": f"v-{uuid4()}",
            "operation": f"{req['entity']}.{req['field']}:{req['inspection_kind']}",
            "input_schema_hash": req["contract_bundle_hash"],
            "input": {
                "partition": _python_value_to_driver_value(req["partition"]),
                "limit": {"type": "u64", "value": str(req["limit"])},
            },
            "options": {
                "deadline_millis": 30000,
                "maximum_attempts": 1,
                "read_after_commit": None,
                "cursor": cursor_b64,
                "query_consistency": None,
                "accept_compact_result": False,
                "accept_packed_result": False,
            },
        }
        with self._lock:
            sock = self._ensure_session(
                req["contract_lineage"],
                int(req["contract_version"]),
                str(req["contract_bundle_hash"]),
            )
            resp = self._send_and_recv(sock, invoke_req)
        if resp.get("type") != "result":
            raise ProtocolError("the RiffDB peer returned an invalid application response")
        record = _driver_record_with_outcome_to_python(resp["value"])
        kind_val = record.get("kind")
        if isinstance(kind_val, dict) and kind_val.get("$riffdb") == "enum":
            record["kind"] = kind_val["value"]
        if record.get("kind") in {"stale_entities", "outdated_model_entities"}:
            next_b64 = resp.get("cursor")
            record["next_cursor"] = (
                base64.b64decode(next_b64).hex() if isinstance(next_b64, str) else None
            )
            for item in record.get("items", []):
                if isinstance(item, dict) and isinstance(item.get("entity_key"), dict):
                    item["entity_key"] = item["entity_key"]["value"]
        return json.dumps(record, separators=(",", ":"))

    def close(self) -> None:
        with self._lock:
            if self._sock is not None:
                try:
                    self._sock.close()
                except OSError:
                    pass
                self._sock = None


class _DriverSocketAsyncClient:
    def __init__(self, socket_path: str, database: str | None) -> None:
        self._sync = _DriverSocketSyncClient(socket_path, database)

    async def execute_named_query(self, encoded: str) -> str:
        import asyncio

        return await asyncio.to_thread(self._sync.execute_named_query, encoded)

    async def execute_command(self, encoded: str) -> str:
        import asyncio

        return await asyncio.to_thread(self._sync.execute_command, encoded)

    async def inspect_vector_state(self, encoded: str) -> str:
        import asyncio

        return await asyncio.to_thread(self._sync.inspect_vector_state, encoded)

    async def consume_event_stream(self, encoded: str) -> str:
        del encoded
        raise ProtocolError("streaming and reactive operations require a direct gRPC transport")

    async def consume_contextual_subscription(self, encoded: str) -> str:
        del encoded
        raise ProtocolError("streaming and reactive operations require a direct gRPC transport")

    async def mutate_contextual_subscription(self, encoded: str) -> str:
        del encoded
        raise ProtocolError("streaming and reactive operations require a direct gRPC transport")

    async def contextual_subscription_status(self, encoded: str) -> str:
        del encoded
        raise ProtocolError("streaming and reactive operations require a direct gRPC transport")

    async def execute_contextual_reaction(self, encoded: str) -> str:
        del encoded
        raise ProtocolError("streaming and reactive operations require a direct gRPC transport")

    async def mutate_event_consumer(self, encoded: str) -> str:
        del encoded
        raise ProtocolError("streaming and reactive operations require a direct gRPC transport")

    async def seek_event_consumer(self, encoded: str) -> str:
        del encoded
        raise ProtocolError("streaming and reactive operations require a direct gRPC transport")

    async def event_consumer_status(self, encoded: str) -> str:
        del encoded
        raise ProtocolError("streaming and reactive operations require a direct gRPC transport")

    async def watch_named_query(self, encoded: str) -> str:
        del encoded
        raise ProtocolError("streaming and reactive operations require a direct gRPC transport")

    async def close(self) -> None:
        self._sync.close()


class SyncApplicationTransport:
    __slots__ = ("_client", "_closed", "_tenant")

    def __init__(
        self,
        client: _native._SyncClient | _DriverSocketSyncClient,
        tenant: str | None = None,
    ) -> None:
        resolved_tenant = tenant if tenant is not None else os.environ.get("RIFFDB_TENANT")
        self._client = client
        self._closed = False
        self._tenant = None if resolved_tenant is None else _validate_tenant(resolved_tenant)

    @property
    def tenant(self) -> str | None:
        return self._tenant

    @classmethod
    def connect_driver_socket(
        cls, socket_path: str, metadata: CallMetadata = CallMetadata()
    ) -> Self:
        return cls(
            _DriverSocketSyncClient(
                socket_path,
                None if metadata.database is None else metadata.database.value,
            ),
            tenant=metadata.tenant,
        )

    @classmethod
    def connect_uri(cls, endpoint: str, metadata: CallMetadata) -> Self:
        if isinstance(endpoint, str) and (
            endpoint.startswith("unix://") or endpoint.startswith("/")
        ):
            return cls.connect_driver_socket(endpoint, metadata)
        try:
            return cls(
                _native._SyncClient.connect_uri(endpoint, metadata._native_value()),
                tenant=metadata.tenant,
            )
        except BaseException as error:
            raise _translate_native(error) from None

    @classmethod
    def connect_verified_tls(cls, config: VerifiedTlsConfig, metadata: CallMetadata) -> Self:
        try:
            return cls(
                _native._SyncClient.connect_verified_tls(
                    config.endpoint,
                    config.trust_root,
                    config.server_name,
                    config.pool_connections,
                    config.streams_per_connection,
                    metadata._native_value(),
                ),
                tenant=metadata.tenant,
            )
        except BaseException as error:
            raise _translate_native(error) from None

    @classmethod
    def connect_from_env(
        cls,
        prefix: str | None = None,
        *,
        driver_socket: str | None = None,
        endpoint: str | None = None,
        credential: BearerCredential | None = None,
        credential_file: str | None = None,
        database: DatabaseAlias | str | None = None,
        trust_root: str | None = None,
        server_name: str | None = None,
        tenant: str | None = None,
        pool_connections: int = 4,
        streams_per_connection: int = 64,
    ) -> Self:
        resolved_endpoint, tls_config, metadata = _resolve_client_env(
            prefix,
            driver_socket=driver_socket,
            endpoint=endpoint,
            credential=credential,
            credential_file=credential_file,
            database=database,
            trust_root=trust_root,
            server_name=server_name,
            tenant=tenant,
            pool_connections=pool_connections,
            streams_per_connection=streams_per_connection,
        )
        if tls_config is not None:
            return cls.connect_verified_tls(tls_config, metadata)
        return cls.connect_uri(resolved_endpoint, metadata)

    def close(self) -> None:
        if not self._closed:
            try:
                self._client.close()
                self._closed = True
            except BaseException as error:
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
        request["query_consistency"] = options.consistency
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
        except BaseException as error:
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
        except BaseException as error:
            raise _translate_native(error) from None

    def _inspect_vector_state(
        self, **request: object
    ) -> TypedVectorInspectionResult[VectorStalenessResult | VectorModelVersionResult]:
        self._require_open()
        options = request.pop("options")
        assert isinstance(options, VectorInspectionOptions)
        request["cursor"] = None if options.cursor is None else options.cursor.hex()
        try:
            encoded = json.dumps(request, separators=(",", ":"))
            return _vector_inspection_result(
                json.loads(self._client.inspect_vector_state(encoded))
            )
        except (
            ProtocolError,
            InvalidInput,
            RiffDbApplicationError,
            ConnectionFailure,
            OutcomeUnknown,
        ):
            raise
        except BaseException as error:
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
    __slots__ = ("_client", "_closed", "_tenant")

    def __init__(
        self,
        client: _native._AsyncClient | _DriverSocketAsyncClient,
        tenant: str | None = None,
    ) -> None:
        resolved_tenant = tenant if tenant is not None else os.environ.get("RIFFDB_TENANT")
        self._client = client
        self._closed = False
        self._tenant = None if resolved_tenant is None else _validate_tenant(resolved_tenant)

    @property
    def tenant(self) -> str | None:
        return self._tenant

    @classmethod
    async def connect_driver_socket(
        cls, socket_path: str, metadata: CallMetadata = CallMetadata()
    ) -> Self:
        return cls(
            _DriverSocketAsyncClient(
                socket_path,
                None if metadata.database is None else metadata.database.value,
            ),
            tenant=metadata.tenant,
        )

    @classmethod
    async def connect_uri(cls, endpoint: str, metadata: CallMetadata) -> Self:
        if isinstance(endpoint, str) and (
            endpoint.startswith("unix://") or endpoint.startswith("/")
        ):
            return await cls.connect_driver_socket(endpoint, metadata)
        try:
            return cls(
                await _native.connect_async(endpoint, metadata._native_value()),
                tenant=metadata.tenant,
            )
        except BaseException as error:
            raise _translate_native(error) from None

    @classmethod
    async def connect_verified_tls(
        cls, config: VerifiedTlsConfig, metadata: CallMetadata
    ) -> Self:
        try:
            return cls(
                await _native.connect_async_verified_tls(
                    config.endpoint,
                    config.trust_root,
                    config.server_name,
                    config.pool_connections,
                    config.streams_per_connection,
                    metadata._native_value(),
                ),
                tenant=metadata.tenant,
            )
        except BaseException as error:
            raise _translate_native(error) from None

    @classmethod
    async def connect_from_env(
        cls,
        prefix: str | None = None,
        *,
        driver_socket: str | None = None,
        endpoint: str | None = None,
        credential: BearerCredential | None = None,
        credential_file: str | None = None,
        database: DatabaseAlias | str | None = None,
        trust_root: str | None = None,
        server_name: str | None = None,
        tenant: str | None = None,
        pool_connections: int = 4,
        streams_per_connection: int = 64,
    ) -> Self:
        resolved_endpoint, tls_config, metadata = _resolve_client_env(
            prefix,
            driver_socket=driver_socket,
            endpoint=endpoint,
            credential=credential,
            credential_file=credential_file,
            database=database,
            trust_root=trust_root,
            server_name=server_name,
            tenant=tenant,
            pool_connections=pool_connections,
            streams_per_connection=streams_per_connection,
        )
        if tls_config is not None:
            return await cls.connect_verified_tls(tls_config, metadata)
        return await cls.connect_uri(resolved_endpoint, metadata)

    async def close(self) -> None:
        if not self._closed:
            try:
                await self._client.close()
                self._closed = True
            except BaseException as error:
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
        request["query_consistency"] = options.consistency
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
        except BaseException as error:
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
        except BaseException as error:
            raise _translate_native(error) from None

    async def _inspect_vector_state(
        self, **request: object
    ) -> TypedVectorInspectionResult[VectorStalenessResult | VectorModelVersionResult]:
        self._require_open()
        options = request.pop("options")
        assert isinstance(options, VectorInspectionOptions)
        request["cursor"] = None if options.cursor is None else options.cursor.hex()
        try:
            encoded = json.dumps(request, separators=(",", ":"))
            return _vector_inspection_result(
                json.loads(await self._client.inspect_vector_state(encoded))
            )
        except (
            ProtocolError,
            InvalidInput,
            RiffDbApplicationError,
            ConnectionFailure,
            OutcomeUnknown,
        ):
            raise
        except BaseException as error:
            raise _translate_native(error) from None

    async def _consume_event_batch(self, **request: object) -> dict[str, object]:
        self._require_open()
        request.setdefault("batch_limit", 1)
        request.setdefault("in_flight_limit", 16)
        request.setdefault("lease_seconds", 60)
        request.setdefault("maximum_wait_nanos", 0)
        try:
            encoded = json.dumps(request, separators=(",", ":"))
            batch = json.loads(await self._client.consume_event_stream(encoded))
            if (
                not isinstance(batch, dict)
                or not isinstance(batch.get("events"), list)
                or not isinstance(batch.get("status"), dict)
                or batch.get("disposition")
                not in {"ready", "wait_timed_out", "bounded_progress"}
                or not isinstance(batch.get("wait_timed_out"), bool)
            ):
                raise ProtocolError("the native RiffDB event response was invalid")
            events: list[dict[str, object]] = []
            for raw in batch["events"]:
                if not isinstance(raw, dict) or not isinstance(raw.get("fields"), dict):
                    raise ProtocolError("the native RiffDB event delivery was invalid")
                events.append(
                    {
                        "type": raw.get("type"),
                        **raw["fields"],
                        "_delivery": {
                            key: value for key, value in raw.items() if key != "fields"
                        },
                    }
                )
            return {**batch, "events": events}
        except (
            ProtocolError,
            InvalidInput,
            RiffDbApplicationError,
            ConnectionFailure,
            OutcomeUnknown,
        ):
            raise
        except BaseException as error:
            raise _translate_native(error) from None

    async def _consume_event_stream(self, **request: object) -> AsyncIterator[dict[str, object]]:
        request.setdefault("maximum_wait_nanos", 30_000_000_000)
        while not self._closed:
            batch = await self._consume_event_batch(**request)
            events = batch["events"]
            assert isinstance(events, list)
            for event in events:
                yield event

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
        except BaseException as error:
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
        except BaseException as error:
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
        except BaseException as error:
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
        except BaseException as error:
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
        except BaseException as error:
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
        except BaseException as error:
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
        except BaseException as error:
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
            except BaseException as error:
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


def _vector_inspection_result(
    value: object,
) -> TypedVectorInspectionResult[VectorStalenessResult | VectorModelVersionResult]:
    if not isinstance(value, dict) or not isinstance(value.get("kind"), str):
        raise ProtocolError("invalid RiffDB vector inspection response")
    try:
        kind = value["kind"]
        if kind == "staleness_summary":
            expected = {
                "kind",
                "total_entities",
                "stale_count",
                "stale_entity_count_threshold",
                "slo_breached",
            }
            if set(value) != expected:
                raise ValueError
            result: VectorStalenessResult | VectorModelVersionResult = VectorStalenessSummary(
                total_entities=_vector_u64(value["total_entities"]),
                stale_count=_vector_u64(value["stale_count"]),
                stale_entity_count_threshold=_vector_u64(
                    value["stale_entity_count_threshold"]
                ),
                slo_breached=_vector_bool(value["slo_breached"]),
            )
            cursor = None
        elif kind == "model_version_summary":
            expected = {"kind", "current_count", "outdated_count"}
            if set(value) != expected:
                raise ValueError
            result = VectorModelVersionSummary(
                current_count=_vector_u64(value["current_count"]),
                outdated_count=_vector_u64(value["outdated_count"]),
            )
            cursor = None
        elif kind in {"stale_entities", "outdated_model_entities"}:
            expected = {"kind", "items", "next_cursor", "observed_frontier"}
            if set(value) != expected or not isinstance(value["items"], list):
                raise ValueError
            cursor_value = value["next_cursor"]
            cursor = None if cursor_value is None else _vector_hex_bytes(cursor_value)
            if kind == "stale_entities":
                items = tuple(
                    VectorStalenessItem(
                        entity_key=_vector_hex_bytes(item["entity_key"]),
                        newest_source_write=_vector_u64(item["newest_source_write"]),
                        embedding_write=(
                            None
                            if item["embedding_write"] is None
                            else _vector_u64(item["embedding_write"])
                        ),
                    )
                    for item in value["items"]
                    if isinstance(item, dict)
                    and set(item)
                    == {"entity_key", "newest_source_write", "embedding_write"}
                )
                if len(items) != len(value["items"]):
                    raise ValueError
                result = VectorStaleEntities(
                    items, _vector_u64(value["observed_frontier"])
                )
            else:
                model_items = tuple(
                    VectorModelVersionItem(
                        entity_key=_vector_hex_bytes(item["entity_key"]),
                        model=_vector_string(item["model"]),
                        model_version=_vector_string(item["model_version"]),
                        embedding_write=_vector_u64(item["embedding_write"]),
                    )
                    for item in value["items"]
                    if isinstance(item, dict)
                    and set(item)
                    == {"entity_key", "model", "model_version", "embedding_write"}
                )
                if len(model_items) != len(value["items"]):
                    raise ValueError
                result = VectorOutdatedModelEntities(
                    model_items, _vector_u64(value["observed_frontier"])
                )
        else:
            raise ValueError
        return TypedVectorInspectionResult(result, cursor)
    except (KeyError, TypeError, ValueError, OverflowError):
        raise ProtocolError("invalid RiffDB vector inspection response") from None


def _vector_u64(value: object) -> int:
    if type(value) is not int or not 0 <= value <= (1 << 64) - 1:
        raise ValueError
    return value


def _vector_bool(value: object) -> bool:
    if type(value) is not bool:
        raise ValueError
    return value


def _vector_string(value: object) -> str:
    if type(value) is not str:
        raise ValueError
    return value


def _vector_hex_bytes(value: object) -> bytes:
    return bytes.fromhex(_vector_string(value))


def _validate_batch(inputs: Sequence[object], options: CommandBatchOptions) -> None:
    if (
        not 1 <= len(inputs) <= 4096
        or type(options.concurrency) is not int
        or not 1 <= options.concurrency <= MAX_COMMAND_BATCH_CONCURRENCY
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
    "MAX_COMMAND_BATCH_CONCURRENCY",
    "CommandBatchProgress",
    "CommandBatchResult",
    "ConnectionFailure",
    "DatabaseAlias",
    "InvalidInput",
    "Money",
    "OutcomeUnknown",
    "ProtocolError",
    "QueryOptions",
    "QueryConsistency",
    "QueryResponseIdentity",
    "RiffDate",
    "RiffDbApplicationError",
    "SourceSpan",
    "SyncApplicationTransport",
    "Timestamp",
    "TraceParent",
    "TypedCommandResult",
    "TypedQueryResult",
    "TypedVectorInspectionResult",
    "VerifiedTlsConfig",
    "VectorInspectionOptions",
    "VectorModelVersionItem",
    "VectorModelVersionResult",
    "VectorModelVersionSummary",
    "VectorOutdatedModelEntities",
    "VectorStaleEntities",
    "VectorStalenessItem",
    "VectorStalenessResult",
    "VectorStalenessSummary",
    "WorkflowSuccessorRevision",
]
