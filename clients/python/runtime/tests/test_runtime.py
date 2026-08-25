from __future__ import annotations

import json
import pickle
import unittest
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Annotated
from uuid import UUID

from riffdb_application import (
    ApplicationErrorCode,
    AsyncApplicationTransport,
    AttemptBudget,
    BearerCredential,
    CallMetadata,
    CommandBatchOptions,
    DatabaseAlias,
    InvalidInput,
    Money,
    ProtocolError,
    RiffDbApplicationError,
    RiffDate,
    SyncApplicationTransport,
    Timestamp,
    TypedCommandResult,
    VectorModelVersionSummary,
    VectorStaleEntities,
    VerifiedTlsConfig,
    WorkflowSuccessorRevision,
)
from riffdb_application import _translate_native, _validate_batch, _vector_inspection_result
from riffdb_application import _native
from riffdb_application._binding import (
    canonical_value_encoded_length,
    decode_record,
    decode_variant,
    encode_reactive_record,
    encode_record,
    encode_value,
)


@dataclass(frozen=True, slots=True)
class Values:
    signed: Annotated[int, "i64"]
    unsigned: Annotated[int, "u64"]
    identifier: UUID
    amount: Decimal
    money: Money
    when: Timestamp
    day: RiffDate
    payload: bytes
    embedding: Annotated[tuple[float, ...], "vector<4>"]


class ReactiveState(StrEnum):
    OPEN = "Open"


@dataclass(frozen=True, slots=True)
class ReactiveParameters:
    amount: Decimal
    state: ReactiveState


class RuntimeTests(unittest.TestCase):
    def test_canonical_value_length_matches_nested_document_shape(self) -> None:
        value = {"kind": "record", "value": {"context": {"kind": "bytes", "value": "00010203"}}}
        self.assertEqual(canonical_value_encoded_length(value), 6 + 4 + 6 + 4)
        with self.assertRaises(ValueError):
            canonical_value_encoded_length({"kind": "peer-kind"})

    def test_vector_inspection_results_are_closed_and_cursor_bytes_are_exact(self) -> None:
        summary = _vector_inspection_result(
            {
                "kind": "model_version_summary",
                "current_count": 7,
                "outdated_count": 2,
            }
        )
        self.assertEqual(summary.value, VectorModelVersionSummary(7, 2))
        page = _vector_inspection_result(
            {
                "kind": "stale_entities",
                "items": [
                    {
                        "entity_key": "0001ff",
                        "newest_source_write": 9,
                        "embedding_write": None,
                    }
                ],
                "next_cursor": "0102",
                "observed_frontier": 11,
            }
        )
        self.assertIsInstance(page.value, VectorStaleEntities)
        self.assertEqual(page.next_cursor, b"\x01\x02")
        self.assertEqual(page.value.items[0].entity_key, b"\x00\x01\xff")
        with self.assertRaises(ProtocolError):
            _vector_inspection_result(
                {
                    "kind": "staleness_summary",
                    "total_entities": 1,
                    "stale_count": 0,
                    "stale_entity_count_threshold": 1,
                    "slo_breached": False,
                    "peer_extension": "must-not-be-accepted",
                }
            )
        with self.assertRaises(ProtocolError):
            _vector_inspection_result(
                {
                    "kind": "staleness_summary",
                    "total_entities": True,
                    "stale_count": 0,
                    "stale_entity_count_threshold": 1,
                    "slo_breached": False,
                }
            )
        with self.assertRaises(ProtocolError):
            _vector_inspection_result(
                {
                    "kind": "model_version_summary",
                    "current_count": "7",
                    "outdated_count": 2,
                }
            )

    def test_generated_value_encoder_supports_exact_partition_types(self) -> None:
        identifier = UUID(int=7)
        self.assertEqual(encode_value(identifier, UUID), {"kind": "uuid", "value": str(identifier)})

    def test_typed_command_mapping_preserves_workflow_revision_evidence(self) -> None:
        revision = WorkflowSuccessorRevision(binding="work", revision=8)
        result = TypedCommandResult(
            outcome={"outcome": "Claimed"},
            commit_sequence=7,
            contract_version=1,
            plan_hash="11" * 32,
            replayed=False,
            workflow_revisions=(revision,),
        )

        mapped = result._map_outcome(lambda value: value["outcome"])

        self.assertEqual(mapped.outcome, "Claimed")
        self.assertEqual(mapped.workflow_revisions, (revision,))

    def test_exact_values_cross_the_private_dto_without_float_or_range_loss(self) -> None:
        value = Values(
            signed=-(2**63),
            unsigned=2**64 - 1,
            identifier=UUID("018f47f0-6dc7-7000-8000-000000000001"),
            amount=Decimal("-12345678901234567890.00100"),
            money=Money("USD", Decimal("0.01")),
            when=Timestamp(-(2**63), 999_999_999),
            day=RiffDate(-(2**31)),
            payload=b"\x00\xff",
            embedding=(0.0, 1.5, -2.25, 0.5),
        )
        encoded = encode_record(value)
        self.assertEqual(encoded["signed"], {"kind": "i64", "value": -(2**63)})
        self.assertEqual(encoded["unsigned"], {"kind": "u64", "value": 2**64 - 1})
        self.assertNotIn("float", json.dumps(encoded))
        self.assertEqual(
            encoded["embedding"],
            {"kind": "vector", "components": [0.0, 1.5, -2.25, 0.5]},
        )
        for field in encoded.values():
            _native.validate_bridge_value(json.dumps(field))

    def test_response_decoder_preserves_exact_special_values(self) -> None:
        value = decode_record(
            Values,
            {
                "signed": -(2**63),
                "unsigned": 2**64 - 1,
                "identifier": {
                    "$riffdb": "uuid",
                    "value": "018f47f0-6dc7-7000-8000-000000000001",
                },
                "amount": {"$riffdb": "decimal", "coefficient": "ff", "scale": 2},
                "money": {
                    "$riffdb": "money",
                    "currency": "USD",
                    "amount": {"$riffdb": "decimal", "coefficient": "01", "scale": 2},
                },
                "when": {"$riffdb": "timestamp", "seconds": 1, "nanos": 2},
                "day": {"$riffdb": "date", "value": 3},
                "payload": {"$riffdb": "bytes", "value": "00ff"},
                "embedding": {
                    "$riffdb": "vector",
                    "components": [0.0, 1.5, -2.25, 0.5],
                },
            },
        )
        self.assertEqual(value.amount, Decimal("-0.01"))
        self.assertEqual(value.payload, b"\x00\xff")
        self.assertEqual(value.embedding, (0.0, 1.5, -2.25, 0.5))

        with self.assertRaises(ProtocolError):
            decode_record(
                Values,
                {
                    "signed": 0,
                    "unsigned": 0,
                    "identifier": {"$riffdb": "uuid", "value": str(UUID(int=0))},
                    "amount": {"$riffdb": "decimal", "coefficient": "00", "scale": 0},
                    "money": {
                        "$riffdb": "money",
                        "currency": "USD",
                        "amount": {"$riffdb": "decimal", "coefficient": "00", "scale": 0},
                    },
                    "when": {"$riffdb": "timestamp", "seconds": 0, "nanos": 0},
                    "day": {"$riffdb": "date", "value": 0},
                    "payload": {"$riffdb": "bytes", "value": ""},
                    "embedding": {"$riffdb": "vector", "components": [1.0]},
                },
            )

    def test_reactive_parameters_pin_decimal_and_enum_identity(self) -> None:
        encoded = encode_reactive_record(
            ReactiveParameters(amount=Decimal("12.30"), state=ReactiveState.OPEN),
            {
                "amount": {"kind": "decimal", "precision": 8, "scale": 2},
                "state": {
                    "kind": "enum",
                    "type_id": 4,
                    "variants": {"Open": 9},
                },
            },
        )
        self.assertEqual(encoded["amount"]["precision"], 8)
        self.assertEqual(encoded["state"]["type_id"], 4)
        self.assertEqual(encoded["state"]["variant_id"], 9)

        with self.assertRaises(InvalidInput):
            encode_reactive_record(
                ReactiveParameters(amount=Decimal("12.3"), state=ReactiveState.OPEN),
                {
                    "amount": {"kind": "decimal", "precision": 8, "scale": 2},
                    "state": {
                        "kind": "enum",
                        "type_id": 4,
                        "variants": {"Open": 9},
                    },
                },
            )

    def test_public_conversion_failures_are_closed(self) -> None:
        with self.assertRaises(InvalidInput):
            encode_record(object())
        with self.assertRaises(ProtocolError):
            decode_record(Values, {"submitted_secret": "must-not-escape"})
        with self.assertRaises(ProtocolError):
            decode_variant({"Expected": Values}, {"outcome": "PeerSupplied"})

        with self.assertRaises(InvalidInput):
            BearerCredential("submitted-secret")
        with self.assertRaises(InvalidInput):
            RiffDate(-(2**31)).to_date()
        with self.assertRaises(InvalidInput):
            Money("USD", "1.00")  # type: ignore[arg-type]
        with self.assertRaises(InvalidInput):
            Timestamp("0", 0)  # type: ignore[arg-type]
        with self.assertRaises(InvalidInput):
            AttemptBudget(True)
        invalid_metadata = CallMetadata().with_database(DatabaseAlias("NOT_CANONICAL"))
        with self.assertRaises(InvalidInput):
            SyncApplicationTransport.connect_uri("http://127.0.0.1:1", invalid_metadata)
        with self.assertRaises(InvalidInput):
            VerifiedTlsConfig("https://127.0.0.1:7443", "/ca.pem", "127.0.0.1", 17, 64)

    def test_credentials_are_redacted_and_non_pickleable(self) -> None:
        credential = BearerCredential("A" * 43)
        self.assertNotIn("A" * 43, repr(credential))
        with self.assertRaises(TypeError):
            pickle.dumps(credential)

    def test_bounds_and_malformed_bridge_values_fail_closed(self) -> None:
        with self.assertRaises(InvalidInput):
            AttemptBudget(0)
        with self.assertRaises(InvalidInput):
            Timestamp(0, 1_000_000_000)
        with self.assertRaises(_native.NativeError) as raised:
            _native.validate_bridge_value('{"kind":"unknown","value":"do-not-echo"}')
        self.assertNotIn("do-not-echo", str(raised.exception))

    def test_native_panic_text_is_closed_without_swallowing_cancellation(self) -> None:
        panic_type = type(
            "PanicException",
            (BaseException,),
            {"__module__": "pyo3_runtime"},
        )
        translated = _translate_native(panic_type("native-secret"))
        self.assertIsInstance(translated, ProtocolError)
        self.assertNotIn("native-secret", str(translated))

        cancellation = KeyboardInterrupt("cancelled")
        with self.assertRaises(KeyboardInterrupt) as raised:
            _translate_native(cancellation)
        self.assertIs(raised.exception, cancellation)

    def test_checked_public_storage_failure_remains_typed_and_retryable(self) -> None:
        encoded = json.dumps(
            {
                "code": "RDB-STORAGE-0101",
                "message": "authoritative storage is temporarily unavailable",
                "category": "storage",
                "recovery_action": "retry",
                "operation": "ApplicationRequest",
                "contract_lineage": None,
                "contract_version": None,
                "operation_symbol": None,
                "symbol_path": [],
                "source_span": None,
                "fixes": ["retry_later"],
                "trace_id": None,
                "incident_id": None,
            }
        )
        translated = _translate_native(_native.NativeError("application", encoded))
        self.assertIsInstance(translated, RiffDbApplicationError)
        assert isinstance(translated, RiffDbApplicationError)
        self.assertEqual(translated.details.code, ApplicationErrorCode.STORAGE_UNAVAILABLE)
        self.assertEqual(translated.details.recovery_action, "retry")

    def test_generated_batch_concurrency_accepts_384_and_rejects_385(self) -> None:
        _validate_batch([object()], CommandBatchOptions(384))
        with self.assertRaises(InvalidInput):
            _validate_batch([object()], CommandBatchOptions(385))


class AsyncRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_event_batch_preserves_empty_bounded_pull_and_uses_zero_wait(self) -> None:
        class NativeClient:
            request: dict[str, object] | None = None

            async def consume_event_stream(self, request: str) -> str:
                self.request = json.loads(request)
                return json.dumps(
                    {
                        "events": [],
                        "status": {"history_incarnation": 1},
                        "wait_timed_out": False,
                        "disposition": "ready",
                    }
                )

        native = NativeClient()
        transport = AsyncApplicationTransport(native)  # type: ignore[arg-type]

        batch = await transport._consume_event_batch(
            reactive_module_hash="11" * 32,
            operation_name="TicketEvents",
            parameters={},
            consumer_name="worker",
        )

        self.assertEqual(batch["events"], [])
        self.assertEqual(batch["disposition"], "ready")
        assert native.request is not None
        self.assertEqual(native.request["maximum_wait_nanos"], 0)
