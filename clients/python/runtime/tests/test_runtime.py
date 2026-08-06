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
    AttemptBudget,
    BearerCredential,
    CallMetadata,
    CommandBatchOptions,
    DatabaseAlias,
    InvalidInput,
    Money,
    ProtocolError,
    RiffDate,
    SyncApplicationTransport,
    Timestamp,
)
from riffdb_application import _validate_batch
from riffdb_application import _native
from riffdb_application._binding import (
    decode_record,
    decode_variant,
    encode_reactive_record,
    encode_record,
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


class ReactiveState(StrEnum):
    OPEN = "Open"


@dataclass(frozen=True, slots=True)
class ReactiveParameters:
    amount: Decimal
    state: ReactiveState


class RuntimeTests(unittest.TestCase):
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
        )
        encoded = encode_record(value)
        self.assertEqual(encoded["signed"], {"kind": "i64", "value": -(2**63)})
        self.assertEqual(encoded["unsigned"], {"kind": "u64", "value": 2**64 - 1})
        self.assertNotIn("float", json.dumps(encoded))
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
            },
        )
        self.assertEqual(value.amount, Decimal("-0.01"))
        self.assertEqual(value.payload, b"\x00\xff")

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

    def test_generated_batch_concurrency_accepts_384_and_rejects_385(self) -> None:
        _validate_batch([object()], CommandBatchOptions(384))
        with self.assertRaises(InvalidInput):
            _validate_batch([object()], CommandBatchOptions(385))
