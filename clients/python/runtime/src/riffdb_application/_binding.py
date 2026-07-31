from __future__ import annotations

import types
import typing
from dataclasses import fields, is_dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, get_args, get_origin, get_type_hints
from uuid import UUID


def _signed_bytes(value: int) -> bytes:
    length = max(1, (value.bit_length() + 8) // 8)
    encoded = value.to_bytes(length, "big", signed=True)
    while len(encoded) > 1:
        if encoded[0] == 0 and encoded[1] < 0x80:
            encoded = encoded[1:]
        elif encoded[0] == 0xFF and encoded[1] >= 0x80:
            encoded = encoded[1:]
        else:
            break
    return encoded


def _encode_decimal(value: Decimal) -> dict[str, object]:
    if not value.is_finite():
        raise ValueError("RiffDB decimals must be finite")
    sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        raise ValueError("RiffDB decimals must be finite")
    coefficient = int("".join(str(digit) for digit in digits) or "0")
    if sign:
        coefficient = -coefficient
    scale = max(-exponent, 0)
    if exponent > 0:
        coefficient *= 10**exponent
    return {
        "kind": "decimal",
        "coefficient": _signed_bytes(coefficient).hex(),
        "scale": scale,
    }


def _integer_kind(annotation: object) -> str | None:
    if get_origin(annotation) is Annotated:
        arguments = get_args(annotation)
        if len(arguments) >= 2 and arguments[0] is int and arguments[1] in {"i64", "u64"}:
            return str(arguments[1])
    return None


def _is_union(origin: object) -> bool:
    return origin in {types.UnionType, typing.Union}


def _encode_typed(value: object, annotation: object) -> dict[str, object]:
    integer_kind = _integer_kind(annotation)
    if integer_kind is not None:
        if type(value) is not int:
            raise TypeError("RiffDB integer field requires int")
        lower, upper = (-(2**63), 2**63 - 1) if integer_kind == "i64" else (0, 2**64 - 1)
        if not lower <= value <= upper:
            raise ValueError("RiffDB integer is outside its declared range")
        return {"kind": integer_kind, "value": value}
    origin = get_origin(annotation)
    arguments = get_args(annotation)
    if _is_union(origin):
        if value is None and type(None) in arguments:
            return {"kind": "null"}
        inner = next(item for item in arguments if item is not type(None))
        return _encode_typed(value, inner)
    if origin is tuple:
        if not isinstance(value, tuple):
            raise TypeError("RiffDB list field requires tuple")
        return {"kind": "list", "value": [_encode_typed(item, arguments[0]) for item in value]}
    if value is None:
        return {"kind": "null"}
    if annotation is bool:
        if type(value) is not bool:
            raise TypeError("RiffDB boolean field requires bool")
        return {"kind": "bool", "value": value}
    if annotation is str:
        if not isinstance(value, str):
            raise TypeError("RiffDB string field requires str")
        return {"kind": "string", "value": value}
    if annotation is bytes:
        if not isinstance(value, bytes):
            raise TypeError("RiffDB bytes field requires bytes")
        return {"kind": "bytes", "value": value.hex()}
    if annotation is Decimal:
        if not isinstance(value, Decimal):
            raise TypeError("RiffDB decimal field requires Decimal")
        return _encode_decimal(value)
    if annotation is UUID:
        if not isinstance(value, UUID):
            raise TypeError("RiffDB UUID field requires UUID")
        return {"kind": "uuid", "value": str(value)}
    from . import Money, RiffDate, Timestamp

    if annotation is RiffDate:
        if not isinstance(value, RiffDate):
            raise TypeError("RiffDB date field requires RiffDate")
        return {"kind": "date", "value": value.days_since_unix_epoch}
    if annotation is Timestamp:
        if not isinstance(value, Timestamp):
            raise TypeError("RiffDB timestamp field requires Timestamp")
        return {"kind": "timestamp", "seconds": value.seconds, "nanos": value.nanos}
    if annotation is Money:
        if not isinstance(value, Money):
            raise TypeError("RiffDB money field requires Money")
        return {
            "kind": "money",
            "currency": value.currency,
            "amount": _encode_decimal(value.amount),
        }
    if isinstance(annotation, type) and issubclass(annotation, StrEnum):
        if not isinstance(value, annotation):
            raise TypeError("RiffDB enum field has the wrong enum type")
        return {"kind": "enum", "value": value.value}
    if isinstance(annotation, type) and is_dataclass(annotation):
        return {"kind": "record", "value": encode_record(value)}
    raise TypeError("unsupported generated RiffDB field type")


def _encode_record(value: object) -> dict[str, dict[str, object]]:
    if not is_dataclass(value) or isinstance(value, type):
        raise TypeError("generated RiffDB input must be a dataclass instance")
    hints = get_type_hints(type(value), include_extras=True)
    return {
        item.name: _encode_typed(getattr(value, item.name), hints[item.name])
        for item in fields(value)
        if item.init
    }


def encode_record(value: object) -> dict[str, dict[str, object]]:
    try:
        return _encode_record(value)
    except (KeyError, TypeError, ValueError, OverflowError):
        from . import InvalidInput

        raise InvalidInput("generated application input is invalid") from None


def _decode_decimal(value: object) -> Decimal:
    if not isinstance(value, dict) or value.get("$riffdb") != "decimal":
        raise TypeError("invalid RiffDB decimal response")
    coefficient = int.from_bytes(bytes.fromhex(str(value["coefficient"])), "big", signed=True)
    return Decimal(coefficient).scaleb(-int(value["scale"]))


def _decode_typed(value: object, annotation: object) -> object:
    if get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]
    origin = get_origin(annotation)
    arguments = get_args(annotation)
    if _is_union(origin):
        if value is None and type(None) in arguments:
            return None
        return _decode_typed(value, next(item for item in arguments if item is not type(None)))
    if origin is tuple:
        if not isinstance(value, list):
            raise TypeError("invalid RiffDB list response")
        return tuple(_decode_typed(item, arguments[0]) for item in value)
    if annotation in {bool, str, int}:
        if type(value) is not annotation:
            raise TypeError("invalid scalar RiffDB response")
        return value
    if annotation is bytes:
        if not isinstance(value, dict) or value.get("$riffdb") != "bytes":
            raise TypeError("invalid RiffDB bytes response")
        return bytes.fromhex(str(value["value"]))
    if annotation is Decimal:
        return _decode_decimal(value)
    if annotation is UUID:
        if not isinstance(value, dict) or value.get("$riffdb") != "uuid":
            raise TypeError("invalid RiffDB UUID response")
        return UUID(str(value["value"]))
    from . import Money, RiffDate, Timestamp

    if annotation is RiffDate:
        if not isinstance(value, dict) or value.get("$riffdb") != "date":
            raise TypeError("invalid RiffDB date response")
        return RiffDate(int(value["value"]))
    if annotation is Timestamp:
        if not isinstance(value, dict) or value.get("$riffdb") != "timestamp":
            raise TypeError("invalid RiffDB timestamp response")
        return Timestamp(int(value["seconds"]), int(value["nanos"]))
    if annotation is Money:
        if not isinstance(value, dict) or value.get("$riffdb") != "money":
            raise TypeError("invalid RiffDB money response")
        return Money(str(value["currency"]), _decode_decimal(value["amount"]))
    if isinstance(annotation, type) and issubclass(annotation, StrEnum):
        if not isinstance(value, dict) or value.get("$riffdb") != "enum":
            raise TypeError("invalid RiffDB enum response")
        return annotation(str(value["value"]))
    if isinstance(annotation, type) and is_dataclass(annotation):
        return decode_record(annotation, value)
    raise TypeError("unsupported generated RiffDB response type")


def _decode_record[T](record_type: type[T], value: object) -> T:
    if not isinstance(value, dict):
        raise TypeError("invalid RiffDB record response")
    hints = get_type_hints(record_type, include_extras=True)
    expected = {item.name for item in fields(record_type) if item.init}  # type: ignore[arg-type]
    if set(value) - {"outcome"} != expected:
        raise TypeError("invalid RiffDB record response")
    return record_type(**{name: _decode_typed(value[name], hints[name]) for name in expected})


def decode_record[T](record_type: type[T], value: object) -> T:
    try:
        return _decode_record(record_type, value)
    except (KeyError, TypeError, ValueError, OverflowError):
        from . import ProtocolError

        raise ProtocolError("generated application response is invalid") from None


def decode_variant[T](variants: dict[str, type[T]], value: object) -> T:
    from . import ProtocolError

    if not isinstance(value, dict) or not isinstance(value.get("outcome"), str):
        raise ProtocolError("generated application outcome is invalid")
    record_type = variants.get(value["outcome"])
    if record_type is None:
        raise ProtocolError("generated application outcome is undeclared")
    return decode_record(record_type, value)
