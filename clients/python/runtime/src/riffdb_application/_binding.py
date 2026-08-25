from __future__ import annotations

import math
import types
import typing
from collections.abc import Hashable
from dataclasses import fields, is_dataclass
from decimal import Decimal
from enum import StrEnum
from functools import lru_cache
from types import MappingProxyType
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


def _vector_dimension(annotation: object) -> int | None:
    if get_origin(annotation) is not Annotated:
        return None
    arguments = get_args(annotation)
    if len(arguments) < 2 or arguments[0] != tuple[float, ...] or not isinstance(arguments[1], str):
        return None
    marker = arguments[1]
    if not marker.startswith("vector<") or not marker.endswith(">"):
        return None
    dimension = int(marker[7:-1])
    return dimension if 1 <= dimension <= 4096 else None


def _is_union(origin: object) -> bool:
    return origin in {types.UnionType, typing.Union}


def _encode_typed(value: object, annotation: object) -> dict[str, object]:
    vector_dimension = _vector_dimension(annotation)
    if vector_dimension is not None:
        if not isinstance(value, tuple) or len(value) != vector_dimension:
            raise TypeError("RiffDB vector field requires its declared tuple dimension")
        components = []
        for component in value:
            if type(component) is not float or not math.isfinite(component):
                raise TypeError("RiffDB vector components must be finite floats")
            components.append(0.0 if component == 0.0 else component)
        return {"kind": "vector", "components": components}
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

@lru_cache(maxsize=None)
def _resolved_hints(record_type: type) -> "MappingProxyType[str, object]":
    """Resolved annotations for one generated dataclass.

    `get_type_hints` re-evaluates string annotations by compiling them, which
    is far too expensive to repeat per decoded record. Generated classes are
    immutable for the life of the process, so the resolution is cached and the
    result is exposed read-only so a caller cannot mutate the shared mapping.
    """
    return MappingProxyType(dict(get_type_hints(record_type, include_extras=True)))


@lru_cache(maxsize=None)
def _init_field_names(record_type: type) -> frozenset[str]:
    """Names of the init fields of one generated dataclass."""
    return frozenset(item.name for item in fields(record_type) if item.init)


def _encode_record(value: object) -> dict[str, dict[str, object]]:
    if not is_dataclass(value) or isinstance(value, type):
        raise TypeError("generated RiffDB input must be a dataclass instance")
    hints = _resolved_hints(type(value))
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


def encode_value(value: object, annotation: object) -> dict[str, object]:
    try:
        return _encode_typed(value, annotation)
    except (KeyError, TypeError, ValueError, OverflowError):
        from . import InvalidInput

        raise InvalidInput("generated application input is invalid") from None


def canonical_value_encoded_length(value: dict[str, object]) -> int:
    """Return the exact ADR-0011 canonical v1 document length of an encoded value."""
    kind = value.get("kind")
    if kind == "null":
        return 2
    if kind == "bool":
        return 3
    if kind in {"i64", "u64"}:
        return 10
    if kind == "decimal":
        return 20
    if kind == "money":
        return 23
    if kind == "string":
        raw = value.get("value")
        if not isinstance(raw, str):
            raise ValueError("invalid RiffDB string value")
        return 6 + len(raw.encode("utf-8"))
    if kind == "bytes":
        raw = value.get("value")
        if not isinstance(raw, str) or len(raw) % 2 != 0:
            raise ValueError("invalid RiffDB bytes value")
        return 6 + len(bytes.fromhex(raw))
    if kind == "timestamp":
        return 14
    if kind == "date":
        return 6
    if kind == "uuid":
        return 18
    if kind == "enum":
        return 10
    if kind == "vector":
        components = value.get("components")
        if not isinstance(components, list):
            raise ValueError("invalid RiffDB vector value")
        return 6 + 4 * len(components)
    if kind == "list":
        items = value.get("value")
        if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
            raise ValueError("invalid RiffDB list value")
        return 6 + sum(
            canonical_value_encoded_length(typing.cast(dict[str, object], item)) for item in items
        )
    if kind == "record":
        record = value.get("value")
        if not isinstance(record, dict) or not all(isinstance(item, dict) for item in record.values()):
            raise ValueError("invalid RiffDB record value")
        return 6 + sum(
            4 + canonical_value_encoded_length(typing.cast(dict[str, object], item))
            for item in record.values()
        )
    raise ValueError("invalid RiffDB value kind")


def encode_reactive_record(
    value: object,
    schema: dict[str, dict[str, object]],
) -> dict[str, dict[str, object]]:
    try:
        encoded = _encode_record(value)
        if set(encoded) != set(schema):
            raise ValueError("reactive parameter schema mismatch")
        for name, metadata in schema.items():
            item = encoded[name]
            kind = metadata.get("kind")
            if item.get("kind") != kind:
                raise ValueError("reactive parameter kind mismatch")
            if kind == "decimal":
                if item.get("scale") != metadata.get("scale"):
                    raise ValueError("reactive decimal scale mismatch")
                item["precision"] = metadata["precision"]
            elif kind == "money":
                amount = item.get("amount")
                if (
                    item.get("currency") != metadata.get("currency")
                    or not isinstance(amount, dict)
                    or amount.get("scale") != metadata.get("scale")
                ):
                    raise ValueError("reactive money type mismatch")
                amount["precision"] = metadata["precision"]
            elif kind == "enum":
                variants = metadata.get("variants")
                variant = item.get("value")
                if not isinstance(variants, dict) or variant not in variants:
                    raise ValueError("reactive enum variant mismatch")
                item["type_id"] = metadata["type_id"]
                item["variant_id"] = variants[variant]
        return encoded
    except (KeyError, TypeError, ValueError, OverflowError):
        from . import InvalidInput

        raise InvalidInput("generated reactive parameters are invalid") from None


def _decode_decimal(value: object) -> Decimal:
    if not isinstance(value, dict) or value.get("$riffdb") != "decimal":
        raise TypeError("invalid RiffDB decimal response")
    coefficient = int.from_bytes(bytes.fromhex(str(value["coefficient"])), "big", signed=True)
    return Decimal(coefficient).scaleb(-int(value["scale"]))


def _decode_typed(value: object, annotation: object) -> object:
    vector_dimension = _vector_dimension(annotation)
    if vector_dimension is not None:
        if (
            not isinstance(value, dict)
            or value.get("$riffdb") != "vector"
            or not isinstance(value.get("components"), list)
            or len(value["components"]) != vector_dimension
        ):
            raise TypeError("invalid RiffDB vector response")
        components = tuple(value["components"])
        if any(type(component) is not float or not math.isfinite(component) for component in components):
            raise TypeError("invalid RiffDB vector response")
        return components
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
    cache_key = typing.cast(Hashable, record_type)
    hints = _resolved_hints(cache_key)
    expected = _init_field_names(cache_key)
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
