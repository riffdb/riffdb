"""Deterministic UUIDv7-shaped identifiers matching the Rust seed."""

from __future__ import annotations

UuidBytes = bytes

NS_ORG = 0x10
NS_USER = 0x11
NS_PROJECT = 0x12
NS_TICKET = 0x13
NS_COMMENT = 0x14
NS_LABEL = 0x15
NS_WRITE_PROBE = 0x7F
NS_LOAD_WRITE = 0x7E

STATUS_OPEN = "open"
STATUS_CLOSED = "closed"
STATUS_IN_PROGRESS = "in_progress"


def uuid_from_ordinal(namespace: int, ordinal: int) -> UuidBytes:
    data = bytearray([namespace & 0xFF] * 16)
    data[6] = 0x70 | (namespace & 0x0F)
    data[8:] = (ordinal & 0xFFFFFFFFFFFFFFFF).to_bytes(8, "big")
    data[8] = 0x80 | (data[8] & 0x3F)
    return bytes(data)


def format_uuid(value: UuidBytes) -> str:
    hexed = value.hex()
    return f"{hexed[0:8]}-{hexed[8:12]}-{hexed[12:16]}-{hexed[16:20]}-{hexed[20:32]}"


def as_uuid(value: UuidBytes):
    from uuid import UUID

    return UUID(format_uuid(value))


def encode_short(value: UuidBytes) -> str:
    return value[12:16].hex()


def sql_status_to_riff(status: str) -> str:
    return {"open": "Open", "closed": "Closed", "in_progress": "InProgress"}[status]
