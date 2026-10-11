# req: END-007, END-008, END-010
"""Collect the native source component; never substitute it for total history."""
import json
import os
from pathlib import Path
import re

from .ticketdesk_lifecycle_provider_budget import ProviderBudgetError, ProviderLog

PREFIX = b"riffdb-retention-source-bytes-v1\t"
PREFIX_V2 = b"riffdb-retention-source-bytes-v2\t"
FAMILY = b"riffdb-retention-source-bytes-"
MAX_U64 = 2**64 - 1
MAX_LINE_BYTES = 4096
# Captures are at least 60 seconds apart; completion is strictly below 30
# seconds. Five seconds of collector scheduling allowance is frozen here.
MAX_GAP_NS = 95000000000
FIELDS = frozenset({"root", "incarnation", "resume", "ceiling", "rows",
    "canonical_receipt_bytes", "key_bytes", "capture_age_us", "collection_us", "pages"})


class SourceBytesError(ProviderBudgetError):
    """Closed numeric-evidence refusal, without source log contents."""


def require(condition, code):
    if not condition:
        raise SourceBytesError(code)


def parse_sample(encoded):
    require(type(encoded) is bytes, "source_bytes_shape")
    if not encoded.startswith(FAMILY):
        return None
    require(encoded.startswith(PREFIX) and len(encoded) <= MAX_LINE_BYTES
            and encoded.endswith(b"\n"), "source_bytes_shape")
    values = {}
    for field in encoded[len(PREFIX):-1].split(b"\t"):
        match = re.fullmatch(rb"([a-z_]+)=(0|[1-9][0-9]{0,19})", field)
        require(match is not None, "source_bytes_shape")
        name, value = match[1].decode("ascii"), int(match[2])
        require(name in FIELDS and name not in values and value <= MAX_U64,
                "source_bytes_shape")
        values[name] = value
    require(values.keys() == FIELDS, "source_bytes_shape")
    rows = max(values["ceiling"] - values["resume"], 0)
    require(values["root"] > 0 and values["root"] % 2 == 0
            and values["incarnation"] > 0 and values["rows"] == rows
            and values["key_bytes"] == rows * 8 <= MAX_U64
            and values["canonical_receipt_bytes"] + values["key_bytes"] <= MAX_U64
            and (values["canonical_receipt_bytes"] == 0) == (rows == 0)
            and values["canonical_receipt_bytes"] >= rows
            and max(1, (rows + 255) // 256) <= values["pages"] <= max(1, rows),
            "source_bytes_accounting")
    require(values["collection_us"] <= values["capture_age_us"] < 30000000,
            "source_bytes_freshness")
    return values


FIELDS_V2 = FIELDS | {"reused_rows", "checked_rows", "subtracted_rows",
    "publication_kind", "publication_hash_a", "publication_hash_b",
    "publication_hash_c", "publication_hash_d"}


def parse_sample_v2(encoded):
    """V2 counts actual new scans separately from qualified reuse and cuts."""
    require(type(encoded) is bytes and encoded.startswith(PREFIX_V2)
            and len(encoded) <= MAX_LINE_BYTES and encoded.endswith(b"\n"),
            "source_bytes_shape")
    values = {}
    for field in encoded[len(PREFIX_V2):-1].split(b"\t"):
        match = re.fullmatch(rb"([a-z_]+)=(0|[1-9][0-9]{0,19})", field)
        require(match is not None, "source_bytes_shape")
        name, value = match[1].decode("ascii"), int(match[2])
        require(name in FIELDS_V2 and name not in values and value <= MAX_U64,
                "source_bytes_shape")
        values[name] = value
    require(values.keys() == FIELDS_V2, "source_bytes_shape")
    publication = [values["publication_hash_" + suffix] for suffix in "abcd"]
    require((values["publication_kind"] == 0 and not any(publication))
            or values["publication_kind"] == 1, "source_bytes_binding")
    rows = values["ceiling"] - values["resume"]
    checked = values["checked_rows"]
    covered = values["reused_rows"] + checked
    require(values["root"] > 0 and values["root"] % 2 == 0
            and values["incarnation"] > 0 and 0 <= rows == values["rows"]
            and values["key_bytes"] == rows * 8 <= MAX_U64
            and values["canonical_receipt_bytes"] + values["key_bytes"] <= MAX_U64
            and (values["canonical_receipt_bytes"] == 0) == (rows == 0)
            and values["canonical_receipt_bytes"] >= rows
            and covered <= MAX_U64 and values["subtracted_rows"] <= covered
            and covered - values["subtracted_rows"] == rows
            and ((checked == 0 and values["pages"] == 0)
                 or (checked > 0 and (checked + 255) // 256 <= values["pages"] <= checked)),
            "source_bytes_accounting")
    require(values["collection_us"] <= values["capture_age_us"] < 30000000,
            "source_bytes_freshness")
    return values


def parse_native_sample(encoded):
    # The legacy V1 reader remains closed; native collection dispatches explicitly.
    if type(encoded) is bytes and encoded.startswith(PREFIX_V2):
        return parse_sample_v2(encoded)
    return parse_sample(encoded)


class SourceBytesSamples:
    """One incarnation and bounded complete observations in every load window."""

    def __init__(self, root, duration, expected_incarnation):
        require(type(duration) is int and 1 <= duration <= 86400, "source_bytes_duration")
        self.path = Path(root) / "source-byte-samples.jsonl"
        self.duration_ns = duration * 1000000000
        self.expected_incarnation = expected_incarnation
        self.coverage = [0] * ((duration + 299) // 300)
        self.last = self.last_ns = self.output = None
        self.count = 0
        self.version = None

    def __enter__(self):
        descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        self.output = os.fdopen(descriptor, "wb")
        return self

    def observe(self, encoded, elapsed_ns):
        require(type(elapsed_ns) is int and 0 <= elapsed_ns <= MAX_U64, "source_bytes_clock")
        version = 2 if type(encoded) is bytes and encoded.startswith(PREFIX_V2) else 1
        value = parse_native_sample(encoded)
        if value is None or elapsed_ns >= self.duration_ns:
            return
        require(self.version is None or self.version == version, "source_bytes_version")
        self.version = version
        expected = self.expected_incarnation()
        require(type(expected) is int and expected == value["incarnation"],
                "source_bytes_incarnation")
        require(self.last_ns is None or elapsed_ns > self.last_ns, "source_bytes_clock")
        require(elapsed_ns - (self.last_ns or 0) <= MAX_GAP_NS, "source_bytes_sample_gap")
        if self.last is not None:
            require(value["incarnation"] == self.last["incarnation"]
                    and value["root"] >= self.last["root"]
                    and value["resume"] >= self.last["resume"], "source_bytes_continuity")
        require(self.output is not None and self.count < 2048, "source_bytes_sample_bound")
        record = dict(value, schema=f"riffdb.ticketdesk-lifecycle-source-byte-sample/v{version}",
                      elapsed_ns=elapsed_ns, basis=("canonical_source_receipts_and_keys" if version == 1
                          else "qualified_canonical_source_receipts_and_keys"),
                      qualifies_complete_resource_model=False)
        data = json.dumps(record, sort_keys=True).encode() + b"\n"
        require(len(data) <= MAX_LINE_BYTES, "source_bytes_sample_bound")
        self.output.write(data)
        self.output.flush()
        self.last, self.last_ns = value, elapsed_ns
        self.count += 1
        self.coverage[elapsed_ns // 300000000000] += 1

    def finish(self, elapsed_ns):
        require(type(elapsed_ns) is int and self.duration_ns <= elapsed_ns <= MAX_U64,
                "source_bytes_clock")
        require(all(self.coverage), "source_bytes_sample_coverage")
        require(self.last_ns is not None and self.duration_ns - self.last_ns <= MAX_GAP_NS,
                "source_bytes_sample_gap")
        return {"schema": f"riffdb.ticketdesk-lifecycle-source-byte-observation/v{self.version}",
                "samples": self.count, "basis": ("canonical_source_receipts_and_keys" if self.version == 1
                    else "qualified_canonical_source_receipts_and_keys"),
                "qualifies_complete_resource_model": False}

    def __exit__(self, _kind, _error, _traceback):
        if self.output is not None:
            self.output.close()
            self.output = None
        return False


class SourceBytesLog(ProviderLog):
    """Reuse the bounded log reader and custody checks for this component."""

    def __init__(self, root, duration, expected_incarnation):
        self.path = Path(root) / "server.stderr"
        self.samples = SourceBytesSamples(root, duration, expected_incarnation)
        self.source = self.identity = None

    def poll(self, elapsed_ns):
        require(type(elapsed_ns) is int and 0 <= elapsed_ns <= MAX_U64,
                "source_bytes_clock")
        super().poll(elapsed_ns)
        # EOF does not mean that the native producer is still reporting.
        # Enforce the existing gap even when no subsequent line arrives.
        through = min(elapsed_ns, self.samples.duration_ns)
        require(0 <= through - (self.samples.last_ns or 0) <= MAX_GAP_NS,
                "source_bytes_sample_gap")
