# req: END-007, END-008, END-010
"""Collect the native logical component; never substitute it for total history."""
import json
import os
from pathlib import Path
import re

from .ticketdesk_lifecycle_provider_budget import ProviderBudgetError, ProviderLog

PREFIX = b"riffdb-retention-logical-bytes-v1\t"
FAMILY = b"riffdb-retention-logical-bytes-"
MAX_U64 = 2**64 - 1
MAX_LINE_BYTES = 4096
# Captures are at least 60 seconds apart; completion is strictly below 30
# seconds. Five seconds of collector scheduling allowance is frozen here.
MAX_GAP_NS = 95000000000
FIELDS = frozenset({"root", "incarnation", "watermark", "head", "ceiling", "through", "rows",
    "canonical_history_value_bytes", "removed_key_bytes", "capture_age_us", "pages"})


class LogicalBytesError(ProviderBudgetError):
    """Closed numeric-evidence refusal, without source log contents."""


def require(condition, code):
    if not condition:
        raise LogicalBytesError(code)


def parse_sample(encoded):
    require(type(encoded) is bytes, "logical_bytes_shape")
    if not encoded.startswith(FAMILY):
        return None
    require(encoded.startswith(PREFIX) and len(encoded) <= MAX_LINE_BYTES
            and encoded.endswith(b"\n"), "logical_bytes_shape")
    values = {}
    for field in encoded[len(PREFIX):-1].split(b"\t"):
        match = re.fullmatch(rb"([a-z_]+)=(0|[1-9][0-9]{0,19})", field)
        require(match is not None, "logical_bytes_shape")
        name, value = match[1].decode("ascii"), int(match[2])
        require(name in FIELDS and name not in values and value <= MAX_U64,
                "logical_bytes_shape")
        values[name] = value
    require(values.keys() == FIELDS, "logical_bytes_shape")
    span = values["through"] - values["watermark"]
    rows = values["rows"]
    require(values["root"] > 0 and values["root"] % 2 == 0
            and values["incarnation"] > 0
            and 0 <= values["watermark"] <= values["head"]
            and values["ceiling"] <= values["head"]
            and values["watermark"] <= values["through"] <= max(values["watermark"], values["ceiling"])
            and (span + 255) // 256 <= rows <= span
            and values["removed_key_bytes"] >= rows * 8
            and values["canonical_history_value_bytes"] + values["removed_key_bytes"] <= MAX_U64
            and (values["canonical_history_value_bytes"] == 0) == (rows == 0)
            and (values["removed_key_bytes"] == 0) == (rows == 0)
            and values["canonical_history_value_bytes"] >= rows
            and max(1, (rows + 255) // 256) <= values["pages"] <= rows + 1,
            "logical_bytes_accounting")
    require(values["capture_age_us"] < 30000000, "logical_bytes_freshness")
    return values


class LogicalBytesSamples:
    """One incarnation and bounded complete observations in every load window."""

    def __init__(self, root, duration, expected_incarnation):
        require(type(duration) is int and 1 <= duration <= 86400, "logical_bytes_duration")
        self.path = Path(root) / "logical-byte-samples.jsonl"
        self.duration_ns = duration * 1000000000
        self.expected_incarnation = expected_incarnation
        self.coverage = [0] * ((duration + 299) // 300)
        self.last = self.last_ns = self.output = None
        self.count = 0

    def __enter__(self):
        descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        self.output = os.fdopen(descriptor, "wb")
        return self

    def observe(self, encoded, elapsed_ns):
        require(type(elapsed_ns) is int and 0 <= elapsed_ns <= MAX_U64, "logical_bytes_clock")
        value = parse_sample(encoded)
        if value is None or elapsed_ns >= self.duration_ns:
            return
        expected = self.expected_incarnation()
        require(type(expected) is int and expected == value["incarnation"],
                "logical_bytes_incarnation")
        require(self.last_ns is None or elapsed_ns > self.last_ns, "logical_bytes_clock")
        require(elapsed_ns - (self.last_ns or 0) <= MAX_GAP_NS, "logical_bytes_sample_gap")
        if self.last is not None:
            require(value["incarnation"] == self.last["incarnation"]
                    and value["root"] >= self.last["root"]
                    and value["watermark"] >= self.last["watermark"]
                    and value["head"] >= self.last["head"], "logical_bytes_continuity")
        require(self.output is not None and self.count < 2048, "logical_bytes_sample_bound")
        record = dict(value, schema="riffdb.ticketdesk-lifecycle-logical-byte-sample/v1",
                      elapsed_ns=elapsed_ns, basis="canonical_logical_history_and_removed_keys",
                      qualifies_complete_resource_model=False)
        data = json.dumps(record, sort_keys=True).encode() + b"\n"
        require(len(data) <= MAX_LINE_BYTES, "logical_bytes_sample_bound")
        self.output.write(data)
        self.output.flush()
        self.last, self.last_ns = value, elapsed_ns
        self.count += 1
        self.coverage[elapsed_ns // 300000000000] += 1

    def finish(self, elapsed_ns):
        require(type(elapsed_ns) is int and self.duration_ns <= elapsed_ns <= MAX_U64,
                "logical_bytes_clock")
        require(all(self.coverage), "logical_bytes_sample_coverage")
        require(self.last_ns is not None and self.duration_ns - self.last_ns <= MAX_GAP_NS,
                "logical_bytes_sample_gap")
        return {"schema": "riffdb.ticketdesk-lifecycle-logical-byte-observation/v1",
                "samples": self.count, "basis": "canonical_logical_history_and_removed_keys",
                "qualifies_complete_resource_model": False}

    def __exit__(self, _kind, _error, _traceback):
        if self.output is not None:
            self.output.close()
            self.output = None
        return False


class LogicalBytesLog(ProviderLog):
    """Reuse the bounded log reader and custody checks for this component."""

    def __init__(self, root, duration, expected_incarnation):
        self.path = Path(root) / "server.stderr"
        self.samples = LogicalBytesSamples(root, duration, expected_incarnation)
        self.source = self.identity = None
