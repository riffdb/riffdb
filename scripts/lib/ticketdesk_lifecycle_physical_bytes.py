# req: END-007, END-008, END-010
"""Collect original sealed-prefix allocation, with disclosed diagnostic protection."""
import json
import os
from pathlib import Path
import re

from .ticketdesk_lifecycle_provider_budget import ProviderBudgetError, ProviderLog

PREFIX = b"riffdb-retention-physical-bytes-v1\t"
FAMILY = b"riffdb-retention-physical-bytes-"
MAX_U64 = 2**64 - 1
MAX_LINE_BYTES = 4096
# Captures are at least 60 seconds apart; completion is strictly below 30
# seconds. Five seconds of collector scheduling allowance is frozen here.
MAX_GAP_NS = 95000000000
FIELDS = frozenset({"root", "incarnation", "watermark", "minimum_resume", "active_generation",
    "sealed_extents", "eligible_allocated_bytes", "protected_allocated_bytes",
    "reference_protected_allocated_bytes", "reader_protected_allocated_bytes", "reader_held",
    "capture_age_us", "pages", "diagnostic_pin_excluded"})


class PhysicalBytesError(ProviderBudgetError):
    """Closed numeric-evidence refusal, without source log contents."""


def require(condition, code):
    if not condition:
        raise PhysicalBytesError(code)


def parse_sample(encoded):
    require(type(encoded) is bytes, "physical_bytes_shape")
    if not encoded.startswith(FAMILY):
        return None
    require(encoded.startswith(PREFIX) and len(encoded) <= MAX_LINE_BYTES
            and encoded.endswith(b"\n"), "physical_bytes_shape")
    values = {}
    for field in encoded[len(PREFIX):-1].split(b"\t"):
        match = re.fullmatch(rb"([a-z_]+)=(0|[1-9][0-9]{0,19})", field)
        require(match is not None, "physical_bytes_shape")
        name, value = match[1].decode("ascii"), int(match[2])
        require(name in FIELDS and name not in values and value <= MAX_U64,
                "physical_bytes_shape")
        values[name] = value
    require(values.keys() == FIELDS, "physical_bytes_shape")
    extents = values["sealed_extents"]
    eligible = values["eligible_allocated_bytes"]
    protected = values["protected_allocated_bytes"]
    reference = values["reference_protected_allocated_bytes"]
    readers = values["reader_protected_allocated_bytes"]
    total = eligible + protected
    require(values["root"] > 0 and values["root"] % 2 == 0
            and values["incarnation"] > 0 and values["minimum_resume"] > 0
            and values["active_generation"] > 0
            and extents <= min(4096, values["active_generation"] - 1)
            and values["reader_held"] in (0, 1) and values["diagnostic_pin_excluded"] == 1
            and protected == reference + readers and total <= MAX_U64
            and all(amount % 512 == 0 for amount in (eligible, protected, reference, readers))
            and total >= extents * 512 and (total == 0) == (extents == 0)
            and (values["reader_held"] == 0 or eligible == 0)
            and (values["reader_held"] == 1 or readers == 0)
            and extents + 1 <= values["pages"] <= extents + 129,
            "physical_bytes_accounting")
    require(values["capture_age_us"] < 30000000, "physical_bytes_freshness")
    return values


class PhysicalBytesSamples:
    """One incarnation and bounded complete observations in every load window."""

    def __init__(self, root, duration, expected_incarnation):
        require(type(duration) is int and 1 <= duration <= 86400, "physical_bytes_duration")
        self.path = Path(root) / "physical-byte-samples.jsonl"
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
        require(type(elapsed_ns) is int and 0 <= elapsed_ns <= MAX_U64, "physical_bytes_clock")
        value = parse_sample(encoded)
        if value is None or elapsed_ns >= self.duration_ns:
            return
        expected = self.expected_incarnation()
        require(type(expected) is int and expected == value["incarnation"],
                "physical_bytes_incarnation")
        require(self.last_ns is None or elapsed_ns > self.last_ns, "physical_bytes_clock")
        require(elapsed_ns - (self.last_ns or 0) <= MAX_GAP_NS, "physical_bytes_sample_gap")
        if self.last is not None:
            require(value["incarnation"] == self.last["incarnation"]
                    and value["root"] >= self.last["root"]
                    and value["watermark"] >= self.last["watermark"]
                    and value["minimum_resume"] >= self.last["minimum_resume"]
                    and value["active_generation"] >= self.last["active_generation"], "physical_bytes_continuity")
        require(self.output is not None and self.count < 2048, "physical_bytes_sample_bound")
        record = dict(value, schema="riffdb.ticketdesk-lifecycle-physical-byte-sample/v1",
                      elapsed_ns=elapsed_ns, basis="native_original_sealed_prefix_allocated_blocks",
                      qualifies_complete_resource_model=False)
        data = json.dumps(record, sort_keys=True).encode() + b"\n"
        require(len(data) <= MAX_LINE_BYTES, "physical_bytes_sample_bound")
        self.output.write(data)
        self.output.flush()
        self.last, self.last_ns = value, elapsed_ns
        self.count += 1
        self.coverage[elapsed_ns // 300000000000] += 1

    def finish(self, elapsed_ns):
        require(type(elapsed_ns) is int and self.duration_ns <= elapsed_ns <= MAX_U64,
                "physical_bytes_clock")
        require(all(self.coverage), "physical_bytes_sample_coverage")
        require(self.last_ns is not None and self.duration_ns - self.last_ns <= MAX_GAP_NS,
                "physical_bytes_sample_gap")
        return {"schema": "riffdb.ticketdesk-lifecycle-physical-byte-observation/v1",
                "samples": self.count, "basis": "native_original_sealed_prefix_allocated_blocks",
                "qualifies_complete_resource_model": False}

    def __exit__(self, _kind, _error, _traceback):
        if self.output is not None:
            self.output.close()
            self.output = None
        return False


class PhysicalBytesLog(ProviderLog):
    """Reuse the bounded log reader and custody checks for this component."""

    def __init__(self, root, duration, expected_incarnation):
        self.path = Path(root) / "server.stderr"
        self.samples = PhysicalBytesSamples(root, duration, expected_incarnation)
        self.source = self.identity = None

    def poll(self, elapsed_ns):
        require(type(elapsed_ns) is int and 0 <= elapsed_ns <= MAX_U64,
                "physical_bytes_clock")
        super().poll(elapsed_ns)
        # EOF does not mean that the native producer is still reporting.
        # Enforce the existing gap even when no subsequent line arrives.
        through = min(elapsed_ns, self.samples.duration_ns)
        require(0 <= through - (self.samples.last_ns or 0) <= MAX_GAP_NS,
                "physical_bytes_sample_gap")
