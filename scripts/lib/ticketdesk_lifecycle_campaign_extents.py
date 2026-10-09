# req: END-003, END-007, END-008, END-010
"""Bind observed disappearances to complete native records in one incarnation.

Pre-seed retirements, post-duration cleanup, restored copies and reused names
cannot satisfy the measured reclamation gate. Crash-truncated diagnostics remain
unknown; only complete native records can corroborate an observed generation.
"""
import os
from pathlib import Path
import stat

from .ticketdesk_lifecycle_inventory import (
    ExtentInventory, InventoryError, parse_retirement,
    MAX_LOG_BYTES, MAX_LOG_LINE_BYTES, MAX_LOG_LINES,
)

SECOND = 10**9


class MeasuredExtentError(ValueError):
    """Closed refusal without storage paths or native source bytes."""


def require(condition, code):
    if not condition: raise MeasuredExtentError(code)


class MeasuredExtents:
    def __init__(self, directory, *, duration_s, incarnation):
        require(type(duration_s) is int and 21900 <= duration_s <= 86400
                and type(incarnation) is int and incarnation in (1, 2), "campaign_extent_identity")
        self.inventory = ExtentInventory(directory)
        self.duration_ns = duration_s * SECOND
        self.incarnation = incarnation
        self.last_ns = self.last_sample_ns = None
        self.observed = {}
        self.closed = False
        self.failed = False

    def poll(self, elapsed_ns, *, force=False):
        require(not self.failed and not self.closed, "campaign_extent_closed")
        try:
            require(type(elapsed_ns) is int and 0 <= elapsed_ns <= self.duration_ns + 75 * SECOND
                    and (self.last_ns is None or elapsed_ns >= self.last_ns)
                    and type(force) is bool, "campaign_extent_clock")
            self.last_ns = elapsed_ns
            require(self.last_sample_ns is None or elapsed_ns - self.last_sample_ns <= 75 * SECOND,
                    "campaign_extent_gap")
            if not force and self.last_sample_ns is not None and elapsed_ns - self.last_sample_ns < SECOND:
                return
            previous = set(self.inventory.removed)
            self.inventory.sample()
            for generation in self.inventory.removed - previous:
                if elapsed_ns < self.duration_ns: self.observed[generation] = elapsed_ns
            self.last_sample_ns = elapsed_ns
        except BaseException:
            self.failed = True
            raise

    def close(self, elapsed_ns):
        self.poll(elapsed_ns, force=True)
        self.closed = True

    def reconcile(self, logs):
        require(self.closed and not self.failed and type(logs) is list and 1 <= len(logs) <= 8,
                "campaign_extent_logs")
        try:
            require(all(type(item) is tuple and len(item) == 2
                        and isinstance(item[0], (str, Path)) and type(item[1]) is bool
                        for item in logs), "campaign_extent_logs")
            return self._reconcile(logs)
        except (OSError, InventoryError) as error:
            self.failed = True
            raise MeasuredExtentError("campaign_extent_log_unavailable") from error
        except BaseException:
            self.failed = True
            raise

    def _reconcile(self, logs):
        native, identities, total, lines = {}, set(), 0, 0
        for path, killed in logs:
            prior_root = 0
            require(type(killed) is bool, "campaign_extent_logs")
            descriptor = os.open(Path(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(descriptor, "rb") as source:
                before = os.fstat(source.fileno())
                identity = (before.st_dev, before.st_ino)
                require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
                        and before.st_uid == os.getuid() and identity not in identities
                        and before.st_size <= MAX_LOG_BYTES, "campaign_extent_log_identity")
                identities.add(identity)
                consumed = 0
                while line := source.readline(MAX_LOG_LINE_BYTES + 1):
                    consumed += len(line)
                    total += len(line)
                    lines += 1
                    require(len(line) <= MAX_LOG_LINE_BYTES and total <= MAX_LOG_BYTES
                            and lines <= MAX_LOG_LINES,
                            "campaign_extent_log_bound")
                    if not line.endswith(b"\n"):
                        require(killed and consumed == before.st_size, "campaign_extent_log_partial")
                        break
                    record = parse_retirement(line)
                    if record is not None:
                        generation = record["generation"]
                        require(generation not in native and len(native) < 4096
                                and record["root"] >= prior_root,
                                "campaign_extent_native_identity")
                        native[generation] = record
                        prior_root = record["root"]
                after = os.fstat(source.fileno())
            named = Path(path).lstat()
            snapshot = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
            require(consumed == before.st_size and snapshot(before) == snapshot(after) == snapshot(named),
                    "campaign_extent_log_changed")
        # A native record after the interval may describe later cleanup of a
        # file present at close. It earns no measured credit. Only a measured
        # disappearance without native corroboration is unexplained here.
        require(self.observed.keys() <= native.keys(),
                "campaign_extent_unexplained_disappearance")
        proven = sorted(self.observed.keys() & native.keys())
        return {"schema": "riffdb.ticketdesk-lifecycle-measured-extents/v1",
                "history_incarnation": self.incarnation, "observed_native_retired_generations": proven,
                "measured_removal_elapsed_ns": [self.observed[generation] for generation in proven],
                "unconfirmed_disappearances": len(self.observed.keys() - native.keys()),
                "qualifies_complete_resource_model": False}
