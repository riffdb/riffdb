# req: END-007, END-008, END-010
"""Count all owned campaign files without adding overlapping history bases.

One snapshot's file bound is max(apparent length, allocated 512-byte blocks).
Retained pre-restore directories count as intentional backup copies. They never
count as physical reclamation. This collector alone qualifies no resource model.
"""
import json
import os
from pathlib import Path
import stat

from .ticketdesk_lifecycle_faults import LocalArchiveOutage

SECOND = 10**9
FIELDS = frozenset({"database_bytes", "journal_bytes", "sink_archive_bytes", "backup_bytes",
                    "derived_bytes", "control_and_evidence_bytes"})
BASIS = "sum_of_per_file_max_apparent_length_or_allocated_512_byte_blocks"


class DiskError(ValueError):
    """Closed diagnostic without file names or local addresses."""


def require(condition, code):
    if not condition: raise DiskError(code)


def file_bytes(info):
    require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and info.st_nlink == 1
            and 0 <= info.st_size < 2**64 and 0 <= info.st_blocks < 2**55,
            "campaign_disk_file")
    return max(info.st_size, info.st_blocks * 512)


class CampaignDiskSamples:
    complete_resource_model_proved = False

    def __init__(self, root, duration_s, limits):
        require(type(duration_s) is int and duration_s % 300 == 0
                and 73 <= duration_s // 300 <= 288, "campaign_disk_duration")
        require(type(limits) is dict and limits.keys() == FIELDS
                and all(type(value) is int and 0 < value < 2**64 for value in limits.values()),
                "campaign_disk_limits")
        self.root, self.duration_ns, self.limits = Path(root), duration_s * SECOND, dict(limits)
        self.windows = [dict.fromkeys(FIELDS, 0) for _ in range(duration_s // 300)]
        self.coverage = [0] * len(self.windows)
        self.last_ns = self.last_sample_ns = None
        self.output = self.archive = None
        self.failed = self.complete = False
        self.count = 0

    def __enter__(self):
        info = self.root.lstat()
        require(stat.S_ISDIR(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o700
                and info.st_uid == os.geteuid(), "campaign_disk_root")
        require({entry.name for entry in (self.root / "archive").iterdir()} == {"archive.redb"},
                "campaign_disk_archive_inventory")
        self.archive = os.open(self.root / "archive/archive.redb", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            self.archive_identity = self.identity(os.fstat(self.archive))
            file_bytes(os.fstat(self.archive))
            descriptor = os.open(self.root / "disk-samples.jsonl", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            self.output = os.fdopen(descriptor, "wb")
            return self
        except BaseException:
            os.close(self.archive); self.archive = None
            raise

    @staticmethod
    def identity(info):
        return info.st_dev, info.st_ino

    def _archive(self, outage):
        opened = os.fstat(self.archive)
        require(self.identity(opened) == self.archive_identity, "campaign_disk_archive_identity")
        if outage is None:
            require({entry.name for entry in (self.root / "archive").iterdir()} == {"archive.redb"},
                    "campaign_disk_archive_inventory")
            named = (self.root / "archive/archive.redb").lstat()
            require(stat.S_ISREG(named.st_mode) and self.identity(named) == self.archive_identity,
                    "campaign_disk_archive_identity")
        else:
            require(isinstance(outage, LocalArchiveOutage) and outage.state == "paused"
                    and outage.path == self.root / "archive"
                    and outage.file_identity == self.archive_identity
                    and self.identity(os.fstat(outage.file)) == self.archive_identity
                    and self.identity((self.root / "archive").lstat()) == outage.directory_identity,
                    "campaign_disk_archive_identity")
        return file_bytes(opened)

    def _snapshot(self, outage):
        totals = dict.fromkeys(FIELDS, 0)
        totals["sink_archive_bytes"] = self._archive(outage)
        pending, entries, directories = [(self.root, 0, None)], 0, 0
        while pending:
            path, depth, category = pending.pop()
            require(depth <= 16, "campaign_disk_depth")
            directories += 1
            require(directories <= 1024, "campaign_disk_directory_bound")
            with os.scandir(path) as children:
                for child in children:
                    entries += 1
                    require(entries <= 8192, "campaign_disk_entry_bound")
                    try: info = child.stat(follow_symlinks=False)
                    except FileNotFoundError: continue
                    selected = category
                    if depth == 0:
                        if child.name == "archive":
                            require(stat.S_ISDIR(info.st_mode), "campaign_disk_archive_identity")
                            continue
                        if child.name == "backups" or child.name.startswith("retained-pre-restore-"):
                            selected = "backup_bytes"
                        elif child.name == "projections": selected = "derived_bytes"
                        elif child.name == "data": selected = "database_bytes"
                        else: selected = "control_and_evidence_bytes"
                    if stat.S_ISDIR(info.st_mode):
                        require(info.st_uid == os.geteuid(), "campaign_disk_directory")
                        pending.append((Path(child.path), depth + 1, selected))
                    else:
                        if selected == "database_bytes" and ".riffjournal" in child.name:
                            selected = "journal_bytes"
                        totals[selected] += file_bytes(info)
                        require(totals[selected] < 2**64, "campaign_disk_overflow")
        return totals, entries, directories

    def poll(self, elapsed_ns, *, archive_outage=None, terminal=False):
        require(not self.failed and not self.complete, "campaign_disk_failed")
        try:
            require(type(elapsed_ns) is int and 0 <= elapsed_ns <= self.duration_ns + 75 * SECOND
                    and (self.last_ns is None or self.last_ns <= elapsed_ns)
                    and type(terminal) is bool, "campaign_disk_clock")
            self.last_ns = elapsed_ns
            require(elapsed_ns - (self.last_sample_ns or 0) <= 75 * SECOND, "campaign_disk_gap")
            if not terminal and self.last_sample_ns is not None and elapsed_ns - self.last_sample_ns < 30 * SECOND:
                return None
            require(self.output is not None and self.count < 4096, "campaign_disk_sample_bound")
            values, entries, directories = self._snapshot(archive_outage)
            require(all(values[key] <= self.limits[key] for key in FIELDS), "campaign_disk_limit")
            record = dict(schema="riffdb.ticketdesk-lifecycle-disk-sample/v1", elapsed_ns=elapsed_ns,
                basis=BASIS, files_and_directories=entries, directories=directories,
                resources=values, complete_resource_model_proved=False)
            encoded = json.dumps(record, sort_keys=True).encode() + b"\n"
            require(len(encoded) <= 4096, "campaign_disk_record_bound")
            self.output.write(encoded); self.output.flush()
            index = min(elapsed_ns // (300 * SECOND), len(self.windows) - 1)
            self.coverage[index] += 1
            for key in FIELDS: self.windows[index][key] = max(self.windows[index][key], values[key])
            self.last_sample_ns = elapsed_ns
            self.count += 1
            if terminal:
                require(elapsed_ns >= self.duration_ns and archive_outage is None
                        and all(self.coverage), "campaign_disk_incomplete")
                self.complete = True
            return record
        except BaseException:
            self.failed = True
            raise

    def __exit__(self, _kind, _error, _traceback):
        if self.output is not None: self.output.close()
        if self.archive is not None: os.close(self.archive)
        return False
