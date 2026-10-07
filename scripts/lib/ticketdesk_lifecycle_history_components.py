# req: END-007, END-008, END-010
"""Independent final-period history trends; never a complete resource claim.

Physical journal allocation overlaps canonical history representations. Keep
each native basis separate: a fall in one cannot excuse growth in another.
Live data, retained evidence, memory, archive, backups and derived state still
need their independent model and observations.
"""
import hashlib
import json
import os
from pathlib import Path
import stat

from . import ticketdesk_lifecycle_logical_bytes as logical
from . import ticketdesk_lifecycle_physical_bytes as physical
from . import ticketdesk_lifecycle_source_bytes as source
from .ticketdesk_lifecycle_provider_budget import ProviderBudgetError

SECOND = 1000000000
WINDOW_NS = 300 * SECOND
MAX_U64 = 2**64 - 1
KINDS = frozenset({"logical", "source", "physical"})
METADATA = frozenset({"schema", "elapsed_ns", "basis", "qualifies_complete_resource_model"})
CONTRACTS = {
    "logical": (logical, "canonical_logical_history_and_removed_keys",
                ("canonical_history_value_bytes", "removed_key_bytes"),
                ("root", "watermark", "head")),
    "source": (source, "canonical_source_receipts_and_keys",
               ("canonical_receipt_bytes", "key_bytes"), ("root", "resume")),
    "physical": (physical, "native_original_sealed_prefix_allocated_blocks",
                 ("eligible_allocated_bytes",),
                 ("root", "watermark", "minimum_resume", "active_generation")),
}
MAX_FILE_BYTES = 2048 * 4096


class HistoryComponentsError(ValueError):
    """Closed refusal without source records, paths or identities."""


def require(condition, code):
    if not condition:
        raise HistoryComponentsError(code)


def integer(value):
    require(type(value) is int and 0 <= value <= MAX_U64, "history_components_number")
    return value


def native_record(kind, record):
    module, basis, _, _ = CONTRACTS[kind]
    require(type(record) is dict and record.keys() == module.FIELDS | METADATA,
            "history_components_shape")
    require(record["schema"] == "riffdb.ticketdesk-lifecycle-" + kind + "-byte-sample/v1"
            and record["basis"] == basis
            and record["qualifies_complete_resource_model"] is False,
            "history_components_basis")
    integer(record["elapsed_ns"])
    encoded = module.PREFIX + b"\t".join(
        name.encode("ascii") + b"=" + str(integer(record[name])).encode("ascii")
        for name in sorted(module.FIELDS)) + b"\n"
    try:
        return module.parse_sample(encoded)
    except ProviderBudgetError as error:
        raise HistoryComponentsError("history_components_native_accounting") from error


def unique_object(pairs):
    value = {}
    for name, item in pairs:
        require(name not in value, "history_components_json")
        value[name] = item
    return value


def parse_integer(encoded):
    require(len(encoded) <= 20, "history_components_number")
    return integer(int(encoded))


def reject_number(_encoded):
    raise HistoryComponentsError("history_components_number")


def file_identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def read_observations(root):
    """Read closed private collector files, binding the exact observed bytes.

    This cannot establish that a campaign produced them. The caller must bind
    their digests to the original log/campaign receipts separately.
    """
    directory = None
    try:
        root = Path(root)
        require(root.is_absolute() and root.resolve() == root, "history_components_custody")
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        selected = os.fstat(directory)
        require(selected.st_uid == os.geteuid() and stat.S_IMODE(selected.st_mode) == 0o700,
                "history_components_custody")
        records, inputs, identities = {}, {}, {}
        for kind in sorted(KINDS):
            name = kind + "-byte-samples.jsonl"
            descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                                 dir_fd=directory)
            with os.fdopen(descriptor, "rb") as stream:
                before = os.fstat(stream.fileno())
                require(stat.S_ISREG(before.st_mode) and before.st_uid == os.geteuid()
                        and stat.S_IMODE(before.st_mode) == 0o600 and before.st_nlink == 1,
                        "history_components_custody")
                require(0 < before.st_size <= MAX_FILE_BYTES, "history_components_bound")
                rows, size, digest = [], 0, hashlib.sha256()
                while line := stream.readline(4097):
                    size += len(line)
                    require(len(rows) < 2048 and size <= MAX_FILE_BYTES
                            and 0 < len(line) <= 4096 and line.endswith(b"\n"),
                            "history_components_bound")
                    digest.update(line)
                    try:
                        row = json.loads(line, object_pairs_hook=unique_object,
                                         parse_int=parse_integer, parse_float=reject_number,
                                         parse_constant=reject_number)
                    except (ValueError, UnicodeError, RecursionError) as error:
                        raise HistoryComponentsError("history_components_json") from error
                    native_record(kind, row)
                    rows.append(row)
                after = os.fstat(stream.fileno())
                named = os.stat(name, dir_fd=directory, follow_symlinks=False)
                require(size == before.st_size and stat.S_ISREG(named.st_mode)
                        and file_identity(before) == file_identity(after) == file_identity(named),
                        "history_components_changed")
                records[kind] = rows
                inputs[kind] = dict(sha256=digest.hexdigest(), bytes=size, samples=len(rows))
                identities[name] = file_identity(before)
        for name, expected in identities.items():
            named = os.stat(name, dir_fd=directory, follow_symlinks=False)
            require(stat.S_ISREG(named.st_mode) and file_identity(named) == expected,
                    "history_components_changed")
        named_root = root.lstat()
        require(stat.S_ISDIR(named_root.st_mode)
                and named_root.st_uid == os.geteuid()
                and stat.S_IMODE(named_root.st_mode) == 0o700
                and (selected.st_dev, selected.st_ino) == (named_root.st_dev, named_root.st_ino),
                "history_components_changed")
        return records, inputs
    except OSError as error:
        raise HistoryComponentsError("history_components_io") from error
    finally:
        if directory is not None:
            os.close(directory)


def reconcile(records, *, duration_s, incarnation):
    """Check bounded complete inputs and all 72 final five-minute windows.

    Input consists of the three collectors' records on the same offered clock.
    The caller must separately bind original logs, artifact custody and actual
    action/incarnation evidence; these arithmetic checks do not supply them.
    Maxima disclose the observed crest in each window rather than a convenient
    final cleanup. Each component must pass both endpoint and trend checks.
    """
    integer(duration_s)
    require(duration_s % 300 == 0 and 73 <= duration_s // 300 <= 288,
            "history_components_duration")
    require(integer(incarnation) > 0, "history_components_incarnation")
    require(type(records) is dict and records.keys() == KINDS, "history_components_shape")
    final_start = duration_s - 21600
    first_window = final_start // 300
    maxima = {kind: [None] * 72 for kind in KINDS}
    counts = {}
    for kind in sorted(KINDS):
        rows = records[kind]
        require(type(rows) is list and 0 < len(rows) <= 2048, "history_components_bound")
        module, _, amounts, continuity = CONTRACTS[kind]
        last_ns = None
        previous_final = None
        final_count = 0
        first_final_ns = None
        for record in rows:
            value = native_record(kind, record)
            elapsed = record["elapsed_ns"]
            require(elapsed < duration_s * SECOND
                    and (last_ns is None or elapsed > last_ns), "history_components_clock")
            require(elapsed - (last_ns or 0) <= module.MAX_GAP_NS,
                    "history_components_gap")
            last_ns = elapsed
            if elapsed < final_start * SECOND:
                continue
            require(value["incarnation"] == incarnation, "history_components_incarnation")
            require(previous_final is None
                    or all(value[name] >= previous_final[name] for name in continuity),
                    "history_components_continuity")
            previous_final = value
            if first_final_ns is None:
                first_final_ns = elapsed
            final_count += 1
            index = elapsed // WINDOW_NS - first_window
            amount = sum(value[name] for name in amounts)
            integer(amount)
            maxima[kind][index] = max(maxima[kind][index] or 0, amount)
        require(first_final_ns is not None
                and first_final_ns - final_start * SECOND <= module.MAX_GAP_NS
                and duration_s * SECOND - last_ns <= module.MAX_GAP_NS,
                "history_components_gap")
        require(all(value is not None for value in maxima[kind]), "history_components_coverage")
        values = maxima[kind]
        trend = sum((2 * index - 71) * value for index, value in enumerate(values))
        require(values[-1] <= values[0] and trend <= 0, "history_components_growth")
        counts[kind] = final_count
    return {
        "schema": "riffdb.ticketdesk-lifecycle-final-history-components/v1",
        "final_start_s": final_start,
        "basis": {kind: CONTRACTS[kind][1] for kind in sorted(KINDS)},
        "final_samples": counts,
        "windows": [dict(start_s=final_start + 300 * index,
                         end_s=final_start + 300 * (index + 1),
                         components={kind: maxima[kind][index] for kind in sorted(KINDS)})
                    for index in range(72)],
        "qualifies_complete_resource_model": False,
    }
