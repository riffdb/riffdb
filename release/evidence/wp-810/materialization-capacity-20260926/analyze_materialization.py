"""WP-810 observation analysis. Overlap is association, not causal attribution."""
from collections import defaultdict
from pathlib import Path
import argparse
import bisect
import json

from validate_cell import validate

LIMITS = {"group": (65536, 20), "checkpoint": (512, 12), "io": (16777216, 9)}
PHASES = ("dispatch", "begin_transaction", "replay", "durable_commit", "installation")
KINDS = {1: "write", 2: "sync", 3: "resize", 4: "read"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def union(intervals):
    result = []
    for start, end in sorted(intervals):
        require(start <= end, "reversed interval")
        if start == end:
            continue
        if result and start <= result[-1][1]:
            result[-1] = (result[-1][0], max(end, result[-1][1]))
        else:
            result.append((start, end))
    return result


class Intervals:
    """Bounded union and logarithmic lookup; never sum concurrent durations."""
    def __init__(self, intervals):
        self.rows = union(intervals)
        self.ends = [end for _, end in self.rows]

    def overlap(self, start, end):
        require(start <= end, "reversed query")
        total = 0
        for index in range(bisect.bisect_right(self.ends, start), len(self.rows)):
            low, high = self.rows[index]
            if low >= end:
                break
            total += min(end, high) - max(start, low)
        return total


def parse_log(path):
    require(path.stat().st_size <= 2048 * 1024 * 1024, "log exceeds bounded input budget")
    processes = {}
    with path.open() as stream:
        for line in stream:
            if not line.startswith("riffdb-wp810-"):
                continue
            require(len(line) <= 4096, "diagnostic row exceeds line budget")
            parts = line.rstrip().split("\t")
            kind = parts[0].removeprefix("riffdb-wp810-").removesuffix("-v1")
            require(parts[0].endswith("-v1") and kind in (*LIMITS, "coverage"), "unknown diagnostic schema")
            require(parts[1].startswith("pid="), "missing process identity")
            pid = int(parts[1][4:])
            if pid not in processes:
                require(len(processes) < 32, "process population exceeds budget")
                processes[pid] = {k: [] for k in (*LIMITS, "coverage")}
            rows = processes[pid][kind]
            if kind == "coverage":
                require(len(rows) < 2048, "coverage snapshots exceed budget")
                rows.append({k: int(v) for k, v in (p.split("=", 1) for p in parts[2:])})
            else:
                limit, width = LIMITS[kind]
                require(len(parts) == 3 and len(rows) < limit, "row population exceeds budget")
                row = [int(value) for value in parts[2].split(",")]
                require(len(row) == width and all(0 <= n < 2**64 for n in row), "invalid row width/value")
                require(row[0] == len(rows) + 1, "missing, repeated or reordered diagnostic row")
                rows.append(row)
    return processes


def check_coverage(process):
    require(bool(process["coverage"]), "missing coverage footer")
    final = process["coverage"][-1]
    require(final["final"] == 1, "missing final coverage footer")
    for kind, plural in (("group", "groups"), ("checkpoint", "checkpoints"), ("io", "io")):
        require(final["dropped_" + plural] == 0, "diagnostic overflow")
        require(final[plural] == final["reserved_" + plural] == len(process[kind]), "incomplete final trace")
    return final


def validate_process(process):
    coverage = check_coverage(process)
    checkpoints = process["checkpoint"]
    previous_end = 0
    for c in checkpoints:
        require(c[8] == 0, "checkpoint failure")
        require(previous_end <= c[1] <= c[9] <= c[10] <= c[11] <= c[2] <= c[3], "invalid checkpoint lifecycle")
        require(c[1] > 0 and 0 < c[7] < 16, "missing checkpoint selection")
        previous_end = c[3]
    for row in process["io"]:
        _, cp, phase, kind, start, end, _, failed, active = row
        require(kind in KINDS and 0 < start <= end and failed == 0, "invalid or failed engine I/O")
        require(active <= len(checkpoints), "unknown active checkpoint")
        if cp:
            require(cp <= len(checkpoints) and phase in (1, 2, 3), "unknown I/O context")
            c = checkpoints[cp - 1]
            bounds = {1: (c[9], c[10]), 2: (c[10], c[11]), 3: (c[11], c[2])}[phase]
            require(bounds[0] <= start <= end <= bounds[1], "I/O outside labeled phase")
        else:
            require(phase == 0, "unscoped I/O with checkpoint phase")
    flushes = {}
    last_sequence = 0
    for g in process["group"]:
        require(last_sequence < g[1] <= g[2] and g[3] == g[2] - g[1] + 1, "overlapping or invalid command range")
        last_sequence = g[2]
        require(g[15] == 0 and g[7] > 0, "failed or missing journal flush")
        require(0 < g[4] <= g[17] <= g[18] <= g[19] <= g[5] <= g[6] <= g[10] <= g[11], "invalid group timing")
        require(g[12] in (0, 1, 2), "invalid drain observation")
        require(g[13] <= len(checkpoints) and g[16] <= len(checkpoints), "unknown group checkpoint")
        if g[14]:
            require(g[4] <= g[8] <= g[9] <= g[11], "invalid first-wait timing")
        else:
            require(g[8] == g[9] == 0, "wait timing without call")
        phases = (g[17], g[18], g[19], g[5], g[6])
        require(g[7] not in flushes or flushes[g[7]] == phases, "inconsistent shared flush")
        flushes[g[7]] = phases
    return coverage, flushes


def distributions(values):
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "sum_ns": 0}
    return {"count": len(values), "sum_ns": sum(values), "max_ns": ordered[-1],
            **{f"p{q}_ns": ordered[(len(values) * q + 99) // 100 - 1] for q in (50, 95, 99)}}


def checkpoint_completion_coverage(census, checkpoints, last_publication):
    # The public census is read before graceful close, which can finish a
    # registered materializer. The final backend-close trace includes that
    # completion. Do not relabel its I/O as load-period work.
    require(census["failures"] == 0 and census["starts"] == len(checkpoints), "checkpoint count mismatch")
    require(0 <= census["installed"] <= census["materialized"] <= census["starts"], "invalid pre-close checkpoint counts")
    for key, column in (("materialized", 2), ("installed", 3)):
        require(all(c[column] > last_publication for c in checkpoints[census[key]:]),
                "unreconciled checkpoint completion within load period")
    return {"census_scope": "before graceful close; no exact snapshot timestamp",
            "trace_scope": "complete process lifetime, including graceful close",
            "preclose_materialized": census["materialized"], "preclose_installed": census["installed"],
            "final_materialized": len(checkpoints), "final_installed": len(checkpoints),
            "additional_completion_constraint": "Each completion absent from the pre-close census must be after the last command publication."}


def phase_indexes(checkpoints, io):
    phases = defaultdict(list)
    for c in checkpoints:
        edges = [c[1], c[9], c[10], c[11], c[2], c[3]]
        for name, start, end in zip(PHASES, edges, edges[1:]):
            phases[name].append((start, end))
    engine = defaultdict(list)
    for row in io:
        engine["all_engine_io"].append((row[4], row[5]))
        engine["checkpoint_io" if row[1] else "unscoped_io"].append((row[4], row[5]))
        engine["engine_" + KINDS[row[3]]].append((row[4], row[5]))
    return ({k: Intervals(v) for k, v in phases.items()},
            {k: Intervals(v) for k, v in engine.items()})


def checkpoint_sync_context(io):
    previous = {}
    syncs = []
    for row in io:
        if row[1] == 0:
            continue
        key = (row[1], row[2])
        prior = previous.get(key)
        if row[3] == 2:
            syncs.append({"io": row[0], "checkpoint": row[1], "phase": row[2],
                          "start_ns": row[4], "end_ns": row[5], "duration_ns": row[5]-row[4],
                          "previous_same_phase_io": prior,
                          "immediately_preceded_by_resize": prior is not None and prior[3] == 3})
        previous[key] = row
    return syncs


def describe_interval(start, end, phases, engine):
    by_phase = {k: intervals.overlap(start, end) for k, intervals in phases.items()}
    by_io = {k: intervals.overlap(start, end) for k, intervals in engine.items()}
    require(sum(by_phase.values()) <= end - start, "checkpoint phases overlap")
    return {"start_ns": start, "end_ns": end, "duration_ns": end - start,
            "checkpoint_phase_overlap_ns": by_phase,
            "outside_checkpoint_ns": end - start - sum(by_phase.values()),
            "engine_io_overlap_ns": by_io,
            "without_observed_engine_io_ns": end - start - by_io.get("all_engine_io", 0)}


def analyze(cell, local=False):
    report_path, raw_path, log_path = cell / "report.json", cell / "raw/raw-request-tail.json", cell / "run.log"
    for path, limit in ((report_path, 64 << 20), (raw_path, 128 << 20), (log_path, 2048 << 20)):
        require(path.stat().st_size <= limit, "evidence exceeds input budget")
    result = validate(report_path, raw_path, log_path)
    report, raw = json.loads(report_path.read_text()), json.loads(raw_path.read_text())
    point = report["curve"][0]
    evidence = point["server_stage_evidence"]
    require(point["clients"] == 32 and point["profile"] == "write_only", "wrong workload")
    require(point["outcomes"]["error"] == point["outcomes"]["unavailable"] == 0, "workload error")
    require(evidence["archive_collection"]["enabled"] is False, "archive must be disabled")
    require(raw["missing_commit_sequences"] == 0, "missing sequence join")
    if not local:
        for name in ("preflight.json", "postflight.json"):
            require(json.loads((cell / name).read_text())["valid"] is True, "invalid host")
    requests = []
    seen = set()
    for worker in raw["workers"]:
        require(len(worker["samples_ns"]) == len(worker["commit_sequences"]), "sequence population drift")
        for ordinal, (elapsed, sequence) in enumerate(zip(worker["samples_ns"], worker["commit_sequences"])):
            require(sequence > 0 and sequence not in seen, "missing or repeated request identity")
            seen.add(sequence)
            requests.append((sequence, elapsed, worker["worker"], ordinal))
    processes = parse_log(log_path)
    low, high = min(seen), max(seen)
    candidates = [(pid, p) for pid, p in processes.items() if p["group"] and p["group"][0][1] <= low and p["group"][-1][2] >= high]
    require(len(candidates) == 1, "ambiguous/missing load process")
    _, process = candidates[0]
    coverage, flushes = validate_process(process)
    groups, checkpoints, io = process["group"], process["checkpoint"], process["io"]
    require(checkpoints and any(row[1] for row in io), "production checkpoint backend path not observed")
    cp_census, journal = evidence["writer_checkpoint_census"], evidence["writer_journal_stages"]
    completion_coverage = checkpoint_completion_coverage(cp_census, checkpoints, max(g[11] for g in groups))
    for column, key in ((4, "selected_transitions"), (5, "selected_commands"), (6, "selected_bytes")):
        require(sum(c[column] for c in checkpoints) == cp_census[key], "checkpoint selection mismatch")
    for i, key in enumerate(("transitions", "bytes", "physical", "reanchor")):
        require(sum(bool(c[7] & (1 << i)) for c in checkpoints) == cp_census["trigger_counts_nonexclusive"][key], "checkpoint trigger mismatch")
    require(len(groups) == journal["queue_observations"] and len(flushes) == journal["physical_flushes"], "journal population mismatch")
    # Encode begin -> write begin includes a small uncensused gap. Do not assert
    # it equals encode_elapsed. Write and sync retain the exact census endpoints.
    for label, a, b in (("positional_write", 1, 2), ("sync", 3, 4)):
        values = [(row[b] - row[a]) // 1000 for row in flushes.values()]
        require(sum(values) == journal[label + "_sum_us"] and max(values) == journal[label + "_max_us"], "journal duration mismatch")
    queue = [(g[17] - g[4]) // 1000 for g in groups]
    require(sum(queue) == journal["queue_sum_us"] and max(queue) == journal["queue_max_us"], "journal queue mismatch")
    publication = evidence["writer_publication_stages"]
    require(publication["instrument_version"] == 2 and publication["receipt_wait_errors"] == 0, "invalid receipt counter")
    require(sum(g[14] for g in groups) == publication["receipt_wait_calls"], "receipt wait population mismatch")
    ends = [g[2] for g in groups]
    measured_groups = set()
    joined = []
    for sequence, elapsed, worker, ordinal in requests:
        g = groups[bisect.bisect_left(ends, sequence)]
        require(g[1] <= sequence <= g[2], "missing request group")
        measured_groups.add(g[0])
        joined.append({"sequence": sequence, "elapsed_ns": elapsed, "worker": worker,
                       "worker_ordinal": ordinal, "group": g[0], "flush": g[7],
                       "queue_ns": g[17] - g[4], "first_receipt_wait_ns": g[9] - g[8],
                       "first_wait_observed": g[14] > 0, "publication_ns": g[11] - g[10]})
    measured_flushes = {g[7] for g in groups if g[0] in measured_groups}
    phases, engine = phase_indexes(checkpoints, io)
    events = []
    for flush, (_, wb, we, sb, se) in flushes.items():
        if flush not in measured_flushes:
            continue
        events.append({"flush": flush, "write": describe_interval(wb, we, phases, engine),
                       "sync": describe_interval(sb, se, phases, engine)})
    slow_syncs = sorted((e for e in events if e["sync"]["duration_ns"] > 10_000_000), key=lambda e: -e["sync"]["duration_ns"])
    io_summary = {}
    for phase in range(4):
        for kind, label in KINDS.items():
            rows = [r for r in io if r[2] == phase and r[3] == kind]
            io_summary[f"phase_{phase}_{label}"] = dict(distributions([r[5] - r[4] for r in rows]), bytes=sum(r[6] for r in rows))
    result.update(scope="Local functional proof only; no performance conclusion." if local else "Single instrumented diagnostic; no observer-overhead, mitigation-effect or package-closure claim.",
                  limits="All server intervals share one process-local clock. Requests join by commit sequence; clocks are never subtracted across processes. Overlap does not prove causation. Engine I/O classes overlap on different threads; only their union is subtracted. Journal flushes are deduplicated. First wait may be absent for nonblocking publication. Census and checkpoint totals include warmup; selected flushes and requests are measured-only.",
                  coverage=coverage, checkpoint_census=cp_census, checkpoint_completion_coverage=completion_coverage, journal_census=journal,
                  populations={"requests": len(requests), "measured_groups": len(measured_groups), "measured_flushes": len(measured_flushes)},
                  checkpoint_phases={k: distributions([c[b]-c[a] for c in checkpoints])
                                     for k,a,b in zip(PHASES, (1,9,10,11,2), (9,10,11,2,3))},
                  engine_io=io_summary,
                  checkpoint_sync_context=checkpoint_sync_context(io),
                  slow_syncs_over_10ms=slow_syncs,
                  slowest_20_joined_requests=sorted(joined, key=lambda r: (-r["elapsed_ns"], r["worker"], r["worker_ordinal"]))[:20],
                  measured_write=distributions([e["write"]["duration_ns"] for e in events]),
                  measured_sync=distributions([e["sync"]["duration_ns"] for e in events]))
    (cell / "joined-requests.json").write_text(json.dumps(joined) + "\n")
    (cell / "materialization-analysis.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("cell", type=Path)
    parser.add_argument("--local-functional", action="store_true")
    args = parser.parse_args()
    result = analyze(args.cell, args.local_functional)
    print(json.dumps({k: result[k] for k in ("scope", "coverage", "populations")}, indent=2))
