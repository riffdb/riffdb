"""Exact WP-674 host-invalid receipt validation policy."""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any

from bounded_evidence_io import bounded_read


SCHEMA = "riffdb.app-baseline-host-validity/v2"
CAMPAIGN_SCHEMA = "riffdb.wp-674-unary-campaign-identity/v1"
MAX_REPORTED_PROCESSES = 32
MAX_SCANNED_PROCESSES = 4096
MAX_HOST_RECEIPT_BYTES = 32 * 1024 * 1024
MAX_STEAL_PERCENT = 1.0
PROFILE_HARDWARE_RULES = {
    "workstation": {"logical_cpus": 32, "cpu_contains": "AMD Ryzen 9 7950X"},
    "n1": {"logical_cpus": 8, "cpu_contains": "Intel"},
    "e2": {"logical_cpus": 8, "cpu_contains": "AMD EPYC 7B12"},
}
SCENARIO_ORDER = (
    "point_get_ticket",
    "point_get_user",
    "list_tickets_by_project_status",
    "list_open_tickets_for_assignee",
    "list_comments_for_ticket",
    "list_project_members",
    "ticket_detail_page",
    "board_page_50",
    "board_page_200",
    "board_page_450",
    "create_comment",
    "close_ticket_with_comment",
    "swap_member_roles",
    "open_ticket_with_labels",
)
SHA256 = re.compile(r"^[0-9a-f]{64}$")
CAMPAIGN_METHOD = {
    "samples": 1000,
    "warmups": 20,
    "reps": 5,
    "comparators": ["safe-app", "minimal"],
    "scenarios": list(SCENARIO_ORDER),
    # ADR-0171: the central-three spread rule binds the gated backend only;
    # the comparator's spread is disclosure. Every campaign records the rule
    # it ran under so a receipt cannot be reinterpreted under another one.
    "stability_rule_binds": "gated_backend",
}


def exact_keys(value: Any, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError(f"{label} shape is invalid")
    return value


def bounded_optional_text(value: Any, maximum: int, label: str) -> None:
    if value is not None and (
        not isinstance(value, str) or not value or len(value) > maximum
    ):
        raise ValueError(f"{label} is invalid")


def nonnegative_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} is invalid")
    return value


def finite_nonnegative_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} is invalid")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{label} is invalid")
    return result


def strict_equal(actual: Any, expected: Any, label: str) -> None:
    if type(actual) is not type(expected):
        raise ValueError(f"{label} type differs")
    if isinstance(expected, dict):
        if set(actual) != set(expected):
            raise ValueError(f"{label} shape differs")
        for key, expected_value in expected.items():
            strict_equal(actual[key], expected_value, f"{label}.{key}")
    elif isinstance(expected, list):
        if len(actual) != len(expected):
            raise ValueError(f"{label} length differs")
        for index, expected_value in enumerate(expected):
            strict_equal(actual[index], expected_value, f"{label}[{index}]")
    elif actual != expected:
        raise ValueError(f"{label} value differs")


def validate_campaign_identity_value(
    value: Any,
    expected_profile: str,
    expected_identity: dict[str, str],
) -> dict[str, Any]:
    campaign = exact_keys(
        value,
        {
            "schema",
            "profile",
            "source_revision",
            "cargo_lock_sha256",
            "harness_sha256",
            "runner_sha256",
            "riffdbd_sha256",
            "method",
        },
        "campaign identity",
    )
    strict_equal(campaign["schema"], CAMPAIGN_SCHEMA, "campaign schema")
    strict_equal(campaign["profile"], expected_profile, "campaign profile")
    for key in (
        "source_revision",
        "cargo_lock_sha256",
        "harness_sha256",
        "runner_sha256",
        "riffdbd_sha256",
    ):
        expected = expected_identity.get(key)
        if not isinstance(expected, str) or not isinstance(campaign[key], str):
            raise ValueError(f"campaign {key} is invalid")
        if key == "source_revision":
            if re.fullmatch(r"[0-9a-f]{40}", campaign[key]) is None:
                raise ValueError("campaign source revision is invalid")
        elif SHA256.fullmatch(campaign[key]) is None:
            raise ValueError(f"campaign {key} is invalid")
        strict_equal(campaign[key], expected, f"campaign {key}")
    strict_equal(campaign["method"], CAMPAIGN_METHOD, "campaign method")
    return campaign


def checked_process(value: Any, label: str) -> dict[str, Any]:
    process = exact_keys(
        value,
        {
            "pid",
            "comm",
            "cpu_percent_of_one_logical_cpu",
            "read_bytes_per_sec",
            "write_bytes_per_sec",
            "rss_bytes",
        },
        label,
    )
    pid = nonnegative_integer(process["pid"], f"{label}.pid")
    if pid == 0 or pid > 2**31 - 1:
        raise ValueError(f"{label}.pid is invalid")
    bounded_optional_text(process["comm"], 64, f"{label}.comm")
    if not isinstance(process["comm"], str):
        raise ValueError(f"{label}.comm is invalid")
    cpu_percent = finite_nonnegative_number(
        process["cpu_percent_of_one_logical_cpu"], f"{label}.cpu_percent"
    )
    if cpu_percent > 1000.0:
        raise ValueError(f"{label}.cpu_percent is invalid")
    for key in ("read_bytes_per_sec", "write_bytes_per_sec", "rss_bytes"):
        if nonnegative_integer(process[key], f"{label}.{key}") > 2**63 - 1:
            raise ValueError(f"{label}.{key} is invalid")
    return process


def validate_host_observation(
    value: Any,
    profile: str,
    *,
    boundary: str,
    expected_valid: bool,
) -> dict[str, Any]:
    observation = exact_keys(
        value,
        {
            "schema",
            "valid",
            "reason",
            "sample_interval_ms",
            "logical_cpus",
            "load_average_before",
            "load_average_after",
            "memory_available_bytes",
            "hardware_identity",
            "boot_identity_sha256",
            "cpu_accounting",
            "whole_cell",
            "baseline_report_sha256",
            "thresholds",
            "active_processes",
            "interfering_processes",
            "inventory_bounds",
            "process_interval",
        },
        f"V2 {boundary}",
    )
    if (
        not isinstance(observation["schema"], str)
        or observation["schema"] != SCHEMA
        or type(observation["valid"]) is not bool
        or observation["valid"] is not expected_valid
        or observation["reason"] != (None if expected_valid else "host_interference")
    ):
        raise ValueError(f"V2 {boundary} classification differs")
    if boundary == "preflight":
        if (
            observation["whole_cell"] is not None
            or observation["baseline_report_sha256"] is not None
        ):
            raise ValueError("V2 preflight boundary differs")
    elif boundary == "postflight":
        if not isinstance(observation["whole_cell"], dict) or not isinstance(
            observation["baseline_report_sha256"], str
        ):
            raise ValueError("V2 postflight boundary differs")
    else:
        raise ValueError("unknown host-validity boundary")
    interval = nonnegative_integer(
        observation["sample_interval_ms"], f"V2 {boundary} sample interval"
    )
    if not 50 <= interval <= 10_000:
        raise ValueError(f"V2 {boundary} sample interval differs")
    rules = PROFILE_HARDWARE_RULES[profile]
    logical_cpus = nonnegative_integer(
        observation["logical_cpus"], f"V2 {boundary} logical CPUs"
    )
    if logical_cpus != rules["logical_cpus"]:
        raise ValueError(f"V2 {boundary} logical CPU profile differs")
    hardware = exact_keys(
        observation["hardware_identity"],
        {"cpu_model", "system_product", "system_vendor", "operating_system"},
        f"V2 {boundary} hardware identity",
    )
    bounded_optional_text(hardware["cpu_model"], 128, "CPU model")
    if not isinstance(hardware["cpu_model"], str) or rules["cpu_contains"] not in hardware[
        "cpu_model"
    ]:
        raise ValueError(f"V2 {boundary} CPU profile differs")
    bounded_optional_text(hardware["system_product"], 128, "system product")
    bounded_optional_text(hardware["system_vendor"], 128, "system vendor")
    operating_system = exact_keys(
        hardware["operating_system"], {"id", "version_id"}, "operating system"
    )
    bounded_optional_text(operating_system["id"], 64, "operating-system id")
    bounded_optional_text(
        operating_system["version_id"], 64, "operating-system version"
    )
    if not isinstance(observation["boot_identity_sha256"], str) or SHA256.fullmatch(
        observation["boot_identity_sha256"]
    ) is None:
        raise ValueError(f"V2 {boundary} boot identity is invalid")
    accounting = exact_keys(
        observation["cpu_accounting"],
        {"total_ticks", "steal_ticks"},
        f"V2 {boundary} CPU accounting",
    )
    total_ticks = nonnegative_integer(
        accounting["total_ticks"], f"V2 {boundary} total CPU ticks"
    )
    steal_ticks = nonnegative_integer(
        accounting["steal_ticks"], f"V2 {boundary} steal CPU ticks"
    )
    if total_ticks > 2**64 - 1 or steal_ticks > total_ticks:
        raise ValueError(f"V2 {boundary} CPU accounting is contradictory")
    for label in ("load_average_before", "load_average_after"):
        load = observation[label]
        if not isinstance(load, list) or len(load) != 3:
            raise ValueError(f"V2 {boundary} {label} is incomplete")
        for item in load:
            finite_nonnegative_number(item, f"V2 {boundary} {label}")
    memory = observation["memory_available_bytes"]
    if (
        memory is not None
        and nonnegative_integer(memory, f"V2 {boundary} available memory") > 2**64 - 1
    ):
        raise ValueError(f"V2 {boundary} available memory is invalid")
    strict_equal(
        exact_keys(
            observation["thresholds"],
            {
                "cpu_percent_of_one_logical_cpu",
                "io_bytes_per_sec",
                "maximum_whole_cell_steal_percent",
            },
            f"V2 {boundary} thresholds",
        ),
        {
            "cpu_percent_of_one_logical_cpu": 5.0,
            "io_bytes_per_sec": 8 * 1024 * 1024,
            "maximum_whole_cell_steal_percent": MAX_STEAL_PERCENT,
        },
        f"V2 {boundary} thresholds",
    )
    strict_equal(
        exact_keys(
            observation["inventory_bounds"],
            {
                "maximum_scanned_processes",
                "maximum_reported_processes",
                "process_arguments_included",
            },
            f"V2 {boundary} inventory bounds",
        ),
        {
            "maximum_scanned_processes": MAX_SCANNED_PROCESSES,
            "maximum_reported_processes": MAX_REPORTED_PROCESSES,
            "process_arguments_included": False,
        },
        f"V2 {boundary} inventory bounds",
    )
    process_interval = exact_keys(
        observation["process_interval"],
        {
            "stable_identity_required",
            "started_processes",
            "exited_processes",
            "pid_reuses",
            "kernel_thread_events_excluded",
        },
        f"V2 {boundary} process interval",
    )
    if process_interval["stable_identity_required"] is not True:
        raise ValueError(f"V2 {boundary} PID identity rule differs")
    for key in (
        "started_processes",
        "exited_processes",
        "pid_reuses",
        "kernel_thread_events_excluded",
    ):
        if nonnegative_integer(
            process_interval[key], f"V2 {boundary} process interval {key}"
        ) > MAX_SCANNED_PROCESSES:
            raise ValueError(f"V2 {boundary} process interval {key} exceeds its bound")
    interval_stable = all(
        process_interval[key] == 0
        for key in ("started_processes", "exited_processes", "pid_reuses")
    )
    active_value = observation["active_processes"]
    interfering_value = observation["interfering_processes"]
    if (
        not isinstance(active_value, list)
        or not isinstance(interfering_value, list)
        or len(active_value) > MAX_REPORTED_PROCESSES
        or len(interfering_value) > MAX_REPORTED_PROCESSES
    ):
        raise ValueError(f"V2 {boundary} process inventory exceeds its bound")
    active = [checked_process(item, f"V2 {boundary} active process") for item in active_value]
    interfering = [
        checked_process(item, f"V2 {boundary} interfering process")
        for item in interfering_value
    ]
    expected_interfering = [
        process
        for process in active
        if process["cpu_percent_of_one_logical_cpu"] >= 5.0
        or process["read_bytes_per_sec"] >= 8 * 1024 * 1024
        or process["write_bytes_per_sec"] >= 8 * 1024 * 1024
    ]
    if len({process["pid"] for process in active}) != len(active):
        raise ValueError(f"V2 {boundary} active-process identities are duplicated")
    if interfering != expected_interfering:
        raise ValueError(f"V2 {boundary} interference inventory is contradictory")
    if expected_valid and (interfering or not interval_stable):
        raise ValueError(f"V2 {boundary} valid classification has interference or churn")
    if boundary == "preflight" and not expected_valid and not interfering and interval_stable:
        raise ValueError("V2 invalid preflight has no independent interference or churn")
    return observation


def validate_invalid_preflight_receipt(
    receipt: Any,
    profile: str,
    expected_comparator: str,
    expected_scenario: str | None,
) -> None:
    receipt = exact_keys(
        receipt,
        {
            "schema",
            "status",
            "reason",
            "requested",
            "partial_phase_evidence",
            "host_validity",
            "evidence_eligibility",
        },
        "invalid-preflight receipt",
    )
    if (
        not isinstance(receipt["schema"], str)
        or receipt["schema"] != "riffdb.app-baseline-non-evidentiary/v1"
        or not isinstance(receipt["status"], str)
        or receipt["status"] != "non_evidentiary"
        or receipt["partial_phase_evidence"] is not None
    ):
        raise ValueError("invalid-preflight receipt classification differs")
    strict_equal(
        exact_keys(receipt["reason"], {"code", "retry"}, "reason"),
        {"code": "host_interference", "retry": "rerun_on_idle_host"},
        "invalid-preflight retry reason",
    )
    expected_requested_keys = {
        "mode",
        "profile",
        "duration_secs",
        "reps",
        "postgres_comparator",
        "concurrency_sweep",
    }
    if expected_comparator == "safe-app":
        expected_requested_keys.add("scenario")
    requested = exact_keys(
        receipt["requested"],
        expected_requested_keys,
        "requested",
    )
    comparator = requested["postgres_comparator"]
    if not isinstance(comparator, str) or comparator != expected_comparator:
        raise ValueError("invalid-preflight comparator differs")
    expected_method: dict[str, Any] = {
            "mode": "full",
            "profile": "parity",
            "duration_secs": 90,
            "reps": 5,
            "concurrency_sweep": False,
    }
    if expected_comparator == "safe-app":
        if expected_scenario not in SCENARIO_ORDER:
            raise ValueError("invalid-preflight expected scenario is invalid")
        expected_method["scenario"] = expected_scenario
    elif expected_scenario is not None:
        raise ValueError("minimal invalid preflight has a scenario selector")
    strict_equal(
        {key: value for key, value in requested.items() if key != "postgres_comparator"},
        expected_method,
        "invalid-preflight requested method",
    )
    strict_equal(
        exact_keys(
            receipt["evidence_eligibility"],
            {"eligible", "host_idle", "reason_codes"},
            "evidence eligibility",
        ),
        {
            "eligible": False,
            "host_idle": False,
            "reason_codes": ["host_interference"],
        },
        "invalid-preflight eligibility",
    )
    host = exact_keys(receipt["host_validity"], {"preflight", "postflight"}, "host")
    if host["postflight"] is not None:
        raise ValueError("invalid-preflight receipt contains a postflight")
    validate_host_observation(
        host["preflight"], profile, boundary="preflight", expected_valid=False
    )


def validate_invalid_postflight_receipt(
    receipt: Any,
    profile: str,
) -> None:
    if not isinstance(receipt, dict):
        raise ValueError("invalid-postflight report root is not an object")
    if (
        receipt.get("schema") != "riffdb.app-baseline/v1"
        or "qualification_candidate" in receipt
    ):
        raise ValueError("invalid-postflight report is not a completed unqualified cell")
    expected_eligibility_keys = {
        "eligible",
        "host_idle",
        "reason_codes",
        "postgres_central_three_spread_disclosed",
        "stable",
        "correctness_clean",
        "same_device_comparable",
        "comparison_complete",
        "missing_required_fields",
        "non_evidentiary_window",
    }
    # ADR-0171 Amendment 2: a minimal-cell receipt also carries RiffDB's
    # spread disclosure; it is validated below like the comparator's.
    if isinstance(receipt.get("evidence_eligibility"), dict) and (
        "riffdb_central_three_spread_disclosed" in receipt["evidence_eligibility"]
    ):
        expected_eligibility_keys.add("riffdb_central_three_spread_disclosed")
    eligibility = exact_keys(
        receipt.get("evidence_eligibility"),
        expected_eligibility_keys,
        "invalid-postflight evidence eligibility",
    )
    for key in (
        "eligible",
        "host_idle",
        "stable",
        "correctness_clean",
        "same_device_comparable",
        "comparison_complete",
        "non_evidentiary_window",
    ):
        if type(eligibility[key]) is not bool:
            raise ValueError(f"invalid-postflight eligibility.{key} type differs")
    if (
        eligibility["eligible"] is not False
        or eligibility["host_idle"] is not False
        or eligibility["correctness_clean"] is not True
        or eligibility["same_device_comparable"] is not True
        or eligibility["comparison_complete"] is not True
        or eligibility["non_evidentiary_window"] is not False
    ):
        raise ValueError("invalid-postflight non-host eligibility differs")
    strict_equal(
        eligibility["reason_codes"],
        ["host_interference"],
        "invalid-postflight reason codes",
    )
    for key in (
        "postgres_central_three_spread_disclosed",
        "missing_required_fields",
        *(
            ("riffdb_central_three_spread_disclosed",)
            if "riffdb_central_three_spread_disclosed" in eligibility
            else ()
        ),
    ):
        value = eligibility[key]
        if (
            not isinstance(value, list)
            or len(value) > 64
            or any(not isinstance(item, str) or len(item) > 256 for item in value)
        ):
            raise ValueError(f"invalid-postflight eligibility.{key} is invalid")
    if eligibility["missing_required_fields"] not in ([], ["stability"]):
        raise ValueError("invalid-postflight has a non-host evidence-shape failure")

    host = exact_keys(receipt.get("host_validity"), {"preflight", "postflight"}, "host")
    preflight = validate_host_observation(
        host["preflight"], profile, boundary="preflight", expected_valid=True
    )
    postflight = validate_host_observation(
        host["postflight"], profile, boundary="postflight", expected_valid=False
    )
    if (
        postflight["boot_identity_sha256"] != preflight["boot_identity_sha256"]
        or postflight["logical_cpus"] != preflight["logical_cpus"]
    ):
        raise ValueError("host identity changed across the invalid cell")
    strict_equal(
        postflight["hardware_identity"],
        preflight["hardware_identity"],
        "host hardware identity",
    )
    canonical_preflight = (json.dumps(preflight, sort_keys=True) + "\n").encode()
    expected_sha256 = hashlib.sha256(canonical_preflight).hexdigest()
    if postflight["baseline_report_sha256"] != expected_sha256:
        raise ValueError("postflight is not bound to the exact preflight")
    expected_whole_cell = whole_cell_accounting(
        preflight,
        postflight["cpu_accounting"],
        current_boot_identity=postflight["boot_identity_sha256"],
        current_hardware_identity=postflight["hardware_identity"],
        current_logical_cpus=postflight["logical_cpus"],
    )
    strict_equal(
        postflight["whole_cell"], expected_whole_cell, "postflight whole-cell accounting"
    )
    interval = postflight["process_interval"]
    interval_stable = all(
        interval[key] == 0
        for key in ("started_processes", "exited_processes", "pid_reuses")
    )
    if (
        expected_whole_cell["valid"] is not False
        and not postflight["interfering_processes"]
        and interval_stable
    ):
        raise ValueError("invalid postflight has no independent host interference")


def validate_valid_host_pair(
    value: Any, profile: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    host = exact_keys(value, {"preflight", "postflight"}, "valid host pair")
    preflight = validate_host_observation(
        host["preflight"], profile, boundary="preflight", expected_valid=True
    )
    postflight = validate_host_observation(
        host["postflight"], profile, boundary="postflight", expected_valid=True
    )
    if (
        postflight["boot_identity_sha256"] != preflight["boot_identity_sha256"]
        or postflight["logical_cpus"] != preflight["logical_cpus"]
    ):
        raise ValueError("host identity changed across the valid cell")
    strict_equal(
        postflight["hardware_identity"],
        preflight["hardware_identity"],
        "valid host hardware identity",
    )
    canonical_preflight = (json.dumps(preflight, sort_keys=True) + "\n").encode()
    expected_sha256 = hashlib.sha256(canonical_preflight).hexdigest()
    if postflight["baseline_report_sha256"] != expected_sha256:
        raise ValueError("valid postflight is not bound to the exact preflight")
    expected_whole_cell = whole_cell_accounting(
        preflight,
        postflight["cpu_accounting"],
        current_boot_identity=postflight["boot_identity_sha256"],
        current_hardware_identity=postflight["hardware_identity"],
        current_logical_cpus=postflight["logical_cpus"],
    )
    strict_equal(
        postflight["whole_cell"], expected_whole_cell, "valid whole-cell accounting"
    )
    if expected_whole_cell["valid"] is not True:
        raise ValueError("valid host pair exceeds the whole-cell steal threshold")
    return preflight, postflight


def validate_host_invalid_receipt(
    path: Path,
    profile: str,
    expected_comparator: str,
    expected_scenario: str | None,
) -> bool:
    encoded = bounded_read(path, MAX_HOST_RECEIPT_BYTES, "host-invalid receipt")
    receipt = json.loads(encoded.decode("utf-8"))
    if not isinstance(receipt, dict):
        raise ValueError("host-invalid receipt root is not an object")
    if receipt.get("schema") == "riffdb.app-baseline-non-evidentiary/v1":
        validate_invalid_preflight_receipt(
            receipt, profile, expected_comparator, expected_scenario
        )
        return False
    elif receipt.get("schema") == "riffdb.app-baseline/v1":
        validate_invalid_postflight_receipt(receipt, profile)
        return True
    else:
        raise ValueError("host-invalid receipt schema differs")


def checked_cpu_accounting(value: Any, label: str) -> tuple[int, int]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} CPU accounting is absent")
    total = value.get("total_ticks")
    steal = value.get("steal_ticks")
    if (
        isinstance(total, bool)
        or not isinstance(total, int)
        or isinstance(steal, bool)
        or not isinstance(steal, int)
        or total < 0
        or steal < 0
        or steal > total
    ):
        raise ValueError(f"{label} CPU accounting is invalid")
    return total, steal


def whole_cell_accounting(
    baseline: dict[str, Any],
    current_accounting: dict[str, int],
    *,
    current_boot_identity: str,
    current_hardware_identity: dict[str, Any],
    current_logical_cpus: int | None,
) -> dict[str, Any]:
    if baseline.get("schema") != SCHEMA:
        raise ValueError("preflight host inventory has the wrong schema")
    if baseline.get("valid") is not True:
        raise ValueError("preflight host inventory is not valid")
    if (
        baseline.get("whole_cell") is not None
        or baseline.get("baseline_report_sha256") is not None
        or (baseline.get("thresholds") or {}).get(
            "maximum_whole_cell_steal_percent"
        )
        != MAX_STEAL_PERCENT
    ):
        raise ValueError("preflight host inventory is not an exact V2 preflight")
    if baseline.get("boot_identity_sha256") != current_boot_identity:
        raise ValueError("host boot identity changed across the measured cell")
    if baseline.get("hardware_identity") != current_hardware_identity:
        raise ValueError("hardware identity changed across the measured cell")
    if baseline.get("logical_cpus") != current_logical_cpus:
        raise ValueError("logical CPU count changed across the measured cell")
    before_total, before_steal = checked_cpu_accounting(
        baseline.get("cpu_accounting"), "preflight"
    )
    after_total, after_steal = checked_cpu_accounting(
        current_accounting, "postflight"
    )
    if after_total <= before_total or after_steal < before_steal:
        raise ValueError("CPU accounting regressed across the measured cell")
    total_delta = after_total - before_total
    steal_delta = after_steal - before_steal
    steal_percent = steal_delta / total_delta * 100.0
    valid = steal_percent <= MAX_STEAL_PERCENT
    return {
        "valid": valid,
        "reason": None if valid else "cpu_steal_exceeded",
        "elapsed_total_ticks": total_delta,
        "elapsed_steal_ticks": steal_delta,
        "steal_percent": round(steal_percent, 6),
        "maximum_steal_percent": MAX_STEAL_PERCENT,
    }
