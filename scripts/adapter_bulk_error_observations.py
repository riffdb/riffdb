#!/usr/bin/env python3
# req: AAA-002, AAA-003, AAA-004, AAA-005, BLK-019, BLK-021
"""Compare the value-free local budget observations from every typed facade."""

from __future__ import annotations

import json
import sys
from pathlib import Path


def aggregate_budget_cross_language_error_observations_are_equal(
    paths: list[Path],
) -> bool:
    expected = [
        {"cause": "collection_count", "collection": "mutations"},
        {
            "cause": "individual_value_bytes",
            "collection": "mutations",
            "index": 0,
            "leaf": "context",
        },
        {
            "cause": "individual_value_bytes",
            "collection": "mutations",
            "index": 0,
            "leaf": "relation",
        },
        {
            "cause": "aggregate_canonical_element_bytes",
            "collection": "mutations",
        },
    ]
    typed = [json.loads(path.read_text(encoding="utf-8")) for path in paths[:4]]
    cli_local = json.loads(paths[4].read_text(encoding="utf-8"))
    cli_service = json.loads(paths[5].read_text(encoding="utf-8"))
    mcp_result = json.loads(paths[6].read_text(encoding="utf-8"))["result"]
    mcp_service = json.loads(mcp_result["content"][0]["text"])
    return (
        all(document.get("budget_errors") == expected for document in typed)
        and cli_local.get("error", {}).get("type") == "local"
        and cli_local["error"].get("code") == "input_invalid"
        and cli_service.get("error", {}).get("type") == "application"
        and cli_service["error"].get("code") == "RDB-INPUT-0101"
        and cli_service["error"].get("operation_symbol") == "WritePolicyMutations"
        and "symbol_path" not in cli_service["error"]
        and mcp_result.get("isError") is True
        and mcp_service.get("class") == "invalid_argument"
        and mcp_service.get("code") == "validation_failed"
        and mcp_service.get("validation_issues")
        == [{"code": "too_long", "path": [{"field_id": 1}]}]
    )


def main() -> int:
    paths = [Path(argument) for argument in sys.argv[1:]]
    if len(paths) != 7:
        print(
            "expected four typed, exact CLI, raw CLI, and MCP observation paths",
            file=sys.stderr,
        )
        return 2
    if not aggregate_budget_cross_language_error_observations_are_equal(paths):
        print("typed facade budget observations diverged", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
