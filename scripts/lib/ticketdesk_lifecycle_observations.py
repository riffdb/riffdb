# req: END-003, END-007, END-008
"""Bounded numeric observations from public native responses, never defaults."""
import re


class ObservationError(ValueError):
    """Closed diagnostics contain neither native response text nor paths."""


def require(condition, code):
    if not condition:
        raise ObservationError(code)


def integer(value):
    require(type(value) is int and 0 <= value <= 2**64 - 1, "observation_integer")
    return value


def decimal(value):
    require(type(value) is str and re.fullmatch(r"0|[1-9][0-9]{0,19}", value),
            "observation_integer")
    return integer(int(value))


def native_result(value, command):
    require(type(value) is dict and value.get("schema") == "riffdb.cli.output/v1"
            and value.get("command") == command and value.get("ok") is True
            and type(value.get("result")) is dict, "observation_response")
    return value["result"]


def projection_backlog(health, projection, expected_identity):
    """Application sequences behind a sampled head, bound to the exact plan.

    The query runs after the health observation and may have advanced further.
    This reports lag against that sampled head; it neither fabricates a current
    head nor treats an unavailable projection as a caught-up projection.
    """
    head = integer(native_result(health, "server.health").get("last_commit_sequence"))
    result = native_result(projection, "projection.query")
    require(result.get("status") == "ready", "projection_unavailable")
    frontier = result.get("frontier")
    require(type(frontier) is dict and frontier.keys() == {"type", "sequence"}
            and frontier["type"] == "applied_through", "projection_frontier")
    applied = decimal(frontier["sequence"])
    fence = result.get("observed_fence")
    require(type(fence) is dict and fence.get("frontier") == frontier,
            "projection_fence")
    require(decimal(fence.get("generation")) > 0, "projection_fence")
    identity = fence.get("identity")
    fields = {"contract_lineage", "projection_id", "projection_plan_hash"}
    require(type(identity) is dict and identity.keys() == fields
            and type(expected_identity) is dict and expected_identity.keys() == fields,
            "projection_identity")
    for value in (identity, expected_identity):
        require(type(value["contract_lineage"]) is str
                and 0 < len(value["contract_lineage"]) <= 128
                and integer(value["projection_id"]) > 0
                and type(value["projection_plan_hash"]) is str
                and re.fullmatch(r"[0-9a-f]{64}", value["projection_plan_hash"]),
                "projection_identity")
    require(identity == expected_identity, "projection_identity")
    return max(head - applied, 0)
