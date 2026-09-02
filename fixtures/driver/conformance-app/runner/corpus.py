from __future__ import annotations

import json
import sys
from pathlib import Path


def classify(target: str, encoded_bytes: int, maximum: int) -> str:
    if encoded_bytes <= maximum:
        return "none"
    if target == "encoded_request_bytes":
        return "request_too_large"
    return "response_too_large"


corpus = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
observations = []
for operation in corpus["operations"]:
    for bound in operation["bounds"]:
        for case in bound["cases"]:
            observations.append(
                {
                    "tag": operation["tag"],
                    "target": bound["target"],
                    "encoded_bytes": case["encoded_bytes"],
                    "error_class": classify(
                        bound["target"], case["encoded_bytes"], bound["maximum"]
                    ),
                }
            )
print(json.dumps(observations, separators=(",", ":")))
