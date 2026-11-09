"""Installed executable assertions for configuration, compiler and version UX."""
from __future__ import annotations

import json
from pathlib import Path

from processes import private_environment, private_write, run


def offline_versions(binary_root: Path, version: str, root: Path):
    environment = private_environment(root) | {
        "RIFFDB_CONFIG": str(root / "absent-secret-config-canary"),
        "RIFFDB_ENDPOINT": "secret-endpoint-canary", "RIFFDB_MAX_ATTEMPTS": "invalid",
        "RIFFDB_LOG_DIRECTORY": str(root / "must-not-exist"),
    }
    for binary in ("riffdb", "riffdbd"):
        for flag in ("--version", "-V"):
            stdout, stderr = run([binary_root / binary, flag], cwd=root, env=environment)
            assert stdout == f"{binary} {version}\n".encode() and stderr == b""
        assert not (root / "must-not-exist").exists()
    return True


def configuration_causes(binary: Path, root: Path):
    environment = private_environment(root)
    observed = {}
    for mode in ("human", "json", "json-v2"):
        stdout, stderr = run([binary, "--output", mode, "--max-attempts", "11", "server", "health"],
                             cwd=root, env=environment, expected=2)
        if mode == "human":
            assert not stdout
            assert b"setting=client.max_attempts source=flag cause=retry_bounds" in stderr
            assert b"fix=" in stderr
        else:
            assert not stderr
            value = json.loads(stdout)
            assert value["error"]["code"] == "configuration_invalid"
            observed[mode] = value
            if mode == "json-v2":
                assert value["diagnostics"][0]["cause"] == "retry_bounds"
                assert value["diagnostics"][0]["setting"] == "client.max_attempts"
            else:
                assert "diagnostics" not in value
    assert observed["json"]["error"] == observed["json-v2"]["error"]
    for mode in ("human", "json", "json-v2"):
        stdout, stderr = run([binary, "--output", mode, "--endpoint", "secret-endpoint-canary", "server", "health"],
                             cwd=root, env=environment, expected=2)
        assert b"secret-endpoint-canary" not in stdout + stderr
        assert str(root).encode() not in stdout + stderr
        if mode == "human":
            assert b"cause=endpoint_invalid" in stderr
        elif mode == "json-v2":
            assert json.loads(stdout)["diagnostics"][0]["cause"] == "endpoint_invalid"
    return True


def compiler_diagnostics(binary: Path, fixtures: Path, root: Path):
    import shutil
    application = root / "invalid-source"
    shutil.copytree(fixtures / "todos", application)
    private_write(application / "riffdb/contract.riff", "// α😀\r\ncontract Test secret_identifier_canary 1 {}")
    for mode in ("human", "json", "json-v2"):
        stdout, stderr = run([binary, "--output", mode, "application", "check", "--source-only"],
                             cwd=application, env=private_environment(root), expected=1)
        assert not stdout
        assert b"secret_identifier_canary" not in stderr
        assert str(root).encode() not in stderr
        if mode == "human":
            assert b"riffdb/contract.riff:2:15" in stderr
            assert b"found: identifier" in stderr
        else:
            value = json.loads(stderr)
            diagnostic = value["diagnostics"][0]
            assert diagnostic["span"] == {"start": 25, "end": 49}
            if mode == "json":
                assert value["version"] == 1 and "location" not in diagnostic
            else:
                assert value["version"] == 2
                assert diagnostic["location"]["start"] == {"line": 2, "column": 15}
                assert diagnostic["found"] == "identifier" and diagnostic["expected"] == ["version"]
    return True
