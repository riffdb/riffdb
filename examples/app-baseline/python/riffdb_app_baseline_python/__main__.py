"""CLI: python -m riffdb_app_baseline_python --backend postgres|riffdb|both ..."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from .load import LoadConfig, SWEEP_CLIENTS, print_load_summary, run_closed_loop
from .riffdb_app import RiffDbDriver
from .safe_app import PostgresDriver
from .schema import OBLIGATIONS
from .seed import Scale, SeedDataset


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Python TicketDesk load: postgres_safe_app vs generated RiffDB client"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--smoke", action="store_true")
    mode.add_argument("--full", action="store_true")
    parser.add_argument("--load", default="interactive", choices=["interactive"])
    parser.add_argument("--load-clients", type=int, default=8)
    parser.add_argument("--load-duration-secs", type=float, default=5)
    parser.add_argument("--load-warmup-secs", type=float, default=1)
    parser.add_argument("--load-concurrency-sweep", action="store_true")
    parser.add_argument("--load-zipf-s", type=float, default=1.0)
    parser.add_argument(
        "--backend",
        default="both",
        choices=["postgres", "riffdb", "both"],
        help="postgres_safe_app (safety in Python SQL), riffdb (safety in Rust), or both",
    )
    parser.add_argument("--postgres-url", default=None)
    parser.add_argument("--riffdb-endpoint", default=None)
    parser.add_argument("--riffdb-credential", default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args(argv)

    scale = Scale.full() if args.full else Scale.smoke()
    dataset = SeedDataset.generate(scale)
    clients = list(SWEEP_CLIENTS) if args.load_concurrency_sweep else [max(1, args.load_clients)]
    drivers = []
    if args.backend in {"postgres", "both"}:
        url = args.postgres_url or os.environ.get("RIFFDB_APP_BASELINE_POSTGRES_URL")
        if not url:
            print("missing --postgres-url or RIFFDB_APP_BASELINE_POSTGRES_URL", file=sys.stderr)
            return 2
        drivers.append(PostgresDriver(url))
    if args.backend in {"riffdb", "both"}:
        endpoint = args.riffdb_endpoint or os.environ.get("RIFFDB_PYTHON_BRIDGE_ENDPOINT")
        credential_path = args.riffdb_credential or os.environ.get("RIFFDB_PYTHON_BRIDGE_CREDENTIAL")
        if not endpoint or not credential_path:
            print(
                "missing --riffdb-endpoint/--riffdb-credential or RIFFDB_PYTHON_BRIDGE_*",
                file=sys.stderr,
            )
            return 2
        token = Path(credential_path).read_text().strip()
        drivers.append(RiffDbDriver(endpoint, token))

    all_reports = []
    for driver in drivers:
        print(f"=== backend {driver.backend_id} seed ===")
        seed_started = time.perf_counter_ns()
        driver.seed(dataset)
        seed_ns = time.perf_counter_ns() - seed_started
        print(f"seed_ns={seed_ns}")
        sample_base = 0
        for client_count in clients:
            config = LoadConfig(
                clients=client_count,
                duration_s=args.load_duration_secs,
                warmup_s=args.load_warmup_secs,
                zipf_s=args.load_zipf_s,
                sample_id_base=sample_base,
            )
            report = run_closed_loop(driver, dataset, config, seed_ns)
            print_load_summary(report)
            all_reports.append(report)
            sample_base += 1_000_000_000

    print("\n== comparison curve ==")
    print(
        f"{'backend':<24} {'clients':>8} {'ops/s':>12} {'p50_ms':>10} {'p99_ms':>10} {'write_p50_ms':>12}"
    )
    for report in all_reports:
        write = report.by_op.get("create_comment")
        write_p50 = (write.latency.percentile_ns(50) / 1e6) if write and write.total() else 0.0
        print(
            f"{report.backend_id:<24} {report.clients:>8} {report.throughput():>12.0f} "
            f"{report.aggregate.latency.percentile_ns(50)/1e6:>10.3f} "
            f"{report.aggregate.latency.percentile_ns(99)/1e6:>10.3f} "
            f"{write_p50:>12.3f}"
        )

    payload = {
        "schema": "riffdb.app-baseline-python-safe-app/v1",
        "evidentiary": False,
        "language": "python",
        "postgres_obligations": list(OBLIGATIONS),
        "scale": {
            "organizations": scale.organizations,
            "users_per_org": scale.users_per_org,
            "projects_per_org": scale.projects_per_org,
            "tickets_per_project": scale.tickets_per_project,
            "board_dense_open": scale.board_dense_open,
        },
        "curve": [report.json() for report in all_reports],
        "notes": [
            "Same Python interactive mix against postgres_safe_app and generated RiffDB client.",
            "Postgres implements authorization/idempotency/audit/event/outbox in Python SQL.",
            "RiffDB uses the generated TicketDesk client with no Python safety code; riffdbd (Rust) enforces those.",
            "Not a substitute for the Rust evidentiary harness in benchmarks/run-app-baseline.",
        ],
    }
    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2) + "\n")
        print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
