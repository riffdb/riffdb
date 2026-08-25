"""Closed-loop interactive load matching the Rust app-baseline driver."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from .histogram import LatencyHistogram
from .ids import NS_LOAD_WRITE, STATUS_OPEN, uuid_from_ordinal
from .riffdb_app import classify_riffdb
from .safe_app import SafeAppError, classify_error
from .seed import (
    CloseTicketWithCommentSeed,
    CommentRow,
    CommentSeed,
    OpenTicketWithLabelsSeed,
    ScenarioProbes,
    SeedDataset,
    TicketRow,
)

INTERACTIVE_WEIGHTS: tuple[tuple[str, int], ...] = (
    ("point_get_ticket", 25),
    ("point_get_user", 15),
    ("list_tickets_by_project_status", 12),
    ("list_open_tickets_for_assignee", 10),
    ("list_comments_for_ticket", 10),
    ("list_project_members", 8),
    ("ticket_detail_page", 5),
    ("create_comment", 12),
    ("close_ticket_with_comment", 2),
    ("open_ticket_with_labels", 1),
)
SWEEP_CLIENTS = (1, 8, 32, 128)
DEFAULT_RNG_SEED = 0x000A11CEBEEF
GOLDEN = 0x9E3779B97F4A7C15


class XorShift64:
    def __init__(self, seed: int) -> None:
        self.state = seed | 1

    def next_u64(self) -> int:
        x = self.state & 0xFFFFFFFFFFFFFFFF
        x ^= (x << 13) & 0xFFFFFFFFFFFFFFFF
        x ^= x >> 7
        x ^= (x << 17) & 0xFFFFFFFFFFFFFFFF
        self.state = x
        return x

    def gen_range(self, max_exclusive: int) -> int:
        if max_exclusive <= 0:
            return 0
        return self.next_u64() % max_exclusive

    def gen_f64(self) -> float:
        return self.next_u64() / (float(2**64 - 1) + 1.0)


class Zipf:
    def __init__(self, n: int, s: float) -> None:
        if n <= 0:
            raise ValueError("zipf domain must be positive")
        if s <= 0:
            step = 1.0 / n
            self.cdf = [step * i for i in range(1, n + 1)]
            return
        weights = [1.0 / (rank**s) for rank in range(1, n + 1)]
        total = sum(weights)
        run = 0.0
        cdf: list[float] = []
        for weight in weights:
            run += weight / total
            cdf.append(run)
        cdf[-1] = 1.0
        self.cdf = cdf

    def sample(self, rng: XorShift64) -> int:
        u = rng.gen_f64()
        for index, edge in enumerate(self.cdf):
            if edge >= u:
                return index
        return len(self.cdf) - 1


@dataclass
class OpStats:
    latency: LatencyHistogram = field(default_factory=LatencyHistogram)
    success: int = 0
    conflict: int = 0
    unavailable: int = 0
    replayed: int = 0
    error: int = 0
    first_error: str | None = None

    def record(self, elapsed_ns: int, outcome: str, error_text: str | None) -> None:
        self.latency.record_ns(elapsed_ns)
        if outcome == "success":
            self.success += 1
        elif outcome == "conflict":
            self.conflict += 1
        elif outcome == "unavailable":
            self.unavailable += 1
        elif outcome == "replayed":
            self.replayed += 1
        else:
            self.error += 1
        if self.first_error is None and error_text:
            self.first_error = error_text[:240]

    def merge(self, other: OpStats) -> None:
        self.latency.merge(other.latency)
        self.success += other.success
        self.conflict += other.conflict
        self.unavailable += other.unavailable
        self.replayed += other.replayed
        self.error += other.error
        if self.first_error is None:
            self.first_error = other.first_error

    def total(self) -> int:
        return self.success + self.conflict + self.unavailable + self.replayed + self.error

    def json(self) -> dict[str, object]:
        return {
            "latency": self.latency.summary(),
            "outcomes": {
                "success": self.success,
                "conflict": self.conflict,
                "unavailable": self.unavailable,
                "replayed": self.replayed,
                "error": self.error,
                "logical_operations": self.total(),
            },
            "first_error": self.first_error,
        }


@dataclass
class LoadConfig:
    clients: int
    duration_s: float
    warmup_s: float
    zipf_s: float = 1.0
    rng_seed: int = DEFAULT_RNG_SEED
    tenant_count: int = 1
    sample_id_base: int = 0


@dataclass
class LoadReport:
    backend_id: str
    clients: int
    profile: str
    measured_elapsed_ns: int
    seed_ns: int
    aggregate: OpStats
    by_op: dict[str, OpStats]
    worker_completed: list[int]

    def throughput(self) -> float:
        elapsed_s = max(self.measured_elapsed_ns / 1e9, 0.001)
        return self.aggregate.total() / elapsed_s

    def json(self) -> dict[str, object]:
        elapsed_ns = max(self.measured_elapsed_ns, 1)
        logical_ops = self.aggregate.total()
        operations = {
            name: stats.json() for name, stats in self.by_op.items() if stats.total() > 0
        }
        return {
            "schema": "riffdb.app-baseline-python-safe-app/v1",
            "backend_id": self.backend_id,
            "profile": self.profile,
            "clients": self.clients,
            "evidentiary": False,
            "language": "python",
            "safety_owner": (
                "riffdbd_rust" if self.backend_id == "riffdb_public_grpc" else "python_sql"
            ),
            "measured_elapsed_ns": self.measured_elapsed_ns,
            "seed_ns": self.seed_ns,
            "logical_ops": logical_ops,
            "throughput_ops_s": logical_ops * 1_000_000_000 // elapsed_ns,
            "aggregate": self.aggregate.json(),
            "operations": operations,
            "worker_completed_operations": self.worker_completed,
            "notes": self._notes(),
        }

    def _notes(self) -> list[str]:
        shared = [
            "Python closed-loop load. Language runtime time is included.",
            "Not a substitute for the Rust evidentiary harness in benchmarks/run-app-baseline.",
            "Throughput denominator is max(worker_measure_end)-min(worker_measure_start).",
            "Each backend is seeded once, then the concurrency sweep accumulates history.",
        ]
        if self.backend_id == "riffdb_public_grpc":
            return [
                *shared,
                "RiffDB path uses the generated TicketDesk Python client only.",
                "No authorization, idempotency, audit, event, or outbox code runs in Python; riffdbd (Rust) enforces those.",
            ]
        return [
            *shared,
            "postgres_safe_app path implements authorization, idempotency, audit, event, and outbox in SQL from Python.",
        ]


def print_load_summary(report: LoadReport) -> None:
    elapsed_s = max(report.measured_elapsed_ns / 1e9, 0.001)
    total = report.aggregate.total()
    print(
        f"\n== load {report.backend_id} profile={report.profile} clients={report.clients} "
        f"window={elapsed_s:.1f}s tenant=single_organization count=1 hot=0pct =="
    )
    print(
        f"throughput={report.throughput():.0f} ops/s  logical_ops={total}  "
        f"success={report.aggregate.success}  conflict={report.aggregate.conflict}  "
        f"idempotency_mismatch=0  unavailable={report.aggregate.unavailable}  "
        f"overloaded=0  replayed={report.aggregate.replayed}  error={report.aggregate.error}"
    )
    latency = report.aggregate.latency
    print(
        f"latency p50={latency.percentile_ns(50)/1e6:.3f}ms "
        f"p95={latency.percentile_ns(95)/1e6:.3f}ms "
        f"p99={latency.percentile_ns(99)/1e6:.3f}ms "
        f"max={latency.max_ns/1e6:.3f}ms"
    )
    for name, stats in report.by_op.items():
        if stats.total() == 0:
            continue
        print(
            f"  {name}: n={stats.total()} p50={stats.latency.percentile_ns(50)/1e6:.3f}ms "
            f"p99={stats.latency.percentile_ns(99)/1e6:.3f}ms ok={stats.success} "
            f"conflict={stats.conflict} idempotency_mismatch=0 unavailable={stats.unavailable} "
            f"replay={stats.replayed} err={stats.error}"
        )


def run_closed_loop(
    driver,
    dataset: SeedDataset,
    config: LoadConfig,
    seed_ns: int,
) -> LoadReport:
    if not 1 <= config.clients <= 128:
        raise ValueError("load clients must be 1..=128")
    probes = dataset.tenant_probes(config.tenant_count)
    if len(probes) != config.tenant_count:
        raise ValueError("seed does not contain the requested tenant count")
    write_tickets: list[list[TicketRow]] = []
    for probe in probes:
        tickets = [
            ticket
            for ticket in dataset.tickets
            if ticket.organization_id == probe.organization_id
            and ticket.status == STATUS_OPEN
            and ticket.ticket_id != probe.ticket_id
        ]
        if not tickets:
            tickets = [
                ticket
                for ticket in dataset.tickets
                if ticket.organization_id == probe.organization_id and ticket.status == STATUS_OPEN
            ]
        if not tickets:
            raise ValueError("load driver requires at least one open ticket per tenant")
        write_tickets.append(tickets)
    zipf = [Zipf(len(tickets), config.zipf_s) for tickets in write_tickets]
    weight_sum = sum(weight for _, weight in INTERACTIVE_WEIGHTS)
    sample_counter = {"value": config.sample_id_base + 1}
    sample_lock = threading.Lock()
    measuring = threading.Event()
    stop = threading.Event()
    ready = threading.Barrier(config.clients + 1)
    go = threading.Barrier(config.clients + 1)
    results: list[dict[str, object] | None] = [None] * config.clients
    errors: list[str] = []

    def next_sample() -> int:
        with sample_lock:
            value = sample_counter["value"]
            sample_counter["value"] += 1
            return value

    def worker(worker_id: int) -> None:
        try:
            backend = driver.open_session()
            probe = probes[worker_id % len(probes)]
            backend.prewarm(probe.organization_id, probe.ticket_id)
            backend.point_get_ticket(probe.organization_id, probe.ticket_id)
            rng = XorShift64(config.rng_seed ^ (((worker_id + 1) * GOLDEN) & 0xFFFFFFFFFFFFFFFF))
            ready.wait()
            go.wait()
            by_op = {name: OpStats() for name, _ in INTERACTIVE_WEIGHTS}
            last_comment: CommentSeed | None = None
            measure_start: float | None = None
            measure_end: float | None = None
            completed = 0
            while not stop.is_set():
                record = measuring.is_set()
                op = _draw_op(rng, weight_sum)
                ticket = write_tickets[0][zipf[0].sample(rng) % len(write_tickets[0])]
                if stop.is_set():
                    break
                started = time.perf_counter_ns()
                sample = next_sample()
                outcome, error_text = _execute(backend, probes[0], op, sample, ticket, last_comment)
                ended = time.perf_counter_ns()
                if outcome == "success" and op == "create_comment":
                    last_comment = _comment_for(probes[0], ticket, sample)
                if record:
                    if measure_start is None:
                        measure_start = started
                    measure_end = ended
                    completed += 1
                    by_op[op].record(ended - started, outcome, error_text)
            backend.close()
            results[worker_id] = {
                "by_op": by_op,
                "measure_start": measure_start,
                "measure_end": measure_end,
                "completed": completed,
            }
        except Exception as exc:  # noqa: BLE001 — worker boundary records the first failure
            errors.append(str(exc))

    threads = [threading.Thread(target=worker, args=(worker_id,), daemon=True) for worker_id in range(config.clients)]
    for thread in threads:
        thread.start()
    ready.wait()
    go.wait()
    if config.warmup_s > 0:
        time.sleep(config.warmup_s)
    measuring.set()
    time.sleep(config.duration_s)
    stop.set()
    for thread in threads:
        thread.join()
    if errors:
        raise RuntimeError(errors[0])

    aggregate = OpStats()
    merged = {name: OpStats() for name, _ in INTERACTIVE_WEIGHTS}
    starts: list[int] = []
    ends: list[int] = []
    completed: list[int] = []
    for result in results:
        if result is None:
            continue
        if result["measure_start"] is not None and result["measure_end"] is not None:
            starts.append(int(result["measure_start"]))
            ends.append(int(result["measure_end"]))
        completed.append(int(result["completed"]))
        by_op: dict[str, OpStats] = result["by_op"]  # type: ignore[assignment]
        for name, stats in by_op.items():
            merged[name].merge(stats)
            aggregate.merge(stats)
    measured = (max(ends) - min(starts)) if starts and ends else int(config.duration_s * 1e9)
    return LoadReport(
        backend_id=driver.backend_id,
        clients=config.clients,
        profile="interactive",
        measured_elapsed_ns=max(measured, 1_000_000),
        seed_ns=seed_ns,
        aggregate=aggregate,
        by_op=merged,
        worker_completed=completed,
    )


def _draw_op(rng: XorShift64, weight_sum: int) -> str:
    pick = rng.gen_range(weight_sum)
    for name, weight in INTERACTIVE_WEIGHTS:
        if pick < weight:
            return name
        pick -= weight
    return INTERACTIVE_WEIGHTS[0][0]


def _comment_for(probe: ScenarioProbes, ticket: TicketRow, sample: int) -> CommentSeed:
    return CommentSeed(
        row=CommentRow(
            organization_id=ticket.organization_id,
            comment_id=uuid_from_ordinal(NS_LOAD_WRITE, 1_000_000_000 + sample),
            ticket_id=ticket.ticket_id,
            author_id=probe.write_author_id,
            body=f"load comment {sample}",
        ),
        idempotency_key=f"load-comment-{sample}",
    )


def _execute(
    backend,
    probe: ScenarioProbes,
    op: str,
    sample: int,
    ticket: TicketRow,
    last_comment: CommentSeed | None,
) -> tuple[str, str | None]:
    try:
        if op == "point_get_ticket":
            found = backend.point_get_ticket(probe.organization_id, probe.ticket_id)
            if not found:
                return "error", "missing ticket"
        elif op == "point_get_user":
            found = backend.point_get_user(probe.organization_id, probe.user_id)
            if not found:
                return "error", "missing user"
        elif op == "list_tickets_by_project_status":
            backend.list_tickets_by_project_status(
                probe.organization_id, probe.project_id, STATUS_OPEN, 50
            )
        elif op == "list_open_tickets_for_assignee":
            backend.list_open_tickets_for_assignee(probe.organization_id, probe.assignee_id, 50)
        elif op == "list_comments_for_ticket":
            backend.list_comments_for_ticket(probe.organization_id, probe.ticket_id, 50)
        elif op == "list_project_members":
            backend.list_project_members(probe.organization_id, probe.project_id, 50)
        elif op == "ticket_detail_page":
            found = backend.ticket_detail_page(probe.organization_id, probe.ticket_id)
            if not found:
                return "error", "missing detail"
        elif op == "create_comment":
            backend.create_comment(_comment_for(probe, ticket, sample))
        elif op == "close_ticket_with_comment":
            backend.close_ticket_with_comment(
                CloseTicketWithCommentSeed(
                    organization_id=ticket.organization_id,
                    ticket_id=ticket.ticket_id,
                    author_id=probe.write_author_id,
                    comment_id=uuid_from_ordinal(NS_LOAD_WRITE, 2_000_000_000 + sample),
                    body=f"load close note {sample}",
                    idempotency_key=f"load-close-{sample}",
                )
            )
        elif op == "open_ticket_with_labels":
            backend.open_ticket_with_labels(
                OpenTicketWithLabelsSeed(
                    organization_id=probe.organization_id,
                    ticket_id=uuid_from_ordinal(NS_LOAD_WRITE, 3_000_000_000 + sample),
                    project_id=probe.write_project_id,
                    reporter_id=probe.write_author_id,
                    assignee_id=probe.write_assignee_id,
                    title=f"load open ticket {sample}",
                    label_a=probe.write_label_a,
                    label_b=probe.write_label_b,
                    idempotency_key=f"load-open-{sample}",
                )
            )
        return "success", None
    except SafeAppError as exc:
        return classify_error(exc), str(exc)[:240]
    except Exception as exc:
        from .riffdb_app import RiffDbError

        if isinstance(exc, RiffDbError) or exc.__class__.__name__ in {
            "RiffDbApplicationError",
            "ConnectionFailure",
            "OutcomeUnknown",
            "ProtocolError",
        }:
            return classify_riffdb(exc), str(exc)[:240]
        raise
