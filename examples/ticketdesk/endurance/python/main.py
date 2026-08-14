from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Final
from uuid import UUID

from riffdb_application import (
    ApplicationErrorCode,
    AsyncApplicationTransport,
    AttemptBudget,
    BearerCredential,
    CallMetadata,
    ConnectionFailure,
    DatabaseAlias,
    OutcomeUnknown,
    RiffDbApplicationError,
    VerifiedTlsConfig,
)

from client import (
    AsyncTicketDeskClient,
    AsyncTicketDeskReactiveClient,
    CreateCommentInput,
    CreateOrganizationInput,
    CreateProjectInput,
    CreateTicketInput,
    CreateTicketCreated,
    CreateUserInput,
    TicketEventsParams,
    TicketPageFound,
    TicketPageParams,
    TicketQueueWatchParams,
    TicketStatus,
    TriageTicketParams,
)

TENANTS: Final = ("tenant_alpha", "tenant_beta", "tenant_gamma", "tenant_delta")
REQUIRED_COVERAGE: Final = {
    "cold_keys",
    "events",
    "hot_keys",
    "live_queries",
    "multiple_tenants",
    "reads",
    "workflows",
    "writes",
}
LATENCY_BOUNDS_US: Final = (
    50, 100, 200, 400, 800, 1_600, 3_200, 6_400,
    12_800, 25_600, 51_200, 102_400, 204_800, 409_600, 819_200,
    9_007_199_254_740_991,
)
MAX_TRANSIENT_RETRIES: Final = 90
MODELED_DURABLE_OPERATION_BYTES: Final = 32 * 1024
METRICS_PUBLICATION_OPERATIONS: Final = 16


class Metrics:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = asyncio.Lock()
        self._started = int(time.time())
        self._operations = 0
        self._transport_attempts = 0
        self._declared_retries = 0
        self._retained_bytes = 0
        self._events_emitted = 0
        self._consumer_acknowledgements = 0
        self._workloads = {
            "events": 0,
            "live_queries": 0,
            "reads": 0,
            "workflows": 0,
            "writes": 0,
        }
        self._tenants = dict.fromkeys(TENANTS, 0)
        self._latency_counts = [0] * len(LATENCY_BOUNDS_US)

    async def record(
        self, workload: str, tenant: str, retained_bytes: int, started_ns: int
    ) -> None:
        async with self._lock:
            self._operations += 1
            self._transport_attempts += 1
            self._retained_bytes += retained_bytes
            self._workloads[workload] += 1
            self._tenants[tenant] += 1
            latency_us = max(0, (time.perf_counter_ns() - started_ns) // 1_000)
            bucket = next(
                (index for index, bound in enumerate(LATENCY_BOUNDS_US) if latency_us <= bound),
                len(LATENCY_BOUNDS_US) - 1,
            )
            self._latency_counts[bucket] += 1
            if self._operations % METRICS_PUBLICATION_OPERATIONS == 0:
                self._publish_locked()

    async def publish(self) -> None:
        async with self._lock:
            self._publish_locked()

    async def event_emitted(self) -> None:
        async with self._lock:
            self._events_emitted += 1

    async def consumer_acknowledged(self) -> None:
        async with self._lock:
            self._consumer_acknowledgements += 1

    async def transient_retry(self) -> None:
        async with self._lock:
            self._transport_attempts += 1
            self._declared_retries += 1

    def _publish_locked(self) -> None:
        value = {
            "schema": "riffdb.alpha-endurance-worker/v1",
            "language": "python",
            "pid": os.getpid(),
            "started_unix_seconds": self._started,
            "logical_operations": self._operations,
            "transport_attempts": self._transport_attempts,
            "declared_retries": self._declared_retries,
            "error_count": 0,
            "events_emitted": self._events_emitted,
            "consumer_acknowledgements": self._consumer_acknowledgements,
            "modeled_retained_bytes": self._retained_bytes,
            "latency_bounds_us": LATENCY_BOUNDS_US,
            "latency_counts": self._latency_counts,
            "workloads": self._workloads,
            "tenants": self._tenants,
        }
        next_path = self._path.with_suffix(".json.next")
        next_path.write_text(json.dumps(value, separators=(",", ":")) + "\n")
        os.chmod(next_path, 0o600)
        next_path.replace(self._path)


class ClientSet:
    def __init__(
        self,
        seeder_transport: AsyncApplicationTransport,
        application_transport: AsyncApplicationTransport,
        agent_transport: AsyncApplicationTransport,
    ) -> None:
        self._transports = (seeder_transport, application_transport, agent_transport)
        self.seeder = AsyncTicketDeskClient(seeder_transport, AttemptBudget(1))
        self.application = AsyncTicketDeskReactiveClient(
            application_transport, AttemptBudget(1)
        )
        self.agent = AsyncTicketDeskReactiveClient(agent_transport, AttemptBudget(1))

    async def close(self) -> None:
        await asyncio.gather(*(transport.close() for transport in self._transports))


def required(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not 1 <= len(value) <= 16_384 or "\0" in value:
        raise ValueError(f"{name} is required")
    return value


def bounded_integer(name: str, minimum: int, maximum: int) -> int:
    try:
        value = int(required(name))
    except ValueError as error:
        raise ValueError(f"{name} is outside its checked bound") from error
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} is outside its checked bound")
    return value


def require_environment() -> tuple[Path, int, int, int]:
    if required("RIFFDB_ENDURANCE_LANGUAGE") != "python":
        raise ValueError("Python endurance language differs from the closed manifest")
    root = Path(required("RIFFDB_ENDURANCE_ARTIFACT_ROOT"))
    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        raise ValueError(
            "RIFFDB_ENDURANCE_ARTIFACT_ROOT must be an absolute non-symlink directory"
        )
    tenants = json.loads(required("RIFFDB_ENDURANCE_TENANTS_JSON"))
    if tenants != list(TENANTS):
        raise ValueError("Python endurance tenants differ from the closed manifest")
    coverage = json.loads(required("RIFFDB_ENDURANCE_WORKLOAD_COVERAGE_JSON"))
    if not isinstance(coverage, list) or not REQUIRED_COVERAGE.issubset(coverage):
        raise ValueError("Python endurance workload coverage is incomplete")
    clients = bounded_integer("RIFFDB_ENDURANCE_CLIENTS", 4, 4)
    rate = bounded_integer("RIFFDB_ENDURANCE_MAXIMUM_OPERATIONS_PER_SECOND", 1, 1_024)
    modeled_bytes = bounded_integer(
        "RIFFDB_ENDURANCE_MODELED_DURABLE_OPERATION_BYTES",
        MODELED_DURABLE_OPERATION_BYTES,
        MODELED_DURABLE_OPERATION_BYTES,
    )
    if modeled_bytes != MODELED_DURABLE_OPERATION_BYTES:
        raise ValueError("Python endurance durable-operation charge differs from the manifest")
    seed = bounded_integer("RIFFDB_ENDURANCE_SEED", 1, 2**64 - 1)
    return root, clients, rate, seed


async def connect(environment_root: Path, role: str) -> AsyncApplicationTransport:
    endpoint = required("RIFFDB_ENDURANCE_ENDPOINT")
    metadata = CallMetadata.authenticated(
        BearerCredential.from_protected_file(
            str(environment_root / f"{role}.credential")
        )
    ).with_database(DatabaseAlias("default"))
    return await AsyncApplicationTransport.connect_verified_tls(
        VerifiedTlsConfig(
            endpoint=endpoint,
            trust_root=str(environment_root / "ca.pem"),
            server_name="127.0.0.1",
            pool_connections=4,
            streams_per_connection=64,
        ),
        metadata,
    )


async def connect_clients(environment_root: Path) -> ClientSet:
    seeder = await connect(environment_root, "seeder")
    try:
        application = await connect(environment_root, "application")
    except BaseException:
        await seeder.close()
        raise
    try:
        agent = await connect(environment_root, "agent")
    except BaseException:
        await asyncio.gather(seeder.close(), application.close())
        raise
    return ClientSet(seeder, application, agent)


async def run_client(
    environment_root: Path,
    metrics: Metrics,
    seed: int,
    client_index: int,
    delay: float,
) -> None:
    clients = await connect_clients(environment_root)
    tenant = TENANTS[client_index]
    namespace = seed + 3
    organization_id = riff_id(namespace, client_index)
    user_id = riff_id(namespace, 100 + client_index)
    project_id = riff_id(namespace, 200 + client_index)
    hot_ticket_id = riff_id(namespace, 300 + client_index)
    try:
        await seed_client(
            clients,
            metrics,
            tenant,
            organization_id,
            user_id,
            project_id,
            hot_ticket_id,
            seed,
            client_index,
        )
        event_parameters = TicketEventsParams(organization_id=organization_id)
        triage_parameters = TriageTicketParams(organization_id=organization_id)
        event_consumer = f"endurance-python-events-{client_index}"
        triage_consumer = f"endurance-python-triage-{client_index}"
        counter = 0
        while True:
            retries = 0
            while True:
                try:
                    await run_iteration(
                        clients,
                        metrics,
                        tenant,
                        namespace,
                        organization_id,
                        user_id,
                        project_id,
                        hot_ticket_id,
                        client_index,
                        counter,
                        event_parameters,
                        event_consumer,
                        triage_parameters,
                        triage_consumer,
                    )
                    break
                except Exception as error:
                    if retries >= MAX_TRANSIENT_RETRIES or not transient_error(error):
                        raise
                    retries += 1
                    await metrics.transient_retry()
                    await asyncio.sleep(min(0.1 * retries, 1.0))
            counter += 1
            await asyncio.sleep(delay)
    finally:
        await clients.close()


async def run_iteration(
    clients: ClientSet,
    metrics: Metrics,
    tenant: str,
    namespace: int,
    organization_id: UUID,
    user_id: UUID,
    project_id: UUID,
    hot_ticket_id: UUID,
    client_index: int,
    counter: int,
    event_parameters: TicketEventsParams,
    event_consumer: str,
    triage_parameters: TriageTicketParams,
    triage_consumer: str,
) -> None:
    operation_started = time.perf_counter_ns()
    slot = counter % 100
    if slot < 35:
        result = await clients.application.ticket_page(
            TicketPageParams(organization_id=organization_id, ticket_id=hot_ticket_id)
        )
        if not isinstance(result.value, TicketPageFound):
            raise RuntimeError("Python endurance read lost its hot ticket")
        await metrics.record("reads", tenant, 0, operation_started)
    elif slot < 60:
        await clients.application.create_comment(
            CreateCommentInput(
                body=f"python endurance comment {counter}",
                author_id=user_id,
                ticket_id=hot_ticket_id,
                comment_id=riff_id(namespace, 10_000 + client_index * 1_000_000 + counter),
                idempotency_key=f"endurance-python-comment-{client_index}-{counter}",
                organization_id=organization_id,
            )
        )
        await metrics.record(
            "writes", tenant, MODELED_DURABLE_OPERATION_BYTES, operation_started
        )
    elif slot < 70:
        item = await clients.agent.next_triage_ticket(
            triage_parameters, triage_consumer, 0
        )
        await metrics.record(
            "workflows", tenant, MODELED_DURABLE_OPERATION_BYTES, operation_started
        )
        operation_started = time.perf_counter_ns()
        if item is not None:
            reaction_identity = item.event_id
            await clients.agent.react_comment(
                triage_parameters,
                triage_consumer,
                item,
                CreateCommentInput(
                    body="python contextual endurance reaction",
                    author_id=item.event.reporter_id,
                    ticket_id=item.event.ticket_id,
                    comment_id=item.event.ticket_id,
                    idempotency_key=f"endurance-python-reaction-{reaction_identity}",
                    organization_id=organization_id,
                ),
            )
            await metrics.record(
                "workflows", tenant, MODELED_DURABLE_OPERATION_BYTES, operation_started
            )
            operation_started = time.perf_counter_ns()
            await clients.agent.ack_triage_ticket(
                triage_parameters, triage_consumer, item
            )
            await metrics.consumer_acknowledged()
            await metrics.record(
                "workflows", tenant, MODELED_DURABLE_OPERATION_BYTES, operation_started
            )
    elif slot < 80:
        batch = await clients.agent.next_ticket_events(
            event_parameters,
            event_consumer,
            batch_limit=1,
            in_flight_limit=4,
            lease_seconds=60,
            maximum_wait_nanos=0,
        )
        await metrics.record(
            "events", tenant, MODELED_DURABLE_OPERATION_BYTES, operation_started
        )
        if batch.events:
            operation_started = time.perf_counter_ns()
            await clients.agent.ack_ticket_events(
                event_parameters, event_consumer, batch.events[0]
            )
            await metrics.consumer_acknowledged()
            await metrics.record(
                "events", tenant, MODELED_DURABLE_OPERATION_BYTES, operation_started
            )
    else:
        stream = clients.application.watch_ticket_queue_watch(
            TicketQueueWatchParams(
                organization_id=organization_id, project_id=project_id
            )
        )
        try:
            async with asyncio.timeout(5):
                await anext(stream)
        finally:
            await stream.aclose()
        await metrics.record("live_queries", tenant, 0, operation_started)
    if slot == 69:
        operation_started = time.perf_counter_ns()
        ordinal = (counter // 100) % 4_096
        created = await clients.application.create_ticket(
            CreateTicketInput(
                title=f"Python cold ticket {client_index}-{ordinal}",
                status=TicketStatus.OPEN,
                ticket_id=riff_id(namespace, 30_000 + client_index * 4_096 + ordinal),
                project_id=project_id,
                assignee_id=user_id,
                reporter_id=user_id,
                idempotency_key=f"endurance-python-cold-{client_index}-{ordinal}",
                organization_id=organization_id,
            )
        )
        if not created.replayed and isinstance(created.outcome, CreateTicketCreated):
            await metrics.event_emitted()
        await metrics.record(
            "workflows", tenant, MODELED_DURABLE_OPERATION_BYTES, operation_started
        )


def transient_error(error: Exception) -> bool:
    if isinstance(error, (ConnectionFailure, OutcomeUnknown, TimeoutError)):
        return True
    return isinstance(error, RiffDbApplicationError) and error.details.code in {
        ApplicationErrorCode.STORAGE_UNAVAILABLE,
        ApplicationErrorCode.OUTCOME_UNKNOWN,
        ApplicationErrorCode.OVERLOADED,
        ApplicationErrorCode.DEADLINE_EXCEEDED,
    }


async def seed_client(
    clients: ClientSet,
    metrics: Metrics,
    tenant: str,
    organization_id: UUID,
    user_id: UUID,
    project_id: UUID,
    ticket_id: UUID,
    seed: int,
    client_index: int,
) -> None:
    operation_started = time.perf_counter_ns()
    await clients.seeder.create_organization(
        CreateOrganizationInput(
            name=f"Endurance {tenant}",
            idempotency_key=f"endurance-python-organization-{tenant}",
            organization_id=organization_id,
        )
    )
    await metrics.record(
        "writes", tenant, MODELED_DURABLE_OPERATION_BYTES, operation_started
    )
    operation_started = time.perf_counter_ns()
    await clients.seeder.create_user(
        CreateUserInput(
            email=f"python-{client_index}@{tenant}.example.test",
            user_id=user_id,
            display_name=f"Python endurance {client_index}",
            idempotency_key=f"endurance-python-user-{client_index}",
            organization_id=organization_id,
        )
    )
    await metrics.record(
        "writes", tenant, MODELED_DURABLE_OPERATION_BYTES, operation_started
    )
    operation_started = time.perf_counter_ns()
    await clients.seeder.create_project(
        CreateProjectInput(
            name=f"Python endurance {client_index}",
            project_id=project_id,
            idempotency_key=f"endurance-python-project-{client_index}",
            organization_id=organization_id,
        )
    )
    await metrics.record(
        "writes", tenant, MODELED_DURABLE_OPERATION_BYTES, operation_started
    )
    operation_started = time.perf_counter_ns()
    created = await clients.application.create_ticket(
        CreateTicketInput(
            title=f"Python hot ticket {client_index}",
            status=TicketStatus.OPEN,
            ticket_id=ticket_id,
            project_id=project_id,
            assignee_id=user_id,
            reporter_id=user_id,
            idempotency_key=f"endurance-python-hot-{seed}-{client_index}",
            organization_id=organization_id,
        )
    )
    if not created.replayed and isinstance(created.outcome, CreateTicketCreated):
        await metrics.event_emitted()
    await metrics.record(
        "writes", tenant, MODELED_DURABLE_OPERATION_BYTES, operation_started
    )
    await metrics.publish()


def riff_id(namespace: int, value: int) -> UUID:
    return UUID(
        f"{namespace & 0xFFFF_FFFF:08x}-0000-8000-8000-"
        f"{value & 0xFFFF_FFFF_FFFF:012x}"
    )


async def main() -> None:
    root, clients, maximum_rate, seed = require_environment()
    environment_root = root / "environment-v1"
    metrics = Metrics(environment_root / "metrics" / "python.json")
    await metrics.publish()
    delay = clients / maximum_rate
    tasks = [
        asyncio.create_task(
            run_client(environment_root, metrics, seed, index, delay)
        )
        for index in range(clients)
    ]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        task.result()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except BaseException as error:
        print(str(error), file=sys.stderr)
        raise
