# req: END-003, END-007, END-008
"""Actual generated Python calls; bounded diagnostics contain no child error."""
import asyncio
import json
from pathlib import Path
import sys
from uuid import UUID

from riffdb_application import (AsyncApplicationTransport, AttemptBudget,
    BearerCredential, CallMetadata, DatabaseAlias, VerifiedTlsConfig)
from client import (AsyncTicketDeskLifecycleReactiveClient, CONTRACT_VERSION,
    GetTicketParams, GetTicketFound, GetTicketNotFound, UpdateTicketInput,
    UpdateTicketStale, TicketStatus, TicketQueueWatchParams, TicketQueueWatchSnapshot)


def require(condition):
    if not condition:
        raise ValueError("lifecycle preparation assertion")


async def run(root):
    require(root.is_absolute() and not root.is_symlink() and root.is_dir())
    config = root / "python.connection.json"
    require(config.is_file() and not config.is_symlink() and 0 < config.stat().st_size <= 4096)
    connection = json.loads(config.read_bytes())
    metadata = CallMetadata.authenticated(BearerCredential.from_protected_file(
        str(root / "application.credential"))).with_database(DatabaseAlias("default"))
    transport = await AsyncApplicationTransport.connect_verified_tls(VerifiedTlsConfig(
        endpoint=connection["endpoint"], trust_root=str(root / "ca.pem"),
        server_name="127.0.0.1", pool_connections=1, streams_per_connection=1), metadata)
    try:
        client = AsyncTicketDeskLifecycleReactiveClient(transport, AttemptBudget(1))
        org, ticket = UUID("01900000-0000-7000-8001-000000000000"), UUID("01900000-0000-7000-8004-000000000000")
        params = GetTicketParams(organization_id=org, ticket_id=ticket)
        row = (await client.get_ticket(params)).value
        require(isinstance(row, GetTicketFound) and row.ticket.ticket_id == ticket
                and row.ticket.title == "Lifecycle ticket 0")
        missing = await client.get_ticket(GetTicketParams(
            organization_id=UUID("01900000-0000-7000-8001-000000000001"), ticket_id=ticket))
        require(isinstance(missing.value, GetTicketNotFound))
        input_value = UpdateTicketInput(organization_id=org, ticket_id=ticket,
            expected_revision=2**64 - 1, status=TicketStatus.OPEN,
            idempotency_key=f"ticketdesk-lifecycle-python-stale-{CONTRACT_VERSION}")
        first = await client.update_ticket(input_value)
        replay = await client.update_ticket(input_value)
        require(isinstance(first.outcome, UpdateTicketStale) and replay.outcome == first.outcome
                and not first.replayed and replay.replayed and first.commit_sequence is not None
                and first.commit_sequence == replay.commit_sequence
                and first.plan_hash == replay.plan_hash and first.outcome_uri == replay.outcome_uri)
        after = (await client.get_ticket(params)).value
        require(isinstance(after, GetTicketFound) and after.ticket.revision == row.ticket.revision
                and after.ticket.status == row.ticket.status)
        stream = client.watch_ticket_queue_watch(TicketQueueWatchParams(
            organization_id=org, project_id=UUID("01900000-0000-7000-8003-000000000000")))
        try:
            update = await anext(stream)
            require(isinstance(update, TicketQueueWatchSnapshot) and update.value is not None
                    and 0 < len(update.value.tickets) <= 50)
        finally:
            await stream.aclose()
        print(json.dumps(dict(schema="riffdb.ticketdesk-lifecycle-python-smoke/v1",
            named_read=True, partition_isolation=True, caller_fence=True,
            idempotency=True, state_unchanged=True, live_query_snapshot=True)))
    finally:
        await transport.close()


if __name__ == "__main__":
    try:
        require(len(sys.argv) == 2 and len(sys.argv[1]) <= 4096)
        asyncio.run(asyncio.wait_for(run(Path(sys.argv[1])), timeout=30))
    except Exception:
        print("ticketdesk-lifecycle-python-smoke: failed", file=sys.stderr)
        sys.exit(1)
