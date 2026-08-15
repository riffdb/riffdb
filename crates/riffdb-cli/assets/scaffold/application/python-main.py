from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from uuid import UUID

from riffdb_application import (
    AsyncApplicationTransport,
    AttemptBudget,
    BearerCredential,
    CallMetadata,
    DatabaseAlias,
    QueryOptions,
    SyncApplicationTransport,
    TypedCommandResult,
    TypedQueryResult,
)

from .generated import (
    Async{{MODULE_CLIENT}},
    CreateItemInput,
    CreateItemOutcome,
    ItemPageFound,
    ItemPageParams,
    ItemPageResult,
    {{MODULE_CLIENT}},
)

ITEM_ID = UUID("018f0f8b-7c6d-7e31-8a4f-2c2d37a52b12")


def metadata(credential_file: str, database: str) -> CallMetadata:
    credential = BearerCredential.from_protected_file(str(Path(credential_file)))
    return CallMetadata.authenticated(credential).with_database(DatabaseAlias(database))


def observation(
    command: TypedCommandResult[CreateItemOutcome],
    page: TypedQueryResult[ItemPageResult],
) -> dict[str, object]:
    if not isinstance(page.value, ItemPageFound):
        raise RuntimeError("generated application result is undeclared")
    commit_sequence = command.commit_sequence
    return {
        "application_head_at_least_read_after_commit": (
            commit_sequence is not None and page.application_head >= commit_sequence
        ),
        "operation": "CreateItem+ItemPage",
        "outcome": command.outcome.outcome,
        "read_after_commit": commit_sequence is not None,
        "schema": "riffdb.application-parity/v1",
        "title": page.value.item.title,
    }


def run_sync(endpoint: str, credential_file: str, database: str) -> dict[str, object]:
    with SyncApplicationTransport.connect_uri(
        endpoint, metadata(credential_file, database)
    ) as transport:
        application = {{MODULE_CLIENT}}(transport, AttemptBudget(3))
        command = application.create_item(
            CreateItemInput(
                idempotency_key="{{APPLICATION_NAME}}-python-item",
                item_id=ITEM_ID,
                title="Python generated application client",
            )
        )
        page = application.item_page(
            ItemPageParams(item_id=ITEM_ID),
            QueryOptions(read_after_commit=command.commit_sequence),
        )
        return observation(command, page)


async def run_async(endpoint: str, credential_file: str, database: str) -> dict[str, object]:
    async with await AsyncApplicationTransport.connect_uri(
        endpoint, metadata(credential_file, database)
    ) as transport:
        application = Async{{MODULE_CLIENT}}(transport, AttemptBudget(3))
        command = await application.create_item(
            CreateItemInput(
                idempotency_key="{{APPLICATION_NAME}}-python-item",
                item_id=ITEM_ID,
                title="Python generated application client",
            )
        )
        page = await application.item_page(
            ItemPageParams(item_id=ITEM_ID),
            QueryOptions(read_after_commit=command.commit_sequence),
        )
        return observation(command, page)


def main() -> None:
    if len(sys.argv) not in {4, 5} or (len(sys.argv) == 5 and sys.argv[4] != "--async"):
        raise SystemExit(
            "usage: python -m {{PACKAGE_NAME}} ENDPOINT CREDENTIAL_FILE DATABASE [--async]"
        )
    endpoint, credential_file, database = sys.argv[1:4]
    result = (
        asyncio.run(run_async(endpoint, credential_file, database))
        if sys.argv[4:] == ["--async"]
        else run_sync(endpoint, credential_file, database)
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
