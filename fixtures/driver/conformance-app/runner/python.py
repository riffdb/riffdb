from __future__ import annotations

import json
import os
from uuid import UUID

from client import (
    CreateItemCreated,
    CreateItemInput,
    DriverConformanceClient,
    ItemPageFound,
    ItemPageParams,
)
from riffdb_application import (
    ApplicationErrorCode,
    AttemptBudget,
    BearerCredential,
    CallMetadata,
    DatabaseAlias,
    QueryOptions,
    RiffDbApplicationError,
    SyncApplicationTransport,
    VerifiedTlsConfig,
)


def main() -> None:
    endpoint = os.environ["RIFFDB_CONFORMANCE_ENDPOINT"]
    trust_root = os.environ["RIFFDB_CONFORMANCE_TRUST_ROOT"]
    credential = BearerCredential.from_protected_file(
        os.environ["RIFFDB_CONFORMANCE_CREDENTIAL"]
    )
    metadata = CallMetadata.authenticated(credential).with_database(DatabaseAlias("default"))
    tls = VerifiedTlsConfig(endpoint=endpoint, trust_root=trust_root, server_name="127.0.0.1")
    item_id = UUID("018f0f8b-7c6d-7e31-8a4f-000000000104")
    idempotency_key = "driver-conformance-python-create-v1"
    input_value = CreateItemInput(
        title="Shared remote Python", item_id=item_id, idempotency_key=idempotency_key
    )
    with SyncApplicationTransport.connect_verified_tls(tls, metadata) as transport:
        client = DriverConformanceClient(transport, AttemptBudget(3))
        if os.environ.get("RIFFDB_CONFORMANCE_EXPECT_REVOKED") == "1":
            try:
                client.item_page(
                    ItemPageParams(
                        item_id=UUID("018f0f8b-7c6d-7e31-8a4f-000000000104")
                    )
                )
            except RiffDbApplicationError as error:
                code = error.details.code.value
            else:
                raise RuntimeError("revoked Python authority remained usable")
            if code != ApplicationErrorCode.AUTHORIZATION_DENIED.value:
                raise RuntimeError("Python revocation error lost semantic details")
            print(
                json.dumps(
                    {
                        "schema": "riffdb.driver-conformance-fault/v1",
                        "fault": "revocation",
                        "code": code,
                    },
                    separators=(",", ":"),
                )
            )
            return
        first = client.create_item(input_value)
        if first.replayed or first.commit_sequence is None or not isinstance(
            first.outcome, CreateItemCreated
        ):
            raise RuntimeError("first Python command did not create the item")
        replay = client.create_item(input_value)
        if not replay.replayed:
            raise RuntimeError("second Python command did not replay")
        page = client.item_page(
            ItemPageParams(item_id=item_id),
            QueryOptions(read_after_commit=first.commit_sequence),
        )
        if (
            not isinstance(page.value, ItemPageFound)
            or page.value.item.item_id != item_id
            or page.value.item.title != "Shared remote Python"
        ):
            raise RuntimeError("Python read-after-commit returned the wrong item")
        reuse_error = None
        try:
            client.create_item(
                CreateItemInput(
                    title="Changed input",
                    item_id=item_id,
                    idempotency_key=idempotency_key,
                )
            )
        except RiffDbApplicationError as error:
            reuse_error = error.details.code.value
        if reuse_error != ApplicationErrorCode.IDEMPOTENCY_KEY_REUSE.value:
            raise RuntimeError("Python reuse error lost semantic details")
    print(
        json.dumps(
            {
                "schema": "riffdb.driver-conformance-observation/v1",
                "language": "python",
                "created": "Created",
                "replayed": True,
                "query": "Found",
                "read_after_commit": True,
                "reuse_error": reuse_error,
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
