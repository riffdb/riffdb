from __future__ import annotations

import json
import os
import time
from uuid import UUID

from client import (
    ConsumeTokenInput,
    ConsumeTokenTokenConsumed,
    ConsumeTokenTokenMissing,
    CreateItemCreated,
    CreateItemInput,
    DriverConformanceClient,
    ItemPageFound,
    ItemPageParams,
    ItemSecretFound,
    ItemSecretParams,
    IssueTokenInput,
    IssueTokenTokenIssued,
    SearchItemsFound,
    SearchItemsParams,
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
    Timestamp,
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
    organization_id = UUID("018f0f8b-7c6d-7e31-8a4f-000000000100")
    idempotency_key = "driver-conformance-python-create-v1"
    input_value = CreateItemInput(
        title="Shared remote Python",
        token_digest="python-secret-digest-must-not-log",
        item_id=item_id,
        idempotency_key=idempotency_key,
        organization_id=organization_id,
    )
    with SyncApplicationTransport.connect_verified_tls(tls, metadata) as transport:
        client = DriverConformanceClient(transport, AttemptBudget(3))
        if os.environ.get("RIFFDB_CONFORMANCE_EXPECT_REVOKED") == "1":
            try:
                client.item_secret(
                    ItemSecretParams(
                        organization_id=organization_id,
                        item_id=UUID("018f0f8b-7c6d-7e31-8a4f-000000000104")
                    )
                )
            except RiffDbApplicationError as error:
                code = error.details.code.value
            else:
                raise RuntimeError("revoked Python authority remained usable")
            if code != ApplicationErrorCode.CAPABILITY_REVOKED.value:
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
        token_id = UUID("018f0f8b-7c6d-7e31-8a4f-000000000141")
        token_value = "python-one-time-secret-must-not-log"
        issued = client.issue_token(
            IssueTokenInput(
                value=token_value,
                token_id=token_id,
                expires_at=Timestamp(4_102_444_800, 0),
                identifier="python-loopback",
                request_id=UUID("018f0f8b-7c6d-7e31-8a4f-000000000142"),
                organization_id=organization_id,
            )
        )
        if not isinstance(issued.outcome, IssueTokenTokenIssued):
            raise RuntimeError("Python token issue did not create the token")
        consume_input = ConsumeTokenInput(
            token_id=token_id,
            request_id=UUID("018f0f8b-7c6d-7e31-8a4f-000000000143"),
            organization_id=organization_id,
        )
        consumed = client.consume_token(consume_input)
        if (
            not isinstance(consumed.outcome, ConsumeTokenTokenConsumed)
            or consumed.outcome.value != token_value
            or consumed.outcome.token_id != token_id
            or consumed.outcome.identifier != "python-loopback"
            or token_value in repr(consumed.outcome)
        ):
            raise RuntimeError("Python deleted preimage or redacted repr was incorrect")
        consumed_replay = client.consume_token(consume_input)
        if (
            not consumed_replay.replayed
            or not isinstance(consumed_replay.outcome, ConsumeTokenTokenConsumed)
            or consumed_replay.outcome.value != token_value
        ):
            raise RuntimeError("Python token consume did not replay the persisted preimage")
        missing = client.consume_token(
            ConsumeTokenInput(
                token_id=token_id,
                request_id=UUID("018f0f8b-7c6d-7e31-8a4f-000000000144"),
                organization_id=organization_id,
            )
        )
        if not isinstance(missing.outcome, ConsumeTokenTokenMissing):
            raise RuntimeError("Python second consumer did not observe the atomic delete")
        page = client.item_page(
            ItemPageParams(organization_id=organization_id, item_id=item_id),
            QueryOptions(read_after_commit=first.commit_sequence),
        )
        if (
            not isinstance(page.value, ItemPageFound)
            or page.value.item.item_id != item_id
            or page.value.item.title != "Shared remote Python"
        ):
            raise RuntimeError("Python read-after-commit returned the wrong item")
        secret = client.item_secret(
            ItemSecretParams(organization_id=organization_id, item_id=item_id),
            QueryOptions(read_after_commit=first.commit_sequence),
        )
        if (
            not isinstance(secret.value, ItemSecretFound)
            or secret.value.secret.token_digest != "python-secret-digest-must-not-log"
            or "python-secret-digest-must-not-log" in repr(secret.value)
        ):
            raise RuntimeError("Python secret query value or redacted repr was incorrect")
        exact = None
        for _ in range(200):
            try:
                exact = client.search_items(
                    SearchItemsParams(
                        organization_id=organization_id,
                        needle="remote Python",
                        item_id=item_id,
                        limit=50,
                        offset=0,
                    ),
                    QueryOptions(read_after_commit=first.commit_sequence),
                )
                break
            except RiffDbApplicationError as error:
                if error.details.code not in {
                    ApplicationErrorCode.QUERY_UNAVAILABLE,
                    ApplicationErrorCode.FRESHNESS_UNSATISFIED,
                }:
                    raise
                time.sleep(0.01)
        if exact is None:
            raise RuntimeError("Python exact provider did not become ready within the retry bound")
        if (
            not isinstance(exact.value, SearchItemsFound)
            or exact.value.total.value != 1
            or len(exact.value.items) != 1
            or exact.value.items[0].item_id != item_id
            or exact.value.items[0].organization_id != organization_id
        ):
            raise RuntimeError("Python exact page and whole-population total diverged")
        reuse_error = None
        try:
            client.create_item(
                CreateItemInput(
                    title="Changed input",
                    token_digest="python-secret-digest-must-not-log",
                    item_id=item_id,
                    idempotency_key=idempotency_key,
                    organization_id=organization_id,
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
                "secret_query": "Found",
                "secret_redacted": True,
                "exact_query": "Found",
                "exact_total": 1,
                "consume": "TokenConsumed",
                "consume_replayed": True,
                "consume_missing": True,
                "delete_preimage_redacted": True,
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
