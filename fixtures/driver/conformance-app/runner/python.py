from __future__ import annotations

import json
import os
from pathlib import Path
import time
from uuid import UUID

from client import (
    ConsumeTokenInput,
    ConsumeTokenTokenConsumed,
    ConsumeTokenTokenMissing,
    CreateItemCreated,
    CreateItemInput,
    DriverConformanceClient,
    IssueTokenInput,
    IssueTokenTokenIssued,
    ItemPageFound,
    ItemPageParams,
    ItemSecretFound,
    ItemSecretParams,
    ItemsByIdsParams,
    ItemsByTitlesParams,
    OpenBudgetAccountAccountOpened,
    OpenBudgetAccountInput,
    RecordSpendInput,
    RecordSpendSpendRecorded,
    ReviseBudgetNoteInput,
    ReviseBudgetNoteNoteRevised,
    ReviseBudgetNoteStaleRevision,
    SearchItemsFound,
    SearchItemsParams,
    WideNotesPageFound,
    WideNotesPageParams,
    WriteWideNoteInput,
)
from riffdb_application import (
    ApplicationErrorCode,
    AttemptBudget,
    BearerCredential,
    CallMetadata,
    DatabaseAlias,
    OutcomeUnknown,
    QueryOptions,
    RiffDbApplicationError,
    SyncApplicationTransport,
    Timestamp,
    VerifiedTlsConfig,
)


def connect_transport(*, uncertain: bool = False) -> SyncApplicationTransport:
    transport_mode = os.environ.get("RIFFDB_CONFORMANCE_TRANSPORT", "grpc")
    role_name = os.environ.get("RIFFDB_CONFORMANCE_ROLE", "DriverConformanceApplication")
    if transport_mode == "driver_socket":
        socket_env = (
            "RIFFDB_CONFORMANCE_UNCERTAIN_SOCKET"
            if uncertain
            else "RIFFDB_CONFORMANCE_SOCKET"
        )
        metadata = CallMetadata().with_database(DatabaseAlias("default"))
        return SyncApplicationTransport.connect_driver_socket(
            os.environ[socket_env], metadata, role=role_name
        )
    endpoint_env = (
        "RIFFDB_CONFORMANCE_UNCERTAIN_ENDPOINT"
        if uncertain
        else "RIFFDB_CONFORMANCE_ENDPOINT"
    )
    credential = BearerCredential.from_protected_file(
        os.environ["RIFFDB_CONFORMANCE_CREDENTIAL"]
    )
    metadata = CallMetadata.authenticated(credential).with_database(
        DatabaseAlias("default")
    )
    tls = VerifiedTlsConfig(
        endpoint=os.environ[endpoint_env],
        trust_root=os.environ["RIFFDB_CONFORMANCE_TRUST_ROOT"],
        server_name="127.0.0.1",
    )
    return SyncApplicationTransport.connect_verified_tls(tls, metadata)


def main() -> None:
    transport_mode = os.environ.get("RIFFDB_CONFORMANCE_TRANSPORT", "grpc")
    is_socket = transport_mode == "driver_socket"
    surface_name = "python-socket" if is_socket else "python"
    prefix = "05" if is_socket else "04"

    organization_id = UUID(f"018f0f8b-7c6d-7e31-8a4f-00000000{prefix}00")
    item_id = UUID(f"018f0f8b-7c6d-7e31-8a4f-00000000{prefix}01")
    extra_1 = UUID(f"018f0f8b-7c6d-7e31-8a4f-00000000{prefix}51")
    extra_2 = UUID(f"018f0f8b-7c6d-7e31-8a4f-00000000{prefix}52")
    extra_3 = UUID(f"018f0f8b-7c6d-7e31-8a4f-00000000{prefix}53")
    extra_4 = UUID(f"018f0f8b-7c6d-7e31-8a4f-00000000{prefix}54")

    if os.environ.get("RIFFDB_CONFORMANCE_EXPECT_UNCERTAIN") == "1":
        uncertain_input = CreateItemInput(
            title=f"Uncertain {surface_name} item",
            token_digest=f"{surface_name}-uncertain-secret",
            item_id=UUID(f"018f0f8b-7c6d-7e31-8a4f-00000000{prefix}99"),
            idempotency_key=f"driver-conformance-{surface_name}-uncertain-v1",
            organization_id=organization_id,
        )
        observed_uncertain = False
        with connect_transport(uncertain=True) as uncertain_transport:
            uncertain_client = DriverConformanceClient(
                uncertain_transport, AttemptBudget(1)
            )
            uncertain_client.item_page(
                ItemPageParams(organization_id=organization_id, item_id=item_id)
            )
            Path(os.environ["RIFFDB_CONFORMANCE_ARM_FILE"]).write_text(
                "arm\n", encoding="utf-8"
            )
            try:
                uncertain_client.create_item(uncertain_input)
            except OutcomeUnknown:
                observed_uncertain = True
        if not observed_uncertain:
            raise RuntimeError(f"expected {surface_name} OutcomeUnknown error")
        with connect_transport(uncertain=False) as recovery_transport:
            recovery_client = DriverConformanceClient(
                recovery_transport, AttemptBudget(3)
            )
            recovered = recovery_client.create_item(uncertain_input)
            if not recovered.replayed:
                raise RuntimeError(
                    f"{surface_name} uncertain recovery did not replay committed outcome"
                )
        print(
            json.dumps(
                {
                    "schema": "riffdb.driver-conformance-fault/v1",
                    "fault": "outcome_unknown",
                    "uncertain": True,
                    "replayed": True,
                },
                separators=(",", ":"),
            )
        )
        return

    with connect_transport(uncertain=False) as transport:
        client = DriverConformanceClient(transport, AttemptBudget(3))

        sync_dir = os.environ.get("RIFFDB_CONFORMANCE_RESTART_SYNC")
        if sync_dir:
            before = client.item_page(
                ItemPageParams(organization_id=organization_id, item_id=item_id)
            )
            if not isinstance(before.value, ItemPageFound):
                raise RuntimeError(
                    f"{surface_name} pre-restart item_page did not find item"
                )
            sync_path = Path(sync_dir)
            (sync_path / f"{surface_name}.ready").write_text(
                "ready\n", encoding="utf-8"
            )
            _ = (sync_path / f"{surface_name}.resume").read_text(encoding="utf-8")
            after = client.item_page(
                ItemPageParams(organization_id=organization_id, item_id=item_id)
            )
            if not isinstance(after.value, ItemPageFound):
                raise RuntimeError(
                    f"{surface_name} post-restart item_page did not find item"
                )
            print(
                json.dumps(
                    {
                        "schema": "riffdb.driver-conformance-fault/v1",
                        "fault": "driver_restart_reconnect",
                        "reconnected": True,
                    },
                    separators=(",", ":"),
                )
            )
            return

        if os.environ.get("RIFFDB_CONFORMANCE_EXPECT_AUTH_DIAG") == "1":
            try:
                client.items_by_ids(
                    ItemsByIdsParams(
                        organization_id=organization_id,
                        item_ids=(item_id, extra_1),
                    )
                )
            except RiffDbApplicationError as error:
                code = error.details.code.value
                diagnostic = error.details.authorization_diagnostic
            else:
                raise RuntimeError(f"narrowed {surface_name} authority was not denied")
            if (
                code != ApplicationErrorCode.AUTHORIZATION_DENIED.value
                or diagnostic != "scan_budget_exceeded"
            ):
                raise RuntimeError(
                    f"{surface_name} auth diagnostic lost semantic details: {code} / {diagnostic}"
                )
            print(
                json.dumps(
                    {
                        "schema": "riffdb.driver-conformance-fault/v1",
                        "fault": "authorization_diagnostic",
                        "code": code,
                        "diagnostic": diagnostic,
                    },
                    separators=(",", ":"),
                )
            )
            return

        if os.environ.get("RIFFDB_CONFORMANCE_EXPECT_REVOKED") == "1":
            try:
                client.item_secret(
                    ItemSecretParams(organization_id=organization_id, item_id=item_id)
                )
            except RiffDbApplicationError as error:
                code = error.details.code.value
            else:
                raise RuntimeError(f"revoked {surface_name} authority remained usable")
            if code != ApplicationErrorCode.CAPABILITY_REVOKED.value:
                raise RuntimeError(f"{surface_name} revocation error lost semantic details")
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

        idempotency_key = f"driver-conformance-{surface_name}-create-v1"
        token_digest = f"{surface_name}-secret-digest-must-not-log"
        title_label = f"Shared remote {surface_name}"
        input_value = CreateItemInput(
            title=title_label,
            token_digest=token_digest,
            item_id=item_id,
            idempotency_key=idempotency_key,
            organization_id=organization_id,
        )
        first = client.create_item(input_value)
        if (
            first.replayed
            or first.commit_sequence is None
            or not isinstance(first.outcome, CreateItemCreated)
        ):
            raise RuntimeError(f"first {surface_name} command did not create the item")
        replay = client.create_item(input_value)
        if not replay.replayed:
            raise RuntimeError(f"second {surface_name} command did not replay")

        commit_seq = first.commit_sequence
        for idx, (extra_id, extra_title) in enumerate(
            (
                (extra_1, "zeta-tag"),
                (extra_2, "Alpha-tag"),
                (extra_3, "alpha-tag"),
                (extra_4, "Zeta-tag"),
            )
        ):
            res = client.create_item(
                CreateItemInput(
                    title=extra_title,
                    token_digest=f"{surface_name}-extra-digest-{idx}",
                    item_id=extra_id,
                    idempotency_key=f"driver-conformance-{surface_name}-extra-{idx}",
                    organization_id=organization_id,
                )
            )
            if res.commit_sequence is None:
                raise RuntimeError(f"{surface_name} extra item creation failed")
            commit_seq = res.commit_sequence

        token_id = UUID(f"018f0f8b-7c6d-7e31-8a4f-00000000{prefix}41")
        token_value = f"{surface_name}-one-time-secret-must-not-log"
        issued = client.issue_token(
            IssueTokenInput(
                value=token_value,
                token_id=token_id,
                expires_at=Timestamp(4_102_444_800, 0),
                identifier=f"{surface_name}-loopback",
                request_id=UUID(f"018f0f8b-7c6d-7e31-8a4f-00000000{prefix}42"),
                organization_id=organization_id,
            )
        )
        if not isinstance(issued.outcome, IssueTokenTokenIssued):
            raise RuntimeError(f"{surface_name} token issue did not create the token")
        consume_input = ConsumeTokenInput(
            token_id=token_id,
            request_id=UUID(f"018f0f8b-7c6d-7e31-8a4f-00000000{prefix}43"),
            organization_id=organization_id,
        )
        consumed = client.consume_token(consume_input)
        if (
            not isinstance(consumed.outcome, ConsumeTokenTokenConsumed)
            or consumed.outcome.value != token_value
            or consumed.outcome.token_id != token_id
            or consumed.outcome.identifier != f"{surface_name}-loopback"
            or token_value in repr(consumed.outcome)
        ):
            raise RuntimeError(
                f"{surface_name} deleted preimage or redacted repr was incorrect"
            )
        consumed_replay = client.consume_token(consume_input)
        if (
            not consumed_replay.replayed
            or not isinstance(consumed_replay.outcome, ConsumeTokenTokenConsumed)
            or consumed_replay.outcome.value != token_value
        ):
            raise RuntimeError(
                f"{surface_name} token consume did not replay the persisted preimage"
            )
        missing = client.consume_token(
            ConsumeTokenInput(
                token_id=token_id,
                request_id=UUID(f"018f0f8b-7c6d-7e31-8a4f-00000000{prefix}44"),
                organization_id=organization_id,
            )
        )
        if not isinstance(missing.outcome, ConsumeTokenTokenMissing):
            raise RuntimeError(
                f"{surface_name} second consumer did not observe the atomic delete"
            )

        page = client.item_page(
            ItemPageParams(organization_id=organization_id, item_id=item_id),
            QueryOptions(read_after_commit=commit_seq),
        )
        if (
            not isinstance(page.value, ItemPageFound)
            or page.value.item.item_id != item_id
            or page.value.item.title != title_label
        ):
            raise RuntimeError(
                f"{surface_name} read-after-commit returned the wrong item"
            )
        secret = client.item_secret(
            ItemSecretParams(organization_id=organization_id, item_id=item_id),
            QueryOptions(read_after_commit=commit_seq),
        )
        if (
            not isinstance(secret.value, ItemSecretFound)
            or secret.value.secret.token_digest != token_digest
            or token_digest in repr(secret.value)
        ):
            raise RuntimeError(
                f"{surface_name} secret query value or redacted repr was incorrect"
            )

        exact = None
        for _ in range(200):
            try:
                exact = client.search_items(
                    SearchItemsParams(
                        organization_id=organization_id,
                        needle=f"remote {surface_name}",
                        item_id=item_id,
                        limit=50,
                        offset=0,
                    ),
                    QueryOptions(read_after_commit=commit_seq),
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
            raise RuntimeError(
                f"{surface_name} exact provider did not become ready within the retry bound"
            )
        if (
            not isinstance(exact.value, SearchItemsFound)
            or exact.value.total.value != 1
            or len(exact.value.items) != 1
            or exact.value.items[0].item_id != item_id
            or exact.value.items[0].organization_id != organization_id
        ):
            raise RuntimeError(
                f"{surface_name} exact page and whole-population total diverged"
            )

        # Post-mutation invariant (`RDB-INPUT-0101`, `correct_request`) + non-terminal idempotency key.
        account_id = UUID(f"018f0f8b-7c6d-7e31-8a4f-00000000{prefix}61")
        opened = client.open_budget_account(
            OpenBudgetAccountInput(
                request_id=f"{surface_name}-open-budget-v1",
                organization_id=organization_id,
                account_id=account_id,
                credit_limit=500,
                note="initial",
            )
        )
        if not isinstance(opened.outcome, OpenBudgetAccountAccountOpened):
            raise RuntimeError(f"{surface_name} open_budget_account failed")
        invariant_error = None
        invariant_recovery = None
        try:
            client.record_spend(
                RecordSpendInput(
                    request_id=f"{surface_name}-over-budget-v1",
                    organization_id=organization_id,
                    account_id=account_id,
                    amount=600,
                )
            )
        except RiffDbApplicationError as error:
            invariant_error = error.details.code.value
            invariant_recovery = error.details.recovery_action
        if (
            invariant_error != ApplicationErrorCode.INPUT_INVALID.value
            or invariant_recovery != "correct_request"
        ):
            raise RuntimeError(
                f"{surface_name} invariant error lost semantic details: {invariant_error} / {invariant_recovery}"
            )
        valid_spend = client.record_spend(
            RecordSpendInput(
                request_id=f"{surface_name}-over-budget-v1",
                organization_id=organization_id,
                account_id=account_id,
                amount=200,
            )
        )
        invariant_non_terminal = (
            not valid_spend.replayed
            and isinstance(valid_spend.outcome, RecordSpendSpendRecorded)
            and valid_spend.outcome.spent == 200
        )
        if not invariant_non_terminal:
            raise RuntimeError(
                f"{surface_name} CommitCheckRejected persisted terminal idempotency state"
            )

        # Optimistic workflow revision update + stale-revision conflict.
        rev_ok = client.revise_budget_note(
            ReviseBudgetNoteInput(
                request_id=f"{surface_name}-revise-1",
                organization_id=organization_id,
                account_id=account_id,
                expected_revision=2,
                note="updated-note",
            )
        )
        if not isinstance(rev_ok.outcome, ReviseBudgetNoteNoteRevised):
            raise RuntimeError(f"{surface_name} revise_budget_note at revision 2 failed")
        rev_stale = client.revise_budget_note(
            ReviseBudgetNoteInput(
                request_id=f"{surface_name}-revise-stale",
                organization_id=organization_id,
                account_id=account_id,
                expected_revision=2,
                note="stale-note",
            )
        )
        if not isinstance(rev_stale.outcome, ReviseBudgetNoteStaleRevision):
            raise RuntimeError(
                f"{surface_name} stale expected_revision did not return StaleRevision"
            )

        # `<query>_all` PK-set chunking & ascending UUID order.
        all_by_ids = client.items_by_ids_all(
            ItemsByIdsParams(
                organization_id=organization_id,
                item_ids=(extra_4, item_id, extra_2, extra_4, extra_1, extra_3),
            ),
            16,
            QueryOptions(read_after_commit=commit_seq),
        )
        pk_ids = [row.item_id for row in all_by_ids.value.items]
        pk_set_all_ordered = pk_ids == sorted(pk_ids)
        if len(pk_ids) != 5 or not pk_set_all_ordered:
            raise RuntimeError(f"{surface_name} items_by_ids_all count or ordering failed")

        # `<query>_all` `binary_utf8_v1` collation ordering.
        all_by_titles = client.items_by_titles_all(
            ItemsByTitlesParams(
                organization_id=organization_id,
                titles=("zeta-tag", "Alpha-tag", "alpha-tag", "Zeta-tag", "Alpha-tag"),
            ),
            16,
            QueryOptions(read_after_commit=commit_seq),
        )
        collation_titles = [row.title for row in all_by_titles.value.items]
        if collation_titles != ["Alpha-tag", "Zeta-tag", "alpha-tag", "zeta-tag"]:
            raise RuntimeError(
                f"{surface_name} items_by_titles_all collation order diverged: {collation_titles}"
            )

        # Wide-row + sibling-binding cursor continuation across >= 2 pages.
        wide_text = "P" * 380_000
        wide_blob = b"\x50" * 520_000
        wide_commit = commit_seq
        for seq in range(1, 7):
            written = client.write_wide_note(
                WriteWideNoteInput(
                    request_id=f"{surface_name}-wide-note-{seq}",
                    organization_id=organization_id,
                    note_seq=seq,
                    note_text=wide_text,
                    note_blob=wide_blob,
                )
            )
            if written.commit_sequence is None:
                raise RuntimeError(f"{surface_name} write_wide_note failed")
            wide_commit = written.commit_sequence
        wide_pages = client.wide_notes_page_pages(
            WideNotesPageParams(
                organization_id=organization_id,
                anchor_item_id=item_id,
                limit=10,
            ),
            16,
            QueryOptions(read_after_commit=wide_commit),
        )
        wide_pages_min_two = len(wide_pages) >= 2
        observed_seqs: list[int] = []
        for wp in wide_pages:
            if not isinstance(wp.value, WideNotesPageFound) or wp.value.anchor.item_id != item_id:
                raise RuntimeError(
                    f"{surface_name} wide_notes_page_pages sibling anchor mismatched"
                )
            for note in wp.value.notes:
                if len(note.note_text) != 380_000 or len(note.note_blob) != 520_000:
                    raise RuntimeError(
                        f"{surface_name} wide_notes_page_pages payload truncated"
                    )
                observed_seqs.append(note.note_seq)
        if not wide_pages_min_two or observed_seqs != [1, 2, 3, 4, 5, 6]:
            raise RuntimeError(
                f"{surface_name} wide_notes_page_pages pagination failed: pages={len(wide_pages)} seqs={observed_seqs}"
            )

        reuse_error = None
        try:
            client.create_item(
                CreateItemInput(
                    title="Changed input",
                    token_digest=token_digest,
                    item_id=item_id,
                    idempotency_key=idempotency_key,
                    organization_id=organization_id,
                )
            )
        except RiffDbApplicationError as error:
            reuse_error = error.details.code.value
        if reuse_error != ApplicationErrorCode.IDEMPOTENCY_KEY_REUSE.value:
            raise RuntimeError(f"{surface_name} reuse error lost semantic details")

    print(
        json.dumps(
            {
                "schema": "riffdb.driver-conformance-observation/v1",
                "language": surface_name,
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
                "invariant_error": invariant_error,
                "invariant_recovery": invariant_recovery,
                "invariant_non_terminal": invariant_non_terminal,
                "revision_updated": "NoteRevised",
                "revision_stale": "StaleRevision",
                "pk_set_all_count": len(pk_ids),
                "pk_set_all_ordered": pk_set_all_ordered,
                "collation_all_titles": collation_titles,
                "wide_cursor_pages_min_two": wide_pages_min_two,
                "wide_cursor_total": len(observed_seqs),
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
