from __future__ import annotations

import json
import os
from uuid import UUID

from client import (
    AdapterRowPolicyConformanceClient,
    AttemptDocumentTransferInput,
    CreateDocumentDocumentCreated,
    CreateDocumentInput,
    DocumentState,
    DocumentSummaryParams,
    FinishRunFinishStale,
    FinishRunInput,
    FinishRunRunFinished,
    GetDocumentFound,
    GetDocumentParams,
    ListDocumentsParams,
    ListDraftDocumentsParams,
    ListExperimentsParams,
    MetricDashboardParams,
    RunPageFound,
    RunPageParams,
    SearchDocumentsParams,
    Visibility,
)
from riffdb_application import (
    ApplicationErrorCode,
    AttemptBudget,
    BearerCredential,
    CallMetadata,
    DatabaseAlias,
    RiffDbApplicationError,
    SyncApplicationTransport,
    VerifiedTlsConfig,
)


def uid(suffix: int) -> UUID:
    return UUID(f"018f0f8b-7c6d-7e31-8a4f-0000000000{suffix:02x}")


def require(condition: bool, label: str) -> None:
    if not condition:
        raise RuntimeError(f"Python row-policy assertion: {label}")


def main() -> None:
    mode = os.environ["RIFFDB_ROW_POLICY_MODE"]
    expected = {
        "owner": (6, 4, 3, 3, True),
        "outsider": (3, 2, 1, 1, False),
    }.get(mode)
    if expected is None:
        raise RuntimeError("unknown row-policy mode")
    document_count, draft_count, experiment_count, metric_count, run_visible = expected
    principal_id = uid(1 if mode == "owner" else 3)
    document_suffix = 60 if mode == "owner" else 61
    request_suffix = 160 if mode == "owner" else 161
    credential = BearerCredential.from_protected_file(
        os.environ["RIFFDB_CONFORMANCE_CREDENTIAL"]
    )
    metadata = CallMetadata.authenticated(credential).with_database(
        DatabaseAlias("default")
    )
    tls = VerifiedTlsConfig(
        endpoint=os.environ["RIFFDB_CONFORMANCE_ENDPOINT"],
        trust_root=os.environ["RIFFDB_CONFORMANCE_TRUST_ROOT"],
        server_name="127.0.0.1",
    )
    with SyncApplicationTransport.connect_verified_tls(tls, metadata) as transport:
        client = AdapterRowPolicyConformanceClient(transport, AttemptBudget(3))
        created = client.create_document(
            CreateDocumentInput(
                body="created through every generated language",
                state=DocumentState.DRAFT,
                title=f"Document shared-{mode}",
                group_id=None,
                owner_id=principal_id,
                request_id=uid(request_suffix),
                visibility=Visibility.PRIVATE,
                document_id=uid(document_suffix),
                organization_id=uid(10),
            )
        )
        require(
            isinstance(created.outcome, CreateDocumentDocumentCreated),
            "typed protected command outcome",
        )
        documents = client.list_documents(
            ListDocumentsParams(organization_id=uid(10))
        ).value
        require(len(documents.documents) == document_count, "document page")
        drafts = client.list_draft_documents(
            ListDraftDocumentsParams(organization_id=uid(10))
        ).value
        require(len(drafts.documents) == draft_count, "draft page")
        search = client.search_documents(
            SearchDocumentsParams(
                organization_id=uid(10), title_prefix="Document"
            )
        ).value
        require(len(search.documents) == document_count, "text search")
        detail = client.get_document(
            GetDocumentParams(organization_id=uid(10), document_id=uid(15))
        ).value
        require(isinstance(detail, GetDocumentFound) == (mode == "owner"), "detail")
        document_summary = client.document_summary(
            DocumentSummaryParams(organization_id=uid(10))
        ).value
        require(
            sum(group.document_count for group in document_summary.summary)
            == document_count,
            "document aggregate",
        )
        experiments = client.list_experiments(
            ListExperimentsParams(organization_id=uid(10))
        ).value
        require(len(experiments.experiments) == experiment_count, "experiment page")
        dashboard = client.metric_dashboard(
            MetricDashboardParams(organization_id=uid(10))
        ).value
        require(
            len(dashboard.summary) == 1
            and dashboard.summary[0].sample_count == metric_count,
            "policy-before-aggregate dashboard",
        )
        run = client.run_page(
            RunPageParams(
                organization_id=uid(10), experiment_id=uid(23), run_id=uid(31)
            )
        ).value
        require(isinstance(run, RunPageFound) == run_visible, "run visibility")
        if isinstance(run, RunPageFound):
            require(
                len(run.metrics) == 1 and len(run.artifacts) == 1,
                "nested policy hydration",
            )
        try:
            client.attempt_document_transfer(
                AttemptDocumentTransferInput(
                    request_id=uid(request_suffix + 10),
                    document_id=uid(document_suffix),
                    new_owner_id=uid(2),
                    organization_id=uid(10),
                )
            )
        except RiffDbApplicationError as error:
            transfer_code = error.details.code
        else:
            raise RuntimeError("successor-row owner escape was accepted")
        require(
            transfer_code is ApplicationErrorCode.AUTHORIZATION_DENIED,
            "typed successor-row authorization error",
        )

        lifecycle_checked = mode == "owner"
        if lifecycle_checked:
            finished = client.finish_run(
                FinishRunInput(
                    run_id=uid(33),
                    request_id=uid(180),
                    experiment_id=uid(21),
                    organization_id=uid(10),
                    expected_revision=1,
                )
            )
            require(
                isinstance(finished.outcome, FinishRunRunFinished),
                "revision-checked MLflow transition",
            )
            stale = client.finish_run(
                FinishRunInput(
                    run_id=uid(33),
                    request_id=uid(181),
                    experiment_id=uid(21),
                    organization_id=uid(10),
                    expected_revision=1,
                )
            )
            require(
                isinstance(stale.outcome, FinishRunFinishStale),
                "stale MLflow transition",
            )

    print(
        json.dumps(
            {
                "schema": "riffdb.adapter-row-policy-observation/v1",
                "language": "python",
                "mode": mode,
                "documents": document_count,
                "drafts": draft_count,
                "experiments": experiment_count,
                "metrics": metric_count,
                "group_run_visible": run_visible,
                "policy_before_aggregate": True,
                "detail_and_search": True,
                "nested_policy": True,
                "protected_command": True,
                "successor_escape_denied": True,
                "lifecycle_checked": lifecycle_checked,
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
