from __future__ import annotations

import json
import os
from uuid import UUID

from client import (
    AdapterOperationalConformanceClient,
    AuthSessionState,
    ExactDocumentsContainsAscParams,
    ExactDocumentsEndsWithDescParams,
    ExactDocumentsStartsWithAscParams,
    GetAuthSessionFound,
    GetAuthSessionParams,
    ListDraftDocumentsParams,
    ListFgaTuplesParams,
    ListPipelinesParams,
    MetricDashboardParams,
    SearchDocumentsParams,
)
from riffdb_application import (
    AttemptBudget,
    BearerCredential,
    CallMetadata,
    DatabaseAlias,
    SyncApplicationTransport,
    VerifiedTlsConfig,
)


def uid(suffix: int) -> UUID:
    return UUID(f"018f0f8b-7c6d-7e31-8a4f-00000000{suffix:04x}")


def require(condition: bool, label: str) -> None:
    if not condition:
        raise RuntimeError(f"Python adapter assertion: {label}")


def main() -> None:
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
        client = AdapterOperationalConformanceClient(transport, AttemptBudget(3))
        tuples = client.list_fga_tuples(
            ListFgaTuplesParams(store_id=uid(10), relation="viewer")
        ).value
        require(len(tuples.tuples) == 1, "OpenFGA optional relation page")

        dashboard = client.metric_dashboard(
            MetricDashboardParams(experiment_id=uid(20))
        ).value
        require(len(dashboard.summary) == 1, "MLflow aggregate page")
        require(dashboard.summary[0].sample_count == 2, "MLflow exact count")
        require(
            dashboard.summary[0].minimum_micros == 125
            and dashboard.summary[0].maximum_micros == 175,
            "MLflow min/max",
        )

        documents = client.search_documents(
            SearchDocumentsParams(site_id=uid(30), title_prefix="Alpha")
        ).value
        require(len(documents.documents) == 2, "Payload binary prefix page")
        drafts = client.list_draft_documents(
            ListDraftDocumentsParams(site_id=uid(30))
        ).value
        require(len(drafts.documents) == 1, "Payload null predicate page")

        contains = client.exact_documents_contains_asc(
            ExactDocumentsContainsAscParams(
                site_id=uid(30), needle="Alpha", limit=1, offset=1
            )
        ).value
        require(contains.total.value == 2, "generic contains exact total")
        require(
            len(contains.documents) == 1
            and contains.documents[0].title == "Alpha Published",
            "generic contains numeric offset",
        )
        starts_with = client.exact_documents_starts_with_asc(
            ExactDocumentsStartsWithAscParams(
                site_id=uid(30),
                needle="Alpha",
                document_id=uid(31),
                limit=25,
                offset=0,
            )
        ).value
        require(
            starts_with.total.value == 1
            and len(starts_with.documents) == 1
            and starts_with.documents[0].document_id == uid(31),
            "generic starts-with typed optional filter",
        )
        ends_with = client.exact_documents_ends_with_desc(
            ExactDocumentsEndsWithDescParams(
                site_id=uid(30), needle="Guide", limit=1, offset=1
            )
        ).value
        require(ends_with.total.value == 2, "generic ends-with exact total")
        require(
            len(ends_with.documents) == 1
            and ends_with.documents[0].title == "Beta Guide",
            "generic ends-with descending ordinal",
        )

        pipelines = client.list_pipelines(
            ListPipelinesParams(organization_id=uid(40), state="queued")
        ).value
        require(len(pipelines.pipelines) == 1, "Woodpecker optional state page")

        auth_session = client.get_auth_session(
            GetAuthSessionParams(
                organization_id=uid(50), user_id=uid(51), session_id=uid(52)
            )
        ).value
        require(isinstance(auth_session, GetAuthSessionFound), "Better Auth session page")
        if not isinstance(auth_session, GetAuthSessionFound):
            raise RuntimeError("Python adapter assertion: Better Auth session page")
        require(
            auth_session.session.state is AuthSessionState.AUTH_ACTIVE
            and auth_session.session.expires_at.seconds == 1_800_000_000,
            "Better Auth typed session graph",
        )

    print(
        json.dumps(
            {
                "schema": "riffdb.adapter-operational-observation/v1",
                "language": "python",
                "catalog_preflight": True,
                "optional_filters": True,
                "stable_cursor": True,
                "null_predicate": True,
                "binary_prefix": True,
                "exact_aggregates": True,
                "exact_text_family": True,
                "exact_total": True,
                "numeric_offset": True,
                "adapters": ["mlflow", "openfga", "better-auth", "woodpecker"],
                "regression_adapters": ["payload"],
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
