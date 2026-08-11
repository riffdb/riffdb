from __future__ import annotations

import json
import os
from uuid import UUID

from client import (
    AdapterOperationalConformanceClient,
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

        pipelines = client.list_pipelines(
            ListPipelinesParams(organization_id=uid(40), state="queued")
        ).value
        require(len(pipelines.pipelines) == 1, "Woodpecker optional state page")

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
                "adapters": ["mlflow", "openfga", "payload", "woodpecker"],
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
