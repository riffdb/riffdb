from __future__ import annotations

# req: OQ-004, OQ-006, OQ-016, OQ-031
import json
import os
from uuid import UUID

from client import (
    AdapterOperationalConformanceClient,
    AuthSessionState,
    DocumentsInTitleWindowParams,
    ExactDocumentsContainsAscParams,
    ExactDocumentsEndsWithDescParams,
    ExactDocumentsStartsWithAscParams,
    FgaObjectsWithRelationsParams,
    GetAuthSessionFound,
    GetAuthSessionParams,
    InventoryBySubtitleAscNullsFirstParams,
    ListDraftDocumentsParams,
    ListFgaTuplesParams,
    ListPipelinesParams,
    MetricDashboardParams,
    MlflowRunsWithTagsParams,
    ReviewedDirectoryUsersParams,
    SearchDirectoryUsersParams,
    SearchDocumentsParams,
    TicketPageWithCommentsParams,
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
        first = client.list_fga_tuples(ListFgaTuplesParams(store_id=uid(10)))
        require(
            len(first.value.tuples) == 25 and first.next_cursor is not None,
            "OpenFGA bounded first page",
        )
        second = client.list_fga_tuples(
            ListFgaTuplesParams(store_id=uid(10), after=first.next_cursor)
        )
        require(
            len(second.value.tuples) == 1 and second.next_cursor is None,
            "OpenFGA generated cursor continuation",
        )
        tuples = client.list_fga_tuples(
            ListFgaTuplesParams(store_id=uid(10), relation="viewer")
        ).value
        require(len(tuples.tuples) == 1, "OpenFGA optional relation page")
        malformed_failed = False
        try:
            client.list_fga_tuples(
                ListFgaTuplesParams(
                    store_id=uid(10), after="not-a-riffdb-cursor"
                )
            )
        except Exception:
            malformed_failed = True
        require(malformed_failed, "malformed generated cursor fails closed")

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

        tickets = client.ticket_page_with_comments(
            TicketPageWithCommentsParams(organization_id=uid(80), state="open")
        ).value
        require(len(tickets.tickets) == 2, "TicketDesk expansion page")
        require(
            len(tickets.tickets[0].comments) == 2
            and len(tickets.tickets[1].comments) == 1,
            "TicketDesk comments per ticket",
        )

        runs = client.mlflow_runs_with_tags(
            MlflowRunsWithTagsParams(experiment_id=uid(90), lifecycle="active")
        ).value
        require(len(runs.runs) == 2, "MLflow expansion page")
        require(
            len(runs.runs[0].tags) == 2 and len(runs.runs[1].tags) == 1,
            "MLflow tags per run",
        )

        objects = client.fga_objects_with_relations(
            FgaObjectsWithRelationsParams(store_id=uid(100), kind="document")
        ).value
        require(len(objects.objects) == 2, "OpenFGA expansion page")
        require(
            len(objects.objects[0].relations) == 2
            and len(objects.objects[1].relations) == 1,
            "OpenFGA relations per object",
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
        rich = client.search_directory_users(
            SearchDirectoryUsersParams(
                organization_id=uid(60), needle="example",
                excluded_states=("disabled", "disabled"), limit=1, offset=1,
            )
        ).value
        require(rich.total.value == 3 and len(rich.users) == 1
                and rich.users[0].email == "beta@example.test",
                "V6 exact predicate optional/set/order page")
        reviewed = client.reviewed_directory_users(
            ReviewedDirectoryUsersParams(
                organization_id=uid(60), states=("active", "archive"),
                before_created_at=35, limit=25, offset=0,
            )
        ).value
        require(reviewed.total.value == 1 and len(reviewed.users) == 1
                and reviewed.users[0].email == "álpha@example.test",
                "V6 exact predicate range/existence page")
        inventory = client.inventory_by_subtitle_asc_nulls_first(
            InventoryBySubtitleAscNullsFirstParams(
                organization_id=uid(70), limit=2, offset=0,
            )
        ).value
        require(inventory.total.value == 6, "nullable exact total")
        require(
            [row.record_id for row in inventory.records] == [uid(73), uid(76)],
            "nullable generated order",
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

        interval_first = client.documents_in_title_window(
            DocumentsInTitleWindowParams(
                site_id=uid(35), after_title="a", horizon_title="😀"
            )
        )
        require(
            [row.title for row in interval_first.value.documents] == ["aa", "b"]
            and interval_first.next_cursor is not None,
            "binary interval first page",
        )
        interval_second = client.documents_in_title_window(
            DocumentsInTitleWindowParams(
                site_id=uid(35),
                after_title="a",
                horizon_title="😀",
                after=interval_first.next_cursor,
            )
        )
        require(
            [row.title for row in interval_second.value.documents] == ["é"]
            and interval_second.next_cursor is None,
            "binary interval continuation",
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
                "exact_predicate_family": True,
                "nullable_exact_order": True,
                "exact_total": True,
                "numeric_offset": True,
                "operator_expansions": True,
                "binary_interval": {
                    "first_page": ["aa", "b"],
                    "second_page": ["é"],
                    "first_cursor": True,
                    "second_cursor": False,
                },
                "adapters": ["mlflow", "openfga", "better-auth", "woodpecker"],
                "regression_adapters": ["payload"],
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
