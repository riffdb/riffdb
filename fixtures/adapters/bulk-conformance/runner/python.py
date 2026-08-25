from __future__ import annotations

import json
import os
from uuid import UUID

from client import (
    AdapterBulkConformanceClient,
    CreateDocumentGraphsDocumentGraphsCreated,
    CreateDocumentGraphsInput,
    CreatePipelinesWithStepsInput,
    CreatePipelinesWithStepsPipelinesCreated,
    DocumentGraphInput,
    FgaTuple,
    LogMetricsInput,
    LogMetricsMetricsLogged,
    Metric,
    PipelineGraphInput,
    PolicyMutation,
    WritePolicyMutationsInput,
    WritePolicyMutationsPolicyMutationsWritten,
    WriteTuplesInput,
    WriteTuplesTuplesWritten,
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
        client = AdapterBulkConformanceClient(transport, AttemptBudget(3))
        try:
            client.write_tuples(WriteTuplesInput(tuples=(), request_id=uid(768)))
        except ValueError:
            bounded = True
        else:
            bounded = False
        require(bounded, "empty collection preflight")

        tuples = WriteTuplesInput(
            request_id=uid(769),
            tuples=(
                FgaTuple(
                    store_id=uid(770),
                    tuple_id=uid(771),
                    object="document:roadmap",
                    relation="viewer",
                    subject="user:agent",
                ),
            ),
        )
        require(
            isinstance(client.write_tuples(tuples).outcome, WriteTuplesTuplesWritten),
            "OpenFGA outcome",
        )
        require(client.write_tuples(tuples).replayed, "OpenFGA replay")

        metrics = LogMetricsInput(
            request_id=uid(772),
            metrics=(
                Metric(
                    experiment_id=uid(773),
                    metric_id=uid(774),
                    name="latency",
                    step=1,
                    value_micros=125,
                ),
            ),
        )
        require(
            isinstance(client.log_metrics(metrics).outcome, LogMetricsMetricsLogged),
            "MLflow outcome",
        )
        require(client.log_metrics(metrics).replayed, "MLflow replay")

        documents = CreateDocumentGraphsInput(
            request_id=uid(775),
            documents=(
                DocumentGraphInput(
                    site_id=uid(776),
                    document_id=uid(777),
                    revision_id=uid(778),
                    title="Document",
                    body="bounded body",
                ),
            ),
        )
        require(
            isinstance(
                client.create_document_graphs(documents).outcome,
                CreateDocumentGraphsDocumentGraphsCreated,
            ),
            "Payload outcome",
        )
        require(client.create_document_graphs(documents).replayed, "Payload replay")

        pipelines = CreatePipelinesWithStepsInput(
            request_id=uid(779),
            pipelines=(
                PipelineGraphInput(
                    organization_id=uid(780),
                    pipeline_id=uid(781),
                    step_id=uid(782),
                    name="verify",
                    run_text="python -m unittest",
                ),
            ),
        )
        require(
            isinstance(
                client.create_pipelines_with_steps(pipelines).outcome,
                CreatePipelinesWithStepsPipelinesCreated,
            ),
            "Woodpecker outcome",
        )
        require(
            client.create_pipelines_with_steps(pipelines).replayed,
            "Woodpecker replay",
        )

        try:
            client.write_policy_mutations(
                WritePolicyMutationsInput(
                    request_id=uid(800),
                    mutations=(
                        PolicyMutation(
                            organization_id=uid(801),
                            mutation_id=uid(802),
                            relation="viewer",
                            context=bytes(450_000),
                        ),
                        PolicyMutation(
                            organization_id=uid(801),
                            mutation_id=uid(803),
                            relation="viewer",
                            context=bytes(450_000),
                        ),
                    ),
                )
            )
        except ValueError:
            aggregate_bounded = True
        else:
            aggregate_bounded = False
        require(aggregate_bounded, "aggregate byte preflight")

        for count, start, organization, request in (
            (9, 900, 890, 891),
            (19, 910, 892, 893),
            (100, 1000, 894, 895),
        ):
            mutations = tuple(
                PolicyMutation(
                    organization_id=uid(organization),
                    mutation_id=uid(start + index),
                    relation="viewer",
                    context=bytes(524_288) if count == 100 and index == 0 else None,
                )
                for index in range(count)
            )
            require(
                isinstance(
                    client.write_policy_mutations(
                        WritePolicyMutationsInput(
                            request_id=uid(request), mutations=mutations
                        )
                    ).outcome,
                    WritePolicyMutationsPolicyMutationsWritten,
                ),
                "neutral aggregate outcome",
            )

    print(
        json.dumps(
            {
                "schema": "riffdb.adapter-bulk-observation/v1",
                "language": "python",
                "bounded_preflight": True,
                "replayed": True,
                "neutral_aggregate": True,
                "adapters": ["mlflow", "openfga", "payload", "woodpecker"],
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
