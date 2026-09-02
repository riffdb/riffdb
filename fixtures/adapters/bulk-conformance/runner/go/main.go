package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	generated "riffdb.dev/adapter-bulk-conformance/generated/go"
	riffdb "riffdb.dev/application"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Go adapter bulk conformance failed:", err)
		os.Exit(1)
	}
}

func run() error {
	identityBytes, err := os.ReadFile(os.Getenv("RIFFDB_CONFORMANCE_IDENTITY"))
	if err != nil {
		return errors.New("driver identity is unavailable")
	}
	var document struct {
		ApplicationManifestHash string `json:"applicationManifestHash"`
		OperationCatalogHash    string `json:"operationCatalogHash"`
		ContractLineage         string `json:"contractLineage"`
		ContractVersion         uint64 `json:"contractVersion"`
		ContractBundleHash      string `json:"contractBundleHash"`
		Database                string `json:"database"`
		Role                    string `json:"role"`
		RoleDefinitionHash      string `json:"roleDefinitionHash"`
		RemoteIdentityHash      string `json:"remoteIdentityHash"`
	}
	if json.Unmarshal(identityBytes, &document) != nil {
		return errors.New("driver identity is invalid")
	}
	identity := riffdb.Identity{
		ApplicationManifestHash: document.ApplicationManifestHash,
		OperationCatalogHash:    document.OperationCatalogHash,
		ContractLineage:         document.ContractLineage,
		ContractVersion:         document.ContractVersion,
		ContractBundleHash:      document.ContractBundleHash,
		Database:                document.Database,
		Role:                    document.Role,
		RoleDefinitionHash:      document.RoleDefinitionHash,
		RemoteIdentityHash:      document.RemoteIdentityHash,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	session, err := riffdb.Connect(ctx, os.Getenv("RIFFDB_CONFORMANCE_SOCKET"), identity)
	if err != nil {
		return err
	}
	defer session.Close()
	client, err := generated.NewClient(session, 3)
	if err != nil {
		return err
	}
	if _, err = client.WriteTuples(ctx, generated.WriteTuplesInput{RequestId: id(256)}); err == nil {
		return errors.New("empty collection crossed Go preflight")
	}

	store := id(257)
	tuples := generated.WriteTuplesInput{RequestId: id(258), Tuples: []generated.FgaTuple{{
		StoreId: store, TupleId: id(259), Object: "document:roadmap", Relation: "viewer", Subject: "user:agent",
	}}}
	firstTuple, err := client.WriteTuples(ctx, tuples)
	if err != nil {
		return err
	}
	if _, ok := firstTuple.Outcome.(generated.WriteTuplesTuplesWritten); !ok {
		return errors.New("Go OpenFGA outcome")
	}
	replayTuple, err := client.WriteTuples(ctx, tuples)
	if err != nil || !replayTuple.Replayed {
		return errors.New("Go OpenFGA replay")
	}

	metrics := generated.LogMetricsInput{RequestId: id(260), ExperimentId: id(261), Metrics: []generated.Metric{{
		ExperimentId: id(261), MetricId: id(262), Name: "latency", Step: 1, ValueMicros: 125,
	}}}
	firstMetric, err := client.LogMetrics(ctx, metrics)
	if err != nil {
		return err
	}
	if _, ok := firstMetric.Outcome.(generated.LogMetricsMetricsLogged); !ok {
		return errors.New("Go MLflow outcome")
	}
	replayMetric, err := client.LogMetrics(ctx, metrics)
	if err != nil || !replayMetric.Replayed {
		return errors.New("Go MLflow replay")
	}

	documents := generated.CreateDocumentGraphsInput{RequestId: id(263), Documents: []generated.DocumentGraphInput{{
		SiteId: id(264), DocumentId: id(265), RevisionId: id(266), Title: "Document", Body: "bounded body",
	}}}
	firstDocument, err := client.CreateDocumentGraphs(ctx, documents)
	if err != nil {
		return err
	}
	if _, ok := firstDocument.Outcome.(generated.CreateDocumentGraphsDocumentGraphsCreated); !ok {
		return errors.New("Go Payload outcome")
	}
	replayDocument, err := client.CreateDocumentGraphs(ctx, documents)
	if err != nil || !replayDocument.Replayed {
		return errors.New("Go Payload replay")
	}

	pipelines := generated.CreatePipelinesWithStepsInput{RequestId: id(267), Pipelines: []generated.PipelineGraphInput{{
		OrganizationId: id(268), PipelineId: id(269), StepId: id(270), Name: "verify", RunText: "go test ./...",
	}}}
	firstPipeline, err := client.CreatePipelinesWithSteps(ctx, pipelines)
	if err != nil {
		return err
	}
	if _, ok := firstPipeline.Outcome.(generated.CreatePipelinesWithStepsPipelinesCreated); !ok {
		return errors.New("Go Woodpecker outcome")
	}
	replayPipeline, err := client.CreatePipelinesWithSteps(ctx, pipelines)
	if err != nil || !replayPipeline.Replayed {
		return errors.New("Go Woodpecker replay")
	}

	largeA := make([]byte, 450_000)
	largeB := make([]byte, 450_000)
	if _, err = client.WritePolicyMutations(ctx, generated.WritePolicyMutationsInput{
		RequestId: id(280),
		Mutations: []generated.PolicyMutation{
			{OrganizationId: id(281), MutationId: id(282), Relation: "viewer", Context: &largeA},
			{OrganizationId: id(281), MutationId: id(283), Relation: "viewer", Context: &largeB},
		},
	}); err == nil {
		return errors.New("aggregate byte overflow crossed Go preflight")
	}
	for _, item := range []struct{ count, start, organization, request int }{
		{9, 900, 890, 891},
		{19, 910, 892, 893},
		{100, 1000, 894, 895},
	} {
		result, writeErr := client.WritePolicyMutations(ctx, generated.WritePolicyMutationsInput{
			RequestId: id(item.request),
			Mutations: policyMutations(item.count, item.start, item.organization),
		})
		if writeErr != nil {
			return fmt.Errorf("write %d neutral aggregate mutations: %w", item.count, writeErr)
		}
		if _, ok := result.Outcome.(generated.WritePolicyMutationsPolicyMutationsWritten); !ok {
			return errors.New("Go neutral aggregate outcome")
		}
	}

	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"schema": "riffdb.adapter-bulk-observation/v1", "language": "go",
		"bounded_preflight": true, "replayed": true, "neutral_aggregate": true,
		"adapters": []string{"mlflow", "openfga", "payload", "woodpecker"},
	})
}

func policyMutations(count, start, organization int) []generated.PolicyMutation {
	mutations := make([]generated.PolicyMutation, count)
	for index := range mutations {
		mutations[index] = generated.PolicyMutation{
			OrganizationId: id(organization),
			MutationId:     id(start + index),
			Relation:       "viewer",
		}
		if count == 100 && index == 0 {
			context := make([]byte, 524_288)
			mutations[index].Context = &context
		}
	}
	return mutations
}

func id(suffix int) string {
	return fmt.Sprintf("018f0f8b-7c6d-7e31-8a4f-00000000%04x", suffix)
}
