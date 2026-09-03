package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
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
		RequestId: id(278), Mutations: nil,
	}); !budgetErrorMatches(err, generated.CollectionCount, "mutations", nil, "") {
		return errors.New("collection count used the wrong Go preflight error")
	}
	individualContext := make([]byte, 524_289)
	zero := 0
	if _, err = client.WritePolicyMutations(ctx, generated.WritePolicyMutationsInput{
		RequestId: id(279),
		Mutations: []generated.PolicyMutation{{OrganizationId: id(281), MutationId: id(282), Relation: "viewer", Context: &individualContext}},
	}); !budgetErrorMatches(err, generated.IndividualValueBytes, "mutations", &zero, "context") {
		return errors.New("individual bytes used the wrong Go preflight error")
	}
	multibyteBoundary := generated.WritePolicyMutationsInput{
		RequestId: id(1282),
		Mutations: []generated.PolicyMutation{{
			OrganizationId: id(1280), MutationId: id(1281), Relation: strings.Repeat("é", 32),
		}},
	}
	if result, boundaryErr := client.WritePolicyMutations(ctx, multibyteBoundary); boundaryErr != nil {
		return fmt.Errorf("64-byte multibyte leaf: %w", boundaryErr)
	} else if _, ok := result.Outcome.(generated.WritePolicyMutationsPolicyMutationsWritten); !ok {
		return errors.New("64-byte multibyte leaf used the wrong Go outcome")
	}
	if _, err = client.WritePolicyMutations(ctx, generated.WritePolicyMutationsInput{
		RequestId: id(1285),
		Mutations: []generated.PolicyMutation{{
			OrganizationId: id(1283), MutationId: id(1284), Relation: strings.Repeat("é", 32) + "a",
		}},
	}); !budgetErrorMatches(err, generated.IndividualValueBytes, "mutations", &zero, "relation") {
		return errors.New("65-byte multibyte leaf used the wrong Go preflight error")
	}
	if _, err = client.WritePolicyMutations(ctx, generated.WritePolicyMutationsInput{
		RequestId: id(280),
		Mutations: []generated.PolicyMutation{
			{OrganizationId: id(281), MutationId: id(282), Relation: "viewer", Context: &largeA},
			{OrganizationId: id(281), MutationId: id(283), Relation: "viewer", Context: &largeB},
		},
	}); !budgetErrorMatches(err, generated.AggregateCanonicalElementBytes, "mutations", nil, "") {
		return errors.New("aggregate bytes used the wrong Go preflight error")
	}
	for _, item := range []struct{ count, start, organization, request int }{
		{1, 880, 888, 889},
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
		"budget_errors": []map[string]any{
			{"cause": "collection_count", "collection": "mutations"},
			{"cause": "individual_value_bytes", "collection": "mutations", "index": 0, "leaf": "context"},
			{"cause": "individual_value_bytes", "collection": "mutations", "index": 0, "leaf": "relation"},
			{"cause": "aggregate_canonical_element_bytes", "collection": "mutations"},
		},
		"adapters": []string{"mlflow", "openfga", "payload", "woodpecker"},
	})
}

func budgetErrorMatches(err error, cause generated.InputBudgetCause, collection string, index *int, leaf string) bool {
	var budget *generated.InputBudgetError
	if !errors.As(err, &budget) || budget.Cause != cause || budget.Path.Collection != collection || budget.Path.Leaf != leaf {
		return false
	}
	if index == nil {
		return budget.Path.Index == nil
	}
	return budget.Path.Index != nil && *budget.Path.Index == *index
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
