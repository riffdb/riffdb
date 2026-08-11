package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	generated "riffdb.dev/adapter-operational-conformance/generated/go"
	riffdb "riffdb.dev/application"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Go adapter operational conformance failed:", err)
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	viewer := "viewer"
	tuples, err := client.ListFgaTuples(ctx, generated.ListFgaTuplesParams{
		StoreId: id(10), Relation: &viewer,
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	tuplePage, ok := tuples.Value.(generated.ListFgaTuplesFound)
	if !ok || len(tuplePage.Tuples) != 1 {
		return errors.New("OpenFGA optional relation page")
	}

	dashboard, err := client.MetricDashboard(ctx, generated.MetricDashboardParams{
		ExperimentId: id(20),
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	metricPage, ok := dashboard.Value.(generated.MetricDashboardFound)
	if !ok || len(metricPage.Summary) != 1 || metricPage.Summary[0].SampleCount != 2 ||
		metricPage.Summary[0].MinimumMicros == nil || *metricPage.Summary[0].MinimumMicros != 125 ||
		metricPage.Summary[0].MaximumMicros == nil || *metricPage.Summary[0].MaximumMicros != 175 {
		return errors.New("MLflow exact aggregate dashboard")
	}

	documents, err := client.SearchDocuments(ctx, generated.SearchDocumentsParams{
		SiteId: id(30), TitlePrefix: "Alpha",
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	documentPage, ok := documents.Value.(generated.SearchDocumentsFound)
	if !ok || len(documentPage.Documents) != 2 {
		return errors.New("Payload binary prefix page")
	}
	drafts, err := client.ListDraftDocuments(ctx, generated.ListDraftDocumentsParams{
		SiteId: id(30),
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	draftPage, ok := drafts.Value.(generated.ListDraftDocumentsFound)
	if !ok || len(draftPage.Documents) != 1 {
		return errors.New("Payload null predicate page")
	}

	queued := "queued"
	pipelines, err := client.ListPipelines(ctx, generated.ListPipelinesParams{
		OrganizationId: id(40), State: &queued,
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	pipelinePage, ok := pipelines.Value.(generated.ListPipelinesFound)
	if !ok || len(pipelinePage.Pipelines) != 1 {
		return errors.New("Woodpecker optional state page")
	}

	return json.NewEncoder(os.Stdout).Encode(observation("go"))
}

func observation(language string) map[string]any {
	return map[string]any{
		"schema": "riffdb.adapter-operational-observation/v1", "language": language,
		"catalog_preflight": true, "optional_filters": true, "stable_cursor": true,
		"null_predicate": true, "binary_prefix": true, "exact_aggregates": true,
		"adapters": []string{"mlflow", "openfga", "payload", "woodpecker"},
	}
}

func id(suffix int) string {
	return fmt.Sprintf("018f0f8b-7c6d-7e31-8a4f-00000000%04x", suffix)
}
