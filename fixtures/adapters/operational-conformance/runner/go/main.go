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
	limitOne := uint32(1)
	offsetOne := uint64(1)
	offsetZero := uint64(0)
	contains, err := client.ExactDocumentsContainsAsc(ctx, generated.ExactDocumentsContainsAscParams{
		SiteId: id(30), Needle: "Alpha", Limit: &limitOne, Offset: &offsetOne,
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	containsPage, ok := contains.Value.(generated.ExactDocumentsContainsAscFound)
	if !ok || containsPage.Total.Value != 2 || len(containsPage.Documents) != 1 ||
		containsPage.Documents[0].Title != "Alpha Published" {
		return errors.New("generic contains page with exact total and numeric offset")
	}
	filterDocument := id(31)
	startsWith, err := client.ExactDocumentsStartsWithAsc(ctx, generated.ExactDocumentsStartsWithAscParams{
		SiteId: id(30), Needle: "Alpha", DocumentId: &filterDocument, Offset: &offsetZero,
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	startsWithPage, ok := startsWith.Value.(generated.ExactDocumentsStartsWithAscFound)
	if !ok || startsWithPage.Total.Value != 1 || len(startsWithPage.Documents) != 1 ||
		startsWithPage.Documents[0].DocumentId != id(31) {
		return errors.New("generic starts-with page with typed optional filter")
	}
	endsWith, err := client.ExactDocumentsEndsWithDesc(ctx, generated.ExactDocumentsEndsWithDescParams{
		SiteId: id(30), Needle: "Guide", Limit: &limitOne, Offset: &offsetOne,
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	endsWithPage, ok := endsWith.Value.(generated.ExactDocumentsEndsWithDescFound)
	if !ok || endsWithPage.Total.Value != 2 || len(endsWithPage.Documents) != 1 ||
		endsWithPage.Documents[0].Title != "Beta Guide" {
		return errors.New("generic ends-with page with descending order and numeric offset")
	}
	rich, err := client.SearchDirectoryUsers(ctx, generated.SearchDirectoryUsersParams{
		OrganizationId: id(60), Needle: "example", ExcludedStates: []string{"disabled", "disabled"},
		Limit: &limitOne, Offset: &offsetOne,
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	richPage, ok := rich.Value.(generated.SearchDirectoryUsersFound)
	if !ok || richPage.Total.Value != 3 || len(richPage.Users) != 1 || richPage.Users[0].Email != "beta@example.test" {
		return errors.New("V6 exact predicate optional/set/order page")
	}
	reviewed, err := client.ReviewedDirectoryUsers(ctx, generated.ReviewedDirectoryUsersParams{
		OrganizationId: id(60), States: []string{"active", "archive"}, BeforeCreatedAt: 35,
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	reviewedPage, ok := reviewed.Value.(generated.ReviewedDirectoryUsersFound)
	if !ok || reviewedPage.Total.Value != 1 || len(reviewedPage.Users) != 1 || reviewedPage.Users[0].Email != "álpha@example.test" {
		return errors.New("V6 exact predicate range/existence page")
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

	authSession, err := client.GetAuthSession(ctx, generated.GetAuthSessionParams{
		OrganizationId: id(50), UserId: id(51), SessionId: id(52),
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	authPage, ok := authSession.Value.(generated.GetAuthSessionFound)
	if !ok || authPage.Session.State != generated.AuthSessionStateAuthActive ||
		authPage.Session.ExpiresAt.Seconds != 1_800_000_000 {
		return errors.New("Better Auth typed session graph")
	}

	return json.NewEncoder(os.Stdout).Encode(observation("go"))
}

func observation(language string) map[string]any {
	return map[string]any{
		"schema": "riffdb.adapter-operational-observation/v1", "language": language,
		"catalog_preflight": true, "optional_filters": true, "stable_cursor": true,
		"null_predicate": true, "binary_prefix": true, "exact_aggregates": true,
		"exact_text_family": true, "exact_predicate_family": true, "exact_total": true, "numeric_offset": true,
		"adapters":            []string{"mlflow", "openfga", "better-auth", "woodpecker"},
		"regression_adapters": []string{"payload"},
	}
}

func id(suffix int) string {
	return fmt.Sprintf("018f0f8b-7c6d-7e31-8a4f-00000000%04x", suffix)
}
