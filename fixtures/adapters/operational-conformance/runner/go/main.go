// req: OQ-004, OQ-006, OQ-016, OQ-031
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
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

	first, err := client.ListFgaTuples(ctx, generated.ListFgaTuplesParams{
		StoreId: id(10),
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	firstPage, ok := first.Value.(generated.ListFgaTuplesFound)
	if !ok || len(firstPage.Tuples) != 25 || first.NextCursor == "" {
		return errors.New("OpenFGA bounded first page")
	}
	second, err := client.ListFgaTuples(ctx, generated.ListFgaTuplesParams{
		StoreId: id(10), After: &first.NextCursor,
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	secondPage, ok := second.Value.(generated.ListFgaTuplesFound)
	if !ok || len(secondPage.Tuples) != 1 || second.NextCursor != "" {
		return errors.New("OpenFGA generated cursor continuation")
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
	malformed := "not-a-riffdb-cursor"
	if _, err := client.ListFgaTuples(ctx, generated.ListFgaTuplesParams{
		StoreId: id(10), After: &malformed,
	}, generated.QueryOptions{}); err == nil {
		return errors.New("malformed generated cursor did not fail closed")
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

	ticketPage, err := client.TicketPageWithComments(ctx, generated.TicketPageWithCommentsParams{
		OrganizationId: id(80), State: "open",
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	tickets, ok := ticketPage.Value.(generated.TicketPageWithCommentsFound)
	if !ok || len(tickets.Tickets) != 2 || len(tickets.Tickets[0].Comments) != 2 || len(tickets.Tickets[1].Comments) != 1 {
		return errors.New("TicketDesk tickets with comments per ticket")
	}

	runPage, err := client.MlflowRunsWithTags(ctx, generated.MlflowRunsWithTagsParams{
		ExperimentId: id(90), Lifecycle: "active",
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	runs, ok := runPage.Value.(generated.MlflowRunsWithTagsFound)
	if !ok || len(runs.Runs) != 2 || len(runs.Runs[0].Tags) != 2 || len(runs.Runs[1].Tags) != 1 {
		return errors.New("MLflow runs with tags per run")
	}

	objectPage, err := client.FgaObjectsWithRelations(ctx, generated.FgaObjectsWithRelationsParams{
		StoreId: id(100), Kind: "document",
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	objects, ok := objectPage.Value.(generated.FgaObjectsWithRelationsFound)
	if !ok || len(objects.Objects) != 2 || len(objects.Objects[0].Relations) != 2 || len(objects.Objects[1].Relations) != 1 {
		return errors.New("OpenFGA objects with relations per object")
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
	limitTwo := uint32(2)
	inventory, err := client.InventoryBySubtitleAscNullsFirst(ctx, generated.InventoryBySubtitleAscNullsFirstParams{
		OrganizationId: id(70), Limit: &limitTwo, Offset: &offsetZero,
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	inventoryPage, ok := inventory.Value.(generated.InventoryBySubtitleAscNullsFirstFound)
	if !ok || inventoryPage.Total.Value != 6 || len(inventoryPage.Records) != 2 ||
		inventoryPage.Records[0].RecordId != id(73) || inventoryPage.Records[1].RecordId != id(76) {
		return errors.New("nullable generated order and exact total")
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

	afterTitle, horizonTitle := "a", "😀"
	intervalFirst, err := client.DocumentsInTitleWindow(ctx, generated.DocumentsInTitleWindowParams{
		SiteId: id(35), AfterTitle: afterTitle, HorizonTitle: horizonTitle,
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	intervalFirstPage, ok := intervalFirst.Value.(generated.DocumentsInTitleWindowFound)
	if !ok || documentTitles(intervalFirstPage.Documents) != "aa,b" || intervalFirst.NextCursor == "" {
		return errors.New("binary interval first page")
	}
	intervalSecond, err := client.DocumentsInTitleWindow(ctx, generated.DocumentsInTitleWindowParams{
		SiteId: id(35), AfterTitle: afterTitle, HorizonTitle: horizonTitle, After: &intervalFirst.NextCursor,
	}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	intervalSecondPage, ok := intervalSecond.Value.(generated.DocumentsInTitleWindowFound)
	if !ok || documentTitles(intervalSecondPage.Documents) != "é" || intervalSecond.NextCursor != "" {
		return errors.New("binary interval continuation")
	}

	return json.NewEncoder(os.Stdout).Encode(observation("go"))
}

func documentTitles(documents []struct {
	DocumentId string
	Title      string
}) string {
	titles := make([]string, 0, len(documents))
	for _, document := range documents {
		titles = append(titles, document.Title)
	}
	return strings.Join(titles, ",")
}

func observation(language string) map[string]any {
	return map[string]any{
		"schema": "riffdb.adapter-operational-observation/v1", "language": language,
		"catalog_preflight": true, "optional_filters": true, "stable_cursor": true,
		"null_predicate": true, "binary_prefix": true, "exact_aggregates": true,
		"exact_text_family": true, "exact_predicate_family": true, "nullable_exact_order": true, "exact_total": true, "numeric_offset": true,
		"operator_expansions": true,
		"binary_interval":     map[string]any{"first_page": []string{"aa", "b"}, "second_page": []string{"é"}, "first_cursor": true, "second_cursor": false},
		"adapters":            []string{"mlflow", "openfga", "better-auth", "woodpecker"},
		"regression_adapters": []string{"payload"},
	}
}

func id(suffix int) string {
	return fmt.Sprintf("018f0f8b-7c6d-7e31-8a4f-00000000%04x", suffix)
}
