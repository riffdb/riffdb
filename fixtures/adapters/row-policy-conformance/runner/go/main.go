package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	riffdb "riffdb.dev/application"
	generated "riffdb.local/adapter-row-policy-conformance/generated/go"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Go adapter row-policy conformance failed:", err)
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
	mode := os.Getenv("RIFFDB_ROW_POLICY_MODE")
	documentCount, draftCount, experimentCount, metricCount, runVisible, err := expectations(mode)
	if err != nil {
		return err
	}
	principalID, documentSuffix, requestSuffix := id(1), 60, 160
	if mode == "outsider" {
		principalID, documentSuffix, requestSuffix = id(3), 61, 161
	}
	created, err := client.CreateDocument(ctx, generated.CreateDocumentInput{
		Body: "created through every generated language", State: generated.DocumentStateDraft,
		Title: "Document shared-" + mode, OwnerId: principalID, RequestId: id(requestSuffix),
		Visibility: generated.VisibilityPrivate, DocumentId: id(documentSuffix), OrganizationId: id(10),
	})
	if err != nil {
		return err
	}
	if _, ok := created.Outcome.(generated.CreateDocumentDocumentCreated); !ok {
		return errors.New("typed protected command outcome")
	}

	documents, err := client.ListDocuments(ctx, generated.ListDocumentsParams{OrganizationId: id(10)}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	documentPage, ok := documents.Value.(generated.ListDocumentsFound)
	if !ok || len(documentPage.Documents) != documentCount {
		return errors.New("policy-filtered document page")
	}
	drafts, err := client.ListDraftDocuments(ctx, generated.ListDraftDocumentsParams{OrganizationId: id(10)}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	draftPage, ok := drafts.Value.(generated.ListDraftDocumentsFound)
	if !ok || len(draftPage.Documents) != draftCount {
		return errors.New("policy-filtered draft page")
	}
	search, err := client.SearchDocuments(ctx, generated.SearchDocumentsParams{OrganizationId: id(10), TitlePrefix: "Document"}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	searchPage, ok := search.Value.(generated.SearchDocumentsFound)
	if !ok || len(searchPage.Documents) != documentCount {
		return errors.New("policy-filtered text search")
	}
	detail, err := client.GetDocument(ctx, generated.GetDocumentParams{OrganizationId: id(10), DocumentId: id(15)}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	_, detailFound := detail.Value.(generated.GetDocumentFound)
	if detailFound != (mode == "owner") {
		return errors.New("policy-filtered document detail")
	}
	summaryResult, err := client.DocumentSummary(ctx, generated.DocumentSummaryParams{OrganizationId: id(10)}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	summary, ok := summaryResult.Value.(generated.DocumentSummaryFound)
	if !ok {
		return errors.New("document summary outcome")
	}
	var summarized uint64
	for _, group := range summary.Summary {
		summarized += group.DocumentCount
	}
	if summarized != uint64(documentCount) {
		return errors.New("policy-before-aggregate document summary")
	}
	experiments, err := client.ListExperiments(ctx, generated.ListExperimentsParams{OrganizationId: id(10)}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	experimentPage, ok := experiments.Value.(generated.ListExperimentsFound)
	if !ok || len(experimentPage.Experiments) != experimentCount {
		return errors.New("policy-filtered experiment page")
	}
	dashboard, err := client.MetricDashboard(ctx, generated.MetricDashboardParams{OrganizationId: id(10)}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	metricPage, ok := dashboard.Value.(generated.MetricDashboardFound)
	if !ok || len(metricPage.Summary) != 1 || metricPage.Summary[0].SampleCount != uint64(metricCount) {
		return errors.New("policy-before-aggregate dashboard")
	}
	run, err := client.RunPage(ctx, generated.RunPageParams{OrganizationId: id(10), ExperimentId: id(23), RunId: id(31)}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	switch page := run.Value.(type) {
	case generated.RunPageFound:
		if !runVisible || len(page.Metrics) != 1 || len(page.Artifacts) != 1 {
			return errors.New("nested policy hydration")
		}
	case generated.RunPageNotFound:
		if runVisible {
			return errors.New("authorized group run was absent")
		}
	default:
		return errors.New("unknown run-page outcome")
	}
	_, err = client.AttemptDocumentTransfer(ctx, generated.AttemptDocumentTransferInput{
		RequestId: id(requestSuffix + 10), DocumentId: id(documentSuffix),
		NewOwnerId: id(2), OrganizationId: id(10),
	})
	var applicationError *riffdb.ApplicationError
	if !errors.As(err, &applicationError) || applicationError.Details.Code != "RDB-AUTH-0214" {
		return fmt.Errorf("successor-row authorization error lost semantic details: %w", err)
	}
	lifecycleChecked := mode == "owner"
	if lifecycleChecked {
		finished, err := client.FinishRun(ctx, generated.FinishRunInput{
			RunId: id(33), RequestId: id(180), ExperimentId: id(21), OrganizationId: id(10), ExpectedRevision: 1,
		})
		if err != nil {
			return err
		}
		if _, ok := finished.Outcome.(generated.FinishRunRunFinished); !ok {
			return errors.New("revision-checked MLflow transition")
		}
		stale, err := client.FinishRun(ctx, generated.FinishRunInput{
			RunId: id(33), RequestId: id(181), ExperimentId: id(21), OrganizationId: id(10), ExpectedRevision: 1,
		})
		if err != nil {
			return err
		}
		if _, ok := stale.Outcome.(generated.FinishRunFinishStale); !ok {
			return errors.New("stale MLflow transition")
		}
	}

	return json.NewEncoder(os.Stdout).Encode(observation("go", mode, documentCount, draftCount, experimentCount, metricCount, runVisible, lifecycleChecked))
}

func expectations(mode string) (int, int, int, int, bool, error) {
	switch mode {
	case "owner":
		return 6, 4, 3, 3, true, nil
	case "outsider":
		return 3, 2, 1, 1, false, nil
	default:
		return 0, 0, 0, 0, false, errors.New("unknown row-policy mode")
	}
}

func observation(language, mode string, documents, drafts, experiments, metrics int, runVisible, lifecycleChecked bool) map[string]any {
	return map[string]any{
		"schema": "riffdb.adapter-row-policy-observation/v1", "language": language,
		"mode": mode, "documents": documents, "drafts": drafts, "experiments": experiments,
		"metrics": metrics, "group_run_visible": runVisible,
		"policy_before_aggregate": true, "detail_and_search": true, "nested_policy": true,
		"protected_command": true, "successor_escape_denied": true,
		"lifecycle_checked": lifecycleChecked,
	}
}

func id(suffix int) string {
	return fmt.Sprintf("018f0f8b-7c6d-7e31-8a4f-0000000000%02x", suffix)
}
