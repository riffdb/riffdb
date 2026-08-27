package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	riffdb "riffdb.dev/application"
	generated "riffdb.dev/driver-conformance/generated/go"
)

type observation struct {
	Schema           string `json:"schema"`
	Language         string `json:"language"`
	Created          string `json:"created"`
	Replayed         bool   `json:"replayed"`
	Query            string `json:"query"`
	ReadAfterCommit  bool   `json:"read_after_commit"`
	ReuseError       string `json:"reuse_error"`
	SecretQuery      string `json:"secret_query"`
	SecretRedacted   bool   `json:"secret_redacted"`
	ExactQuery       string `json:"exact_query"`
	ExactTotal       uint64 `json:"exact_total"`
	Consume          string `json:"consume"`
	ConsumeReplayed  bool   `json:"consume_replayed"`
	ConsumeMissing   bool   `json:"consume_missing"`
	PreimageRedacted bool   `json:"delete_preimage_redacted"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Go remote driver conformance failed:", err)
		os.Exit(1)
	}
}

func run() error {
	identityPath := os.Getenv("RIFFDB_CONFORMANCE_IDENTITY")
	identityBytes, err := os.ReadFile(identityPath)
	if err != nil {
		return errors.New("driver identity is unavailable")
	}
	var identityDocument struct {
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
	if json.Unmarshal(identityBytes, &identityDocument) != nil {
		return errors.New("driver identity is invalid")
	}
	identity := riffdb.Identity{
		ApplicationManifestHash: identityDocument.ApplicationManifestHash,
		OperationCatalogHash:    identityDocument.OperationCatalogHash,
		ContractLineage:         identityDocument.ContractLineage,
		ContractVersion:         identityDocument.ContractVersion,
		ContractBundleHash:      identityDocument.ContractBundleHash,
		Database:                identityDocument.Database,
		Role:                    identityDocument.Role,
		RoleDefinitionHash:      identityDocument.RoleDefinitionHash,
		RemoteIdentityHash:      identityDocument.RemoteIdentityHash,
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
	if os.Getenv("RIFFDB_CONFORMANCE_EXPECT_REVOKED") == "1" {
		_, err = client.ItemSecret(ctx, generated.ItemSecretParams{OrganizationId: "018f0f8b-7c6d-7e31-8a4f-000000000100", ItemId: "018f0f8b-7c6d-7e31-8a4f-000000000102"}, riffdb.Options{})
		var applicationError *riffdb.ApplicationError
		if !errors.As(err, &applicationError) || applicationError.Details.Code != "RDB-AUTH-0215" {
			return fmt.Errorf("Go revocation error lost semantic details: %w", err)
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]string{
			"schema": "riffdb.driver-conformance-fault/v1",
			"fault":  "revocation",
			"code":   applicationError.Details.Code,
		})
	}
	itemID := "018f0f8b-7c6d-7e31-8a4f-000000000102"
	organizationID := "018f0f8b-7c6d-7e31-8a4f-000000000100"
	idempotency := "driver-conformance-go-create-v1"
	tokenDigest := "go-secret-digest-must-not-log"
	input := generated.CreateItemInput{Title: "Shared remote Go", TokenDigest: tokenDigest, ItemId: itemID, IdempotencyKey: idempotency, OrganizationId: organizationID}
	first, err := client.CreateItem(ctx, input)
	if err != nil || first.Replayed || first.CommitSequence == nil {
		return errors.New("first Go command did not create the item")
	}
	if _, ok := first.Outcome.(generated.CreateItemCreated); !ok {
		return errors.New("first Go outcome was not Created")
	}
	replay, err := client.CreateItem(ctx, input)
	if err != nil || !replay.Replayed {
		return errors.New("second Go command did not replay")
	}
	tokenID := "018f0f8b-7c6d-7e31-8a4f-000000000121"
	tokenValue := "go-one-time-secret-must-not-log"
	issued, err := client.IssueToken(ctx, generated.IssueTokenInput{Value: tokenValue, TokenId: tokenID, ExpiresAt: riffdb.Instant{Seconds: 4_102_444_800}, Identifier: "go-loopback", RequestId: "018f0f8b-7c6d-7e31-8a4f-000000000122", OrganizationId: organizationID})
	if err != nil {
		return err
	}
	if _, ok := issued.Outcome.(generated.IssueTokenTokenIssued); !ok {
		return errors.New("Go token issue did not create the token")
	}
	consumeInput := generated.ConsumeTokenInput{TokenId: tokenID, RequestId: "018f0f8b-7c6d-7e31-8a4f-000000000123", OrganizationId: organizationID}
	consumed, err := client.ConsumeToken(ctx, consumeInput)
	if err != nil {
		return err
	}
	consumedOutcome, ok := consumed.Outcome.(generated.ConsumeTokenTokenConsumed)
	if !ok || consumedOutcome.Value != tokenValue || consumedOutcome.TokenId != tokenID || consumedOutcome.Identifier != "go-loopback" || strings.Contains(fmt.Sprintf("%#v", consumedOutcome), tokenValue) {
		return errors.New("Go deleted preimage or redacted diagnostic was incorrect")
	}
	consumedReplay, err := client.ConsumeToken(ctx, consumeInput)
	if err != nil || !consumedReplay.Replayed {
		return errors.New("Go token consume did not replay the persisted preimage")
	}
	replayedOutcome, ok := consumedReplay.Outcome.(generated.ConsumeTokenTokenConsumed)
	if !ok || replayedOutcome.Value != tokenValue || replayedOutcome.TokenId != tokenID {
		return errors.New("Go replay returned a different deleted preimage")
	}
	missing, err := client.ConsumeToken(ctx, generated.ConsumeTokenInput{TokenId: tokenID, RequestId: "018f0f8b-7c6d-7e31-8a4f-000000000124", OrganizationId: organizationID})
	if err != nil {
		return err
	}
	if _, ok := missing.Outcome.(generated.ConsumeTokenTokenMissing); !ok {
		return errors.New("Go second consumer did not observe the atomic delete")
	}
	page, err := client.ItemPage(ctx, generated.ItemPageParams{OrganizationId: organizationID, ItemId: itemID}, riffdb.Options{ReadAfterCommit: first.CommitSequence})
	if err != nil {
		return err
	}
	found, ok := page.Value.(generated.ItemPageFound)
	if !ok || found.Item.ItemId != itemID || found.Item.Title != "Shared remote Go" {
		return errors.New("Go read-after-commit returned the wrong item")
	}
	secret, err := client.ItemSecret(ctx, generated.ItemSecretParams{OrganizationId: organizationID, ItemId: itemID}, riffdb.Options{ReadAfterCommit: first.CommitSequence})
	if err != nil {
		return err
	}
	secretFound, ok := secret.Value.(generated.ItemSecretFound)
	if !ok || secretFound.Secret.TokenDigest != tokenDigest || strings.Contains(fmt.Sprintf("%#v", secretFound), tokenDigest) {
		return errors.New("Go secret query value or redacted diagnostic was incorrect")
	}
	var exact generated.QueryResult[generated.SearchItemsResult]
	for attempt := 0; attempt < 200; attempt++ {
		exact, err = client.SearchItems(ctx, generated.SearchItemsParams{OrganizationId: organizationID, Needle: "remote Go", ItemId: &itemID}, riffdb.Options{ReadAfterCommit: first.CommitSequence})
		if err == nil {
			break
		}
		var applicationError *riffdb.ApplicationError
		if !errors.As(err, &applicationError) || (applicationError.Details.Code != "RDB-QUERY-0102" && applicationError.Details.Code != "RDB-PROJECTION-0103") {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		return errors.New("Go exact provider did not become ready within the retry bound")
	}
	exactFound, ok := exact.Value.(generated.SearchItemsFound)
	if !ok || exactFound.Total.Value != 1 || len(exactFound.Items) != 1 || exactFound.Items[0].ItemId != itemID || exactFound.Items[0].OrganizationId != organizationID {
		return errors.New("Go exact page and whole-population total diverged")
	}
	_, err = client.CreateItem(ctx, generated.CreateItemInput{Title: "Changed input", TokenDigest: tokenDigest, ItemId: itemID, IdempotencyKey: idempotency, OrganizationId: organizationID})
	var applicationError *riffdb.ApplicationError
	if !errors.As(err, &applicationError) || applicationError.Details.Code != "RDB-COMMAND-0101" {
		return errors.New("Go reuse error lost semantic details")
	}
	return json.NewEncoder(os.Stdout).Encode(observation{
		Schema: "riffdb.driver-conformance-observation/v1", Language: "go", Created: "Created",
		Replayed: true, Query: "Found", ReadAfterCommit: true, ReuseError: applicationError.Details.Code,
		SecretQuery: "Found", SecretRedacted: true,
		ExactQuery: "Found", ExactTotal: 1,
		Consume: "TokenConsumed", ConsumeReplayed: true, ConsumeMissing: true,
		PreimageRedacted: true,
	})
}
