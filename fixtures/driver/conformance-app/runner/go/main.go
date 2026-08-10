package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	riffdb "riffdb.dev/application"
	generated "riffdb.dev/driver-conformance/generated/go"
)

type observation struct {
	Schema          string `json:"schema"`
	Language        string `json:"language"`
	Created         string `json:"created"`
	Replayed        bool   `json:"replayed"`
	Query           string `json:"query"`
	ReadAfterCommit bool   `json:"read_after_commit"`
	ReuseError      string `json:"reuse_error"`
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
		_, err = client.ItemPage(ctx, generated.ItemPageParams{ItemId: "018f0f8b-7c6d-7e31-8a4f-000000000102"}, riffdb.Options{})
		var applicationError *riffdb.ApplicationError
		if !errors.As(err, &applicationError) || applicationError.Details.Code != "RDB-AUTH-0215" {
			return fmt.Errorf("Go revocation error lost semantic details: %w", err)
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]string{
			"schema": "riffdb.driver-conformance-fault/v1",
			"fault": "revocation",
			"code": applicationError.Details.Code,
		})
	}
	itemID := "018f0f8b-7c6d-7e31-8a4f-000000000102"
	idempotency := "driver-conformance-go-create-v1"
	input := generated.CreateItemInput{Title: "Shared remote Go", ItemId: itemID, IdempotencyKey: idempotency}
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
	page, err := client.ItemPage(ctx, generated.ItemPageParams{ItemId: itemID}, riffdb.Options{ReadAfterCommit: first.CommitSequence})
	if err != nil {
		return err
	}
	found, ok := page.Value.(generated.ItemPageFound)
	if !ok || found.Item.ItemId != itemID || found.Item.Title != "Shared remote Go" {
		return errors.New("Go read-after-commit returned the wrong item")
	}
	_, err = client.CreateItem(ctx, generated.CreateItemInput{Title: "Changed input", ItemId: itemID, IdempotencyKey: idempotency})
	var applicationError *riffdb.ApplicationError
	if !errors.As(err, &applicationError) || applicationError.Details.Code != "RDB-COMMAND-0101" {
		return errors.New("Go reuse error lost semantic details")
	}
	return json.NewEncoder(os.Stdout).Encode(observation{
		Schema: "riffdb.driver-conformance-observation/v1", Language: "go", Created: "Created",
		Replayed: true, Query: "Found", ReadAfterCommit: true, ReuseError: applicationError.Details.Code,
	})
}
