package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	riffdb "riffdb.dev/application"
	generated "riffdb.dev/apps/{{APPLICATION_NAME}}/generated/go"
)

const itemID = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b11"

func main() {
	if len(os.Args) != 11 {
		panic("usage: app SOCKET MANIFEST_HASH CATALOG_HASH DATABASE ROLE ROLE_HASH REMOTE_HASH LINEAGE VERSION BUNDLE_HASH")
	}
	version, err := parseVersion(os.Args[9])
	if err != nil {
		panic(err)
	}
	session, err := riffdb.Connect(context.Background(), os.Args[1], riffdb.Identity{
		ApplicationManifestHash: os.Args[2], OperationCatalogHash: os.Args[3],
		Database: os.Args[4], Role: os.Args[5], RoleDefinitionHash: os.Args[6],
		RemoteIdentityHash: os.Args[7], ContractLineage: os.Args[8],
		ContractVersion: version, ContractBundleHash: os.Args[10],
	})
	if err != nil {
		panic(err)
	}
	defer session.Close()
	client, err := generated.NewClient(session, 3)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	created, err := client.CreateItem(ctx, generated.CreateItemInput{
		IdempotencyKey: "{{APPLICATION_NAME}}-go-item", ItemId: itemID,
		Title: "Go generated application",
	})
	if err != nil {
		panic(err)
	}
	page, err := client.ItemPage(ctx, generated.ItemPageParams{ItemId: itemID}, riffdb.Options{ReadAfterCommit: created.CommitSequence})
	if err != nil {
		panic(err)
	}
	createdOutcome, ok := created.Outcome.(generated.CreateItemCreated)
	if !ok {
		panic("generated command returned an unexpected outcome")
	}
	found, ok := page.Value.(generated.ItemPageFound)
	if !ok {
		panic("generated page did not find the item")
	}
	if created.CommitSequence == nil {
		panic("generated command omitted its commit sequence")
	}
	observation := map[string]any{
		"application_head_at_least_read_after_commit": page.ApplicationHead >= *created.CommitSequence,
		"operation":         "CreateItem+ItemPage",
		"outcome":           createdOutcome.Outcome,
		"read_after_commit": true,
		"schema":            "riffdb.application-parity/v1",
		"title":             found.Item.Title,
	}
	if err := json.NewEncoder(os.Stdout).Encode(observation); err != nil {
		panic(err)
	}
}

func parseVersion(value string) (uint64, error) {
	var version uint64
	_, err := fmt.Sscan(value, &version)
	if err != nil || version == 0 {
		return 0, fmt.Errorf("invalid contract version")
	}
	return version, nil
}
