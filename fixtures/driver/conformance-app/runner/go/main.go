package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	riffdb "riffdb.dev/application"
	generated "riffdb.dev/driver-conformance/generated/go"
)

type observation struct {
	Schema                string   `json:"schema"`
	Language              string   `json:"language"`
	Created               string   `json:"created"`
	Replayed              bool     `json:"replayed"`
	Query                 string   `json:"query"`
	ReadAfterCommit       bool     `json:"read_after_commit"`
	ReuseError            string   `json:"reuse_error"`
	SecretQuery           string   `json:"secret_query"`
	SecretRedacted        bool     `json:"secret_redacted"`
	ExactQuery            string   `json:"exact_query"`
	ExactTotal            uint64   `json:"exact_total"`
	Consume               string   `json:"consume"`
	ConsumeReplayed       bool     `json:"consume_replayed"`
	ConsumeMissing        bool     `json:"consume_missing"`
	PreimageRedacted      bool     `json:"delete_preimage_redacted"`
	InvariantError        string   `json:"invariant_error"`
	InvariantRecovery     string   `json:"invariant_recovery"`
	InvariantNonTerminal  bool     `json:"invariant_non_terminal"`
	RevisionUpdated       string   `json:"revision_updated"`
	RevisionStale         string   `json:"revision_stale"`
	PkSetAllCount         int      `json:"pk_set_all_count"`
	PkSetAllOrdered       bool     `json:"pk_set_all_ordered"`
	CollationAllTitles    []string `json:"collation_all_titles"`
	WideCursorPagesMinTwo bool     `json:"wide_cursor_pages_min_two"`
	WideCursorTotal       int      `json:"wide_cursor_total"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Go remote driver conformance failed:", err)
		os.Exit(1)
	}
}

func loadIdentity(path string) (riffdb.Identity, error) {
	identityBytes, err := os.ReadFile(path)
	if err != nil {
		return riffdb.Identity{}, errors.New("driver identity is unavailable")
	}
	var doc struct {
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
	if json.Unmarshal(identityBytes, &doc) != nil {
		return riffdb.Identity{}, errors.New("driver identity is invalid")
	}
	return riffdb.Identity{
		ApplicationManifestHash: doc.ApplicationManifestHash,
		OperationCatalogHash:    doc.OperationCatalogHash,
		ContractLineage:         doc.ContractLineage,
		ContractVersion:         doc.ContractVersion,
		ContractBundleHash:      doc.ContractBundleHash,
		Database:                doc.Database,
		Role:                    doc.Role,
		RoleDefinitionHash:      doc.RoleDefinitionHash,
		RemoteIdentityHash:      doc.RemoteIdentityHash,
	}, nil
}

func run() error {
	identity, err := loadIdentity(os.Getenv("RIFFDB_CONFORMANCE_IDENTITY"))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	organizationID := "018f0f8b-7c6d-7e31-8a4f-000000000200"
	itemID := "018f0f8b-7c6d-7e31-8a4f-000000000201"

	if os.Getenv("RIFFDB_CONFORMANCE_EXPECT_UNCERTAIN") == "1" {
		uncertainIdentity, err := loadIdentity(os.Getenv("RIFFDB_CONFORMANCE_UNCERTAIN_IDENTITY"))
		if err != nil {
			return err
		}
		uncertainSession, err := riffdb.Connect(ctx, os.Getenv("RIFFDB_CONFORMANCE_UNCERTAIN_SOCKET"), uncertainIdentity)
		if err != nil {
			return err
		}
		defer uncertainSession.Close()
		uncertainClient, err := generated.NewClient(uncertainSession, 1)
		if err != nil {
			return err
		}
		if _, err := uncertainClient.ItemPage(ctx, generated.ItemPageParams{OrganizationId: organizationID, ItemId: itemID}); err != nil {
			return err
		}
		if err := os.WriteFile(os.Getenv("RIFFDB_CONFORMANCE_ARM_FILE"), []byte("arm\n"), 0o600); err != nil {
			return err
		}
		uncertainInput := generated.CreateItemInput{
			Title:          "Uncertain Go item",
			TokenDigest:    "go-uncertain-secret",
			ItemId:         "018f0f8b-7c6d-7e31-8a4f-000000000299",
			IdempotencyKey: "driver-conformance-go-uncertain-v1",
			OrganizationId: organizationID,
		}
		_, err = uncertainClient.CreateItem(ctx, uncertainInput)
		var appErr *riffdb.ApplicationError
		if !errors.As(err, &appErr) || appErr.Details.Code != "RDB-UNCERTAIN-0101" || !appErr.Details.OutcomeUncertain {
			return fmt.Errorf("expected Go RDB-UNCERTAIN-0101 OutcomeUncertain, got: %w", err)
		}
		recoverySession, err := riffdb.Connect(ctx, os.Getenv("RIFFDB_CONFORMANCE_SOCKET"), identity)
		if err != nil {
			return err
		}
		defer recoverySession.Close()
		recoveryClient, err := generated.NewClient(recoverySession, 3)
		if err != nil {
			return err
		}
		recovered, err := recoveryClient.CreateItem(ctx, uncertainInput)
		if err != nil || !recovered.Replayed {
			return fmt.Errorf("Go uncertain recovery did not replay committed outcome: %w", err)
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{
			"schema":    "riffdb.driver-conformance-fault/v1",
			"fault":     "outcome_unknown",
			"uncertain": true,
			"replayed":  true,
		})
	}

	session, err := riffdb.Connect(ctx, os.Getenv("RIFFDB_CONFORMANCE_SOCKET"), identity)
	if err != nil {
		return err
	}
	defer session.Close()
	client, err := generated.NewClient(session, 3)
	if err != nil {
		return err
	}

	if syncDir := os.Getenv("RIFFDB_CONFORMANCE_RESTART_SYNC"); syncDir != "" {
		before, err := client.ItemPage(ctx, generated.ItemPageParams{OrganizationId: organizationID, ItemId: itemID})
		if err != nil {
			return err
		}
		if _, ok := before.Value.(generated.ItemPageFound); !ok {
			return errors.New("Go pre-restart ItemPage did not find item")
		}
		if err := os.WriteFile(filepath.Join(syncDir, "go.ready"), []byte("ready\n"), 0o600); err != nil {
			return err
		}
		resumeFifo, err := os.Open(filepath.Join(syncDir, "go.resume"))
		if err != nil {
			return err
		}
		_, _ = bufio.NewReader(resumeFifo).ReadString('\n')
		_ = resumeFifo.Close()
		after, err := client.ItemPage(ctx, generated.ItemPageParams{OrganizationId: organizationID, ItemId: itemID})
		if err != nil {
			return fmt.Errorf("Go in-session reconnect after driverd restart failed: %w", err)
		}
		if _, ok := after.Value.(generated.ItemPageFound); !ok {
			return errors.New("Go post-restart ItemPage did not find item")
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{
			"schema":      "riffdb.driver-conformance-fault/v1",
			"fault":       "driver_restart_reconnect",
			"reconnected": true,
		})
	}

	if os.Getenv("RIFFDB_CONFORMANCE_EXPECT_AUTH_DIAG") == "1" {
		_, err = client.ItemsByIds(ctx, generated.ItemsByIdsParams{
			OrganizationId: organizationID,
			ItemIds:        []string{itemID, "018f0f8b-7c6d-7e31-8a4f-000000000251"},
		})
		var applicationError *riffdb.ApplicationError
		if !errors.As(err, &applicationError) || applicationError.Details.Code != "RDB-AUTH-0214" || applicationError.Details.AuthorizationDiagnostic != "scan_budget_exceeded" {
			return fmt.Errorf("Go auth diagnostic error lost semantic details: %w", err)
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]string{
			"schema":     "riffdb.driver-conformance-fault/v1",
			"fault":      "authorization_diagnostic",
			"code":       applicationError.Details.Code,
			"diagnostic": applicationError.Details.AuthorizationDiagnostic,
		})
	}

	if os.Getenv("RIFFDB_CONFORMANCE_EXPECT_REVOKED") == "1" {
		_, err = client.ItemSecret(ctx, generated.ItemSecretParams{OrganizationId: organizationID, ItemId: itemID}, riffdb.Options{})
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

	extraItems := []struct {
		id    string
		title string
	}{
		{"018f0f8b-7c6d-7e31-8a4f-000000000251", "zeta-tag"},
		{"018f0f8b-7c6d-7e31-8a4f-000000000252", "Alpha-tag"},
		{"018f0f8b-7c6d-7e31-8a4f-000000000253", "alpha-tag"},
		{"018f0f8b-7c6d-7e31-8a4f-000000000254", "Zeta-tag"},
	}
	commitSeq := first.CommitSequence
	for idx, extra := range extraItems {
		res, createErr := client.CreateItem(ctx, generated.CreateItemInput{
			Title:          extra.title,
			TokenDigest:    fmt.Sprintf("go-extra-digest-%d", idx),
			ItemId:         extra.id,
			IdempotencyKey: fmt.Sprintf("driver-conformance-go-extra-%d", idx),
			OrganizationId: organizationID,
		})
		if createErr != nil || res.CommitSequence == nil {
			return errors.New("Go extra item creation failed")
		}
		commitSeq = res.CommitSequence
	}

	tokenID := "018f0f8b-7c6d-7e31-8a4f-000000000221"
	tokenValue := "go-one-time-secret-must-not-log"
	issued, err := client.IssueToken(ctx, generated.IssueTokenInput{Value: tokenValue, TokenId: tokenID, ExpiresAt: riffdb.Instant{Seconds: 4_102_444_800}, Identifier: "go-loopback", RequestId: "018f0f8b-7c6d-7e31-8a4f-000000000222", OrganizationId: organizationID})
	if err != nil {
		return err
	}
	if _, ok := issued.Outcome.(generated.IssueTokenTokenIssued); !ok {
		return errors.New("Go token issue did not create the token")
	}
	consumeInput := generated.ConsumeTokenInput{TokenId: tokenID, RequestId: "018f0f8b-7c6d-7e31-8a4f-000000000223", OrganizationId: organizationID}
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
	missing, err := client.ConsumeToken(ctx, generated.ConsumeTokenInput{TokenId: tokenID, RequestId: "018f0f8b-7c6d-7e31-8a4f-000000000224", OrganizationId: organizationID})
	if err != nil {
		return err
	}
	if _, ok := missing.Outcome.(generated.ConsumeTokenTokenMissing); !ok {
		return errors.New("Go second consumer did not observe the atomic delete")
	}
	page, err := client.ItemPage(ctx, generated.ItemPageParams{OrganizationId: organizationID, ItemId: itemID}, riffdb.Options{ReadAfterCommit: commitSeq})
	if err != nil {
		return err
	}
	found, ok := page.Value.(generated.ItemPageFound)
	if !ok || found.Item.ItemId != itemID || found.Item.Title != "Shared remote Go" {
		return errors.New("Go read-after-commit returned the wrong item")
	}
	secret, err := client.ItemSecret(ctx, generated.ItemSecretParams{OrganizationId: organizationID, ItemId: itemID}, riffdb.Options{ReadAfterCommit: commitSeq})
	if err != nil {
		return err
	}
	secretFound, ok := secret.Value.(generated.ItemSecretFound)
	if !ok || secretFound.Secret.TokenDigest != tokenDigest || strings.Contains(fmt.Sprintf("%#v", secretFound), tokenDigest) {
		return errors.New("Go secret query value or redacted diagnostic was incorrect")
	}
	var exact generated.QueryResult[generated.SearchItemsResult]
	for attempt := 0; attempt < 200; attempt++ {
		exact, err = client.SearchItems(ctx, generated.SearchItemsParams{OrganizationId: organizationID, Needle: "remote Go", ItemId: &itemID}, riffdb.Options{ReadAfterCommit: commitSeq})
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

	// Post-mutation invariant (`RDB-INPUT-0101`, `correct_request`) + non-terminal idempotency key.
	accountID := "018f0f8b-7c6d-7e31-8a4f-000000000261"
	opened, err := client.OpenBudgetAccount(ctx, generated.OpenBudgetAccountInput{
		RequestId:      "go-open-budget-v1",
		OrganizationId: organizationID,
		AccountId:      accountID,
		CreditLimit:    500,
		Note:           "initial",
	})
	if err != nil {
		return err
	}
	if _, ok := opened.Outcome.(generated.OpenBudgetAccountAccountOpened); !ok {
		return errors.New("Go OpenBudgetAccount failed")
	}
	_, err = client.RecordSpend(ctx, generated.RecordSpendInput{
		RequestId:      "go-over-budget-v1",
		OrganizationId: organizationID,
		AccountId:      accountID,
		Amount:         600,
	})
	var invAppErr *riffdb.ApplicationError
	if !errors.As(err, &invAppErr) || invAppErr.Details.Code != "RDB-INPUT-0101" || invAppErr.Details.RecoveryAction != "correct_request" {
		return fmt.Errorf("Go invariant error lost semantic details: %w", err)
	}
	validSpend, err := client.RecordSpend(ctx, generated.RecordSpendInput{
		RequestId:      "go-over-budget-v1",
		OrganizationId: organizationID,
		AccountId:      accountID,
		Amount:         200,
	})
	if err != nil {
		return err
	}
	spendOutcome, ok := validSpend.Outcome.(generated.RecordSpendSpendRecorded)
	invariantNonTerminal := !validSpend.Replayed && ok && spendOutcome.Spent == 200
	if !invariantNonTerminal {
		return errors.New("Go CommitCheckRejected persisted terminal idempotency state")
	}

	// Optimistic workflow revision update + stale-revision conflict.
	revOk, err := client.ReviseBudgetNote(ctx, generated.ReviseBudgetNoteInput{
		RequestId:        "go-revise-1",
		OrganizationId:   organizationID,
		AccountId:        accountID,
		ExpectedRevision: 2,
		Note:             "updated-note",
	})
	if err != nil {
		return err
	}
	if _, ok := revOk.Outcome.(generated.ReviseBudgetNoteNoteRevised); !ok {
		return errors.New("Go ReviseBudgetNote at revision 2 failed")
	}
	revStale, err := client.ReviseBudgetNote(ctx, generated.ReviseBudgetNoteInput{
		RequestId:        "go-revise-stale",
		OrganizationId:   organizationID,
		AccountId:        accountID,
		ExpectedRevision: 2,
		Note:             "stale-note",
	})
	if err != nil {
		return err
	}
	if _, ok := revStale.Outcome.(generated.ReviseBudgetNoteStaleRevision); !ok {
		return errors.New("Go stale expected_revision did not return StaleRevision")
	}

	// `<query>All` PK-set chunking & ascending UUID order.
	allByIds, err := client.ItemsByIdsAll(ctx, generated.ItemsByIdsParams{
		OrganizationId: organizationID,
		ItemIds: []string{
			"018f0f8b-7c6d-7e31-8a4f-000000000254",
			itemID,
			"018f0f8b-7c6d-7e31-8a4f-000000000252",
			"018f0f8b-7c6d-7e31-8a4f-000000000254",
			"018f0f8b-7c6d-7e31-8a4f-000000000251",
			"018f0f8b-7c6d-7e31-8a4f-000000000253",
		},
	}, 16, riffdb.Options{ReadAfterCommit: commitSeq})
	if err != nil {
		return err
	}
	pkSetOrdered := true
	for i := 1; i < len(allByIds.Value.Items); i++ {
		if !(allByIds.Value.Items[i-1].ItemId < allByIds.Value.Items[i].ItemId) {
			pkSetOrdered = false
		}
	}
	if len(allByIds.Value.Items) != 5 || !pkSetOrdered {
		return errors.New("Go ItemsByIdsAll count or ordering failed")
	}

	// `<query>All` `binary_utf8_v1` collation ordering.
	allByTitles, err := client.ItemsByTitlesAll(ctx, generated.ItemsByTitlesParams{
		OrganizationId: organizationID,
		Titles:         []string{"zeta-tag", "Alpha-tag", "alpha-tag", "Zeta-tag", "Alpha-tag"},
	}, 16, riffdb.Options{ReadAfterCommit: commitSeq})
	if err != nil {
		return err
	}
	collationTitles := make([]string, 0, len(allByTitles.Value.Items))
	for _, row := range allByTitles.Value.Items {
		collationTitles = append(collationTitles, row.Title)
	}
	if len(collationTitles) != 4 || collationTitles[0] != "Alpha-tag" || collationTitles[1] != "Zeta-tag" || collationTitles[2] != "alpha-tag" || collationTitles[3] != "zeta-tag" {
		return fmt.Errorf("Go ItemsByTitlesAll collation order diverged: %v", collationTitles)
	}

	// Wide-row + sibling-binding cursor continuation across >= 2 pages.
	wideText := strings.Repeat("G", 380_000)
	wideBlob := bytes.Repeat([]byte{0x47}, 520_000)
	var wideCommit *uint64
	for seq := int64(1); seq <= 6; seq++ {
		written, writeErr := client.WriteWideNote(ctx, generated.WriteWideNoteInput{
			RequestId:      fmt.Sprintf("go-wide-note-%d", seq),
			OrganizationId: organizationID,
			NoteSeq:        seq,
			NoteText:       wideText,
			NoteBlob:       wideBlob,
		})
		if writeErr != nil || written.CommitSequence == nil {
			return fmt.Errorf("Go WriteWideNote failed: %w", writeErr)
		}
		wideCommit = written.CommitSequence
	}
	limit := uint32(10)
	widePages, err := client.WideNotesPagePages(ctx, generated.WideNotesPageParams{
		OrganizationId: organizationID,
		AnchorItemId:   itemID,
		Limit:          &limit,
	}, 16, riffdb.Options{ReadAfterCommit: wideCommit})
	if err != nil {
		return err
	}
	widePagesMinTwo := len(widePages) >= 2
	observedSeqs := make([]int64, 0, 6)
	for _, wp := range widePages {
		wf, ok := wp.Value.(generated.WideNotesPageFound)
		if !ok || wf.Anchor.ItemId != itemID {
			return errors.New("Go WideNotesPagePages sibling anchor mismatched")
		}
		for _, note := range wf.Notes {
			if len(note.NoteText) != 380_000 || len(note.NoteBlob) != 520_000 {
				return errors.New("Go WideNotesPagePages payload truncated")
			}
			observedSeqs = append(observedSeqs, note.NoteSeq)
		}
	}
	if !widePagesMinTwo || len(observedSeqs) != 6 {
		return fmt.Errorf("Go WideNotesPagePages pagination failed: pages=%d total=%d", len(widePages), len(observedSeqs))
	}
	for i, seq := range observedSeqs {
		if seq != int64(i+1) {
			return fmt.Errorf("Go WideNotesPagePages sequence mismatch: %v", observedSeqs)
		}
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
		PreimageRedacted:      true,
		InvariantError:        invAppErr.Details.Code,
		InvariantRecovery:     invAppErr.Details.RecoveryAction,
		InvariantNonTerminal:  invariantNonTerminal,
		RevisionUpdated:       "NoteRevised",
		RevisionStale:         "StaleRevision",
		PkSetAllCount:         len(allByIds.Value.Items),
		PkSetAllOrdered:       pkSetOrdered,
		CollationAllTitles:    collationTitles,
		WideCursorPagesMinTwo: widePagesMinTwo,
		WideCursorTotal:       len(observedSeqs),
	})
}
