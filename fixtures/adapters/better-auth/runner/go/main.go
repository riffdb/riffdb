package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	riffdb "riffdb.dev/application"
	generated "riffdb.local/better-auth-acceptance/generated/go"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Go Better Auth cascade acceptance failed:", err)
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
		Database:                document.Database, Role: document.Role,
		RoleDefinitionHash: document.RoleDefinitionHash,
		RemoteIdentityHash: document.RemoteIdentityHash,
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

	organizationID, userID := id(201), os.Getenv("RIFFDB_BETTER_AUTH_USER_ID")
	signup := generated.CreateUserAccountSessionsInput{RequestId: id(210), Signups: []generated.SignupGraphInput{{
		Email: "go@example.test", UserId: userID, Provider: "password",
		AccountId: id(203), ExpiresAt: future(), SessionId: id(204),
		TokenDigest: "sha256:go-session", OrganizationId: organizationID,
		ProviderAccountId: "go@example.test",
	}}}
	created, err := client.CreateUserAccountSessions(ctx, signup)
	if err != nil {
		return err
	}
	if _, ok := created.Outcome.(generated.CreateUserAccountSessionsUserAccountSessionsCreated); !ok {
		return errors.New("typed signup")
	}
	sessionResult, err := client.GetSession(ctx, generated.GetSessionParams{OrganizationId: organizationID, UserId: userID, SessionId: id(204)}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	foundSession, ok := sessionResult.Value.(generated.GetSessionFound)
	if !ok || foundSession.Session.TokenDigest != "sha256:go-session" {
		return errors.New("named secret read")
	}
	if err := expectUser(ctx, client, organizationID, userID, true); err != nil {
		return err
	}

	tokenID := id(220)
	issued, err := client.IssueVerificationToken(ctx, generated.IssueVerificationTokenInput{
		UserId: userID, ExpiresAt: future(), RequestId: id(221), TokenDigest: "sha256:go-verification",
		OrganizationId: organizationID, VerificationTokenId: tokenID,
	})
	if err != nil {
		return err
	}
	if _, ok := issued.Outcome.(generated.IssueVerificationTokenVerificationTokenIssued); !ok {
		return errors.New("token issue")
	}
	consume := generated.ConsumeVerificationTokenInput{UserId: userID, RequestId: id(222), OrganizationId: organizationID, VerificationTokenId: tokenID}
	consumed, err := client.ConsumeVerificationToken(ctx, consume)
	if err != nil {
		return err
	}
	replayedConsume, err := client.ConsumeVerificationToken(ctx, consume)
	if err != nil {
		return err
	}
	if _, ok := consumed.Outcome.(generated.ConsumeVerificationTokenVerificationTokenConsumed); !ok || !replayedConsume.Replayed {
		return errors.New("atomic consume replay")
	}

	deletion := generated.DeleteUsersInput{UserIds: []string{userID}, RequestId: id(230), OrganizationId: organizationID}
	deleted, err := client.DeleteUsers(ctx, deletion)
	if err != nil {
		return err
	}
	replayedDelete, err := client.DeleteUsers(ctx, deletion)
	if err != nil {
		return err
	}
	if _, ok := deleted.Outcome.(generated.DeleteUsersUserAccountsDeleted); !ok || !replayedDelete.Replayed {
		return errors.New("cascade replay")
	}
	missingSession, err := client.GetSession(ctx, generated.GetSessionParams{OrganizationId: organizationID, UserId: userID, SessionId: id(204)}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	if _, ok := missingSession.Value.(generated.GetSessionMissing); !ok {
		return errors.New("session cleanup")
	}

	signup.RequestId = id(240)
	recreated, err := client.CreateUserAccountSessions(ctx, signup)
	if err != nil {
		return err
	}
	if _, ok := recreated.Outcome.(generated.CreateUserAccountSessionsUserAccountSessionsCreated); !ok {
		return errors.New("recreate")
	}
	for ordinal := 0; ordinal < 9; ordinal++ {
		issued, err := client.IssueVerificationToken(ctx, generated.IssueVerificationTokenInput{
			UserId: userID, ExpiresAt: future(), RequestId: id(250 + ordinal),
			TokenDigest: fmt.Sprintf("sha256:go-overflow-%d", ordinal), OrganizationId: organizationID,
			VerificationTokenId: id(270 + ordinal),
		})
		if err != nil {
			return err
		}
		if _, ok := issued.Outcome.(generated.IssueVerificationTokenVerificationTokenIssued); !ok {
			return errors.New("overflow setup")
		}
	}
	overflow, err := client.DeleteUsers(ctx, generated.DeleteUsersInput{UserIds: []string{userID}, RequestId: id(290), OrganizationId: organizationID})
	if err != nil {
		return err
	}
	if _, ok := overflow.Outcome.(generated.DeleteUsersCascadeLimitExceeded); !ok {
		return errors.New("typed overflow")
	}
	if err := expectUser(ctx, client, organizationID, userID, true); err != nil {
		return err
	}

	return json.NewEncoder(os.Stdout).Encode(observation("go"))
}

func expectUser(ctx context.Context, client *generated.Client, organizationID, userID string, found bool) error {
	result, err := client.GetUser(ctx, generated.GetUserParams{OrganizationId: organizationID, UserId: userID}, generated.QueryOptions{})
	if err != nil {
		return err
	}
	_, exists := result.Value.(generated.GetUserFound)
	if exists != found {
		return errors.New("named exact user read")
	}
	return nil
}

func future() riffdb.Instant { return riffdb.Instant{Seconds: 2_000_000_000, Nanos: 0} }
func id(value int) string    { return fmt.Sprintf("018f0f8b-7c6d-7e31-8a4f-%012x", value) }
func observation(language string) map[string]any {
	return map[string]any{
		"schema": "riffdb.adapter-cascade-observation/v1", "language": language,
		"signup": true, "named_exact_read": true, "named_secret_read": true,
		"session_account_cleanup": true, "bounded_full_user_delete": true,
		"atomic_token_consume": true, "idempotent_replay": true,
		"typed_overflow": true, "overflow_zero_mutation": true,
	}
}
