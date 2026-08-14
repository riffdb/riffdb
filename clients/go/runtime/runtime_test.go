package application

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testHash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

var testIdentity = Identity{
	ApplicationManifestHash: testHash,
	OperationCatalogHash:    testHash,
	ContractLineage:         "TicketDesk",
	ContractVersion:         5,
	ContractBundleHash:      testHash,
	Database:                "default",
	Role:                    "TicketDeskApplication",
	RoleDefinitionHash:      testHash,
	RemoteIdentityHash:      testHash,
}

func TestRetainedSessionPreservesExactFrontier(t *testing.T) {
	fixture := newFixture(t, func(request map[string]any) map[string]any {
		if request["type"] != "invoke" {
			return nil
		}
		return map[string]any{
			"type": "result", "request_id": request["request_id"],
			"value":            map[string]any{"type": "u64", "value": "18446744073709551615"},
			"application_head": uint64(^uint64(0)), "cursor": nil, "replayed": false,
		}
	})
	session, err := Connect(context.Background(), fixture.path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	frontier := uint64(^uint64(0))
	result, err := session.Invoke(context.Background(), Operation{Name: "ticketdesk_get_ticket", InputSchemaHash: testHash}, map[string]Value{"ticket_id": UUID("018f0f79-7b5e-7c03-9b12-b16f57a4c998")}, Options{ReadAfterCommit: &frontier})
	if err != nil {
		t.Fatal(err)
	}
	if result.ApplicationHead == nil || *result.ApplicationHead != frontier {
		t.Fatalf("frontier lost: %#v", result.ApplicationHead)
	}
	if fixture.connections.Load() != 1 || fixture.invocations.Load() != 1 {
		t.Fatalf("session was not retained")
	}
}

func TestStructuredErrorPreservesUncertainty(t *testing.T) {
	fixture := newFixture(t, func(request map[string]any) map[string]any {
		if request["type"] != "invoke" {
			return nil
		}
		return map[string]any{
			"type": "error", "request_id": request["request_id"],
			"code": "RDB-UNCERTAIN-0101", "category": "uncertainty",
			"operation": "ticketdesk_create_comment", "symbol_path": []string{"CreateComment"},
			"contract_lineage": "TicketDesk", "contract_version": uint64(5),
			"trace_id": nil, "incident_id": nil, "message": "command outcome is not yet known",
			"retryability": "not_retryable", "recovery_action": "resolve_with_same_idempotency_key",
			"outcome_uncertain": true,
		}
	})
	session, err := Connect(context.Background(), fixture.path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	_, err = session.Invoke(context.Background(), Operation{Name: "ticketdesk_create_comment", InputSchemaHash: testHash}, map[string]Value{"body": String("hello")}, Options{})
	applicationError, ok := err.(*ApplicationError)
	if !ok || !applicationError.Details.OutcomeUncertain || applicationError.Details.RecoveryAction != "resolve_with_same_idempotency_key" {
		t.Fatalf("uncertainty was weakened: %#v", err)
	}
}

func TestBoundaryContainsNoRemoteTransport(t *testing.T) {
	source, err := os.ReadFile("runtime.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"google.golang.org/grpc", "credential_file", "authorization: bearer", "riffdb-kernel"} {
		if strings.Contains(strings.ToLower(string(source)), forbidden) {
			t.Fatalf("runtime contains forbidden surface %q", forbidden)
		}
	}
}

func TestRequestIdentityPrefixesAreUniqueAcrossConcurrentSessions(t *testing.T) {
	first := &Session{requestPrefix: newSessionRequestPrefix()}
	second := &Session{requestPrefix: newSessionRequestPrefix()}
	firstID := first.requestID("invoke")
	secondID := second.requestID("invoke")
	if firstID == secondID || !requestPattern.MatchString(firstID) || !requestPattern.MatchString(secondID) {
		t.Fatalf("session request identities are not distinct valid protocol values: %q %q", firstID, secondID)
	}
}

func TestSchemaBoundDecimalAcceptsMissingWirePrecision(t *testing.T) {
	decimal, err := DecimalValueWithSchema(Value{
		Type: "decimal",
		Value: Decimal{
			Coefficient: "ASw=",
			Scale:       0,
		},
	}, 39, 0)
	if err != nil {
		t.Fatal(err)
	}
	if decimal.Precision != 39 || decimal.Scale != 0 {
		t.Fatalf("schema identity was not restored: %#v", decimal)
	}
	wrong := uint32(38)
	if _, err = DecimalValueWithSchema(Value{
		Type: "decimal",
		Value: Decimal{
			Coefficient: "ASw=",
			Scale:       0,
			Precision:   &wrong,
		},
	}, 39, 0); err == nil {
		t.Fatal("conflicting wire precision was accepted")
	}
}

type fixture struct {
	path        string
	listener    net.Listener
	connections atomic.Uint32
	invocations atomic.Uint32
	invoked     chan struct{}
	cancelled   chan struct{}
}

func newFixture(t *testing.T, handler func(map[string]any) map[string]any) *fixture {
	t.Helper()
	root, err := os.MkdirTemp(filepath.Join(os.Getenv("HOME"), "tmp"), "riffdb-go-driver-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	listener, err := net.Listen("unix", filepath.Join(root, "driver.sock"))
	if err != nil {
		t.Fatal(err)
	}
	result := &fixture{path: filepath.Join(root, "driver.sock"), listener: listener, invoked: make(chan struct{}, 1), cancelled: make(chan struct{}, 1)}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		result.connections.Add(1)
		defer connection.Close()
		reader := bufio.NewReader(connection)
		for {
			body, readErr := readTestFrame(reader)
			if readErr != nil {
				return
			}
			decoder := json.NewDecoder(strings.NewReader(string(body)))
			decoder.UseNumber()
			var request map[string]any
			if decoder.Decode(&request) != nil {
				return
			}
			if request["type"] == "handshake" {
				writeTestFrame(connection, map[string]any{
					"type": "handshake", "request_id": request["request_id"], "protocol_version": ProtocolVersion,
					"driver_identity": "riffdb-driver-host/v1", "application_manifest_hash": testIdentity.ApplicationManifestHash,
					"operation_catalog_hash": testIdentity.OperationCatalogHash, "contract_lineage": testIdentity.ContractLineage,
					"contract_version": testIdentity.ContractVersion, "contract_bundle_hash": testIdentity.ContractBundleHash,
					"database": testIdentity.Database, "role": testIdentity.Role, "role_definition_hash": testIdentity.RoleDefinitionHash,
					"remote_identity_hash": testIdentity.RemoteIdentityHash,
				})
				continue
			}
			if request["type"] == "invoke" {
				result.invocations.Add(1)
				result.invoked <- struct{}{}
			}
			if request["type"] == "cancel" {
				result.cancelled <- struct{}{}
			}
			if response := handler(request); response != nil {
				writeTestFrame(connection, response)
			}
		}
	}()
	return result
}

func readTestFrame(reader io.Reader) ([]byte, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(reader, prefix[:]); err != nil {
		return nil, err
	}
	body := make([]byte, binary.BigEndian.Uint32(prefix[:]))
	_, err := io.ReadFull(reader, body)
	return body, err
}
func writeTestFrame(writer io.Writer, value any) {
	body, _ := json.Marshal(value)
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(body)))
	_, _ = writer.Write(append(prefix[:], body...))
}

func TestContextCancellationSendsCancel(t *testing.T) {
	fixture := newFixture(t, func(request map[string]any) map[string]any {
		if request["type"] == "cancel" {
			return map[string]any{"type": "cancelled", "request_id": request["request_id"], "target_request_id": request["target_request_id"], "terminal": false, "outcome_uncertain": true}
		}
		return nil
	})
	session, err := Connect(context.Background(), fixture.path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = session.Invoke(ctx, Operation{Name: "ticketdesk_create_comment", InputSchemaHash: testHash}, map[string]Value{"body": String("hello")}, Options{Deadline: time.Second})
		close(done)
	}()
	<-fixture.invoked
	cancel()
	<-fixture.cancelled
	select {
	case <-done:
		t.Fatal("cancel acknowledgement was mistaken for a terminal command result")
	default:
	}
}
