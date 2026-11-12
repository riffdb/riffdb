package application

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
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

func TestPackedNegotiationRequiresCompactPredecessor(t *testing.T) {
	if _, err := (Options{AcceptPackedResult: true}).wire(); err == nil {
		t.Fatal("packed result was accepted without compact predecessor")
	}
	if _, err := (Options{AcceptCompactResult: true, AcceptPackedResult: true}).wire(); err != nil {
		t.Fatalf("closed packed negotiation was rejected: %v", err)
	}
}

func TestAdmissionHeadIsTheOnlyStrongerQueryConsistency(t *testing.T) {
	wire, err := (Options{QueryConsistency: AdmissionHead}).wire()
	if err != nil {
		t.Fatalf("admission-head consistency was rejected: %v", err)
	}
	if wire.QueryConsistency == nil || *wire.QueryConsistency != AdmissionHead {
		t.Fatalf("admission-head consistency was not carried: %#v", wire.QueryConsistency)
	}
	if _, err := (Options{QueryConsistency: QueryConsistency("stale_ok")}).wire(); err == nil {
		t.Fatal("unknown query consistency was accepted")
	}
}

func TestPackedCanonicalScalarDecodersRejectWrongTags(t *testing.T) {
	value := []byte{1, 6, 0, 0, 0, 2, 'o', 'k'}
	decoded, err := PackedString(value, 2)
	if err != nil || decoded != "ok" {
		t.Fatalf("packed string changed: %q %v", decoded, err)
	}
	if _, err = PackedString(value, 1); err == nil {
		t.Fatal("packed string exceeded compiler byte bound")
	}
	if _, err = PackedU64(value); err == nil {
		t.Fatal("packed string was accepted as u64")
	}
}

func TestCanonicalValueEncodedLengthMatchesNestedDocumentShape(t *testing.T) {
	value := Record(map[string]Value{
		"context": BytesFrom([]byte{0, 1, 2, 3}),
	})
	length, err := CanonicalValueEncodedLength(value)
	if err != nil {
		t.Fatal(err)
	}
	// record version/tag/count + field ID + bytes version/tag/length/payload
	if length != 6+4+6+4 {
		t.Fatalf("canonical length changed: %d", length)
	}
	if _, err = CanonicalValueEncodedLength(Value{Type: "peer-kind"}); err == nil {
		t.Fatal("unknown canonical value kind was accepted")
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

func TestVectorRegistryPreservesCanonicalBinary32Components(t *testing.T) {
	value := VectorFrom([]float32{-0.0, 1.5, -2.25})
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Value
	if err = json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	vector, err := VectorValue(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(vector) != 3 || math.Signbit(float64(vector[0])) || vector[1] != 1.5 || vector[2] != -2.25 {
		t.Fatalf("vector changed across driver registry: %#v", vector)
	}
	if validateValue(Value{Type: "vector", Value: Vector{ComponentBits: []uint32{f32bits(float32(math.NaN()))}}}, 0) == nil {
		t.Fatal("non-finite vector was accepted")
	}
}

func f32bits(value float32) uint32 { return math.Float32bits(value) }

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

// req: DRV-005, DRV-007, DRV-008, DX-004, DX-046
func TestSessionResolvesBoundTenantFromIdentityOrEnvironment(t *testing.T) {
	fixture := newFixture(t, func(request map[string]any) map[string]any { return nil })
	t.Setenv("RIFFDB_TENANT", "dev-tenant")
	session, err := Connect(context.Background(), fixture.path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	if session.Tenant() != "dev-tenant" {
		t.Fatalf("expected bound tenant dev-tenant, got %q", session.Tenant())
	}
	badIdentity := testIdentity
	badIdentity.Tenant = "bad tenant"
	if _, err := Connect(context.Background(), fixture.path, badIdentity); err == nil {
		t.Fatal("expected invalid tenant with whitespace to fail closed")
	}
}

// req: DRV-005, DRV-006, DX-004
func TestConnectFromEnvAndJsonMarshaling(t *testing.T) {
	fixture := newFixture(t, func(request map[string]any) map[string]any { return nil })
	t.Setenv("RIFFDB_DRIVER_SOCKET", fixture.path)
	t.Setenv("RIFFDB_ROLE", "TicketDeskApplication")
	session, err := ConnectFromEnv(context.Background(), Identity{
		ContractLineage:    "TicketDesk",
		ContractVersion:    5,
		ContractBundleHash: testHash,
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	resolved := session.Identity()
	if resolved.Database != "default" || resolved.ApplicationManifestHash != testHash || resolved.OperationCatalogHash != testHash {
		t.Fatalf("expected handshake to resolve optional hashes and database, got %#v", resolved)
	}

	ts := Instant{Seconds: 1711886400, Nanos: 500_000_000}
	if ts.String() != "2024-03-31T12:00:00.5Z" {
		t.Fatalf("unexpected Instant.String(): %q", ts.String())
	}
	roundTripped := InstantFromTime(ts.Time())
	if roundTripped != ts {
		t.Fatalf("expected InstantFromTime(ts.Time()) == %#v, got %#v", ts, roundTripped)
	}
	encodedTS, err := json.Marshal(ts)
	if err != nil || string(encodedTS) != `"2024-03-31T12:00:00.5Z"` {
		t.Fatalf("unexpected Instant JSON: %s (%v)", string(encodedTS), err)
	}
	var decodedTS Instant
	if err := json.Unmarshal(encodedTS, &decodedTS); err != nil || decodedTS != ts {
		t.Fatalf("unexpected Instant unmarshal: %#v (%v)", decodedTS, err)
	}
	for _, tc := range []struct {
		raw     string
		seconds int64
		nanos   uint32
	}{
		{raw: `{"seconds":9007199254740993,"nanos":123}`, seconds: 9007199254740993, nanos: 123},
		{raw: `{"seconds":9223372036854775807,"nanos":999999999}`, seconds: math.MaxInt64, nanos: 999_999_999},
		{raw: `{"seconds":-9223372036854775808}`, seconds: math.MinInt64, nanos: 0},
		{raw: `{"seconds":"9007199254740993","nanos":0}`, seconds: 9007199254740993, nanos: 0},
	} {
		var exact Instant
		if err := json.Unmarshal([]byte(tc.raw), &exact); err != nil {
			t.Fatalf("expected %s to unmarshal cleanly, got %v", tc.raw, err)
		}
		if exact.Seconds != tc.seconds || exact.Nanos != tc.nanos {
			t.Fatalf("expected %s -> (%d, %d), got (%d, %d)", tc.raw, tc.seconds, tc.nanos, exact.Seconds, exact.Nanos)
		}
	}
	for _, invalidRaw := range []string{
		`{"seconds":9223372036854775808,"nanos":0}`,
		`{"seconds":-9223372036854775809,"nanos":0}`,
		`{"seconds":1e100,"nanos":0}`,
		`{"seconds":1e3,"nanos":0}`,
		`{"seconds":1.5,"nanos":0}`,
		`{"seconds":100,"nanos":1.5}`,
		`{"seconds":100,"nanos":1000000000}`,
		`{"seconds":100,"nanos":-1}`,
		`{}`,
		`null`,
	} {
		var rejected Instant
		if err := json.Unmarshal([]byte(invalidRaw), &rejected); err == nil {
			t.Fatalf("expected invalid Instant JSON %s to fail closed, got %#v", invalidRaw, rejected)
		}
	}

	prefixedFixture := newFixture(t, func(request map[string]any) map[string]any { return nil })
	t.Setenv("AGENT_ALPHA_RIFFDB_DRIVER_SOCKET", prefixedFixture.path)
	t.Setenv("AGENT_ALPHA_RIFFDB_ROLE", "TicketDeskApplication")
	prefixedSession, err := ConnectFromEnv(context.Background(), Identity{
		ContractLineage:    "TicketDesk",
		ContractVersion:    5,
		ContractBundleHash: testHash,
	}, "AGENT_ALPHA_")
	if err != nil {
		t.Fatalf("expected AGENT_ALPHA_ prefix to resolve AGENT_ALPHA_RIFFDB_* env vars, got %v", err)
	}
	_ = prefixedSession.Close()

	if _, err := ConnectFromEnv(context.Background(), Identity{
		ContractLineage:    "TicketDesk",
		ContractVersion:    5,
		ContractBundleHash: testHash,
	}, "MISSING_APP_"); err == nil {
		t.Fatal("expected missing prefixed env vars to fail closed instead of falling back to global RIFFDB_*")
	}

	dec := ExactDecimal{CoefficientTwosComplement: []byte{0xff, 0x7f}, Scale: 2, Precision: 4}
	if dec.String() != "-1.29" {
		t.Fatalf("unexpected ExactDecimal.String(): %q", dec.String())
	}
	money := ExactMoney{
		Currency: "USD",
		Amount:   ExactDecimal{CoefficientTwosComplement: []byte{0x09, 0xc4}, Scale: 2, Precision: 38},
	}
	encodedMoney, err := json.Marshal(money)
	if err != nil || string(encodedMoney) != `{"currency":"USD","amount":"25.00"}` {
		t.Fatalf("unexpected ExactMoney JSON: %s (%v)", string(encodedMoney), err)
	}
}

func TestAlreadyCancelledContextDoesNotSubmitRequest(t *testing.T) {
	fixture := newFixture(t, func(request map[string]any) map[string]any {
		return map[string]any{
			"type": "result", "request_id": request["request_id"],
			"value":            map[string]any{"type": "u64", "value": "1"},
			"application_head": uint64(1), "cursor": nil, "replayed": false,
		}
	})
	session, err := Connect(context.Background(), fixture.path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = session.Invoke(
		cancelledCtx,
		Operation{Name: "ticketdesk_create_comment", InputSchemaHash: testHash},
		map[string]Value{"body": String("hello")},
		Options{},
	)
	if err == nil {
		t.Fatal("expected already-cancelled context to fail before submission")
	}
	if fixture.invocations.Load() != 0 {
		t.Fatalf("expected 0 driver invocations with pre-cancelled context, got %d", fixture.invocations.Load())
	}
}

func TestSessionRejectsCallsAfterPeerEOF(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "eof.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	peerClosed := make(chan struct{})
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		reader := bufio.NewReader(conn)
		body, readErr := readTestFrame(reader)
		if readErr != nil {
			_ = conn.Close()
			return
		}
		var request map[string]any
		if json.Unmarshal(body, &request) != nil {
			_ = conn.Close()
			return
		}
		writeTestFrame(conn, map[string]any{
			"type": "handshake", "request_id": request["request_id"], "protocol_version": ProtocolVersion,
			"driver_identity": "riffdb-driver-host/v1", "application_manifest_hash": testIdentity.ApplicationManifestHash,
			"operation_catalog_hash": testIdentity.OperationCatalogHash, "contract_lineage": testIdentity.ContractLineage,
			"contract_version": testIdentity.ContractVersion, "contract_bundle_hash": testIdentity.ContractBundleHash,
			"database": testIdentity.Database, "role": testIdentity.Role, "role_definition_hash": testIdentity.RoleDefinitionHash,
			"remote_identity_hash": testIdentity.RemoteIdentityHash,
		})
		_, _ = readTestFrame(reader)
		_ = conn.Close()
		_ = listener.Close()
		close(peerClosed)
	}()

	session, err := Connect(context.Background(), socketPath, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })

	_, err = session.Invoke(
		context.Background(),
		Operation{Name: "ticketdesk_get_ticket", InputSchemaHash: testHash},
		map[string]Value{"ticket_id": UUID("018f0f79-7b5e-7c03-9b12-b16f57a4c998")},
		Options{},
	)
	if err == nil {
		t.Fatal("expected first call to fail on peer EOF")
	}
	<-peerClosed

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = session.Invoke(
		ctx,
		Operation{Name: "ticketdesk_get_ticket", InputSchemaHash: testHash},
		map[string]Value{"ticket_id": UUID("018f0f79-7b5e-7c03-9b12-b16f57a4c998")},
		Options{},
	)
	if err == nil {
		t.Fatal("expected subsequent call after peer EOF to fail immediately")
	}
	if ctx.Err() != nil {
		t.Fatalf("subsequent call after peer EOF hung until context timeout: %v", err)
	}
}

func TestInvalidUTF8InputIsRejectedBeforeSerialization(t *testing.T) {
	fixture := newFixture(t, func(request map[string]any) map[string]any {
		return map[string]any{
			"type": "result", "request_id": request["request_id"],
			"value":            map[string]any{"type": "u64", "value": "1"},
			"application_head": uint64(1), "cursor": nil, "replayed": false,
		}
	})
	session, err := Connect(context.Background(), fixture.path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })

	invalidUTF8 := string([]byte{0xff})
	_, err = session.Invoke(
		context.Background(),
		Operation{Name: "ticketdesk_create_comment", InputSchemaHash: testHash},
		map[string]Value{"body": String(invalidUTF8)},
		Options{},
	)
	if err == nil {
		t.Fatal("expected invalid UTF-8 string input to be rejected before serialization")
	}
	_, err = session.Invoke(
		context.Background(),
		Operation{Name: "ticketdesk_create_comment", InputSchemaHash: testHash},
		map[string]Value{"nested": Record(map[string]Value{"body": String(invalidUTF8)})},
		Options{},
	)
	if err == nil {
		t.Fatal("expected nested invalid UTF-8 string input to be rejected before serialization")
	}
	if fixture.invocations.Load() != 0 {
		t.Fatalf("expected 0 driver invocations for invalid UTF-8 inputs, got %d", fixture.invocations.Load())
	}
}

func TestExtendedYearInstantJSONRoundTripAndIncompleteBatchRejection(t *testing.T) {
	for _, sample := range []Instant{
		{Seconds: 253_402_300_800, Nanos: 123_000_000}, // Year 10000
		{Seconds: -62_167_219_200, Nanos: 0},           // Year 0000
		{Seconds: -62_198_755_200, Nanos: 500_000_000}, // Year -0001
		{Seconds: math.MaxInt64, Nanos: 999_999_999},
		{Seconds: math.MinInt64, Nanos: 0},
	} {
		encoded, err := json.Marshal(sample)
		if err != nil {
			t.Fatalf("expected Instant %#v to marshal, got %v", sample, err)
		}
		var decoded Instant
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatalf("expected Instant %s to unmarshal, got %v", string(encoded), err)
		}
		if decoded != sample {
			t.Fatalf("expected round-tripped Instant %#v, got %#v (JSON %s)", sample, decoded, string(encoded))
		}
	}

	fixture := newFixture(t, func(request map[string]any) map[string]any {
		if request["type"] == "batch" {
			return map[string]any{
				"type":       "batch_result",
				"request_id": request["request_id"],
				"checkpoint": uint32(2),
				"total":      uint32(2),
				"items":      []any{},
			}
		}
		return nil
	})
	session, err := Connect(context.Background(), fixture.path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })

	_, err = session.Batch(
		context.Background(),
		Operation{Name: "ticketdesk_create_comment", InputSchemaHash: testHash},
		[]map[string]Value{
			{"body": String("first")},
			{"body": String("second")},
		},
		2,
		0,
		Options{},
	)
	if err == nil {
		t.Fatal("expected incomplete batch receipt with empty items to be rejected")
	}
}

func TestMinimalEnvHandshakeAndAutoReconnectAfterDriverRestart(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "restart.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	var handshakes atomic.Uint32
	firstDropDone := make(chan struct{})
	go func() {
		// First connection: handshake, then drop on first invoke
		conn1, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		reader1 := bufio.NewReader(conn1)
		body1, readErr := readTestFrame(reader1)
		if readErr != nil {
			_ = conn1.Close()
			return
		}
		var req1 map[string]any
		_ = json.Unmarshal(body1, &req1)
		handshakes.Add(1)
		writeTestFrame(conn1, map[string]any{
			"type": "handshake", "request_id": req1["request_id"], "protocol_version": ProtocolVersion,
			"driver_identity": "riffdb-driver-host/v1", "application_manifest_hash": testIdentity.ApplicationManifestHash,
			"operation_catalog_hash": testIdentity.OperationCatalogHash, "contract_lineage": testIdentity.ContractLineage,
			"contract_version": testIdentity.ContractVersion, "contract_bundle_hash": testIdentity.ContractBundleHash,
			"database": testIdentity.Database, "role": testIdentity.Role, "role_definition_hash": testIdentity.RoleDefinitionHash,
			"remote_identity_hash": testIdentity.RemoteIdentityHash,
		})
		_, _ = readTestFrame(reader1)
		_ = conn1.Close()
		close(firstDropDone)

		// Second connection (after reconnect): handshake and answer invoke
		conn2, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn2.Close()
		reader2 := bufio.NewReader(conn2)
		for {
			body2, readErr := readTestFrame(reader2)
			if readErr != nil {
				return
			}
			var req2 map[string]any
			if json.Unmarshal(body2, &req2) != nil {
				return
			}
			if req2["type"] == "handshake" {
				handshakes.Add(1)
				writeTestFrame(conn2, map[string]any{
					"type": "handshake", "request_id": req2["request_id"], "protocol_version": ProtocolVersion,
					"driver_identity": "riffdb-driver-host/v1", "application_manifest_hash": testIdentity.ApplicationManifestHash,
					"operation_catalog_hash": testIdentity.OperationCatalogHash, "contract_lineage": testIdentity.ContractLineage,
					"contract_version": testIdentity.ContractVersion, "contract_bundle_hash": testIdentity.ContractBundleHash,
					"database": testIdentity.Database, "role": testIdentity.Role, "role_definition_hash": testIdentity.RoleDefinitionHash,
					"remote_identity_hash": testIdentity.RemoteIdentityHash,
				})
				continue
			}
			if req2["type"] == "invoke" {
				writeTestFrame(conn2, map[string]any{
					"type": "result", "request_id": req2["request_id"],
					"value":            map[string]any{"type": "u64", "value": "42"},
					"application_head": uint64(42), "cursor": nil, "replayed": false,
				})
			}
		}
	}()

	t.Setenv("RIFFDB_DRIVER_SOCKET", socketPath)
	session, err := ConnectFromEnv(context.Background(), Identity{}, "")
	if err != nil {
		t.Fatalf("expected minimal RIFFDB_DRIVER_SOCKET ConnectFromEnv to succeed, got %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	resolved := session.Identity()
	if resolved.ContractLineage != testIdentity.ContractLineage || resolved.ContractVersion != testIdentity.ContractVersion || resolved.Role != testIdentity.Role {
		t.Fatalf("expected handshake to resolve full identity, got %#v", resolved)
	}

	op := Operation{Name: "ticketdesk_get_ticket", InputSchemaHash: testHash}
	input := map[string]Value{"ticket_id": UUID("018f0f79-7b5e-7c03-9b12-b16f57a4c998")}
	if _, err := session.Invoke(context.Background(), op, input, Options{}); err == nil {
		t.Fatal("expected first call on dropped connection to fail")
	}
	<-firstDropDone

	result, err := session.Invoke(context.Background(), op, input, Options{})
	if err != nil {
		t.Fatalf("expected second call to automatically reconnect and succeed, got %v", err)
	}
	if result.ApplicationHead == nil || *result.ApplicationHead != 42 || handshakes.Load() != 2 {
		t.Fatalf("unexpected reconnect result: head=%v handshakes=%d", result.ApplicationHead, handshakes.Load())
	}

	missingSocket := filepath.Join(t.TempDir(), "missing.sock")
	_, dialErr := Connect(context.Background(), missingSocket, testIdentity)
	if dialErr == nil || !strings.Contains(dialErr.Error(), "RiffDB driver session failed:") || !strings.Contains(dialErr.Error(), "missing.sock") {
		t.Fatalf("expected dial error to preserve underlying cause, got %v", dialErr)
	}
}
