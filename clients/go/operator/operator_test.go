package operator

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
)

const operatorTestCampaign = "018f2f85-3c20-7a31-8f11-112233445566"
const operatorTestSource = "018f2f85-3c20-7a31-8f11-112233445577"
const operatorTestTarget = "018f2f85-3c20-7a31-8f11-112233445588"
const operatorTestHash = "0101010101010101010101010101010101010101010101010101010101010101"

func TestOperatorSessionIsCampaignBoundAndDisjointFromApplicationCalls(t *testing.T) {
	base := os.Getenv("TMPDIR")
	if base == "" {
		t.Fatal("TMPDIR must name the protected test scratch root")
	}
	directory, err := os.MkdirTemp(base, "riffdb-go-operator-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, "operator.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverDone := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer connection.Close()
		reader := bufio.NewReader(connection)
		for index := 0; index < 3; index++ {
			body, readErr := readOperatorTestFrame(reader)
			if readErr != nil {
				serverDone <- readErr
				return
			}
			var request map[string]any
			if json.Unmarshal(body, &request) != nil || request["operation"] != nil || request["credential"] != nil {
				serverDone <- io.ErrUnexpectedEOF
				return
			}
			requestID, _ := request["request_id"].(string)
			var response any
			switch index {
			case 0:
				response = operatorHandshakeResponse{Type: "handshake", RequestID: requestID, ProtocolVersion: OperatorProtocolVersion, DriverIdentity: "riffdb-driver-host/v1", Database: "restored", CampaignID: operatorTestCampaign, PortabilityManifestHash: operatorTestHash}
			case 1:
				response = struct {
					Type      string            `json:"type"`
					RequestID string            `json:"request_id"`
					Operation ReimportOperation `json:"operation"`
				}{Type: "operation", RequestID: requestID, Operation: testReimportOperation()}
			default:
				response = struct {
					Type      string `json:"type"`
					RequestID string `json:"request_id"`
				}{Type: "not_found", RequestID: requestID}
			}
			encoded, encodeErr := marshalOperator(response)
			if encodeErr != nil {
				serverDone <- encodeErr
				return
			}
			prefix := []byte{byte(len(encoded) >> 24), byte(len(encoded) >> 16), byte(len(encoded) >> 8), byte(len(encoded))}
			if _, writeErr := connection.Write(append(prefix, encoded...)); writeErr != nil {
				serverDone <- writeErr
				return
			}
		}
		serverDone <- nil
	}()

	identity := OperatorIdentity{Database: "restored", CampaignID: operatorTestCampaign, PortabilityManifestHash: operatorTestHash}
	session, err := ConnectOperator(context.Background(), path, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	operation, err := session.Start(context.Background(), "{}", "{}", 3)
	if err != nil || operation.RowsApplied != "2" {
		t.Fatalf("unexpected operation: %#v %v", operation, err)
	}
	status, err := session.Status(context.Background())
	if err != nil || status != nil {
		t.Fatalf("unexpected status: %#v %v", status, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestOperatorPageValidationRejectsCallerSelectedUnsafeShapes(t *testing.T) {
	if validOperatorIdentity(OperatorIdentity{Database: "restored", CampaignID: operatorTestCampaign, PortabilityManifestHash: operatorTestHash}) != true {
		t.Fatal("valid identity rejected")
	}
	if validOperatorIdentity(OperatorIdentity{Database: "restored", CampaignID: operatorTestCampaign, PortabilityManifestHash: "bad"}) {
		t.Fatal("invalid manifest hash accepted")
	}
	page := ReimportPage{ExportOperationID: operatorTestSource, PageNumber: 1, CanonicalJSONLines: []string{"{\"entity\":\"Ticket\"}"}, ClassComplete: true, OperationComplete: true, PageHashHex: operatorTestHash, MaximumAttempts: 3}
	if !validReimportPage(page) {
		t.Fatal("valid page rejected")
	}
	cursor := "cursor"
	page.NextCursorBase64 = &cursor
	if validReimportPage(page) {
		t.Fatal("ambiguous terminal cursor accepted")
	}
}

func testReimportOperation() ReimportOperation {
	return ReimportOperation{
		CampaignID: operatorTestCampaign, ContractLineage: "TicketDesk", Scope: "whole_application",
		PortabilityManifestHash: operatorTestHash, ExportManifestHash: operatorTestHash, ExportReceiptHash: operatorTestHash,
		SourceDatabaseID: operatorTestSource, TargetDatabaseID: operatorTestTarget, SourceRows: "3", SourcePages: "1",
		NextPage: "2", RowsApplied: "2", Phase: "applying", Failure: nil, CanonicalReimportReceiptJSON: nil, ReimportReceiptHash: nil,
	}
}

func readOperatorTestFrame(reader io.Reader) ([]byte, error) {
	prefix := make([]byte, 4)
	if _, err := io.ReadFull(reader, prefix); err != nil {
		return nil, err
	}
	length := int(prefix[0])<<24 | int(prefix[1])<<16 | int(prefix[2])<<8 | int(prefix[3])
	body := make([]byte, length)
	_, err := io.ReadFull(reader, body)
	return body, err
}
