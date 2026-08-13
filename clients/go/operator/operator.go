// Package operator is the closed local binding for one Rust-owned reimport campaign.
package operator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// OperatorProtocolVersion is the exact private reimport-driver protocol generation.
const OperatorProtocolVersion = uint32(1)

const maxOperatorFrameBytes = 4*1_024*1_024 + 256*1_024

var operatorHashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// OperatorIdentity binds one local connection to one Rust-configured campaign.
type OperatorIdentity struct {
	Database                string
	CampaignID              string
	PortabilityManifestHash string
}

// ReimportOperation is the public-safe progress view returned by the Rust operator host.
type ReimportOperation struct {
	CampaignID                   string  `json:"campaign_id"`
	ContractLineage              string  `json:"contract_lineage"`
	Scope                        string  `json:"scope"`
	PortabilityManifestHash      string  `json:"portability_manifest_hash"`
	ExportManifestHash           string  `json:"export_manifest_hash"`
	ExportReceiptHash            string  `json:"export_receipt_hash"`
	SourceDatabaseID             string  `json:"source_database_id"`
	TargetDatabaseID             string  `json:"target_database_id"`
	SourceRows                   string  `json:"source_rows"`
	SourcePages                  string  `json:"source_pages"`
	NextPage                     string  `json:"next_page"`
	RowsApplied                  string  `json:"rows_applied"`
	Phase                        string  `json:"phase"`
	Failure                      *string `json:"failure"`
	CanonicalReimportReceiptJSON *string `json:"canonical_reimport_receipt_json"`
	ReimportReceiptHash          *string `json:"reimport_receipt_hash"`
}

// ReimportPage is one exact canonical page from the bound portability export.
type ReimportPage struct {
	ExportOperationID  string
	PageNumber         uint64
	CanonicalJSONLines []string
	NextCursorBase64   *string
	ClassComplete      bool
	OperationComplete  bool
	PageHashHex        string
	MaximumAttempts    uint32
}

// OperatorError is a typed public-safe error from the Rust operator host.
type OperatorError struct {
	Code             string
	Category         string
	Message          string
	Retryability     string
	RecoveryAction   string
	OutcomeUncertain bool
}

func (value *OperatorError) Error() string { return value.Code + ": " + value.Message }

// OperatorSession is a serial, campaign-bound connection to riffdb-operator-driverd.
// It cannot invoke normal application operations or carry remote credentials.
type OperatorSession struct {
	connection net.Conn
	reader     *bufio.Reader
	identity   OperatorIdentity
	mu         sync.Mutex
	next       atomic.Uint64
	closed     bool
}

// ConnectOperator opens the private operator socket and proves the exact campaign identity.
func ConnectOperator(ctx context.Context, socketPath string, identity OperatorIdentity) (*OperatorSession, error) {
	if !strings.HasPrefix(socketPath, "/") || len(socketPath) > 4_096 || !validOperatorIdentity(identity) {
		return nil, errors.New("invalid RiffDB operator configuration")
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	if err != nil {
		return nil, errors.New("RiffDB operator session failed")
	}
	session := &OperatorSession{connection: connection, reader: bufio.NewReader(connection), identity: identity}
	requestID := session.requestID("handshake")
	request := operatorHandshakeRequest{
		Type: "handshake", RequestID: requestID, ProtocolVersion: OperatorProtocolVersion,
		Database: identity.Database, CampaignID: identity.CampaignID,
		PortabilityManifestHash: identity.PortabilityManifestHash,
	}
	var response operatorHandshakeResponse
	if err := session.exchange(ctx, requestID, request, &response); err != nil {
		_ = connection.Close()
		return nil, err
	}
	if response.Type != "handshake" || response.ProtocolVersion != OperatorProtocolVersion ||
		response.DriverIdentity == "" || response.Database != identity.Database ||
		response.CampaignID != identity.CampaignID || response.PortabilityManifestHash != identity.PortabilityManifestHash {
		_ = connection.Close()
		return nil, errors.New("RiffDB operator identity mismatch")
	}
	return session, nil
}

// Start starts or exactly replays the Rust-configured reimport campaign.
func (session *OperatorSession) Start(ctx context.Context, canonicalManifest, canonicalReceipt string, maximumAttempts uint32) (ReimportOperation, error) {
	if !validCanonicalOperatorDocument(canonicalManifest, 256*1_024) ||
		!validCanonicalOperatorDocument(canonicalReceipt, 256*1_024) || !validOperatorAttempts(maximumAttempts) {
		return ReimportOperation{}, errors.New("invalid RiffDB operator start request")
	}
	requestID := session.requestID("start")
	request := operatorStartRequest{Type: "start", RequestID: requestID, CanonicalExportManifestJSON: canonicalManifest, CanonicalExportReceiptJSON: canonicalReceipt, MaximumAttempts: maximumAttempts}
	return session.operation(ctx, requestID, request)
}

// ApplyPage applies one exact source page through compiler-owned reimport commands.
func (session *OperatorSession) ApplyPage(ctx context.Context, page ReimportPage) (ReimportOperation, error) {
	if !validReimportPage(page) {
		return ReimportOperation{}, errors.New("invalid RiffDB operator page")
	}
	requestID := session.requestID("page")
	request := operatorPageRequest{
		Type: "apply_page", RequestID: requestID, ExportOperationID: page.ExportOperationID,
		PageNumber: page.PageNumber, CanonicalJSONLines: page.CanonicalJSONLines,
		NextCursorBase64: page.NextCursorBase64, ClassComplete: page.ClassComplete,
		OperationComplete: page.OperationComplete, PageHashHex: page.PageHashHex,
		MaximumAttempts: page.MaximumAttempts,
	}
	return session.operation(ctx, requestID, request)
}

// Status returns nil when the configured campaign does not yet exist.
func (session *OperatorSession) Status(ctx context.Context) (*ReimportOperation, error) {
	requestID := session.requestID("status")
	return session.optionalOperation(ctx, requestID, operatorStatusRequest{Type: "status", RequestID: requestID})
}

// Cancel cancels the campaign or returns its already-terminal state.
func (session *OperatorSession) Cancel(ctx context.Context, maximumAttempts uint32) (*ReimportOperation, error) {
	if !validOperatorAttempts(maximumAttempts) {
		return nil, errors.New("invalid RiffDB operator attempt bound")
	}
	requestID := session.requestID("cancel")
	return session.optionalOperation(ctx, requestID, operatorCancelRequest{Type: "cancel", RequestID: requestID, MaximumAttempts: maximumAttempts})
}

// Close closes the local operator session. It does not cancel the durable campaign.
func (session *OperatorSession) Close() error {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return nil
	}
	session.closed = true
	return session.connection.Close()
}

func (session *OperatorSession) operation(ctx context.Context, requestID string, request any) (ReimportOperation, error) {
	value, err := session.optionalOperation(ctx, requestID, request)
	if err != nil {
		return ReimportOperation{}, err
	}
	if value == nil {
		return ReimportOperation{}, errors.New("RiffDB operator campaign was not found")
	}
	return *value, nil
}

func (session *OperatorSession) optionalOperation(ctx context.Context, requestID string, request any) (*ReimportOperation, error) {
	var response operatorResponse
	if err := session.exchange(ctx, requestID, request, &response); err != nil {
		return nil, err
	}
	switch response.Type {
	case "operation":
		if response.Operation == nil || !validReimportOperation(*response.Operation) {
			return nil, errors.New("RiffDB operator returned invalid progress")
		}
		return response.Operation, nil
	case "not_found":
		return nil, nil
	case "error":
		if response.Code == "" || response.Category == "" || response.Message == "" ||
			response.Retryability == "" || response.RecoveryAction == "" {
			return nil, errors.New("RiffDB operator returned an invalid error")
		}
		return nil, &OperatorError{
			Code: response.Code, Category: response.Category, Message: response.Message,
			Retryability: response.Retryability, RecoveryAction: response.RecoveryAction,
			OutcomeUncertain: response.OutcomeUncertain,
		}
	default:
		return nil, errors.New("RiffDB operator returned an invalid response")
	}
}

func (session *OperatorSession) exchange(ctx context.Context, requestID string, request, response any) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return errors.New("RiffDB operator session closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	body, err := marshalOperator(request)
	if err != nil || len(body) < 2 || len(body) > maxOperatorFrameBytes {
		return errors.New("invalid RiffDB operator request")
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := session.connection.SetDeadline(deadline); err != nil {
			return errors.New("RiffDB operator session failed")
		}
		defer func() { _ = session.connection.SetDeadline(noDeadline) }()
	}
	prefix := []byte{byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body))}
	if _, err := session.connection.Write(append(prefix, body...)); err != nil {
		return errors.New("RiffDB operator session failed")
	}
	frame, err := readOperatorFrame(session.reader)
	if err != nil {
		return err
	}
	var header struct {
		RequestID *string `json:"request_id"`
	}
	if json.Unmarshal(frame, &header) != nil || header.RequestID == nil || *header.RequestID != requestID {
		return errors.New("RiffDB operator returned an invalid request identity")
	}
	if err := decodeExactOperator(frame, response); err != nil {
		return errors.New("RiffDB operator returned an invalid response")
	}
	return nil
}

var noDeadline = func() (value time.Time) { return value }()

func readOperatorFrame(reader io.Reader) ([]byte, error) {
	prefix := make([]byte, 4)
	if _, err := io.ReadFull(reader, prefix); err != nil {
		return nil, errors.New("RiffDB operator session failed")
	}
	length := int(prefix[0])<<24 | int(prefix[1])<<16 | int(prefix[2])<<8 | int(prefix[3])
	if length < 2 || length > maxOperatorFrameBytes {
		return nil, errors.New("RiffDB operator returned an invalid frame")
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(reader, body); err != nil {
		return nil, errors.New("RiffDB operator session failed")
	}
	return body, nil
}

func marshalOperator(value any) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(output.Bytes(), []byte{'\n'}), nil
}

func decodeExactOperator(body []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return errors.New("trailing operator JSON")
	}
	return nil
}

func validOperatorIdentity(value OperatorIdentity) bool {
	return value.Database != "" && len(value.Database) <= 4_096 && !strings.ContainsAny(value.Database, "\n\r\x00") &&
		validUUID7(value.CampaignID) && operatorHashPattern.MatchString(value.PortabilityManifestHash)
}

func validReimportOperation(value ReimportOperation) bool {
	return validUUID7(value.CampaignID) && validUUID7(value.SourceDatabaseID) && validUUID7(value.TargetDatabaseID) &&
		operatorHashPattern.MatchString(value.PortabilityManifestHash) && operatorHashPattern.MatchString(value.ExportManifestHash) &&
		operatorHashPattern.MatchString(value.ExportReceiptHash) && value.ContractLineage != "" && value.Phase != "" &&
		(value.ReimportReceiptHash == nil || operatorHashPattern.MatchString(*value.ReimportReceiptHash))
}

func validReimportPage(value ReimportPage) bool {
	if !validUUID7(value.ExportOperationID) || value.PageNumber == 0 || len(value.CanonicalJSONLines) < 1 ||
		len(value.CanonicalJSONLines) > 500 || !operatorHashPattern.MatchString(value.PageHashHex) ||
		value.OperationComplete != (value.NextCursorBase64 == nil) ||
		(value.OperationComplete && !value.ClassComplete) || !validOperatorAttempts(value.MaximumAttempts) {
		return false
	}
	bytes := 0
	for _, line := range value.CanonicalJSONLines {
		bytes += len(line) + 1
		if bytes > 4*1_024*1_024 || !validCanonicalOperatorDocument(line, 64*1_024) {
			return false
		}
	}
	return value.NextCursorBase64 == nil || (len(*value.NextCursorBase64) >= 1 && len(*value.NextCursorBase64) <= 1_024)
}

func validCanonicalOperatorDocument(value string, maximum int) bool {
	return len(value) >= 2 && len(value) <= maximum && value[0] == '{' && value[len(value)-1] == '}' &&
		!strings.ContainsAny(value, "\n\r")
}

func validOperatorAttempts(value uint32) bool { return value >= 1 && value <= 10 }

func validUUID7(value string) bool {
	if len(value) != 36 || value[14] != '7' || !strings.Contains("89ab", value[19:20]) {
		return false
	}
	for index, character := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if character != '-' {
				return false
			}
		} else if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

func (session *OperatorSession) requestID(kind string) string {
	return fmt.Sprintf("go.operator.%s.%d", kind, session.next.Add(1))
}

type operatorHandshakeRequest struct {
	Type                    string `json:"type"`
	RequestID               string `json:"request_id"`
	ProtocolVersion         uint32 `json:"protocol_version"`
	Database                string `json:"database"`
	CampaignID              string `json:"campaign_id"`
	PortabilityManifestHash string `json:"portability_manifest_hash"`
}
type operatorStartRequest struct {
	Type                        string `json:"type"`
	RequestID                   string `json:"request_id"`
	CanonicalExportManifestJSON string `json:"canonical_export_manifest_json"`
	CanonicalExportReceiptJSON  string `json:"canonical_export_receipt_json"`
	MaximumAttempts             uint32 `json:"maximum_attempts"`
}
type operatorPageRequest struct {
	Type               string   `json:"type"`
	RequestID          string   `json:"request_id"`
	ExportOperationID  string   `json:"export_operation_id"`
	PageNumber         uint64   `json:"page_number"`
	CanonicalJSONLines []string `json:"canonical_json_lines"`
	NextCursorBase64   *string  `json:"next_cursor_base64"`
	ClassComplete      bool     `json:"class_complete"`
	OperationComplete  bool     `json:"operation_complete"`
	PageHashHex        string   `json:"page_hash_hex"`
	MaximumAttempts    uint32   `json:"maximum_attempts"`
}
type operatorStatusRequest struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id"`
}
type operatorCancelRequest struct {
	Type            string `json:"type"`
	RequestID       string `json:"request_id"`
	MaximumAttempts uint32 `json:"maximum_attempts"`
}
type operatorHandshakeResponse struct {
	Type                    string `json:"type"`
	RequestID               string `json:"request_id"`
	ProtocolVersion         uint32 `json:"protocol_version"`
	DriverIdentity          string `json:"driver_identity"`
	Database                string `json:"database"`
	CampaignID              string `json:"campaign_id"`
	PortabilityManifestHash string `json:"portability_manifest_hash"`
}
type operatorResponse struct {
	Type             string             `json:"type"`
	RequestID        string             `json:"request_id"`
	Operation        *ReimportOperation `json:"operation,omitempty"`
	Code             string             `json:"code,omitempty"`
	Category         string             `json:"category,omitempty"`
	Message          string             `json:"message,omitempty"`
	Retryability     string             `json:"retryability,omitempty"`
	RecoveryAction   string             `json:"recovery_action,omitempty"`
	OutcomeUncertain bool               `json:"outcome_uncertain,omitempty"`
}
