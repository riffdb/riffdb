// Package application is the application-only local transport for generated
// RiffDB Go bindings. Remote trust, credentials, retries, and gRPC remain owned
// by the first-party Rust driver host.
package application

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const (
	ProtocolVersion   = uint32(4)
	ValueRegistryHash = "8e1681ddf5e6a82e7fa646f9737128ad7e36f54f8b5846ac6e33e732125407e5"
	ErrorRegistryHash = "b94d685ecbc18f2369a2bfa1a53139d06100699c4ee41b31c86d6a7e17039850"
	maxFrameBytes     = 8 * 1_024 * 1_024
	maxPending        = 256
	maxCollection     = 4_096
	maxDepth          = 32
)

var (
	hashPattern    = regexp.MustCompile(`^[0-9a-f]{64}$`)
	symbolPattern  = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,256}$`)
	requestPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
	nextSession    atomic.Uint64
)

// Value is the closed target-neutral application value registry.
type Value struct {
	Type  string `json:"type"`
	Value any    `json:"value,omitempty"`
}

type Timestamp struct {
	Seconds string `json:"seconds"`
	Nanos   uint32 `json:"nanos"`
}
type Decimal struct {
	Coefficient string  `json:"coefficient"`
	Scale       uint32  `json:"scale"`
	Precision   *uint32 `json:"precision,omitempty"`
}
type Money struct {
	Currency string  `json:"currency"`
	Amount   Decimal `json:"amount"`
}
type Vector struct {
	ComponentBits []uint32 `json:"component_bits"`
}

type ExactDecimal struct {
	CoefficientTwosComplement []byte
	Scale                     uint32
	Precision                 uint32
}
type ExactMoney struct {
	Currency string
	Amount   ExactDecimal
}
type Instant struct {
	Seconds int64
	Nanos   uint32
}

func Null() Value                          { return Value{Type: "null"} }
func Bool(value bool) Value                { return Value{Type: "bool", Value: value} }
func I64(value int64) Value                { return Value{Type: "i64", Value: strconv.FormatInt(value, 10)} }
func U64(value uint64) Value               { return Value{Type: "u64", Value: strconv.FormatUint(value, 10)} }
func String(value string) Value            { return Value{Type: "string", Value: value} }
func UUID(value string) Value              { return Value{Type: "uuid", Value: value} }
func Enum(value string) Value              { return Value{Type: "enum", Value: value} }
func Bytes(value string) Value             { return Value{Type: "bytes", Value: value} }
func Date(days int32) Value                { return Value{Type: "date", Value: strconv.FormatInt(int64(days), 10)} }
func List(values []Value) Value            { return Value{Type: "list", Value: values} }
func Record(values map[string]Value) Value { return Value{Type: "record", Value: values} }
func BytesFrom(value []byte) Value         { return Bytes(base64.StdEncoding.EncodeToString(value)) }
func TimestampFrom(value Instant) Value {
	return Value{Type: "timestamp", Value: Timestamp{Seconds: strconv.FormatInt(value.Seconds, 10), Nanos: value.Nanos}}
}
func DecimalFrom(value ExactDecimal) Value {
	precision := value.Precision
	return Value{Type: "decimal", Value: Decimal{Coefficient: base64.StdEncoding.EncodeToString(value.CoefficientTwosComplement), Scale: value.Scale, Precision: &precision}}
}
func MoneyFrom(value ExactMoney) Value {
	decimal := DecimalFrom(value.Amount).Value.(Decimal)
	return Value{Type: "money", Value: Money{Currency: value.Currency, Amount: decimal}}
}
func VectorFrom(value []float32) Value {
	bits := make([]uint32, len(value))
	for index, component := range value {
		if component == 0 {
			component = 0
		}
		bits[index] = math.Float32bits(component)
	}
	return Value{Type: "vector", Value: Vector{ComponentBits: bits}}
}
func Optional[T any](value *T, encode func(T) Value) Value {
	if value == nil {
		return Null()
	}
	return encode(*value)
}
func Values[T any](items []T, encode func(T) Value) Value {
	output := make([]Value, len(items))
	for index, item := range items {
		output[index] = encode(item)
	}
	return List(output)
}

// CanonicalValueEncodedLength returns the exact ADR-0011 canonical v1 document
// length for one already-typed application value without allocating that document.
func CanonicalValueEncodedLength(value Value) (int, error) {
	add := func(left, right int) (int, error) {
		if right < 0 || left > int(^uint(0)>>1)-right {
			return 0, errors.New("RiffDB canonical value length overflow")
		}
		return left + right, nil
	}
	switch value.Type {
	case "null":
		return 2, nil
	case "bool":
		return 3, nil
	case "i64", "u64":
		return 10, nil
	case "decimal":
		return 20, nil
	case "money":
		return 23, nil
	case "string":
		text, ok := value.Value.(string)
		if !ok {
			return 0, errors.New("invalid RiffDB string value")
		}
		return add(6, len([]byte(text)))
	case "bytes":
		text, ok := value.Value.(string)
		if !ok {
			return 0, errors.New("invalid RiffDB bytes value")
		}
		decoded, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			return 0, errors.New("invalid RiffDB bytes value")
		}
		return add(6, len(decoded))
	case "timestamp":
		return 14, nil
	case "date":
		return 6, nil
	case "uuid":
		return 18, nil
	case "enum":
		return 10, nil
	case "vector":
		vector, ok := value.Value.(Vector)
		if !ok {
			return 0, errors.New("invalid RiffDB vector value")
		}
		components, err := add(0, len(vector.ComponentBits)*4)
		if err != nil {
			return 0, err
		}
		return add(6, components)
	case "list":
		items, err := ListItems(value)
		if err != nil {
			return 0, err
		}
		total := 6
		for _, item := range items {
			length, err := CanonicalValueEncodedLength(item)
			if err != nil {
				return 0, err
			}
			total, err = add(total, length)
			if err != nil {
				return 0, err
			}
		}
		return total, nil
	case "record":
		fields, err := RecordFields(value)
		if err != nil {
			return 0, err
		}
		total := 6
		for _, field := range fields {
			length, err := CanonicalValueEncodedLength(field)
			if err != nil {
				return 0, err
			}
			total, err = add(total, 4+length)
			if err != nil {
				return 0, err
			}
		}
		return total, nil
	default:
		return 0, errors.New("invalid RiffDB value kind")
	}
}

func RecordFields(value Value) (map[string]Value, error) {
	fields, ok := value.Value.(map[string]any)
	if value.Type != "record" || !ok {
		if typed, valid := value.Value.(map[string]Value); valid && value.Type == "record" {
			return typed, nil
		}
		return nil, errors.New("invalid RiffDB record value")
	}
	output := make(map[string]Value, len(fields))
	for name, raw := range fields {
		field, ok := valueFromDecoded(raw)
		if !ok || validateValue(field, 0) != nil {
			return nil, errors.New("invalid RiffDB record value")
		}
		output[name] = field
	}
	return output, nil
}

// valueFromDecoded reads one already-decoded driver value without a second
// pass through encoding/json.
//
// Value carries only Type and Value, so marshalling a decoded field and
// immediately unmarshalling it back cost two reflective JSON operations per
// field and per list item -- 250 of them for a twenty-five row, five column
// response -- to reproduce the map that was already in hand. Unmarshalling into
// Value ignores unknown keys and requires a string "type", which is what this
// reproduces; nested values stay the decoded any they already were.
func valueFromDecoded(raw any) (Value, bool) {
	object, ok := raw.(map[string]any)
	if !ok {
		return Value{}, false
	}
	kind, ok := object["type"].(string)
	if !ok {
		return Value{}, false
	}
	return Value{Type: kind, Value: object["value"]}, true
}
func ListItems(value Value) ([]Value, error) {
	if value.Type != "list" {
		return nil, errors.New("invalid RiffDB list value")
	}
	if typed, ok := value.Value.([]Value); ok {
		return typed, nil
	}
	raw, ok := value.Value.([]any)
	if !ok {
		return nil, errors.New("invalid RiffDB list value")
	}
	output := make([]Value, len(raw))
	for index, item := range raw {
		value, ok := valueFromDecoded(item)
		if !ok || validateValue(value, 0) != nil {
			return nil, errors.New("invalid RiffDB list value")
		}
		output[index] = value
	}
	return output, nil
}
func BoolValue(value Value) (bool, error) {
	result, ok := value.Value.(bool)
	if value.Type != "bool" || !ok {
		return false, errors.New("invalid RiffDB bool value")
	}
	return result, nil
}
func I64Value(value Value) (int64, error) {
	text, ok := value.Value.(string)
	if value.Type != "i64" || !ok {
		return 0, errors.New("invalid RiffDB i64 value")
	}
	result, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, errors.New("invalid RiffDB i64 value")
	}
	return result, nil
}
func U64Value(value Value) (uint64, error) {
	text, ok := value.Value.(string)
	if value.Type != "u64" || !ok {
		return 0, errors.New("invalid RiffDB u64 value")
	}
	result, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, errors.New("invalid RiffDB u64 value")
	}
	return result, nil
}
func StringValue(value Value) (string, error) {
	result, ok := value.Value.(string)
	if value.Type != "string" || !ok {
		return "", errors.New("invalid RiffDB string value")
	}
	return result, nil
}
func UUIDValue(value Value) (string, error) {
	result, ok := value.Value.(string)
	if value.Type != "uuid" || !ok {
		return "", errors.New("invalid RiffDB UUID value")
	}
	return result, nil
}
func EnumValue(value Value) (string, error) {
	result, ok := value.Value.(string)
	if value.Type != "enum" || !ok || !symbolPattern.MatchString(result) {
		return "", errors.New("invalid RiffDB enum value")
	}
	return result, nil
}
func BytesValue(value Value) ([]byte, error) {
	text, ok := value.Value.(string)
	if value.Type != "bytes" || !ok {
		return nil, errors.New("invalid RiffDB bytes value")
	}
	result, err := base64.StdEncoding.Strict().DecodeString(text)
	if err != nil {
		return nil, errors.New("invalid RiffDB bytes value")
	}
	return result, nil
}
func DateValue(value Value) (int32, error) {
	text, ok := value.Value.(string)
	if value.Type != "date" || !ok {
		return 0, errors.New("invalid RiffDB date value")
	}
	result, err := strconv.ParseInt(text, 10, 32)
	if err != nil {
		return 0, errors.New("invalid RiffDB date value")
	}
	return int32(result), nil
}
func TimestampValue(value Value) (Instant, error) {
	if value.Type != "timestamp" {
		return Instant{}, errors.New("invalid RiffDB timestamp value")
	}
	encoded, err := json.Marshal(value.Value)
	if err != nil {
		return Instant{}, errors.New("invalid RiffDB timestamp value")
	}
	var wire Timestamp
	if json.Unmarshal(encoded, &wire) != nil || wire.Nanos > 999_999_999 {
		return Instant{}, errors.New("invalid RiffDB timestamp value")
	}
	seconds, err := strconv.ParseInt(wire.Seconds, 10, 64)
	if err != nil {
		return Instant{}, errors.New("invalid RiffDB timestamp value")
	}
	return Instant{Seconds: seconds, Nanos: wire.Nanos}, nil
}
func DecimalValue(value Value) (ExactDecimal, error) {
	if value.Type != "decimal" {
		return ExactDecimal{}, errors.New("invalid RiffDB decimal value")
	}
	encoded, err := json.Marshal(value.Value)
	if err != nil {
		return ExactDecimal{}, errors.New("invalid RiffDB decimal value")
	}
	var wire Decimal
	if json.Unmarshal(encoded, &wire) != nil || wire.Precision == nil {
		return ExactDecimal{}, errors.New("invalid RiffDB decimal value")
	}
	coefficient, err := base64.StdEncoding.Strict().DecodeString(wire.Coefficient)
	if err != nil || len(coefficient) < 1 || len(coefficient) > 16 {
		return ExactDecimal{}, errors.New("invalid RiffDB decimal value")
	}
	return ExactDecimal{CoefficientTwosComplement: coefficient, Scale: wire.Scale, Precision: *wire.Precision}, nil
}
func DecimalValueWithSchema(value Value, precision uint32, scale uint32) (ExactDecimal, error) {
	if value.Type != "decimal" || precision == 0 {
		return ExactDecimal{}, errors.New("invalid RiffDB decimal value")
	}
	encoded, err := json.Marshal(value.Value)
	if err != nil {
		return ExactDecimal{}, errors.New("invalid RiffDB decimal value")
	}
	var wire Decimal
	if json.Unmarshal(encoded, &wire) != nil || wire.Scale != scale || (wire.Precision != nil && *wire.Precision != precision) {
		return ExactDecimal{}, errors.New("invalid RiffDB decimal value")
	}
	coefficient, err := base64.StdEncoding.Strict().DecodeString(wire.Coefficient)
	if err != nil || len(coefficient) < 1 || len(coefficient) > 16 {
		return ExactDecimal{}, errors.New("invalid RiffDB decimal value")
	}
	return ExactDecimal{CoefficientTwosComplement: coefficient, Scale: scale, Precision: precision}, nil
}
func MoneyValue(value Value) (ExactMoney, error) {
	if value.Type != "money" {
		return ExactMoney{}, errors.New("invalid RiffDB money value")
	}
	encoded, err := json.Marshal(value.Value)
	if err != nil {
		return ExactMoney{}, errors.New("invalid RiffDB money value")
	}
	var wire Money
	if json.Unmarshal(encoded, &wire) != nil || !regexp.MustCompile(`^[A-Z]{3}$`).MatchString(wire.Currency) {
		return ExactMoney{}, errors.New("invalid RiffDB money value")
	}
	decimal, err := DecimalValue(Value{Type: "decimal", Value: wire.Amount})
	if err != nil {
		return ExactMoney{}, err
	}
	return ExactMoney{Currency: wire.Currency, Amount: decimal}, nil
}
func VectorValue(value Value) ([]float32, error) {
	if value.Type != "vector" {
		return nil, errors.New("invalid RiffDB vector value")
	}
	var bits []uint32
	if typed, ok := value.Value.(Vector); ok {
		bits = typed.ComponentBits
	} else {
		object, ok := value.Value.(map[string]any)
		if !ok || len(object) != 1 {
			return nil, errors.New("invalid RiffDB vector value")
		}
		raw, ok := object["component_bits"].([]any)
		if !ok {
			return nil, errors.New("invalid RiffDB vector value")
		}
		bits = make([]uint32, len(raw))
		for index, item := range raw {
			number, ok := item.(float64)
			if !ok || number < 0 || number > 4_294_967_295 || math.Trunc(number) != number {
				return nil, errors.New("invalid RiffDB vector value")
			}
			bits[index] = uint32(number)
		}
	}
	if len(bits) == 0 || len(bits) > maxCollection {
		return nil, errors.New("invalid RiffDB vector value")
	}
	output := make([]float32, len(bits))
	for index, bitPattern := range bits {
		component := math.Float32frombits(bitPattern)
		if math.IsNaN(float64(component)) || math.IsInf(float64(component), 0) {
			return nil, errors.New("invalid RiffDB vector value")
		}
		if component == 0 {
			component = 0
		}
		output[index] = component
	}
	return output, nil
}
func DecodeOptional[T any](value Value, decode func(Value) (T, error)) (*T, error) {
	if value.Type == "null" {
		return nil, nil
	}
	decoded, err := decode(value)
	if err != nil {
		return nil, err
	}
	return &decoded, nil
}
func DecodeValues[T any](value Value, decode func(Value) (T, error)) ([]T, error) {
	items, err := ListItems(value)
	if err != nil {
		return nil, err
	}
	output := make([]T, len(items))
	for index, item := range items {
		output[index], err = decode(item)
		if err != nil {
			return nil, err
		}
	}
	return output, nil
}

type Identity struct {
	ApplicationManifestHash string
	OperationCatalogHash    string
	ContractLineage         string
	ContractVersion         uint64
	ContractBundleHash      string
	Database                string
	Role                    string
	RoleDefinitionHash      string
	RemoteIdentityHash      string
}

type Operation struct {
	Name            string
	InputSchemaHash string
}

type Options struct {
	Deadline            time.Duration
	MaximumAttempts     uint32
	ReadAfterCommit     *uint64
	Cursor              string
	QueryConsistency    QueryConsistency
	AcceptCompactResult bool
	AcceptPackedResult  bool
}

// QueryConsistency is the closed set of stronger generated-query guarantees.
type QueryConsistency string

const (
	// AdmissionHead fences the first page at the server-observed admission head.
	AdmissionHead QueryConsistency = "admission_head"
)

func (options Options) wire() (wireOptions, error) {
	deadline := options.Deadline
	if deadline == 0 {
		deadline = 30 * time.Second
	}
	attempts := options.MaximumAttempts
	if attempts == 0 {
		attempts = 3
	}
	if deadline < time.Millisecond || deadline > 5*time.Minute || attempts < 1 || attempts > 10 || len(options.Cursor) > 16_384 || (options.QueryConsistency != "" && options.QueryConsistency != AdmissionHead) || options.AcceptPackedResult && !options.AcceptCompactResult {
		return wireOptions{}, errors.New("invalid RiffDB driver invocation options")
	}
	deadlineMillis := deadline.Milliseconds()
	return wireOptions{DeadlineMillis: uint64(deadlineMillis), MaximumAttempts: attempts, ReadAfterCommit: options.ReadAfterCommit, Cursor: optionalString(options.Cursor), QueryConsistency: optionalQueryConsistency(options.QueryConsistency), AcceptCompactResult: options.AcceptCompactResult, AcceptPackedResult: options.AcceptPackedResult}, nil
}

type CompactQueryResult struct {
	Outcome    string
	ResultName string
	Entity     string
	Fields     []string
	Rows       [][]Value
}

type PackedColumn struct {
	Data    []byte
	Offsets []uint32
}

type PackedQueryResult struct {
	Outcome    string
	ResultName string
	Entity     string
	Fields     []string
	RowCount   uint32
	Columns    []PackedColumn
}

func packedTag(value []byte, tag byte, size int) error {
	if len(value) != size || len(value) < 2 || value[0] != 1 || value[1] != tag {
		return errors.New("invalid RiffDB packed value")
	}
	return nil
}

func PackedIsNull(value []byte) bool { return len(value) == 2 && value[0] == 1 && value[1] == 0 }
func PackedBool(value []byte) (bool, error) {
	if err := packedTag(value, 1, 3); err != nil || value[2] > 1 {
		return false, errors.New("invalid RiffDB packed bool")
	}
	return value[2] == 1, nil
}
func PackedI64(value []byte) (int64, error) {
	if err := packedTag(value, 2, 10); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(value[2:])), nil
}
func PackedU64(value []byte) (uint64, error) {
	if err := packedTag(value, 3, 10); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(value[2:]), nil
}
func PackedString(value []byte, maximum int) (string, error) {
	if len(value) < 6 || value[0] != 1 || value[1] != 6 {
		return "", errors.New("invalid RiffDB packed string")
	}
	length := int(binary.BigEndian.Uint32(value[2:6]))
	if length > maximum || length != len(value)-6 || !utf8.Valid(value[6:]) {
		return "", errors.New("invalid RiffDB packed string")
	}
	return string(value[6:]), nil
}
func PackedTimestamp(value []byte) (Instant, error) {
	if err := packedTag(value, 8, 14); err != nil {
		return Instant{}, err
	}
	nanos := binary.BigEndian.Uint32(value[10:])
	if nanos >= 1_000_000_000 {
		return Instant{}, errors.New("invalid RiffDB packed timestamp")
	}
	return Instant{Seconds: int64(binary.BigEndian.Uint64(value[2:10])), Nanos: nanos}, nil
}
func PackedDate(value []byte) (int32, error) {
	if err := packedTag(value, 9, 6); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(value[2:])), nil
}
func PackedUUID(value []byte) (string, error) {
	if err := packedTag(value, 10, 18); err != nil {
		return "", err
	}
	b := value[2:]
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
func PackedEnum(value []byte) (uint32, uint32, error) {
	if err := packedTag(value, 11, 10); err != nil {
		return 0, 0, err
	}
	typeID, variantID := binary.BigEndian.Uint32(value[2:6]), binary.BigEndian.Uint32(value[6:10])
	if typeID == 0 || variantID == 0 {
		return 0, 0, errors.New("invalid RiffDB packed enum")
	}
	return typeID, variantID, nil
}

type Result struct {
	Value           Value
	Compact         *CompactQueryResult
	Packed          *PackedQueryResult
	ApplicationHead *uint64
	Cursor          string
	Replayed        bool
}
type BatchSuccess struct {
	Value          Value
	CommitSequence *uint64
	OutcomeURI     string
	Replayed       bool
}
type BatchItem struct {
	Index  uint32
	Result *BatchSuccess
	Error  *ApplicationError
}
type BatchResult struct {
	Items      []BatchItem
	Checkpoint uint32
	Total      uint32
}

type ErrorDetails struct {
	Code             string
	Category         string
	Operation        string
	SymbolPath       []string
	ContractLineage  string
	ContractVersion  *uint64
	TraceID          string
	IncidentID       string
	Message          string
	Retryability     string
	RecoveryAction   string
	OutcomeUncertain bool
}

type ApplicationError struct{ Details ErrorDetails }

func (err *ApplicationError) Error() string { return err.Details.Code + ": " + err.Details.Message }

type Session struct {
	connection    net.Conn
	reader        *bufio.Reader
	identity      Identity
	writeMu       sync.Mutex
	mu            sync.Mutex
	pending       map[string]chan packet
	requestPrefix string
	next          atomic.Uint64
	closed        chan struct{}
	closeOnce     sync.Once
}

type packet struct {
	body []byte
	err  error
}

func Connect(ctx context.Context, socketPath string, identity Identity) (*Session, error) {
	if !strings.HasPrefix(socketPath, "/") || len(socketPath) > 4_096 || !validIdentity(identity) {
		return nil, errors.New("invalid RiffDB driver configuration")
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	if err != nil {
		return nil, errors.New("RiffDB driver session failed")
	}
	session := &Session{connection: connection, reader: bufio.NewReader(connection), identity: identity, pending: make(map[string]chan packet), requestPrefix: newSessionRequestPrefix(), closed: make(chan struct{})}
	go session.readLoop()
	requestID := session.requestID("handshake")
	request := handshakeRequest{Type: "handshake", RequestID: requestID, ProtocolVersion: ProtocolVersion,
		ApplicationManifestHash: identity.ApplicationManifestHash, OperationCatalogHash: identity.OperationCatalogHash,
		ContractLineage: identity.ContractLineage, ContractVersion: identity.ContractVersion, ContractBundleHash: identity.ContractBundleHash,
		ValueRegistryHash: ValueRegistryHash, ErrorRegistryHash: ErrorRegistryHash, Database: identity.Database, Role: identity.Role,
		RoleDefinitionHash: identity.RoleDefinitionHash, RemoteIdentityHash: identity.RemoteIdentityHash}
	body, err := session.call(ctx, requestID, request, false)
	if err != nil {
		session.Close()
		return nil, err
	}
	var response handshakeResponse
	if json.Unmarshal(body, &response) != nil || response.Type != "handshake" || response.ProtocolVersion != ProtocolVersion ||
		response.ApplicationManifestHash != identity.ApplicationManifestHash || response.OperationCatalogHash != identity.OperationCatalogHash ||
		response.ContractLineage != identity.ContractLineage || response.ContractVersion != identity.ContractVersion ||
		response.ContractBundleHash != identity.ContractBundleHash || response.Database != identity.Database || response.Role != identity.Role ||
		response.RoleDefinitionHash != identity.RoleDefinitionHash || response.RemoteIdentityHash != identity.RemoteIdentityHash {
		session.Close()
		return nil, errors.New("RiffDB driver identity mismatch")
	}
	return session, nil
}

func (session *Session) Invoke(ctx context.Context, operation Operation, input map[string]Value, options Options) (Result, error) {
	if err := validateOperation(operation, input); err != nil {
		return Result{}, err
	}
	wire, err := options.wire()
	if err != nil {
		return Result{}, err
	}
	requestID := session.requestID("invoke")
	body, err := session.call(ctx, requestID, invokeRequest{Type: "invoke", RequestID: requestID, Operation: operation.Name, InputSchemaHash: operation.InputSchemaHash, Input: input, Options: wire}, true)
	if err != nil {
		return Result{}, err
	}
	return decodeResult(body)
}

func (session *Session) Batch(ctx context.Context, operation Operation, items []map[string]Value, concurrency, checkpoint uint32, options Options) (BatchResult, error) {
	if len(items) < 1 || len(items) > maxCollection || concurrency < 1 || concurrency > 384 || int(checkpoint) > len(items) {
		return BatchResult{}, errors.New("invalid RiffDB driver batch bounds")
	}
	for _, input := range items {
		if err := validateOperation(operation, input); err != nil {
			return BatchResult{}, err
		}
	}
	wire, err := options.wire()
	if err != nil {
		return BatchResult{}, err
	}
	if wire.ReadAfterCommit != nil || wire.Cursor != nil || wire.QueryConsistency != nil {
		return BatchResult{}, errors.New("invalid RiffDB driver batch options")
	}
	requestID := session.requestID("batch")
	body, err := session.call(ctx, requestID, batchRequest{Type: "batch", RequestID: requestID, Operation: operation.Name, InputSchemaHash: operation.InputSchemaHash, Items: items, Concurrency: concurrency, Checkpoint: checkpoint, Options: wire}, true)
	if err != nil {
		return BatchResult{}, err
	}
	return decodeBatch(body)
}

func (session *Session) Close() error {
	var err error
	session.closeOnce.Do(func() {
		close(session.closed)
		err = session.connection.Close()
		session.fail(errors.New("RiffDB driver session closed"))
	})
	return err
}

func (session *Session) call(ctx context.Context, requestID string, request any, cancellable bool) ([]byte, error) {
	body, err := json.Marshal(request)
	if err != nil || len(body) < 2 || len(body) > maxFrameBytes {
		return nil, errors.New("invalid RiffDB driver request")
	}
	response := make(chan packet, 1)
	session.mu.Lock()
	if len(session.pending) >= maxPending {
		session.mu.Unlock()
		return nil, errors.New("RiffDB driver session is over capacity")
	}
	select {
	case <-session.closed:
		session.mu.Unlock()
		return nil, errors.New("RiffDB driver session closed")
	default:
	}
	session.pending[requestID] = response
	session.mu.Unlock()
	if err := session.writeFrame(body); err != nil {
		session.remove(requestID)
		return nil, err
	}
	contextDone := ctx.Done()
	for {
		select {
		case packet := <-response:
			if packet.err != nil {
				return nil, packet.err
			}
			return decodeEnvelope(packet.body)
		case <-contextDone:
			if !cancellable {
				session.remove(requestID)
				return nil, ctx.Err()
			}
			contextDone = nil
			cancelID := session.requestID("cancel")
			cancelBody, _ := json.Marshal(cancelRequest{Type: "cancel", RequestID: cancelID, TargetRequestID: requestID})
			_ = session.writeFrame(cancelBody)
		}
	}
}

func (session *Session) writeFrame(body []byte) error {
	session.writeMu.Lock()
	defer session.writeMu.Unlock()
	if len(body) < 2 || len(body) > maxFrameBytes {
		return errors.New("invalid RiffDB driver frame")
	}
	prefix := []byte{byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body))}
	if _, err := session.connection.Write(append(prefix, body...)); err != nil {
		return errors.New("RiffDB driver session failed")
	}
	return nil
}

func (session *Session) readLoop() {
	for {
		prefix := make([]byte, 4)
		if _, err := io.ReadFull(session.reader, prefix); err != nil {
			session.fail(errors.New("RiffDB driver session failed"))
			return
		}
		length := int(prefix[0])<<24 | int(prefix[1])<<16 | int(prefix[2])<<8 | int(prefix[3])
		if length < 2 || length > maxFrameBytes {
			session.fail(errors.New("RiffDB driver returned an invalid frame"))
			session.connection.Close()
			return
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(session.reader, body); err != nil {
			session.fail(errors.New("RiffDB driver session failed"))
			return
		}
		var header struct {
			RequestID *string `json:"request_id"`
		}
		if json.Unmarshal(body, &header) != nil || header.RequestID == nil || !requestPattern.MatchString(*header.RequestID) {
			session.fail(errors.New("RiffDB driver returned an invalid message"))
			session.connection.Close()
			return
		}
		session.mu.Lock()
		response := session.pending[*header.RequestID]
		delete(session.pending, *header.RequestID)
		session.mu.Unlock()
		if response != nil {
			response <- packet{body: body}
		}
	}
}

func (session *Session) fail(err error) {
	session.mu.Lock()
	pending := session.pending
	session.pending = make(map[string]chan packet)
	session.mu.Unlock()
	for _, response := range pending {
		response <- packet{err: err}
	}
}
func (session *Session) remove(id string) {
	session.mu.Lock()
	delete(session.pending, id)
	session.mu.Unlock()
}
func (session *Session) requestID(kind string) string {
	return fmt.Sprintf("%s.%s.%d", session.requestPrefix, kind, session.next.Add(1))
}

func newSessionRequestPrefix() string {
	return fmt.Sprintf("go.%d.%d", os.Getpid(), nextSession.Add(1))
}

type wireOptions struct {
	DeadlineMillis      uint64            `json:"deadline_millis"`
	MaximumAttempts     uint32            `json:"maximum_attempts"`
	ReadAfterCommit     *uint64           `json:"read_after_commit"`
	Cursor              *string           `json:"cursor"`
	QueryConsistency    *QueryConsistency `json:"query_consistency"`
	AcceptCompactResult bool              `json:"accept_compact_result"`
	AcceptPackedResult  bool              `json:"accept_packed_result"`
}
type handshakeRequest struct {
	Type                    string `json:"type"`
	RequestID               string `json:"request_id"`
	ProtocolVersion         uint32 `json:"protocol_version"`
	ApplicationManifestHash string `json:"application_manifest_hash"`
	OperationCatalogHash    string `json:"operation_catalog_hash"`
	ContractLineage         string `json:"contract_lineage"`
	ContractVersion         uint64 `json:"contract_version"`
	ContractBundleHash      string `json:"contract_bundle_hash"`
	ValueRegistryHash       string `json:"value_registry_hash"`
	ErrorRegistryHash       string `json:"error_registry_hash"`
	Database                string `json:"database"`
	Role                    string `json:"role"`
	RoleDefinitionHash      string `json:"role_definition_hash"`
	RemoteIdentityHash      string `json:"remote_identity_hash"`
}
type handshakeResponse struct {
	Type                    string `json:"type"`
	RequestID               string `json:"request_id"`
	ProtocolVersion         uint32 `json:"protocol_version"`
	ApplicationManifestHash string `json:"application_manifest_hash"`
	OperationCatalogHash    string `json:"operation_catalog_hash"`
	ContractLineage         string `json:"contract_lineage"`
	ContractVersion         uint64 `json:"contract_version"`
	ContractBundleHash      string `json:"contract_bundle_hash"`
	Database                string `json:"database"`
	Role                    string `json:"role"`
	RoleDefinitionHash      string `json:"role_definition_hash"`
	RemoteIdentityHash      string `json:"remote_identity_hash"`
}
type invokeRequest struct {
	Type            string           `json:"type"`
	RequestID       string           `json:"request_id"`
	Operation       string           `json:"operation"`
	InputSchemaHash string           `json:"input_schema_hash"`
	Input           map[string]Value `json:"input"`
	Options         wireOptions      `json:"options"`
}
type batchRequest struct {
	Type            string             `json:"type"`
	RequestID       string             `json:"request_id"`
	Operation       string             `json:"operation"`
	InputSchemaHash string             `json:"input_schema_hash"`
	Items           []map[string]Value `json:"items"`
	Concurrency     uint32             `json:"concurrency"`
	Checkpoint      uint32             `json:"checkpoint"`
	Options         wireOptions        `json:"options"`
}
type cancelRequest struct {
	Type            string `json:"type"`
	RequestID       string `json:"request_id"`
	TargetRequestID string `json:"target_request_id"`
}
type resultResponse struct {
	Type            string  `json:"type"`
	RequestID       string  `json:"request_id"`
	Value           Value   `json:"value"`
	ApplicationHead *uint64 `json:"application_head"`
	Cursor          *string `json:"cursor"`
	Replayed        bool    `json:"replayed"`
}
type compactQueryResultResponse struct {
	Type            string    `json:"type"`
	RequestID       string    `json:"request_id"`
	Outcome         string    `json:"outcome"`
	ResultName      string    `json:"result_name"`
	Entity          string    `json:"entity"`
	Fields          []string  `json:"fields"`
	Rows            [][]Value `json:"rows"`
	ApplicationHead uint64    `json:"application_head"`
	Cursor          *string   `json:"cursor"`
}
type packedQueryResultResponse struct {
	Type       string   `json:"type"`
	RequestID  string   `json:"request_id"`
	Outcome    string   `json:"outcome"`
	ResultName string   `json:"result_name"`
	Entity     string   `json:"entity"`
	Fields     []string `json:"fields"`
	RowCount   uint32   `json:"row_count"`
	Columns    []struct {
		Data    []byte   `json:"data"`
		Offsets []uint32 `json:"offsets"`
	} `json:"columns"`
	ApplicationHead uint64  `json:"application_head"`
	Cursor          *string `json:"cursor"`
}
type batchResponse struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id"`
	Items     []struct {
		Index   uint32          `json:"index"`
		Outcome json.RawMessage `json:"outcome"`
	} `json:"items"`
	Checkpoint uint32 `json:"checkpoint"`
	Total      uint32 `json:"total"`
}
type errorResponse struct {
	Type             string   `json:"type"`
	Code             string   `json:"code"`
	Category         string   `json:"category"`
	Operation        *string  `json:"operation"`
	SymbolPath       []string `json:"symbol_path"`
	ContractLineage  *string  `json:"contract_lineage"`
	ContractVersion  *uint64  `json:"contract_version"`
	TraceID          *string  `json:"trace_id"`
	IncidentID       *string  `json:"incident_id"`
	Message          string   `json:"message"`
	Retryability     *string  `json:"retryability"`
	RecoveryAction   string   `json:"recovery_action"`
	OutcomeUncertain bool     `json:"outcome_uncertain"`
}

func decodeEnvelope(body []byte) ([]byte, error) {
	var header struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(body, &header) != nil {
		return nil, errors.New("RiffDB driver returned an invalid message")
	}
	if header.Type == "error" {
		err, decodeErr := decodeApplicationError(body)
		if decodeErr != nil {
			return nil, decodeErr
		}
		return nil, err
	}
	return body, nil
}
func decodeResult(body []byte) (Result, error) {
	var header struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(body, &header) != nil {
		return Result{}, errors.New("RiffDB driver returned an invalid result")
	}
	if header.Type == "compact_query_result" {
		var response compactQueryResultResponse
		if json.Unmarshal(body, &response) != nil || !symbolPattern.MatchString(response.Outcome) || !symbolPattern.MatchString(response.ResultName) || !symbolPattern.MatchString(response.Entity) || len(response.Fields) < 1 || len(response.Fields) > maxCollection || len(response.Rows) > maxCollection || (response.Cursor != nil && len(*response.Cursor) > 16_384) {
			return Result{}, errors.New("RiffDB driver returned an invalid compact result")
		}
		for index, field := range response.Fields {
			if !symbolPattern.MatchString(field) || (index > 0 && response.Fields[index-1] >= field) {
				return Result{}, errors.New("RiffDB driver returned an invalid compact result")
			}
		}
		for _, row := range response.Rows {
			if len(row) != len(response.Fields) {
				return Result{}, errors.New("RiffDB driver returned an invalid compact result")
			}
			for _, value := range row {
				if validateValue(value, 0) != nil {
					return Result{}, errors.New("RiffDB driver returned an invalid compact result")
				}
			}
		}
		compact := &CompactQueryResult{Outcome: response.Outcome, ResultName: response.ResultName, Entity: response.Entity, Fields: response.Fields, Rows: response.Rows}
		result := Result{Compact: compact, ApplicationHead: &response.ApplicationHead}
		if response.Cursor != nil {
			result.Cursor = *response.Cursor
		}
		return result, nil
	}
	if header.Type == "packed_query_result" {
		var response packedQueryResultResponse
		if json.Unmarshal(body, &response) != nil || !symbolPattern.MatchString(response.Outcome) || !symbolPattern.MatchString(response.ResultName) || !symbolPattern.MatchString(response.Entity) || len(response.Fields) < 1 || len(response.Fields) > maxCollection || response.RowCount > maxCollection || len(response.Columns) != len(response.Fields) || (response.Cursor != nil && len(*response.Cursor) > 16_384) {
			return Result{}, errors.New("RiffDB driver returned an invalid packed result")
		}
		for index, field := range response.Fields {
			if !symbolPattern.MatchString(field) || (index > 0 && response.Fields[index-1] >= field) {
				return Result{}, errors.New("RiffDB driver returned an invalid packed result")
			}
		}
		columns := make([]PackedColumn, len(response.Columns))
		for index, column := range response.Columns {
			if len(column.Offsets) != int(response.RowCount)+1 || len(column.Offsets) == 0 || column.Offsets[0] != 0 || uint64(column.Offsets[len(column.Offsets)-1]) != uint64(len(column.Data)) {
				return Result{}, errors.New("RiffDB driver returned an invalid packed result")
			}
			for offset := 1; offset < len(column.Offsets); offset++ {
				if column.Offsets[offset-1] > column.Offsets[offset] || uint64(column.Offsets[offset]) > uint64(len(column.Data)) {
					return Result{}, errors.New("RiffDB driver returned an invalid packed result")
				}
			}
			columns[index] = PackedColumn{Data: column.Data, Offsets: column.Offsets}
		}
		packed := &PackedQueryResult{Outcome: response.Outcome, ResultName: response.ResultName, Entity: response.Entity, Fields: response.Fields, RowCount: response.RowCount, Columns: columns}
		result := Result{Packed: packed, ApplicationHead: &response.ApplicationHead}
		if response.Cursor != nil {
			result.Cursor = *response.Cursor
		}
		return result, nil
	}
	var response resultResponse
	if json.Unmarshal(body, &response) != nil || response.Type != "result" || validateValue(response.Value, 0) != nil || (response.Cursor != nil && len(*response.Cursor) > 16_384) {
		return Result{}, errors.New("RiffDB driver returned an invalid result")
	}
	result := Result{Value: response.Value, ApplicationHead: response.ApplicationHead, Replayed: response.Replayed}
	if response.Cursor != nil {
		result.Cursor = *response.Cursor
	}
	return result, nil
}
func decodeBatch(body []byte) (BatchResult, error) {
	var response batchResponse
	if json.Unmarshal(body, &response) != nil || response.Type != "batch_result" || len(response.Items) > maxCollection || response.Total == 0 || response.Checkpoint > response.Total {
		return BatchResult{}, errors.New("RiffDB driver returned an invalid batch result")
	}
	result := BatchResult{Checkpoint: response.Checkpoint, Total: response.Total, Items: make([]BatchItem, 0, len(response.Items))}
	for _, item := range response.Items {
		if item.Index >= response.Total {
			return BatchResult{}, errors.New("RiffDB driver returned an invalid batch item")
		}
		var header struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(item.Outcome, &header) != nil {
			return BatchResult{}, errors.New("RiffDB driver returned an invalid batch item")
		}
		if header.Type == "result" {
			var outcome struct {
				Type           string  `json:"type"`
				Value          Value   `json:"value"`
				CommitSequence *uint64 `json:"commit_sequence"`
				OutcomeURI     *string `json:"outcome_uri"`
				Replayed       bool    `json:"replayed"`
			}
			if json.Unmarshal(item.Outcome, &outcome) != nil || validateValue(outcome.Value, 0) != nil {
				return BatchResult{}, errors.New("RiffDB driver returned an invalid batch item")
			}
			success := &BatchSuccess{Value: outcome.Value, CommitSequence: outcome.CommitSequence, Replayed: outcome.Replayed}
			if outcome.OutcomeURI != nil {
				success.OutcomeURI = *outcome.OutcomeURI
			}
			result.Items = append(result.Items, BatchItem{Index: item.Index, Result: success})
		} else if header.Type == "error" {
			appErr, err := decodeApplicationError(item.Outcome)
			if err != nil {
				return BatchResult{}, err
			}
			result.Items = append(result.Items, BatchItem{Index: item.Index, Error: appErr})
		} else {
			return BatchResult{}, errors.New("RiffDB driver returned an invalid batch item")
		}
	}
	return result, nil
}

func decodeApplicationError(body []byte) (*ApplicationError, error) {
	var response errorResponse
	if json.Unmarshal(body, &response) != nil || response.Type != "error" || !regexp.MustCompile(`^[A-Z0-9-]{1,64}$`).MatchString(response.Code) || !regexp.MustCompile(`^[a-z_]{1,64}$`).MatchString(response.Category) || len(response.Message) < 1 || len(response.Message) > 4_096 || len(response.SymbolPath) > 16 || len(response.RecoveryAction) > 64 {
		return nil, errors.New("RiffDB driver returned an invalid error")
	}
	for _, symbol := range response.SymbolPath {
		if !symbolPattern.MatchString(symbol) {
			return nil, errors.New("RiffDB driver returned an invalid error")
		}
	}
	if (response.ContractLineage == nil) != (response.ContractVersion == nil) {
		return nil, errors.New("RiffDB driver returned an invalid error")
	}
	retryability := "not_retryable"
	if response.Retryability != nil {
		retryability = *response.Retryability
	}
	details := ErrorDetails{Code: response.Code, Category: response.Category, SymbolPath: append([]string(nil), response.SymbolPath...), ContractVersion: response.ContractVersion, Message: response.Message, Retryability: retryability, RecoveryAction: response.RecoveryAction, OutcomeUncertain: response.OutcomeUncertain}
	if response.Operation != nil {
		details.Operation = *response.Operation
	}
	if response.ContractLineage != nil {
		details.ContractLineage = *response.ContractLineage
	}
	if response.TraceID != nil {
		details.TraceID = *response.TraceID
	}
	if response.IncidentID != nil {
		details.IncidentID = *response.IncidentID
	}
	return &ApplicationError{Details: details}, nil
}

func validateOperation(operation Operation, input map[string]Value) error {
	if !symbolPattern.MatchString(operation.Name) || !hashPattern.MatchString(operation.InputSchemaHash) || len(input) > maxCollection {
		return errors.New("invalid generated RiffDB operation")
	}
	names := make([]string, 0, len(input))
	for name := range input {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !symbolPattern.MatchString(name) || validateValue(input[name], 0) != nil {
			return errors.New("invalid generated RiffDB operation input")
		}
	}
	return nil
}
func validateValue(value Value, depth int) error {
	if depth > maxDepth {
		return errors.New("RiffDB driver value exceeds its depth bound")
	}
	switch value.Type {
	case "null":
		if value.Value != nil {
			return errors.New("invalid value")
		}
	case "bool":
		if _, ok := value.Value.(bool); !ok {
			return errors.New("invalid value")
		}
	case "i64":
		text, ok := value.Value.(string)
		if !ok {
			return errors.New("invalid value")
		}
		if _, err := strconv.ParseInt(text, 10, 64); err != nil {
			return errors.New("invalid value")
		}
	case "u64":
		text, ok := value.Value.(string)
		if !ok {
			return errors.New("invalid value")
		}
		if _, err := strconv.ParseUint(text, 10, 64); err != nil {
			return errors.New("invalid value")
		}
	case "string", "uuid", "enum", "bytes", "date":
		if _, ok := value.Value.(string); !ok {
			return errors.New("invalid value")
		}
	case "timestamp":
		if _, ok := value.Value.(map[string]any); !ok {
			if _, typed := value.Value.(Timestamp); !typed {
				return errors.New("invalid value")
			}
		}
	case "decimal":
		if _, ok := value.Value.(map[string]any); !ok {
			if _, typed := value.Value.(Decimal); !typed {
				return errors.New("invalid value")
			}
		}
	case "money":
		if _, ok := value.Value.(map[string]any); !ok {
			if _, typed := value.Value.(Money); !typed {
				return errors.New("invalid value")
			}
		}
	case "vector":
		var bits []uint32
		if typed, ok := value.Value.(Vector); ok {
			bits = typed.ComponentBits
		} else {
			object, ok := value.Value.(map[string]any)
			if !ok || len(object) != 1 {
				return errors.New("invalid value")
			}
			raw, ok := object["component_bits"].([]any)
			if !ok {
				return errors.New("invalid value")
			}
			bits = make([]uint32, len(raw))
			for index, item := range raw {
				number, ok := item.(float64)
				if !ok || number < 0 || number > 4_294_967_295 || math.Trunc(number) != number {
					return errors.New("invalid value")
				}
				bits[index] = uint32(number)
			}
		}
		if len(bits) == 0 || len(bits) > maxCollection {
			return errors.New("invalid value")
		}
		for _, bitPattern := range bits {
			if component := math.Float32frombits(bitPattern); math.IsNaN(float64(component)) || math.IsInf(float64(component), 0) {
				return errors.New("invalid value")
			}
		}
	case "list":
		values, ok := value.Value.([]any)
		if ok {
			if len(values) > maxCollection {
				return errors.New("invalid value")
			}
			for _, item := range values {
				object, valid := item.(map[string]any)
				if !valid {
					return errors.New("invalid value")
				}
				encoded, _ := json.Marshal(object)
				var nested Value
				if json.Unmarshal(encoded, &nested) != nil || validateValue(nested, depth+1) != nil {
					return errors.New("invalid value")
				}
			}
		} else if typed, valid := value.Value.([]Value); valid {
			for _, nested := range typed {
				if validateValue(nested, depth+1) != nil {
					return errors.New("invalid value")
				}
			}
		} else {
			return errors.New("invalid value")
		}
	case "record":
		record, ok := value.Value.(map[string]any)
		if ok {
			if len(record) > maxCollection {
				return errors.New("invalid value")
			}
			for name, item := range record {
				if !symbolPattern.MatchString(name) {
					return errors.New("invalid value")
				}
				encoded, _ := json.Marshal(item)
				var nested Value
				if json.Unmarshal(encoded, &nested) != nil || validateValue(nested, depth+1) != nil {
					return errors.New("invalid value")
				}
			}
		} else if typed, valid := value.Value.(map[string]Value); valid {
			for name, nested := range typed {
				if !symbolPattern.MatchString(name) || validateValue(nested, depth+1) != nil {
					return errors.New("invalid value")
				}
			}
		} else {
			return errors.New("invalid value")
		}
	default:
		return errors.New("invalid value")
	}
	return nil
}
func validIdentity(identity Identity) bool {
	return hashPattern.MatchString(identity.ApplicationManifestHash) && hashPattern.MatchString(identity.OperationCatalogHash) && symbolPattern.MatchString(identity.ContractLineage) && identity.ContractVersion > 0 && hashPattern.MatchString(identity.ContractBundleHash) && symbolPattern.MatchString(identity.Database) && symbolPattern.MatchString(identity.Role) && hashPattern.MatchString(identity.RoleDefinitionHash) && hashPattern.MatchString(identity.RemoteIdentityHash)
}
func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
func optionalQueryConsistency(value QueryConsistency) *QueryConsistency {
	if value == "" {
		return nil
	}
	return &value
}
