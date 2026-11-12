// Package application is the application-only local transport for generated
// RiffDB Go bindings. Remote trust, credentials, retries, and gRPC remain owned
// by the first-party Rust driver host.
package application

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
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
	hashPattern             = regexp.MustCompile(`^[0-9a-f]{64}$`)
	symbolPattern           = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,256}$`)
	requestPattern          = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
	tenantPattern           = regexp.MustCompile("^[\x21-\x7e]{1,128}$")
	envPrefixPattern        = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	int64Pattern            = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)$`)
	uint32Pattern           = regexp.MustCompile(`^(?:0|[1-9][0-9]*)$`)
	instantIsoPattern       = regexp.MustCompile(`^([+-]?\d{4,19})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|([+-])(\d{2}):(\d{2}))$`)
	negativeZeroYearPattern = regexp.MustCompile(`^-0+$`)
	nextSession             atomic.Uint64
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

func (decimal ExactDecimal) String() string {
	if len(decimal.CoefficientTwosComplement) == 0 {
		return "0"
	}
	coeff := new(big.Int).SetBytes(decimal.CoefficientTwosComplement)
	if decimal.CoefficientTwosComplement[0]&0x80 != 0 {
		modulus := new(big.Int).Lsh(big.NewInt(1), uint(len(decimal.CoefficientTwosComplement)*8))
		coeff.Sub(coeff, modulus)
	}
	negative := coeff.Sign() < 0
	if negative {
		coeff.Neg(coeff)
	}
	digits := coeff.String()
	scale := int(decimal.Scale)
	if scale == 0 {
		if negative {
			return "-" + digits
		}
		return digits
	}
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	split := len(digits) - scale
	formatted := digits[:split] + "." + digits[split:]
	if negative {
		return "-" + formatted
	}
	return formatted
}

func (decimal ExactDecimal) MarshalJSON() ([]byte, error) {
	return json.Marshal(decimal.String())
}

type ExactMoney struct {
	Currency string
	Amount   ExactDecimal
}

func (money ExactMoney) String() string {
	return money.Currency + " " + money.Amount.String()
}

func (money ExactMoney) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Currency string `json:"currency"`
		Amount   string `json:"amount"`
	}{
		Currency: money.Currency,
		Amount:   money.Amount.String(),
	})
}

type Instant struct {
	Seconds int64
	Nanos   uint32
}

func InstantFromTime(value time.Time) Instant {
	utc := value.UTC()
	return Instant{
		Seconds: utc.Unix(),
		Nanos:   uint32(utc.Nanosecond()),
	}
}

func (instant Instant) Time() time.Time {
	return time.Unix(instant.Seconds, int64(instant.Nanos)).UTC()
}

func (instant Instant) String() string {
	return formatInstantISO(instant.Seconds, instant.Nanos)
}

func isGregorianLeapYear(year int64) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

func daysInGregorianMonth(year int64, month int) int {
	switch month {
	case 1, 3, 5, 7, 8, 10, 12:
		return 31
	case 4, 6, 9, 11:
		return 30
	case 2:
		if isGregorianLeapYear(year) {
			return 29
		}
		return 28
	default:
		return 0
	}
}

func daysFromCivil(year int64, month int, day int) *big.Int {
	adjustedYear := big.NewInt(year)
	if month <= 2 {
		adjustedYear.Sub(adjustedYear, big.NewInt(1))
	}
	era := new(big.Int).Set(adjustedYear)
	if adjustedYear.Sign() < 0 {
		era.Sub(era, big.NewInt(399))
	}
	era.Quo(era, big.NewInt(400))
	yoe := new(big.Int).Sub(adjustedYear, new(big.Int).Mul(era, big.NewInt(400)))
	mp := int64(month + 9)
	if month > 2 {
		mp = int64(month - 3)
	}
	doy := (153*mp+2)/5 + int64(day) - 1
	yoeInt := yoe.Int64()
	doe := yoeInt*365 + yoeInt/4 - yoeInt/100 + doy
	result := new(big.Int).Mul(era, big.NewInt(146097))
	result.Add(result, big.NewInt(doe-719468))
	return result
}

func civilFromDays(days int64) (int64, int, int) {
	z := new(big.Int).Add(big.NewInt(days), big.NewInt(719468))
	era := new(big.Int).Set(z)
	if z.Sign() < 0 {
		era.Sub(era, big.NewInt(146096))
	}
	era.Quo(era, big.NewInt(146097))
	doe := new(big.Int).Sub(z, new(big.Int).Mul(era, big.NewInt(146097))).Int64()
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365
	y := new(big.Int).Add(big.NewInt(yoe), new(big.Int).Mul(era, big.NewInt(400))).Int64()
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	mp := (5*doy + 2) / 153
	d := int(doy - (153*mp+2)/5 + 1)
	var m int
	if mp < 10 {
		m = int(mp + 3)
	} else {
		m = int(mp - 9)
	}
	if m <= 2 {
		y++
	}
	return y, m, d
}

func formatInstantISO(seconds int64, nanos uint32) string {
	secBig := big.NewInt(seconds)
	daySecBig := new(big.Int).Mod(secBig, big.NewInt(86400))
	daysBig := new(big.Int).Quo(new(big.Int).Sub(secBig, daySecBig), big.NewInt(86400))
	year, month, day := civilFromDays(daysBig.Int64())
	daySec := daySecBig.Int64()
	hour := daySec / 3600
	minute := (daySec % 3600) / 60
	second := daySec % 60
	var yearPart string
	if year < 0 {
		yearPart = fmt.Sprintf("-%04d", -year)
	} else {
		yearPart = fmt.Sprintf("%04d", year)
	}
	var fracPart string
	if nanos > 0 {
		fracPart = "." + strings.TrimRight(fmt.Sprintf("%09d", nanos), "0")
	}
	return fmt.Sprintf("%s-%02d-%02dT%02d:%02d:%02d%sZ", yearPart, month, day, hour, minute, second, fracPart)
}

func parseInstantString(text string) (Instant, error) {
	matches := instantIsoPattern.FindStringSubmatch(text)
	if matches == nil || negativeZeroYearPattern.MatchString(matches[1]) {
		return Instant{}, errors.New("invalid RiffDB timestamp")
	}
	yearSource := matches[1]
	if strings.HasPrefix(yearSource, "+") {
		yearSource = yearSource[1:]
	}
	year, err := strconv.ParseInt(yearSource, 10, 64)
	if err != nil {
		return Instant{}, errors.New("invalid RiffDB timestamp")
	}
	month, _ := strconv.Atoi(matches[2])
	day, _ := strconv.Atoi(matches[3])
	hour, _ := strconv.Atoi(matches[4])
	minute, _ := strconv.Atoi(matches[5])
	second, _ := strconv.Atoi(matches[6])
	if month < 1 || month > 12 || day < 1 || day > daysInGregorianMonth(year, month) || hour > 23 || minute > 59 || second > 59 {
		return Instant{}, errors.New("invalid RiffDB timestamp")
	}
	var offsetSeconds int64
	if matches[8] != "Z" {
		offsetHours, _ := strconv.Atoi(matches[10])
		offsetMinutes, _ := strconv.Atoi(matches[11])
		if offsetHours > 23 || offsetMinutes > 59 {
			return Instant{}, errors.New("invalid RiffDB timestamp")
		}
		offsetSeconds = int64((offsetHours*60 + offsetMinutes) * 60)
		if matches[9] == "-" {
			offsetSeconds = -offsetSeconds
		}
	}
	totalSeconds := new(big.Int).Mul(daysFromCivil(year, month, day), big.NewInt(86400))
	totalSeconds.Add(totalSeconds, big.NewInt(int64(hour*3600+minute*60+second)-offsetSeconds))
	if !totalSeconds.IsInt64() {
		return Instant{}, errors.New("invalid RiffDB timestamp")
	}
	var nanos uint32
	if matches[7] != "" {
		frac := matches[7] + strings.Repeat("0", 9-len(matches[7]))
		parsedNanos, err := strconv.ParseUint(frac, 10, 32)
		if err != nil || parsedNanos > 999_999_999 {
			return Instant{}, errors.New("invalid RiffDB timestamp")
		}
		nanos = uint32(parsedNanos)
	}
	return Instant{Seconds: totalSeconds.Int64(), Nanos: nanos}, nil
}

func (instant Instant) MarshalJSON() ([]byte, error) {
	if instant.Nanos > 999_999_999 {
		return nil, errors.New("invalid RiffDB timestamp")
	}
	return json.Marshal(instant.String())
}

func (instant *Instant) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return errors.New("invalid RiffDB timestamp")
	}
	if trimmed[0] == '"' {
		var text string
		if err := json.Unmarshal(trimmed, &text); err != nil {
			return errors.New("invalid RiffDB timestamp")
		}
		parsed, parseErr := parseInstantString(text)
		if parseErr != nil {
			return errors.New("invalid RiffDB timestamp")
		}
		*instant = parsed
		return nil
	}
	var wire struct {
		Seconds any `json:"seconds"`
		Nanos   any `json:"nanos"`
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil || decoder.More() {
		return errors.New("invalid RiffDB timestamp")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("invalid RiffDB timestamp")
	}
	var nanos uint32
	switch rawNanos := wire.Nanos.(type) {
	case nil:
		nanos = 0
	case json.Number:
		nanosText := rawNanos.String()
		if !uint32Pattern.MatchString(nanosText) {
			return errors.New("invalid RiffDB timestamp")
		}
		parsedNanos, err := strconv.ParseUint(nanosText, 10, 32)
		if err != nil || parsedNanos > 999_999_999 {
			return errors.New("invalid RiffDB timestamp")
		}
		nanos = uint32(parsedNanos)
	default:
		return errors.New("invalid RiffDB timestamp")
	}
	var secText string
	switch sec := wire.Seconds.(type) {
	case string:
		secText = sec
	case json.Number:
		secText = sec.String()
	default:
		return errors.New("invalid RiffDB timestamp")
	}
	if !int64Pattern.MatchString(secText) {
		return errors.New("invalid RiffDB timestamp")
	}
	parsedSeconds, err := strconv.ParseInt(secText, 10, 64)
	if err != nil {
		return errors.New("invalid RiffDB timestamp")
	}
	*instant = Instant{Seconds: parsedSeconds, Nanos: nanos}
	return nil
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
func DefaultIdempotencyKey(value string) string {
	if value != "" {
		return value
	}
	return fmt.Sprintf("riffdb-auto-%d-%d", time.Now().UnixNano(), nextSession.Add(1))
}
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
		if !ok || !utf8.ValidString(text) {
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
	if value.Type != "string" || !ok || !utf8.ValidString(result) {
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
	Tenant                  string
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
	if deadline < time.Millisecond || deadline > 5*time.Minute || attempts < 1 || attempts > 10 || len(options.Cursor) > 16_384 || !utf8.ValidString(options.Cursor) || (options.QueryConsistency != "" && options.QueryConsistency != AdmissionHead) || options.AcceptPackedResult && !options.AcceptCompactResult {
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

func (err *ApplicationError) Error() string {
	if len(err.Details.SymbolPath) > 0 {
		return err.Details.Code + ": " + err.Details.Message + " (" + strings.Join(err.Details.SymbolPath, ".") + ")"
	}
	return err.Details.Code + ": " + err.Details.Message
}

type Session struct {
	socketPath    string
	connection    net.Conn
	reader        *bufio.Reader
	identity      Identity
	writeMu       sync.Mutex
	mu            sync.Mutex
	reconnectMu   sync.Mutex
	pending       map[string]chan packet
	requestPrefix string
	next          atomic.Uint64
	closed        chan struct{}
	closedErr     error
	disconnected  bool
	protocolFatal bool
	userClosed    bool
}

type packet struct {
	body []byte
	err  error
}

func Connect(ctx context.Context, socketPath string, identity Identity) (*Session, error) {
	return connectSession(ctx, socketPath, identity, true)
}

func connectSession(ctx context.Context, socketPath string, identity Identity, allowEnvTenantFallback bool) (*Session, error) {
	if allowEnvTenantFallback && identity.Tenant == "" {
		if envTenant := os.Getenv("RIFFDB_TENANT"); envTenant != "" {
			identity.Tenant = envTenant
		}
	}
	if !strings.HasPrefix(socketPath, "/") || len(socketPath) > 4_096 || !validIdentity(identity) {
		return nil, errors.New("invalid RiffDB driver configuration")
	}
	session := &Session{
		socketPath:    socketPath,
		identity:      identity,
		pending:       make(map[string]chan packet),
		requestPrefix: newSessionRequestPrefix(),
		closed:        make(chan struct{}),
		disconnected:  true,
	}
	if err := session.ensureConnected(ctx); err != nil {
		return nil, err
	}
	return session, nil
}

func (session *Session) ensureConnected(ctx context.Context) error {
	if ctx == nil {
		return errors.New("invalid RiffDB driver context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	session.mu.Lock()
	if session.userClosed || session.protocolFatal {
		err := session.closedErr
		session.mu.Unlock()
		return err
	}
	if !session.disconnected && session.connection != nil {
		session.mu.Unlock()
		return nil
	}
	session.mu.Unlock()

	session.reconnectMu.Lock()
	defer session.reconnectMu.Unlock()

	session.mu.Lock()
	if session.userClosed || session.protocolFatal {
		err := session.closedErr
		session.mu.Unlock()
		return err
	}
	if !session.disconnected && session.connection != nil {
		session.mu.Unlock()
		return nil
	}
	session.mu.Unlock()

	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", session.socketPath)
	if err != nil {
		return fmt.Errorf("RiffDB driver session failed: %w", err)
	}
	reader := bufio.NewReader(connection)

	session.mu.Lock()
	if session.userClosed || session.protocolFatal {
		err := session.closedErr
		session.mu.Unlock()
		_ = connection.Close()
		return err
	}
	session.connection = connection
	session.reader = reader
	session.disconnected = false
	session.closedErr = nil
	identity := session.identity
	session.mu.Unlock()

	go session.readLoop(connection, reader)

	requestID := session.requestID("handshake")
	request := handshakeRequest{
		Type:                    "handshake",
		RequestID:               requestID,
		ProtocolVersion:         ProtocolVersion,
		ApplicationManifestHash: identity.ApplicationManifestHash,
		OperationCatalogHash:    identity.OperationCatalogHash,
		ContractLineage:         identity.ContractLineage,
		ContractVersion:         identity.ContractVersion,
		ContractBundleHash:      identity.ContractBundleHash,
		ValueRegistryHash:       ValueRegistryHash,
		ErrorRegistryHash:       ErrorRegistryHash,
		Database:                identity.Database,
		Role:                    identity.Role,
		RoleDefinitionHash:      identity.RoleDefinitionHash,
		RemoteIdentityHash:      identity.RemoteIdentityHash,
	}
	body, err := session.call(ctx, requestID, request, false)
	if err != nil {
		session.failConn(connection, err, false)
		return err
	}
	var response handshakeResponse
	if json.Unmarshal(body, &response) != nil || response.Type != "handshake" || response.ProtocolVersion != ProtocolVersion ||
		!hashPattern.MatchString(response.ApplicationManifestHash) ||
		(identity.ApplicationManifestHash != "" && response.ApplicationManifestHash != identity.ApplicationManifestHash) ||
		!hashPattern.MatchString(response.OperationCatalogHash) ||
		(identity.OperationCatalogHash != "" && response.OperationCatalogHash != identity.OperationCatalogHash) ||
		!symbolPattern.MatchString(response.ContractLineage) ||
		(identity.ContractLineage != "" && response.ContractLineage != identity.ContractLineage) ||
		response.ContractVersion == 0 ||
		(identity.ContractVersion != 0 && response.ContractVersion != identity.ContractVersion) ||
		!hashPattern.MatchString(response.ContractBundleHash) ||
		(identity.ContractBundleHash != "" && response.ContractBundleHash != identity.ContractBundleHash) ||
		!symbolPattern.MatchString(response.Database) ||
		(identity.Database != "" && response.Database != identity.Database) ||
		!symbolPattern.MatchString(response.Role) ||
		(identity.Role != "" && response.Role != identity.Role) ||
		!hashPattern.MatchString(response.RoleDefinitionHash) ||
		(identity.RoleDefinitionHash != "" && response.RoleDefinitionHash != identity.RoleDefinitionHash) ||
		!hashPattern.MatchString(response.RemoteIdentityHash) ||
		(identity.RemoteIdentityHash != "" && response.RemoteIdentityHash != identity.RemoteIdentityHash) {
		mismatchErr := errors.New("RiffDB driver identity mismatch")
		session.failConn(connection, mismatchErr, true)
		return mismatchErr
	}
	session.mu.Lock()
	session.identity.ApplicationManifestHash = response.ApplicationManifestHash
	session.identity.OperationCatalogHash = response.OperationCatalogHash
	session.identity.ContractLineage = response.ContractLineage
	session.identity.ContractVersion = response.ContractVersion
	session.identity.ContractBundleHash = response.ContractBundleHash
	session.identity.Database = response.Database
	session.identity.Role = response.Role
	session.identity.RoleDefinitionHash = response.RoleDefinitionHash
	session.identity.RemoteIdentityHash = response.RemoteIdentityHash
	session.mu.Unlock()
	return nil
}

func ResolveIdentityFromEnv(base Identity, prefix string) (string, Identity, error) {
	var candidatePrefixes []string
	if prefix == "" {
		candidatePrefixes = []string{"RIFFDB_"}
	} else {
		if !envPrefixPattern.MatchString(prefix) {
			return "", Identity{}, errors.New("invalid RiffDB driver configuration")
		}
		normalized := prefix
		if !strings.HasSuffix(normalized, "_") {
			normalized += "_"
		}
		if normalized == "RIFFDB_" || strings.HasSuffix(normalized, "RIFFDB_") {
			candidatePrefixes = []string{normalized}
		} else {
			candidatePrefixes = []string{normalized + "RIFFDB_", normalized}
		}
	}
	readEnv := func(suffix string) string {
		for _, candidatePrefix := range candidatePrefixes {
			if val := strings.TrimSpace(os.Getenv(candidatePrefix + suffix)); val != "" {
				return val
			}
		}
		return ""
	}
	socketPath := readEnv("DRIVER_SOCKET")
	identity := base
	if identity.ContractLineage == "" {
		identity.ContractLineage = readEnv("LINEAGE")
	}
	if identity.ContractVersion == 0 {
		if rawVersion := readEnv("CONTRACT_VERSION"); rawVersion != "" {
			parsed, err := strconv.ParseUint(rawVersion, 10, 64)
			if err != nil {
				return "", Identity{}, errors.New("invalid RiffDB driver configuration")
			}
			identity.ContractVersion = parsed
		}
	}
	if identity.ContractBundleHash == "" {
		identity.ContractBundleHash = readEnv("BUNDLE_HASH")
	}
	if identity.Role == "" {
		identity.Role = readEnv("ROLE")
	}
	if identity.Database == "" {
		identity.Database = readEnv("DATABASE")
	}
	if identity.ApplicationManifestHash == "" {
		identity.ApplicationManifestHash = readEnv("MANIFEST_HASH")
	}
	if identity.OperationCatalogHash == "" {
		identity.OperationCatalogHash = readEnv("CATALOG_HASH")
	}
	if identity.RoleDefinitionHash == "" {
		identity.RoleDefinitionHash = readEnv("ROLE_HASH")
	}
	if identity.RemoteIdentityHash == "" {
		identity.RemoteIdentityHash = readEnv("REMOTE_HASH")
	}
	if identity.Tenant == "" {
		identity.Tenant = readEnv("TENANT")
	}
	return socketPath, identity, nil
}

func ConnectFromEnv(ctx context.Context, base Identity, prefix string) (*Session, error) {
	socketPath, identity, err := ResolveIdentityFromEnv(base, prefix)
	if err != nil {
		return nil, err
	}
	return connectSession(ctx, socketPath, identity, false)
}

func (session *Session) Identity() Identity {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.identity
}

func (session *Session) Tenant() string {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.identity.Tenant
}

func (session *Session) Invoke(ctx context.Context, operation Operation, input map[string]Value, options Options) (Result, error) {
	if err := validateOperation(operation, input); err != nil {
		return Result{}, err
	}
	wire, err := options.wire()
	if err != nil {
		return Result{}, err
	}
	if err := session.ensureConnected(ctx); err != nil {
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
	if err := session.ensureConnected(ctx); err != nil {
		return BatchResult{}, err
	}
	requestID := session.requestID("batch")
	body, err := session.call(ctx, requestID, batchRequest{Type: "batch", RequestID: requestID, Operation: operation.Name, InputSchemaHash: operation.InputSchemaHash, Items: items, Concurrency: concurrency, Checkpoint: checkpoint, Options: wire}, true)
	if err != nil {
		return BatchResult{}, err
	}
	return decodeBatch(body, uint32(len(items)), checkpoint)
}

func (session *Session) Close() error {
	session.mu.Lock()
	if session.userClosed {
		session.mu.Unlock()
		return nil
	}
	session.userClosed = true
	conn := session.connection
	session.mu.Unlock()
	var err error
	if conn != nil {
		err = conn.Close()
	}
	session.failConn(conn, errors.New("RiffDB driver session closed"), true)
	return err
}

func (session *Session) call(ctx context.Context, requestID string, request any, cancellable bool) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("invalid RiffDB driver context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := json.Marshal(request)
	if err != nil || len(body) < 2 || len(body) > maxFrameBytes {
		return nil, errors.New("invalid RiffDB driver request")
	}
	response := make(chan packet, 1)
	session.mu.Lock()
	if session.userClosed || session.protocolFatal || session.disconnected {
		err := session.closedErr
		if err == nil {
			err = errors.New("RiffDB driver session failed")
		}
		session.mu.Unlock()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		session.mu.Unlock()
		return nil, err
	}
	if len(session.pending) >= maxPending {
		session.mu.Unlock()
		return nil, errors.New("RiffDB driver session is over capacity")
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
	session.mu.Lock()
	conn := session.connection
	session.mu.Unlock()
	if conn == nil {
		return errors.New("RiffDB driver session failed")
	}
	prefix := []byte{byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body))}
	if _, err := conn.Write(append(prefix, body...)); err != nil {
		session.failConn(conn, errors.New("RiffDB driver session failed"), false)
		return errors.New("RiffDB driver session failed")
	}
	return nil
}

func (session *Session) readLoop(connection net.Conn, reader *bufio.Reader) {
	for {
		prefix := make([]byte, 4)
		if _, err := io.ReadFull(reader, prefix); err != nil {
			session.failConn(connection, fmt.Errorf("RiffDB driver session failed: socket closed by peer (%w)", err), false)
			return
		}
		length := int(prefix[0])<<24 | int(prefix[1])<<16 | int(prefix[2])<<8 | int(prefix[3])
		if length < 2 || length > maxFrameBytes {
			session.failConn(connection, errors.New("RiffDB driver returned an invalid frame"), true)
			_ = connection.Close()
			return
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(reader, body); err != nil {
			session.failConn(connection, fmt.Errorf("RiffDB driver session failed: socket closed by peer (%w)", err), false)
			return
		}
		var header struct {
			RequestID *string `json:"request_id"`
		}
		if json.Unmarshal(body, &header) != nil || header.RequestID == nil || !requestPattern.MatchString(*header.RequestID) {
			session.failConn(connection, errors.New("RiffDB driver returned an invalid message"), true)
			_ = connection.Close()
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

func (session *Session) failConn(connection net.Conn, err error, fatal bool) {
	session.mu.Lock()
	if connection != nil && session.connection != nil && session.connection != connection {
		session.mu.Unlock()
		_ = connection.Close()
		return
	}
	if fatal {
		session.protocolFatal = true
	}
	session.disconnected = true
	if session.closedErr == nil {
		session.closedErr = err
		select {
		case <-session.closed:
		default:
			close(session.closed)
		}
	} else {
		err = session.closedErr
	}
	pending := session.pending
	session.pending = make(map[string]chan packet)
	currentConn := session.connection
	session.mu.Unlock()
	if currentConn != nil {
		_ = currentConn.Close()
	}
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
func decodeBatch(body []byte, expectedTotal uint32, requestedCheckpoint uint32) (BatchResult, error) {
	var response batchResponse
	if json.Unmarshal(body, &response) != nil || response.Type != "batch_result" || len(response.Items) > maxCollection || response.Total == 0 || response.Total != expectedTotal || response.Checkpoint < requestedCheckpoint || response.Checkpoint > response.Total || uint32(len(response.Items)) != response.Total-requestedCheckpoint {
		return BatchResult{}, errors.New("RiffDB driver returned an invalid batch result")
	}
	result := BatchResult{Checkpoint: response.Checkpoint, Total: response.Total, Items: make([]BatchItem, 0, len(response.Items))}
	seen := make(map[uint32]struct{}, len(response.Items))
	for _, item := range response.Items {
		if item.Index < requestedCheckpoint || item.Index >= response.Total {
			return BatchResult{}, errors.New("RiffDB driver returned an invalid batch item")
		}
		if _, duplicate := seen[item.Index]; duplicate {
			return BatchResult{}, errors.New("RiffDB driver returned an invalid batch item")
		}
		seen[item.Index] = struct{}{}
		var header struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(item.Outcome, &header) != nil {
			return BatchResult{}, errors.New("RiffDB driver returned an invalid batch item")
		}
		if header.Type == "result" {
			if item.Index == response.Checkpoint {
				return BatchResult{}, errors.New("RiffDB driver returned an invalid batch result")
			}
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
			if item.Index < response.Checkpoint {
				return BatchResult{}, errors.New("RiffDB driver returned an invalid batch result")
			}
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
		text, ok := value.Value.(string)
		if !ok || !utf8.ValidString(text) {
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
				nested, valid := valueFromDecoded(item)
				if !valid || validateValue(nested, depth+1) != nil {
					return errors.New("invalid value")
				}
			}
		} else if typed, valid := value.Value.([]Value); valid {
			if len(typed) > maxCollection {
				return errors.New("invalid value")
			}
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
				nested, valid := valueFromDecoded(item)
				if !valid || validateValue(nested, depth+1) != nil {
					return errors.New("invalid value")
				}
			}
		} else if typed, valid := value.Value.(map[string]Value); valid {
			if len(typed) > maxCollection {
				return errors.New("invalid value")
			}
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
	return (identity.ApplicationManifestHash == "" || hashPattern.MatchString(identity.ApplicationManifestHash)) &&
		(identity.OperationCatalogHash == "" || hashPattern.MatchString(identity.OperationCatalogHash)) &&
		(identity.ContractLineage == "" || symbolPattern.MatchString(identity.ContractLineage)) &&
		(identity.ContractBundleHash == "" || hashPattern.MatchString(identity.ContractBundleHash)) &&
		(identity.Database == "" || symbolPattern.MatchString(identity.Database)) &&
		(identity.Role == "" || symbolPattern.MatchString(identity.Role)) &&
		(identity.RoleDefinitionHash == "" || hashPattern.MatchString(identity.RoleDefinitionHash)) &&
		(identity.RemoteIdentityHash == "" || hashPattern.MatchString(identity.RemoteIdentityHash)) &&
		(identity.Tenant == "" || tenantPattern.MatchString(identity.Tenant))
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
