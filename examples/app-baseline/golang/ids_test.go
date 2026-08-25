package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestUUIDFromOrdinalMatchesRustLayout(t *testing.T) {
	value := uuidFromOrdinal(0x10, 0)
	if len(value) != 16 {
		t.Fatalf("len=%d", len(value))
	}
	if value[0] != 0x10 {
		t.Fatalf("byte0=%x", value[0])
	}
	if value[6] != 0x70 {
		t.Fatalf("version nibble=%x", value[6])
	}
	if value[8]&0xc0 != 0x80 {
		t.Fatalf("variant=%x", value[8])
	}
	if got := formatUUID(value); got != "10101010-1010-7010-8000-000000000000" {
		t.Fatalf("format=%s", got)
	}
}

func TestFingerprintIsCanonicalAndLengthPrefixed(t *testing.T) {
	first := safeInputFingerprint([][]byte{[]byte("a/b"), []byte("c")})
	second := safeInputFingerprint([][]byte{[]byte("a"), []byte("b/c")})
	repeated := safeInputFingerprint([][]byte{[]byte("a/b"), []byte("c")})
	if len(first) != 64 {
		t.Fatalf("len=%d", len(first))
	}
	if first != repeated {
		t.Fatal("fingerprint was not stable")
	}
	if first == second {
		t.Fatal("length prefix was ignored")
	}
	if strings.Contains(first, "a/b") {
		t.Fatal("raw field leaked into digest")
	}
}

func TestSmokeAndFullSeedShapes(t *testing.T) {
	smoke := generateSeed(smokeScale())
	if len(smoke.organizations) != 2 {
		t.Fatalf("orgs=%d", len(smoke.organizations))
	}
	if smoke.scale.boardDenseOpen != 0 {
		t.Fatalf("board dense=%d", smoke.scale.boardDenseOpen)
	}
	if boardDenseOpenCount(smoke) >= 50 {
		t.Fatalf("smoke board is unexpectedly dense")
	}
	probes := probesFor(smoke)
	if probes.ticketID == probes.writeTicketID {
		t.Fatal("write ticket collided with probe ticket")
	}
	full := generateSeed(fullScale())
	if boardDenseOpenCount(full) != 600 {
		t.Fatalf("full board=%d", boardDenseOpenCount(full))
	}
	if got := len(tenantProbes(full, 3)); got != 3 {
		t.Fatalf("tenants=%d", got)
	}
}

func TestSchemaContainsSafeAppObligations(t *testing.T) {
	for _, table := range []string{
		"app_permission",
		"app_idempotency",
		"app_audit",
		"app_domain_event",
		"app_outbox_intent",
	} {
		if !strings.Contains(schemaSQL, "CREATE TABLE "+table) {
			t.Fatalf("missing table %s", table)
		}
	}
	want := []string{
		"symbolic_operation_authorization",
		"idempotency_admission_and_equal_input_replay",
		"domain_mutation",
		"audit_and_provenance",
		"domain_event",
		"outbox_intent",
		"one_atomic_transaction",
	}
	if len(obligations) != len(want) {
		t.Fatalf("obligations=%v", obligations)
	}
	for i := range want {
		if obligations[i] != want[i] {
			t.Fatalf("obligations=%v", obligations)
		}
	}
}

func TestGoSchemaTablesMatchRustAdapter(t *testing.T) {
	rust, err := os.ReadFile(filepath.Join("..", "postgres", "src", "lib.rs"))
	if err != nil {
		t.Fatal(err)
	}
	tableRe := regexp.MustCompile(`CREATE TABLE (\w+)`)
	rustTables := map[string]struct{}{}
	for _, match := range tableRe.FindAllStringSubmatch(string(rust), -1) {
		rustTables[match[1]] = struct{}{}
	}
	goTables := map[string]struct{}{}
	for _, match := range tableRe.FindAllStringSubmatch(schemaSQL, -1) {
		goTables[match[1]] = struct{}{}
	}
	if len(rustTables) != len(goTables) {
		t.Fatalf("rust=%v go=%v", rustTables, goTables)
	}
	for table := range rustTables {
		if _, ok := goTables[table]; !ok {
			t.Fatalf("missing table %s", table)
		}
	}
}

func TestInteractiveWeightsAreMostlyReads(t *testing.T) {
	total := 0
	writes := 0
	hasSwap := false
	for _, item := range interactiveWeights {
		total += item.weight
		switch item.name {
		case "create_comment", "close_ticket_with_comment", "open_ticket_with_labels":
			writes += item.weight
		case "swap_member_roles":
			hasSwap = true
		}
	}
	if total-writes <= writes {
		t.Fatal("interactive mix is not read-heavy")
	}
	if hasSwap {
		t.Fatal("swap_member_roles must stay out of the interactive mix")
	}
}

func TestRiffdbDriverContainsNoGoSafetyImplementation(t *testing.T) {
	source, err := os.ReadFile("riffdb.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, forbidden := range []string{
		"app_permission",
		"app_idempotency",
		"app_audit",
		"app_domain_event",
		"app_outbox_intent",
		"permissionSQL",
		"idempotencyLockSQL",
		"insertAuditSQL",
		"insertEventSQL",
		"insertOutboxSQL",
		"safeAdmit",
		"safeComplete",
		"safeInputFingerprint",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("riffdb.go contains %s", forbidden)
		}
	}
	if !strings.Contains(text, "riffdb.dev/ticketdesk") {
		t.Fatal("generated TicketDesk client is unused")
	}
}

func TestPostgresDriverOwnsTheSQLSafetyTables(t *testing.T) {
	source, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, required := range []string{"safeAdmit", "safeComplete", "insertOutboxSQL"} {
		if !strings.Contains(text, required) {
			t.Fatalf("postgres.go missing %s", required)
		}
	}
}

func TestReportRecordsWhichSideOwnsSafety(t *testing.T) {
	empty := newOpStats()
	riff := loadReport{
		backendID:         riffdbBackendID,
		clients:           1,
		profile:           "interactive",
		measuredElapsedNs: 1_000_000_000,
		seedNs:            1,
		aggregate:         empty,
		byOp:              map[string]*opStats{},
		workerCompleted:   []int{0},
	}.json()
	postgres := loadReport{
		backendID:         postgresBackendID,
		clients:           1,
		profile:           "interactive",
		measuredElapsedNs: 1_000_000_000,
		seedNs:            1,
		aggregate:         empty,
		byOp:              map[string]*opStats{},
		workerCompleted:   []int{0},
	}.json()
	if riff["safety_owner"] != "riffdbd_rust" {
		t.Fatalf("riff safety=%v", riff["safety_owner"])
	}
	if postgres["safety_owner"] != "golang_sql" {
		t.Fatalf("postgres safety=%v", postgres["safety_owner"])
	}
	if riff["evidentiary"] != false {
		t.Fatal("golang reports must not be evidentiary")
	}
	notes, _ := riff["notes"].([]string)
	joined := strings.Join(notes, " ")
	if !strings.Contains(joined, "No authorization, idempotency, audit, event, or outbox code runs in Go") {
		t.Fatalf("notes=%v", notes)
	}
}

func TestPercentilesTrackInjectedLatencies(t *testing.T) {
	histogram := newLatencyHistogram()
	for i := 0; i < 90; i++ {
		histogram.recordNs(100_000)
	}
	for i := 0; i < 9; i++ {
		histogram.recordNs(1_000_000)
	}
	histogram.recordNs(10_000_000)
	if histogram.percentileNs(50) < 100_000 || histogram.percentileNs(50) >= 1_000_000 {
		t.Fatalf("p50=%d", histogram.percentileNs(50))
	}
	if histogram.percentileNs(99) < 1_000_000 {
		t.Fatalf("p99=%d", histogram.percentileNs(99))
	}
}
