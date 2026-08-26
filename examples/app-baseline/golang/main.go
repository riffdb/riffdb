package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const reportSchema = "riffdb.app-baseline-golang-safe-app/v1"

func argValue(argv []string, name string) (string, bool) {
	for i, arg := range argv {
		if arg == name && i+1 < len(argv) {
			return argv[i+1], true
		}
	}
	return "", false
}

func hasFlag(argv []string, name string) bool {
	for _, arg := range argv {
		if arg == name {
			return true
		}
	}
	return false
}

func run(argv []string) int {
	backend, _ := argValue(argv, "--backend")
	if backend == "" {
		backend = "both"
	}
	if backend != "postgres" && backend != "riffdb" && backend != "both" {
		fmt.Fprintln(os.Stderr, "backend must be postgres|riffdb|both")
		return 2
	}
	var s scale
	switch {
	case hasFlag(argv, "--production"):
		s = productionScale()
	case hasFlag(argv, "--full"):
		s = fullScale()
	default:
		s = smokeScale()
	}
	dataset := generateSeed(s)
	var clients []int
	if hasFlag(argv, "--load-concurrency-sweep") {
		clients = append(clients, sweepClients...)
	} else {
		n := 8
		if text, ok := argValue(argv, "--load-clients"); ok {
			parsed, err := strconv.Atoi(text)
			if err != nil {
				fmt.Fprintf(os.Stderr, "invalid --load-clients: %v\n", err)
				return 2
			}
			n = parsed
		}
		if n < 1 {
			n = 1
		}
		clients = []int{n}
	}
	durationS := 5.0
	if text, ok := argValue(argv, "--load-duration-secs"); ok {
		parsed, err := strconv.ParseFloat(text, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid --load-duration-secs: %v\n", err)
			return 2
		}
		durationS = parsed
	}
	warmupS := 1.0
	if text, ok := argValue(argv, "--load-warmup-secs"); ok {
		parsed, err := strconv.ParseFloat(text, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid --load-warmup-secs: %v\n", err)
			return 2
		}
		warmupS = parsed
	}
	zipfS := 1.0
	if text, ok := argValue(argv, "--load-zipf-s"); ok {
		parsed, err := strconv.ParseFloat(text, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid --load-zipf-s: %v\n", err)
			return 2
		}
		zipfS = parsed
	}

	ctx := context.Background()
	var drivers []loadDriver
	if backend == "postgres" || backend == "both" {
		url, ok := argValue(argv, "--postgres-url")
		if !ok {
			url = os.Getenv("RIFFDB_APP_BASELINE_POSTGRES_URL")
		}
		if url == "" {
			fmt.Fprintln(os.Stderr, "missing --postgres-url or RIFFDB_APP_BASELINE_POSTGRES_URL")
			return 2
		}
		drivers = append(drivers, postgresDriver{url: url})
	}
	if backend == "riffdb" || backend == "both" {
		identityPath, ok := argValue(argv, "--riffdb-identity")
		if !ok {
			identityPath = os.Getenv("RIFFDB_GOLANG_DRIVER_IDENTITY")
		}
		if identityPath == "" {
			fmt.Fprintln(os.Stderr, "missing --riffdb-identity or RIFFDB_GOLANG_DRIVER_IDENTITY")
			return 2
		}
		identity, err := readDriverIdentity(identityPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "riffdb identity: %v\n", err)
			return 2
		}
		drivers = append(drivers, riffDbDriver{identity: identity})
	}

	var allReports []loadReport
	for _, driver := range drivers {
		fmt.Printf("=== backend %s seed ===\n", driver.BackendID())
		seedStarted := time.Now()
		if err := driver.Seed(ctx, dataset); err != nil {
			fmt.Fprintf(os.Stderr, "seed failed: %v\n", err)
			return 1
		}
		seedNs := uint64(time.Since(seedStarted).Nanoseconds())
		fmt.Printf("seed_ns=%d\n", seedNs)
		sampleBase := 0
		for _, clientCount := range clients {
			config := loadConfig{
				clients:      clientCount,
				durationS:    durationS,
				warmupS:      warmupS,
				zipfS:        zipfS,
				rngSeed:      defaultRngSeed,
				tenantCount:  1,
				sampleIDBase: sampleBase,
			}
			report, err := runClosedLoop(ctx, driver, dataset, config, seedNs)
			if err != nil {
				fmt.Fprintf(os.Stderr, "load failed: %v\n", err)
				return 1
			}
			printLoadSummary(report)
			allReports = append(allReports, report)
			sampleBase += 1_000_000_000
		}
	}

	fmt.Println("\n== comparison curve ==")
	fmt.Printf("%-24s %8s %12s %10s %10s %12s\n", "backend", "clients", "ops/s", "p50_ms", "p99_ms", "write_p50_ms")
	for _, report := range allReports {
		writeP50 := 0.0
		if write, ok := report.byOp["create_comment"]; ok && write.total() > 0 {
			writeP50 = float64(write.latency.percentileNs(50)) / 1e6
		}
		fmt.Printf("%-24s %8d %12.0f %10.3f %10.3f %12.3f\n",
			report.backendID, report.clients, report.throughput(),
			float64(report.aggregate.latency.percentileNs(50))/1e6,
			float64(report.aggregate.latency.percentileNs(99))/1e6,
			writeP50)
	}

	curve := make([]map[string]any, 0, len(allReports))
	for _, report := range allReports {
		curve = append(curve, report.json())
	}
	payload := map[string]any{
		"schema":               reportSchema,
		"evidentiary":          false,
		"language":             "golang",
		"postgres_obligations": obligations,
		"scale": map[string]any{
			"organizations":       s.organizations,
			"users_per_org":       s.usersPerOrg,
			"projects_per_org":    s.projectsPerOrg,
			"tickets_per_project": s.ticketsPerProject,
			"board_dense_open":    s.boardDenseOpen,
		},
		"curve": curve,
		"notes": []string{
			"Same Go interactive mix against postgres_safe_app and generated RiffDB client.",
			"Postgres implements authorization/idempotency/audit/event/outbox in Go SQL.",
			"RiffDB uses the generated TicketDesk client with no Go safety code; riffdbd (Rust) enforces those.",
			"Not a substitute for the Rust evidentiary harness in benchmarks/run-app-baseline.",
		},
	}
	if output, ok := argValue(argv, "--output"); ok {
		if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "output dir: %v\n", err)
			return 1
		}
		encoded, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "encode report: %v\n", err)
			return 1
		}
		if err := os.WriteFile(output, append(encoded, '\n'), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write report: %v\n", err)
			return 1
		}
		fmt.Printf("wrote %s\n", output)
	}
	return 0
}

func main() {
	os.Exit(run(os.Args[1:]))
}
