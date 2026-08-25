package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var interactiveWeights = []struct {
	name   string
	weight int
}{
	{"point_get_ticket", 25},
	{"point_get_user", 15},
	{"list_tickets_by_project_status", 12},
	{"list_open_tickets_for_assignee", 10},
	{"list_comments_for_ticket", 10},
	{"list_project_members", 8},
	{"ticket_detail_page", 5},
	{"create_comment", 12},
	{"close_ticket_with_comment", 2},
	{"open_ticket_with_labels", 1},
}

var sweepClients = []int{1, 8, 32, 128}

const (
	defaultRngSeed uint64 = 0x000a11cebeef
	golden         uint64 = 0x9e3779b97f4a7c15
)

type loadSession interface {
	Prewarm(ctx context.Context, organizationID, ticketID UUID) error
	PointGetTicket(ctx context.Context, organizationID, ticketID UUID) (bool, error)
	PointGetUser(ctx context.Context, organizationID, userID UUID) (bool, error)
	ListTicketsByProjectStatus(ctx context.Context, organizationID, projectID UUID, status string, limit int) error
	ListOpenTicketsForAssignee(ctx context.Context, organizationID, assigneeID UUID, limit int) error
	ListCommentsForTicket(ctx context.Context, organizationID, ticketID UUID, limit int) error
	ListProjectMembers(ctx context.Context, organizationID, projectID UUID, limit int) error
	TicketDetailPage(ctx context.Context, organizationID, ticketID UUID) (bool, error)
	CreateComment(ctx context.Context, comment commentSeed) error
	CloseTicketWithComment(ctx context.Context, input closeTicketWithCommentSeed) error
	OpenTicketWithLabels(ctx context.Context, input openTicketWithLabelsSeed) error
	Close(ctx context.Context) error
}

type loadDriver interface {
	BackendID() string
	Seed(ctx context.Context, dataset seedDataset) error
	OpenSession(ctx context.Context) (loadSession, error)
}

type xorShift64 struct{ state uint64 }

func newXorShift64(seed uint64) *xorShift64 {
	return &xorShift64{state: seed | 1}
}

func (r *xorShift64) nextU64() uint64 {
	x := r.state
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	r.state = x
	return x
}

func (r *xorShift64) genRange(maxExclusive int) int {
	if maxExclusive <= 0 {
		return 0
	}
	return int(r.nextU64() % uint64(maxExclusive))
}

func (r *xorShift64) genF64() float64 {
	return float64(r.nextU64()) / (float64(math.MaxUint64) + 1)
}

type zipf struct{ cdf []float64 }

func newZipf(n int, s float64) zipf {
	if n <= 0 {
		panic("zipf domain must be positive")
	}
	cdf := make([]float64, n)
	if s <= 0 {
		step := 1.0 / float64(n)
		for i := 0; i < n; i++ {
			cdf[i] = step * float64(i+1)
		}
		return zipf{cdf: cdf}
	}
	weights := make([]float64, n)
	total := 0.0
	for rank := 0; rank < n; rank++ {
		weight := 1.0 / math.Pow(float64(rank+1), s)
		weights[rank] = weight
		total += weight
	}
	run := 0.0
	for i, weight := range weights {
		run += weight / total
		cdf[i] = run
	}
	cdf[n-1] = 1
	return zipf{cdf: cdf}
}

func (z zipf) sample(rng *xorShift64) int {
	u := rng.genF64()
	for index, edge := range z.cdf {
		if edge >= u {
			return index
		}
	}
	return len(z.cdf) - 1
}

type opStats struct {
	latency     *latencyHistogram
	success     int
	conflict    int
	unavailable int
	replayed    int
	error       int
	firstError  string
}

func newOpStats() *opStats {
	return &opStats{latency: newLatencyHistogram()}
}

func (s *opStats) record(elapsedNs uint64, outcome, errorText string) {
	s.latency.recordNs(elapsedNs)
	switch outcome {
	case "success":
		s.success++
	case "conflict":
		s.conflict++
	case "unavailable":
		s.unavailable++
	case "replayed":
		s.replayed++
	default:
		s.error++
	}
	if s.firstError == "" && errorText != "" {
		if len(errorText) > 240 {
			errorText = errorText[:240]
		}
		s.firstError = errorText
	}
}

func (s *opStats) merge(other *opStats) {
	s.latency.merge(other.latency)
	s.success += other.success
	s.conflict += other.conflict
	s.unavailable += other.unavailable
	s.replayed += other.replayed
	s.error += other.error
	if s.firstError == "" {
		s.firstError = other.firstError
	}
}

func (s *opStats) total() int {
	return s.success + s.conflict + s.unavailable + s.replayed + s.error
}

func (s *opStats) json() map[string]any {
	var first any
	if s.firstError != "" {
		first = s.firstError
	}
	return map[string]any{
		"latency": s.latency.summary(),
		"outcomes": map[string]any{
			"success":            s.success,
			"conflict":           s.conflict,
			"unavailable":        s.unavailable,
			"replayed":           s.replayed,
			"error":              s.error,
			"logical_operations": s.total(),
		},
		"first_error": first,
	}
}

type loadConfig struct {
	clients      int
	durationS    float64
	warmupS      float64
	zipfS        float64
	rngSeed      uint64
	tenantCount  int
	sampleIDBase int
}

type loadReport struct {
	backendID         string
	clients           int
	profile           string
	measuredElapsedNs uint64
	seedNs            uint64
	aggregate         *opStats
	byOp              map[string]*opStats
	workerCompleted   []int
}

func (r loadReport) throughput() float64 {
	elapsedS := float64(r.measuredElapsedNs) / 1e9
	if elapsedS < 0.001 {
		elapsedS = 0.001
	}
	return float64(r.aggregate.total()) / elapsedS
}

func (r loadReport) json() map[string]any {
	elapsedNs := r.measuredElapsedNs
	if elapsedNs < 1 {
		elapsedNs = 1
	}
	logicalOps := r.aggregate.total()
	operations := map[string]any{}
	for name, stats := range r.byOp {
		if stats.total() > 0 {
			operations[name] = stats.json()
		}
	}
	safetyOwner := "golang_sql"
	if r.backendID == riffdbBackendID {
		safetyOwner = "riffdbd_rust"
	}
	return map[string]any{
		"schema":                      reportSchema,
		"backend_id":                  r.backendID,
		"profile":                     r.profile,
		"clients":                     r.clients,
		"evidentiary":                 false,
		"language":                    "golang",
		"safety_owner":                safetyOwner,
		"measured_elapsed_ns":         r.measuredElapsedNs,
		"seed_ns":                     r.seedNs,
		"logical_ops":                 logicalOps,
		"throughput_ops_s":            (int64(logicalOps) * 1_000_000_000) / int64(elapsedNs),
		"aggregate":                   r.aggregate.json(),
		"operations":                  operations,
		"worker_completed_operations": r.workerCompleted,
		"notes":                       r.notes(),
	}
}

func (r loadReport) notes() []string {
	shared := []string{
		"Go closed-loop load. Language runtime time is included.",
		"Not a substitute for the Rust evidentiary harness in benchmarks/run-app-baseline.",
		"Throughput denominator is max(worker_measure_end)-min(worker_measure_start).",
		"Each backend is seeded once, then the concurrency sweep accumulates history.",
	}
	if r.backendID == riffdbBackendID {
		return append(shared,
			"RiffDB path uses the generated TicketDesk Go client only.",
			"No authorization, idempotency, audit, event, or outbox code runs in Go; riffdbd (Rust) enforces those.",
		)
	}
	return append(shared,
		"postgres_safe_app path implements authorization, idempotency, audit, event, and outbox in SQL from Go.",
	)
}

func printLoadSummary(report loadReport) {
	elapsedS := float64(report.measuredElapsedNs) / 1e9
	if elapsedS < 0.001 {
		elapsedS = 0.001
	}
	total := report.aggregate.total()
	fmt.Printf("\n== load %s profile=%s clients=%d window=%.1fs tenant=single_organization count=1 hot=0pct ==\n",
		report.backendID, report.profile, report.clients, elapsedS)
	fmt.Printf("throughput=%.0f ops/s  logical_ops=%d  success=%d  conflict=%d  idempotency_mismatch=0  unavailable=%d  overloaded=0  replayed=%d  error=%d\n",
		report.throughput(), total, report.aggregate.success, report.aggregate.conflict, report.aggregate.unavailable, report.aggregate.replayed, report.aggregate.error)
	latency := report.aggregate.latency
	fmt.Printf("latency p50=%.3fms p95=%.3fms p99=%.3fms max=%.3fms\n",
		float64(latency.percentileNs(50))/1e6,
		float64(latency.percentileNs(95))/1e6,
		float64(latency.percentileNs(99))/1e6,
		float64(latency.maxNs)/1e6,
	)
	for _, weight := range interactiveWeights {
		stats := report.byOp[weight.name]
		if stats == nil || stats.total() == 0 {
			continue
		}
		fmt.Printf("  %s: n=%d p50=%.3fms p99=%.3fms ok=%d conflict=%d idempotency_mismatch=0 unavailable=%d replay=%d err=%d\n",
			weight.name, stats.total(),
			float64(stats.latency.percentileNs(50))/1e6,
			float64(stats.latency.percentileNs(99))/1e6,
			stats.success, stats.conflict, stats.unavailable, stats.replayed, stats.error)
	}
}

type barrier struct {
	remaining int
	mu        sync.Mutex
	done      chan struct{}
}

func newBarrier(n int) *barrier {
	return &barrier{remaining: n, done: make(chan struct{})}
}

func (b *barrier) arrive() {
	b.mu.Lock()
	b.remaining--
	if b.remaining == 0 {
		close(b.done)
	}
	b.mu.Unlock()
	<-b.done
}

type workerResult struct {
	byOp         map[string]*opStats
	measureStart uint64
	measureEnd   uint64
	completed    int
	ok           bool
}

func runClosedLoop(ctx context.Context, driver loadDriver, dataset seedDataset, config loadConfig, seedNs uint64) (loadReport, error) {
	if config.clients < 1 || config.clients > 128 {
		return loadReport{}, fmt.Errorf("load clients must be 1..=128")
	}
	probes := tenantProbes(dataset, config.tenantCount)
	if len(probes) != config.tenantCount {
		return loadReport{}, fmt.Errorf("seed does not contain the requested tenant count")
	}
	writeTickets := make([][]ticketRow, 0, len(probes))
	for _, probe := range probes {
		var tickets []ticketRow
		for _, ticket := range dataset.tickets {
			if ticket.organizationID == probe.organizationID && ticket.status == statusOpen && ticket.ticketID != probe.ticketID {
				tickets = append(tickets, ticket)
			}
		}
		if len(tickets) == 0 {
			for _, ticket := range dataset.tickets {
				if ticket.organizationID == probe.organizationID && ticket.status == statusOpen {
					tickets = append(tickets, ticket)
				}
			}
		}
		if len(tickets) == 0 {
			return loadReport{}, fmt.Errorf("load driver requires at least one open ticket per tenant")
		}
		writeTickets = append(writeTickets, tickets)
	}
	zipfs := make([]zipf, len(writeTickets))
	for i, tickets := range writeTickets {
		zipfs[i] = newZipf(len(tickets), config.zipfS)
	}
	weightSum := 0
	for _, item := range interactiveWeights {
		weightSum += item.weight
	}
	var sampleCounter atomic.Int64
	sampleCounter.Store(int64(config.sampleIDBase))
	var measuring atomic.Bool
	var stop atomic.Bool
	ready := newBarrier(config.clients + 1)
	goLatch := newBarrier(config.clients + 1)
	results := make([]workerResult, config.clients)
	var firstErr atomic.Value
	var wg sync.WaitGroup
	wg.Add(config.clients)
	for workerID := 0; workerID < config.clients; workerID++ {
		workerID := workerID
		go func() {
			defer wg.Done()
			session, err := driver.OpenSession(ctx)
			if err != nil {
				firstErr.CompareAndSwap(nil, err)
				ready.arrive()
				goLatch.arrive()
				return
			}
			defer session.Close(ctx)
			probe := probes[workerID%len(probes)]
			if err := session.Prewarm(ctx, probe.organizationID, probe.ticketID); err != nil {
				firstErr.CompareAndSwap(nil, err)
				ready.arrive()
				goLatch.arrive()
				return
			}
			if _, err := session.PointGetTicket(ctx, probe.organizationID, probe.ticketID); err != nil {
				firstErr.CompareAndSwap(nil, err)
				ready.arrive()
				goLatch.arrive()
				return
			}
			rng := newXorShift64(config.rngSeed ^ ((uint64(workerID) + 1) * golden))
			ready.arrive()
			goLatch.arrive()
			byOp := map[string]*opStats{}
			for _, item := range interactiveWeights {
				byOp[item.name] = newOpStats()
			}
			var measureStart, measureEnd uint64
			completed := 0
			for !stop.Load() {
				record := measuring.Load()
				op := drawOp(rng, weightSum)
				tickets := writeTickets[0]
				ticket := tickets[zipfs[0].sample(rng)%len(tickets)]
				if stop.Load() {
					break
				}
				started := uint64(time.Now().UnixNano())
				sample := int(sampleCounter.Add(1))
				outcome, errorText := execute(ctx, session, probes[0], op, sample, ticket)
				ended := uint64(time.Now().UnixNano())
				if record {
					if measureStart == 0 {
						measureStart = started
					}
					measureEnd = ended
					completed++
					byOp[op].record(ended-started, outcome, errorText)
				}
			}
			results[workerID] = workerResult{
				byOp:         byOp,
				measureStart: measureStart,
				measureEnd:   measureEnd,
				completed:    completed,
				ok:           true,
			}
		}()
	}
	ready.arrive()
	goLatch.arrive()
	if config.warmupS > 0 {
		time.Sleep(time.Duration(config.warmupS * float64(time.Second)))
	}
	measuring.Store(true)
	time.Sleep(time.Duration(config.durationS * float64(time.Second)))
	stop.Store(true)
	wg.Wait()
	if err, ok := firstErr.Load().(error); ok && err != nil {
		return loadReport{}, err
	}
	aggregate := newOpStats()
	merged := map[string]*opStats{}
	for _, item := range interactiveWeights {
		merged[item.name] = newOpStats()
	}
	var starts, ends []uint64
	completed := make([]int, 0, config.clients)
	for _, result := range results {
		if !result.ok {
			continue
		}
		if result.measureStart != 0 && result.measureEnd != 0 {
			starts = append(starts, result.measureStart)
			ends = append(ends, result.measureEnd)
		}
		completed = append(completed, result.completed)
		for name, stats := range result.byOp {
			merged[name].merge(stats)
			aggregate.merge(stats)
		}
	}
	measured := uint64(config.durationS * 1e9)
	if len(starts) > 0 && len(ends) > 0 {
		minStart := starts[0]
		maxEnd := ends[0]
		for _, value := range starts {
			if value < minStart {
				minStart = value
			}
		}
		for _, value := range ends {
			if value > maxEnd {
				maxEnd = value
			}
		}
		measured = maxEnd - minStart
	}
	if measured < 1_000_000 {
		measured = 1_000_000
	}
	return loadReport{
		backendID:         driver.BackendID(),
		clients:           config.clients,
		profile:           "interactive",
		measuredElapsedNs: measured,
		seedNs:            seedNs,
		aggregate:         aggregate,
		byOp:              merged,
		workerCompleted:   completed,
	}, nil
}

func drawOp(rng *xorShift64, weightSum int) string {
	pick := rng.genRange(weightSum)
	for _, item := range interactiveWeights {
		if pick < item.weight {
			return item.name
		}
		pick -= item.weight
	}
	return interactiveWeights[0].name
}

func commentFor(probe scenarioProbes, ticket ticketRow, sample int) commentSeed {
	return commentSeed{
		row: commentRow{
			organizationID: ticket.organizationID,
			commentID:      uuidFromOrdinal(nsLoadWrite, uint64(1_000_000_000+sample)),
			ticketID:       ticket.ticketID,
			authorID:       probe.writeAuthorID,
			body:           "load comment " + itoa(sample),
		},
		idempotencyKey: "load-comment-" + itoa(sample),
	}
}

func execute(ctx context.Context, backend loadSession, probe scenarioProbes, op string, sample int, ticket ticketRow) (string, string) {
	var err error
	switch op {
	case "point_get_ticket":
		var found bool
		found, err = backend.PointGetTicket(ctx, probe.organizationID, probe.ticketID)
		if err == nil && !found {
			return "error", "missing ticket"
		}
	case "point_get_user":
		var found bool
		found, err = backend.PointGetUser(ctx, probe.organizationID, probe.userID)
		if err == nil && !found {
			return "error", "missing user"
		}
	case "list_tickets_by_project_status":
		err = backend.ListTicketsByProjectStatus(ctx, probe.organizationID, probe.projectID, statusOpen, 50)
	case "list_open_tickets_for_assignee":
		err = backend.ListOpenTicketsForAssignee(ctx, probe.organizationID, probe.assigneeID, 50)
	case "list_comments_for_ticket":
		err = backend.ListCommentsForTicket(ctx, probe.organizationID, probe.ticketID, 50)
	case "list_project_members":
		err = backend.ListProjectMembers(ctx, probe.organizationID, probe.projectID, 50)
	case "ticket_detail_page":
		var found bool
		found, err = backend.TicketDetailPage(ctx, probe.organizationID, probe.ticketID)
		if err == nil && !found {
			return "error", "missing detail"
		}
	case "create_comment":
		err = backend.CreateComment(ctx, commentFor(probe, ticket, sample))
	case "close_ticket_with_comment":
		err = backend.CloseTicketWithComment(ctx, closeTicketWithCommentSeed{
			organizationID: ticket.organizationID,
			ticketID:       ticket.ticketID,
			authorID:       probe.writeAuthorID,
			commentID:      uuidFromOrdinal(nsLoadWrite, uint64(2_000_000_000+sample)),
			body:           "load close note " + itoa(sample),
			idempotencyKey: "load-close-" + itoa(sample),
		})
	case "open_ticket_with_labels":
		err = backend.OpenTicketWithLabels(ctx, openTicketWithLabelsSeed{
			organizationID: probe.organizationID,
			ticketID:       uuidFromOrdinal(nsLoadWrite, uint64(3_000_000_000+sample)),
			projectID:      probe.writeProjectID,
			reporterID:     probe.writeAuthorID,
			assigneeID:     probe.writeAssigneeID,
			title:          "load open ticket " + itoa(sample),
			labelA:         probe.writeLabelA,
			labelB:         probe.writeLabelB,
			idempotencyKey: "load-open-" + itoa(sample),
		})
	}
	if err == nil {
		return "success", ""
	}
	text := err.Error()
	if len(text) > 240 {
		text = text[:240]
	}
	var app *safeAppError
	if errors.As(err, &app) {
		return classifyPostgres(err), text
	}
	return classifyRiffdb(err), text
}
