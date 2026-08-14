package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	riffdb "riffdb.dev/application"
	ticketdesk "riffdb.dev/examples/ticketdesk/generated/go"
)

var tenants = [...]string{"tenant_alpha", "tenant_beta", "tenant_gamma", "tenant_delta"}

var requiredCoverage = [...]string{
	"cold_keys", "events", "hot_keys", "live_queries",
	"multiple_tenants", "reads", "workflows", "writes",
}

type identityFile struct {
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

func (value identityFile) runtime() riffdb.Identity {
	return riffdb.Identity{
		ApplicationManifestHash: value.ApplicationManifestHash,
		OperationCatalogHash:    value.OperationCatalogHash,
		ContractLineage:         value.ContractLineage,
		ContractVersion:         value.ContractVersion,
		ContractBundleHash:      value.ContractBundleHash,
		Database:                value.Database,
		Role:                    value.Role,
		RoleDefinitionHash:      value.RoleDefinitionHash,
		RemoteIdentityHash:      value.RemoteIdentityHash,
	}
}

type metricFile struct {
	Schema               string            `json:"schema"`
	Language             string            `json:"language"`
	PID                  int               `json:"pid"`
	StartedUnixSeconds   int64             `json:"started_unix_seconds"`
	LogicalOperations    uint64            `json:"logical_operations"`
	TransportAttempts    uint64            `json:"transport_attempts"`
	DeclaredRetries      uint64            `json:"declared_retries"`
	ErrorCount           uint64            `json:"error_count"`
	ModeledRetainedBytes uint64            `json:"modeled_retained_bytes"`
	Workloads            map[string]uint64 `json:"workloads"`
	Tenants              map[string]uint64 `json:"tenants"`
}

type metrics struct {
	mu    sync.Mutex
	path  string
	value metricFile
}

func newMetrics(path string) *metrics {
	workloads := map[string]uint64{"events": 0, "live_queries": 0, "reads": 0, "workflows": 0, "writes": 0}
	tenantCounts := make(map[string]uint64, len(tenants))
	for _, tenant := range tenants {
		tenantCounts[tenant] = 0
	}
	return &metrics{path: path, value: metricFile{
		Schema: "riffdb.alpha-endurance-worker/v1", Language: "go", PID: os.Getpid(),
		StartedUnixSeconds: time.Now().Unix(), Workloads: workloads, Tenants: tenantCounts,
	}}
}

func (value *metrics) record(workload, tenant string, retained uint64) error {
	value.mu.Lock()
	defer value.mu.Unlock()
	value.value.LogicalOperations++
	value.value.TransportAttempts++
	value.value.ModeledRetainedBytes += retained
	value.value.Workloads[workload]++
	value.value.Tenants[tenant]++
	if value.value.LogicalOperations%64 == 0 {
		return value.publishLocked()
	}
	return nil
}

func (value *metrics) publish() error {
	value.mu.Lock()
	defer value.mu.Unlock()
	return value.publishLocked()
}

func (value *metrics) publishLocked() error {
	body, err := json.Marshal(value.value)
	if err != nil {
		return err
	}
	body = append(body, '\n')
	next := value.path + ".next"
	if err := os.WriteFile(next, body, 0o600); err != nil {
		return err
	}
	return os.Rename(next, value.path)
}

type clients struct {
	seeder      *ticketdesk.Client
	application *ticketdesk.Client
	agent       *ticketdesk.Client
	sessions    []*riffdb.Session
}

func (value *clients) close() {
	for _, session := range value.sessions {
		_ = session.Close()
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	root, clientsCount, rate, seed, err := requireEnvironment()
	if err != nil {
		return err
	}
	metric := newMetrics(filepath.Join(root, "environment-v1", "metrics", "go.json"))
	if err := metric.publish(); err != nil {
		return err
	}
	delay := time.Duration((uint64(time.Second) * clientsCount) / rate)
	ctx := context.Background()
	errorsChannel := make(chan error, clientsCount)
	for index := uint64(0); index < clientsCount; index++ {
		go func() { errorsChannel <- runClient(ctx, root, seed, index, delay, metric) }()
	}
	return <-errorsChannel
}

func requireEnvironment() (string, uint64, uint64, uint64, error) {
	if os.Getenv("RIFFDB_ENDURANCE_LANGUAGE") != "go" {
		return "", 0, 0, 0, errors.New("Go endurance language differs from the closed manifest")
	}
	root := os.Getenv("RIFFDB_ENDURANCE_ARTIFACT_ROOT")
	if !filepath.IsAbs(root) || root == "" {
		return "", 0, 0, 0, errors.New("RIFFDB_ENDURANCE_ARTIFACT_ROOT must be absolute")
	}
	info, err := os.Lstat(root)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", 0, 0, 0, errors.New("RIFFDB_ENDURANCE_ARTIFACT_ROOT must be a non-symlink directory")
	}
	var tenantNames []string
	if json.Unmarshal([]byte(os.Getenv("RIFFDB_ENDURANCE_TENANTS_JSON")), &tenantNames) != nil || len(tenantNames) != len(tenants) {
		return "", 0, 0, 0, errors.New("Go endurance tenants differ from the closed manifest")
	}
	for index, tenant := range tenants {
		if tenantNames[index] != tenant {
			return "", 0, 0, 0, errors.New("Go endurance tenants differ from the closed manifest")
		}
	}
	var coverage []string
	if json.Unmarshal([]byte(os.Getenv("RIFFDB_ENDURANCE_WORKLOAD_COVERAGE_JSON")), &coverage) != nil {
		return "", 0, 0, 0, errors.New("Go endurance workload coverage is invalid")
	}
	present := make(map[string]bool, len(coverage))
	for _, name := range coverage {
		present[name] = true
	}
	for _, name := range requiredCoverage {
		if !present[name] {
			return "", 0, 0, 0, errors.New("Go endurance workload coverage is incomplete")
		}
	}
	clientsCount, err := boundedUint("RIFFDB_ENDURANCE_CLIENTS", 4, 4)
	if err != nil {
		return "", 0, 0, 0, err
	}
	rate, err := boundedUint("RIFFDB_ENDURANCE_MAXIMUM_OPERATIONS_PER_SECOND", 1, 1024)
	if err != nil {
		return "", 0, 0, 0, err
	}
	seed, err := boundedUint("RIFFDB_ENDURANCE_SEED", 1, ^uint64(0))
	return root, clientsCount, rate, seed, err
}

func boundedUint(name string, minimum, maximum uint64) (uint64, error) {
	value, err := strconv.ParseUint(os.Getenv(name), 10, 64)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s is outside its checked bound", name)
	}
	return value, nil
}

func connect(ctx context.Context, environmentRoot, role string) (*ticketdesk.Client, *riffdb.Session, error) {
	body, err := os.ReadFile(filepath.Join(environmentRoot, role+".identity.json"))
	if err != nil {
		return nil, nil, err
	}
	var wire identityFile
	if err := json.Unmarshal(body, &wire); err != nil {
		return nil, nil, err
	}
	session, err := riffdb.Connect(ctx, filepath.Join(environmentRoot, role+".sock"), wire.runtime())
	if err != nil {
		return nil, nil, err
	}
	client, err := ticketdesk.NewClient(session, 1)
	if err != nil {
		_ = session.Close()
		return nil, nil, err
	}
	return client, session, nil
}

func connectClients(ctx context.Context, environmentRoot string) (*clients, error) {
	value := &clients{}
	for _, role := range []string{"seeder", "application", "agent"} {
		client, session, err := connect(ctx, environmentRoot, role)
		if err != nil {
			value.close()
			return nil, err
		}
		value.sessions = append(value.sessions, session)
		switch role {
		case "seeder":
			value.seeder = client
		case "application":
			value.application = client
		case "agent":
			value.agent = client
		}
	}
	return value, nil
}

func runClient(ctx context.Context, root string, seed, index uint64, delay time.Duration, metric *metrics) error {
	environmentRoot := filepath.Join(root, "environment-v1")
	clientSet, err := connectClients(ctx, environmentRoot)
	if err != nil {
		return err
	}
	defer clientSet.close()
	tenant := tenants[index]
	organizationID := id(10, index)
	userID := id(seed+1, 100+index)
	projectID := id(seed+1, 200+index)
	hotTicketID := id(seed+1, 300+index)
	if err := seedClient(ctx, clientSet, metric, tenant, organizationID, userID, projectID, hotTicketID, seed, index); err != nil {
		return err
	}
	events := clientSet.agent.TicketEvents(ticketdesk.TicketEventsParams{OrganizationId: organizationID}, fmt.Sprintf("endurance-go-events-%d", index))
	triage := clientSet.agent.TriageTicket(ticketdesk.TriageTicketParams{OrganizationId: organizationID}, fmt.Sprintf("endurance-go-triage-%d", index))
	for counter := uint64(0); ; counter++ {
		slot := counter % 100
		switch {
		case slot < 35:
			result, err := clientSet.application.TicketPage(ctx, ticketdesk.TicketPageParams{OrganizationId: organizationID, TicketId: hotTicketID}, ticketdesk.QueryOptions{MaximumAttempts: 1})
			if err != nil {
				return err
			}
			if _, ok := result.Value.(ticketdesk.TicketPageFound); !ok {
				return errors.New("Go endurance read lost its hot ticket")
			}
			err = metric.record("reads", tenant, 0)
		case slot < 60:
			_, err = clientSet.application.CreateComment(ctx, ticketdesk.CreateCommentInput{
				Body: fmt.Sprintf("go endurance comment %d", counter), AuthorId: userID,
				TicketId: hotTicketID, CommentId: id(seed+1, 10_000+index*1_000_000+counter),
				IdempotencyKey: fmt.Sprintf("endurance-go-comment-%d-%d", index, counter), OrganizationId: organizationID,
			})
			if err == nil {
				err = metric.record("writes", tenant, 512)
			}
		case slot < 70:
			var batch ticketdesk.ContextualBatch[ticketdesk.TicketEventsEvent]
			batch, err = triage.Next(ctx, 0)
			if err == nil {
				err = metric.record("workflows", tenant, 0)
			}
			if err == nil && len(batch.Items) > 0 {
				item := batch.Items[0]
				event, ok := item.Delivery.Event.(ticketdesk.TicketEventsTicketCreated)
				if !ok {
					return errors.New("Go endurance contextual event has the wrong type")
				}
				var reaction *ticketdesk.ContextualReaction
				for reactionIndex := range item.AvailableReactions {
					if item.AvailableReactions[reactionIndex].Name == "comment" {
						reaction = &item.AvailableReactions[reactionIndex]
						break
					}
				}
				if reaction == nil {
					return errors.New("Go endurance contextual reaction is absent")
				}
				_, err = triage.ReactComment(ctx, *reaction, ticketdesk.CreateCommentInput{
					Body: "go contextual endurance reaction", AuthorId: event.ReporterId,
					TicketId: event.TicketId, CommentId: id(seed+1, 20_000+index*1_000_000+counter),
					IdempotencyKey: fmt.Sprintf("endurance-go-reaction-%d-%d", index, counter), OrganizationId: organizationID,
				})
				if err == nil {
					err = metric.record("workflows", tenant, 512)
				}
			}
		case slot < 80:
			var batch ticketdesk.ConsumerBatch[ticketdesk.TicketEventsEvent]
			batch, err = events.Next(ctx, ticketdesk.ConsumerOptions{BatchLimit: 1, InFlightLimit: 4, LeaseSeconds: 60})
			if err == nil {
				err = metric.record("events", tenant, 0)
			}
			if err == nil && len(batch.Events) > 0 {
				_, err = events.Ack(ctx, batch.Events[0])
				if err == nil {
					err = metric.record("events", tenant, 0)
				}
			}
		default:
			watch := clientSet.application.WatchTicketQueueWatch(ticketdesk.TicketQueueWatchParams{OrganizationId: organizationID, ProjectId: projectID}, "")
			watchContext, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err = watch.Next(watchContext)
			cancel()
			if err == nil {
				err = metric.record("live_queries", tenant, 0)
			}
		}
		if err != nil {
			return err
		}
		if slot == 69 {
			ordinal := (counter / 100) % 4096
			_, err = clientSet.application.CreateTicket(ctx, ticketdesk.CreateTicketInput{
				Title: fmt.Sprintf("Go cold ticket %d-%d", index, ordinal), Status: ticketdesk.TicketStatus("Open"),
				TicketId: id(seed+1, 30_000+index*4096+ordinal), ProjectId: projectID,
				AssigneeId: userID, ReporterId: userID,
				IdempotencyKey: fmt.Sprintf("endurance-go-cold-%d-%d", index, ordinal), OrganizationId: organizationID,
			})
			if err != nil {
				return err
			}
			if err := metric.record("workflows", tenant, 768); err != nil {
				return err
			}
		}
		time.Sleep(delay)
	}
}

func seedClient(ctx context.Context, clients *clients, metric *metrics, tenant, organizationID, userID, projectID, ticketID string, seed, index uint64) error {
	operations := []struct {
		call func() error
		size uint64
	}{
		{func() error {
			_, err := clients.seeder.CreateOrganization(ctx, ticketdesk.CreateOrganizationInput{Name: "Endurance " + tenant, OrganizationId: organizationID, IdempotencyKey: "endurance-organization-" + tenant})
			return err
		}, 512},
		{func() error {
			_, err := clients.seeder.CreateUser(ctx, ticketdesk.CreateUserInput{Email: fmt.Sprintf("go-%d@%s.example.test", index, tenant), UserId: userID, DisplayName: fmt.Sprintf("Go endurance %d", index), IdempotencyKey: fmt.Sprintf("endurance-go-user-%d", index), OrganizationId: organizationID})
			return err
		}, 512},
		{func() error {
			_, err := clients.seeder.CreateProject(ctx, ticketdesk.CreateProjectInput{Name: fmt.Sprintf("Go endurance %d", index), ProjectId: projectID, IdempotencyKey: fmt.Sprintf("endurance-go-project-%d", index), OrganizationId: organizationID})
			return err
		}, 512},
		{func() error {
			_, err := clients.application.CreateTicket(ctx, ticketdesk.CreateTicketInput{Title: fmt.Sprintf("Go hot ticket %d", index), Status: ticketdesk.TicketStatus("Open"), TicketId: ticketID, ProjectId: projectID, AssigneeId: userID, ReporterId: userID, IdempotencyKey: fmt.Sprintf("endurance-go-hot-%d-%d", seed, index), OrganizationId: organizationID})
			return err
		}, 768},
	}
	for _, operation := range operations {
		if err := operation.call(); err != nil {
			return err
		}
		if err := metric.record("writes", tenant, operation.size); err != nil {
			return err
		}
	}
	return metric.publish()
}

func id(namespace, value uint64) string {
	return fmt.Sprintf("%08x-0000-8000-8000-%012x", namespace&0xffffffff, value&0xffffffffffff)
}
