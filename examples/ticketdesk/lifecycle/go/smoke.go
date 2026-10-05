// req: END-003, END-007, END-008
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	app "lifecycle-preparation/generated"
	riffdb "riffdb.dev/application"
)

func require(ok bool) {
	if !ok {
		panic("lifecycle preparation assertion")
	}
}

func run(root string) error {
	encoded, err := os.ReadFile(filepath.Join(root, "application.identity.json"))
	if err != nil {
		return err
	}
	require(len(encoded) > 0 && len(encoded) <= 4096)
	var identity riffdb.Identity
	if err := json.Unmarshal(encoded, &identity); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	session, err := riffdb.Connect(ctx, filepath.Join(root, "application.sock"), identity)
	if err != nil {
		return err
	}
	defer session.Close()
	client, err := app.NewClient(session, 1)
	if err != nil {
		return err
	}
	org, ticket := "01900000-0000-7000-8001-000000000000", "01900000-0000-7000-8004-000000000000"
	params := app.GetTicketParams{OrganizationId: org, TicketId: ticket}
	before, err := client.GetTicket(ctx, params, app.QueryOptions{})
	if err != nil {
		return err
	}
	row, ok := before.Value.(app.GetTicketFound)
	require(ok && row.Ticket.TicketId == ticket && row.Ticket.Title == "Lifecycle ticket 0")
	missing, err := client.GetTicket(ctx, app.GetTicketParams{OrganizationId: "01900000-0000-7000-8001-000000000001", TicketId: ticket}, app.QueryOptions{})
	if err != nil {
		return err
	}
	_, ok = missing.Value.(app.GetTicketNotFound)
	require(ok)
	input := app.UpdateTicketInput{OrganizationId: org, TicketId: ticket,
		ExpectedRevision: ^uint64(0), Status: app.TicketStatusOpen,
		IdempotencyKey: fmt.Sprintf("ticketdesk-lifecycle-go-stale-%d", app.ContractVersion)}
	first, err := client.UpdateTicket(ctx, input)
	if err != nil {
		return err
	}
	replay, err := client.UpdateTicket(ctx, input)
	if err != nil {
		return err
	}
	_, ok = first.Outcome.(app.UpdateTicketStale)
	require(ok)
	_, ok = replay.Outcome.(app.UpdateTicketStale)
	require(ok && !first.Replayed && replay.Replayed && first.CommitSequence != nil && replay.CommitSequence != nil)
	require(*first.CommitSequence == *replay.CommitSequence && first.PlanHash == replay.PlanHash && first.OutcomeURI == replay.OutcomeURI)
	after, err := client.GetTicket(ctx, params, app.QueryOptions{})
	if err != nil {
		return err
	}
	unchanged, ok := after.Value.(app.GetTicketFound)
	require(ok && unchanged.Ticket.Revision == row.Ticket.Revision && unchanged.Ticket.Status == row.Ticket.Status)
	update, err := client.WatchTicketQueueWatch(app.TicketQueueWatchParams{OrganizationId: org, ProjectId: "01900000-0000-7000-8003-000000000000"}, "").Next(ctx)
	if err != nil {
		return err
	}
	require(update.Type == "snapshot" && update.HasValue)
	queue, ok := update.Value.(app.TicketQueueFound)
	require(ok && len(queue.Tickets) > 0 && len(queue.Tickets) <= 50)
	fmt.Println(`{"schema":"riffdb.ticketdesk-lifecycle-go-smoke/v1","named_read":true,"partition_isolation":true,"caller_fence":true,"idempotency":true,"state_unchanged":true,"live_query_snapshot":true}`)
	return nil
}

func main() {
	defer func() {
		if recover() != nil {
			fmt.Fprintln(os.Stderr, "ticketdesk-lifecycle-go-smoke: failed")
			os.Exit(1)
		}
	}()
	if len(os.Args) != 2 || len(os.Args[1]) > 4096 {
		os.Exit(2)
	}
	if run(os.Args[1]) != nil {
		fmt.Fprintln(os.Stderr, "ticketdesk-lifecycle-go-smoke: failed")
		os.Exit(1)
	}
}
