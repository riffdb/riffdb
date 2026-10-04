# Sustainable TicketDesk preparation

This separate application lineage preserves the existing TicketDesk example
and the alpha endurance workload. It extends the TicketDesk domain with a
stable-population `UpdateTicket` command, explicit expected-revision evidence,
a `TicketUpdated` event, and the `TicketUpdates` counting projection. Named
`GetTicket` results include revision; generated event consumers and the
`TicketQueueWatch` live query retain the normal public authorization path.

Regenerate or check the compiler-owned closure from the repository root:

```bash
./scripts/generate-ticketdesk-lifecycle
./scripts/generate-ticketdesk-lifecycle --check
```

The check also compiles a missing-fence counterexample and requires both writes
to be rejected at their source spans. Generated Rust, Go, TypeScript, Python
and MCP artifacts are source artifacts; never edit them manually.

Build and run the real public TLS preparation smoke in a fresh private root:

```bash
cargo build -p riffdb-server --bin riffdbd -p riffdb-cli --bin riffdb \
  -p riffdb-ticketdesk --bin lifecycle
./scripts/ticketdesk-lifecycle-smoke \
  --root /tmp/riffdb-lifecycle-preparation \
  --server target/debug/riffdbd --cli target/debug/riffdb \
  --worker target/debug/lifecycle
```

The script creates one owned preparation database, bootstraps an operator,
deploys the exact application, and binds narrowly declared roles. The Rust
client checks original-command replay, stale-fence refusal, named state reads,
partition isolation, event consumption/acknowledgement and a live-query snapshot.
The script stops its daemon through the production shutdown signal. Logs and
credentials remain private; use a new root for each attempt.

The Rust `seed` mode creates exactly 65,536 tickets across 64 organizations
through 16 clients and generated compiled commands. It requires the protected
`RIFFDB_LIFECYCLE_SEED_CREDENTIAL`, `RIFFDB_LIFECYCLE_CA`, and verified HTTPS
`RIFFDB_LIFECYCLE_ENDPOINT` environment configuration. Original seed identities
are deterministic so an operator can repeat the same preparation safely.

These are preparation tools. The sustained workload, fault controller,
TypeScript smoke, independent campaign reconciliation, and real rehearsal
remain WP-825 work. No preparation output is a qualifying campaign receipt.
See the [milestone handoff](../../../docs/architecture/SUSTAINABLE-TICKETDESK.md).
