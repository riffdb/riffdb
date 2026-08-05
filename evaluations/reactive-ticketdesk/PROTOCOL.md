# WP-421 sealed reactive evaluation protocol

The evaluator receives one sealed bundle, one existing empty application
workspace, and one existing empty evidence directory. Network access is
disabled. The bundle contains public binaries, documentation, SDK/runtime
artifacts, and TicketDesk symbolic author sources. It contains no RiffDB
implementation source, generated TicketDesk bindings, browser relay, worker,
or acceptance implementation.

Use only the supplied bundle and workspace. Do not inspect another filesystem
tree or ask for product guidance. Missing public instructions or capabilities
are product failures, not permission to inspect the source repository.

## Runtime role setup

TicketDesk deliberately separates `TicketDeskSeeder`,
`TicketDeskApplication`, and `TicketDeskAgent`. Its symbolic source has no
manifest seed inputs, and the one-role development helper cannot retain all
three authorities for this evaluation. For live acceptance, start one
disposable server, deploy the exact generated application once without role
provisioning, then bind each role independently against
`generated/riffdb.application.exact.json` with the operator credential:

```text
riffdb ... application deploy
riffdb ... role bind generated/riffdb.application.exact.json \
  --role TicketDeskSeeder --principal service:seeder --actor-kind service \
  --audience riffdb-grpc-loopback --credential-output runtime/seeder.credential
riffdb ... role bind generated/riffdb.application.exact.json \
  --role TicketDeskApplication --principal service:application --actor-kind service \
  --audience riffdb-grpc-loopback --credential-output runtime/application.credential
riffdb ... role bind generated/riffdb.application.exact.json \
  --role TicketDeskAgent --principal agent:triage --actor-kind agent \
  --audience riffdb-grpc-loopback --credential-output runtime/agent.credential
```

Use only the seeder credential for external seed batches, only the application
credential for the browser relay, and only the agent credential for contextual
work and MCP. Do not provision the three roles through successive
`application deploy --provision-role` calls: replacing the deployment's
retained role credential revokes its predecessor. Do not combine the roles or
widen any role to simplify the evaluation.

Write these value-free evidence files:

- `events.jsonl`, conforming to `event-schema.json`;
- `report.json`, conforming to `report-schema.json`; and
- `riffdb.application.lock.json`, copied byte-for-byte from the generated
  application.

Never record credentials, endpoints, command inputs, entity identifiers,
fixture values, returned records, event payloads, hydrated context, lease or
causation tokens, or source text. Stable operation names, compiler identities,
hashes, test names, and boolean results are permitted.

Every successful report check must be backed by an executed deterministic test
or boundary command. Browser convergence requires two independent browser
sessions or stores, not two references to one state object. Reaction recovery
must inject the failure after a committed generated reaction and before ack,
then prove redelivery resolves the original outcome without a second business
mutation. MCP wakeup inspection must reject any event or context payload.

Record chronological failures and checks in `events.jsonl`. Record the
independent rating before the single final `complete` event; `complete` must be
the last event in the transcript. Set `completed` only when every required
check passes. The rating is the evaluator's independent assessment and must not
be chosen to satisfy a gate.
