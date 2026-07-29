# Agent Application Alpha sealed evaluation

This directory defines the WP-340 evaluation protocol. It contains no completed
run and no synthetic score. A gate report is valid only when four fresh agents
independently produce the required raw reports and transcripts from the same
sealed bundle.

Prepare a release-derived bundle:

```bash
./scripts/agent-application-alpha-package /absolute/empty/output
```

Run one brief per isolated agent/context:

- `briefs/blog-rust.md`
- `briefs/blog-typescript.md`
- `briefs/orders-rust.md`
- `briefs/orders-typescript.md`

The evaluator provides the bundle and the selected brief, an empty writable
repository, network disabled, and no RiffDB implementation or TicketDesk
source. It sets `PATH` to the bundle binaries and `CARGO_HOME` to the bundle's
sealed `.cargo` directory. Agents may use only bundled public docs, CLI, MCP, and
generated application packages.

Each runner records value-free events using `event-schema.json`, writes its raw
JSONL transcript under `runs/<run-id>/events.jsonl`, and writes a report
conforming to `report-schema.json`. Credentials and application values must
never enter either artifact.

The gate is intentionally fail-closed:

```bash
./scripts/agent-application-alpha-acceptance --runs 4 --sealed --assert-gate
```

Missing runs, duplicate agent identities, source access, a kernel escape,
handwritten glue, an unresolved query shape, a missed time threshold, or a
rating below 8.5 fails the gate. A maintainer cannot replace an independent run
with a harness self-test.
