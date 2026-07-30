# Agent Application Alpha sealed evaluation

This directory defines the sealed evaluation protocol. The `runs` directory
contains the original four Terra evaluations from one sealed bundle. They are
raw failed-run evidence, not synthetic scores and not a passing gate.
`campaigns/campaign-01.json` is the immutable, hash-qualified selection of
those files; later campaigns must never discover evidence by scanning a shared
directory or replace a prior selection.

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
JSONL transcript, and writes a report conforming to `report-schema.json`.
First-write and first-page-read timings count only when a separate
`qualified-events.jsonl` record conforms to `qualified-event-schema.json` and
matches the compiler-owned application lock: contract, module, query, plan,
commit sequence, application head, and read-after-commit fence are all exact.
Credentials and application values must never enter any evidence artifact.

The report schema accepts both successes and failures. It records booleans,
counts, nullable first-success timings, and bounded unsupported-shape
diagnostics without embedding the release thresholds. The gate checker—not
the evidence schema—decides whether a valid report passes. This distinction
ensures a failed run remains publishable evidence instead of becoming an
unrepresentable result.

The gate is intentionally fail-closed:

```bash
./scripts/agent-application-alpha-acceptance --runs 4 --sealed --assert-gate
```

Missing runs, duplicate agent identities, source access, a kernel escape,
handwritten glue, an unresolved query shape, a missed time threshold, or a
rating below 8.5 fails the gate. A maintainer cannot replace an independent run
with a harness self-test.

WP-365 adds a public-only four-way rehearsal and a two-run canary:

```bash
./scripts/agent-application-alpha-rehearsal --assert-complete
./scripts/agent-application-alpha-canary-acceptance \
  --runs 2 --sealed --assert-gate
```

The canary campaign is selected by
`campaigns/wp365-canary-01.json`. It must contain exactly Blog/Rust and
Orders/TypeScript from distinct fresh agents, and it pins the report,
transcript, qualified events, and compiler lock by SHA-256. The checker also
revalidates every hash in campaign 01 before considering the canary.

The current four reports intentionally fail this command. They remain
publishable because the report schema records observed outcomes while the gate
checker enforces release thresholds. See
`release/evidence/agent-application-alpha-gate-v1.json` for the aggregate
`not_eligible` decision.
