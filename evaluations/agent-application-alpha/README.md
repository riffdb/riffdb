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
- `briefs/orders-python.md`

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

The official successor command selects a campaign manifest rather than
scanning all historical runs:

```bash
TMPDIR="$HOME/tmp" ./scripts/agent-application-alpha-acceptance \
  --campaign campaign-02 --runs 4 --sealed --assert-gate
```

Do not publish `campaign-02` until WP-379 has an eligible successor decision.
The passing WP-365 and satisfaction canaries have different profile matrices
and cannot substitute for the four official Blog/Orders by Rust/TypeScript
runs.

The first WP-365 canary attempt is also retained under
`runs/wp365-blog-rust-terra-02` and
`runs/wp365-orders-typescript-terra-02`. Both are honest failures from sealed
bundle `0e19842b...`: the public development workflow did not keep a reachable
generated application attached, and Blog additionally deferred a valid
whole-query role-bound rejection until live binding. These files are not
selected as passing evidence and must not be rewritten by a later canary.

`campaigns/wp365-canary-01.json` selects the replacement fresh-agent runs from
bundle `b72679d6...`. Blog/Rust rated 8.8 and Orders/TypeScript rated 8.5; both
qualified a generated command and one-snapshot named page read with exact
returned contract/module/query/plan/commit identities, passed the application
boundary and golden workload, and used no kernel API or handwritten RiffDB
transport glue.

## Satisfaction gate above 9

The accepted Agent Application Alpha threshold remains 8.5 so prior immutable
evidence keeps its original meaning. Product satisfaction is a stricter
follow-on gate:

```bash
TMPDIR="$HOME/tmp" ./scripts/agent-satisfaction-canary-acceptance \
  --campaign <new-sealed-campaign-id>
```

The selected campaign must contain three fresh agents covering Blog/Rust,
Orders/TypeScript, and Orders/Python over one current sealed bundle. The
ordinary canary checker first
re-proves the exact bundle and evidence hashes, fresh independent contexts,
golden workloads, qualified runtime identities, zero intervention, zero
kernel attempts, zero handwritten glue, and zero unsupported shapes. The
satisfaction checker then requires **each** agent's independent rating to be
strictly greater than 9; a high score cannot average away a weaker experience.
Past reports and campaigns remain immutable and cannot satisfy this gate.
