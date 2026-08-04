# Reactive TicketDesk fresh-agent evidence

This directory owns the independent implementation evidence required by
WP-421. It is separate from the in-repository TicketDesk acceptance tests: an
evaluator receives only a sealed public bundle, symbolic author sources, and an
empty workspace.

Build a candidate bundle outside the repository:

```bash
TMPDIR="$HOME/tmp" \
  ./scripts/reactive-ticketdesk-evaluation-package \
  "$HOME/tmp/riffdb-reactive-ticketdesk-bundle"
```

The bundle contains public binaries, documentation, SDK/runtime artifacts, and
the Application Source V4 contract/query/reactive tree. It deliberately omits
the checked Lock V5, generated clients, application server, browser, worker,
tests, RiffDB implementation source, and repository TicketDesk implementation.

Published runs are immutable and content-addressed by a campaign manifest. The
gate re-creates the bundle from the current revision, validates every evidence
hash, checks the report and value-free transcript, recompiles the supplied
author sources against the evaluator's lock, and requires every WP-421 check:

```bash
TMPDIR="$HOME/tmp" \
  ./scripts/reactive-ticketdesk-evaluation-acceptance \
  --campaign wp421-reactive-01 --assert-gate
```

A failed evaluator remains published as failed evidence. Do not change its
rating, checks, or explanation to satisfy the gate; fix the owning public
product surface and run a new campaign with a new bundle and agent identity.
