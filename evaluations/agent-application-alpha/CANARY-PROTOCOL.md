# WP-365 sealed canary protocol

The evaluator supplies one sealed bundle, one brief, one existing empty
application directory, and one existing empty evidence directory. The agent
has no RiffDB implementation or TicketDesk source. Network access is disabled.

Use only:

- binaries on the supplied `PATH`;
- public documentation and SDK/runtime files inside the bundle;
- the selected brief;
- the empty application and evidence directories.

Do not inspect another filesystem tree to find examples or implementation
details. Do not ask for product guidance. A missing public instruction is a
compiler/runtime failure or unsupported shape, not a reason to access source.

Write these evidence files:

- `events.jsonl`, conforming to `event-schema.json` for predecessor campaigns
  or `event-schema-v2.json` for package-first campaign 03;
- `qualified-events.jsonl`, conforming to
  `qualified-event-schema.json`;
- `report.json`, conforming to `report-schema.json` for predecessor campaigns
  or `report-schema-v2.json` for package-first campaign 03;
- `riffdb.application.lock.json`, copied byte-for-byte from the completed
  application.

All evidence is value-free. Never include credentials, endpoints, command
inputs, entity identifiers, fixture values, returned application records, or
source text. Stable operation names and compiler/runtime identity hashes are
required and are not application values.

The ordinary event transcript records chronology. Record `first_write` only
from the original successful command response with `replayed: false`; a later
idempotent replay proves recovery behavior but cannot substitute for the first
committed row. Record `first_page_read` only after a read fenced by that exact
commit. Each must have exactly one qualified event:

- first write: elapsed time, generated command name, returned/replay-validated
  command plan hash, contract identity from the generated package, durable
  commit sequence, and replay status;
- first page read: elapsed time, generated query name, returned contract,
  module, query, and plan identity, application head, and the exact prior
  commit used as the read-after-commit fence.

If the public generated result does not expose and verify an identity required
by the qualified schema, do not infer it or read product source. Record no
qualified success, report the public product defect, and leave the run failed.

Before `complete`, run the bundled application-boundary checker against the
application and the domain's complete workload. `complete` is the final event
in the transcript; nothing may retroactively alter a completed run. The rating
is the agent's independent assessment; it must never be chosen to satisfy the
gate.

`handwritten_glue_lines` counts only application-authored RiffDB adaptation:
transport or RPC wrappers, parameter/result maps, wire-value encoders or
decoders, capability/grant construction, or response-shape decoding written in
place of the generated application client and product runtime. Do not count
ordinary domain logic, HTTP handlers, generated-client method calls, typed
outcome branching, view rendering, tests, or value-free identity evidence.
When the boundary checker passes and none of the counted adaptation exists,
record zero. Product-owned generated files and bundled runtimes never count as
handwritten glue.

For the final deployable-alpha campaign, evaluators run Blog/Go, Blog/Rust,
Blog/TypeScript, Orders/Python, Orders/Rust, and Orders/TypeScript from one
sealed bundle. This is a language-coverage extension, not a weakening or
replacement of the immutable four-run campaign 02.

Campaign 03 starts with an empty application directory and the signed package
distribution identified by the v2 bundle. The evaluator may place the
verified package CLI and server harness on `PATH`; every application runtime
must come from the bundle's npm, PyPI, Go-proxy, or Cargo package mirror. A
repository checkout or compatibility-vendored runtime fails the cell. Record
one `package_install` event, every `identity_change_ceremony` event, and every
`rescue` event. In the report, `time_to_first_committed_row_seconds` must equal
the qualified first-write time, the ceremony and rescue counts must equal the
corresponding transcript counts, and the package-distribution digest must
equal `bundle.json`. Package installation itself is not an identity-change
ceremony. Ordinary compiler diagnostics that the agent resolves without
outside product guidance are not rescues.

The sealed bundle contains a source-pruned runtime subset, not a second copy of
the complete signed distribution. Verify the outer bundle inventory and
`packages/runtime-subset-checksums.sha256`. Treat
`packages/qualification/receipt.json` and its retained original inventory and
signature as evidence that the complete distribution was verified before
pruning. A missing selected runtime file fails the cell; the absence of an
unselected implementation archive does not. The runtime subset must not carry
a stale `checksums.sha256` or detached signature claiming that the pruned tree
is the complete distribution.

Use the bundle-owned package environment exactly. Rust sets
`CARGO_HOME=<bundle>/.cargo` and never copies an ambient Cargo configuration
into the application; its generated lock must pass offline `--locked` checks
unchanged. Go/npm caches may be redirected to a new writable directory under
the application root when the host default is read-only. For Go and TypeScript
HTTP applications, the bundled `riffdb dev --seed --run` process is expected to
remain alive while serving: wait for the public application-ready marker,
exercise the required HTTP page, and then terminate it. A live server is not a
runtime failure, and bypassing the bundle's development runner loses the
qualified driver/compiler environment.

For a satisfaction campaign, evaluators follow this identical protocol; they
are not told to manufacture a target score. After the Python, Rust, and
TypeScript reports are published, `scripts/agent-satisfaction-canary-acceptance`
independently checks that every rating is strictly greater than 9 in addition
to all ordinary canary guarantees.
