# Release Verification

WP-200 assembles evidence; it does not turn a skipped test or missing dependency
into a pass.

Run from a clean checkout:

```bash
./scripts/ci-all
./scripts/demo --assert
./scripts/release-poc --verify
```

`scripts/demo --assert` verifies the candidate requirement map and writes a
revision-specific `demo_verified` report to `target/wp200/demo-report.json`
only after its live and process-level suites pass. POC-010 remains
`requires_release_verification` in that intermediate report. The demo consumes
the WP-139 report with the exact
`supported_application_mutation_surface` qualifier and rejects any attempt to
mark the unsafe variants benchmark eligible.

The same evidence check pins WP-190's checked report hash, command list, case
order, coverage counts, and production-gap inventory. Any unresolved
`production-gap` row rejects POC-009; a partial executed-case report is never
treated as complete crash coverage.

`scripts/release-poc --verify`:

1. rejects a dirty checkout or missing repository ownership or license notice;
2. reruns `./scripts/demo --assert` to replace all ignored target evidence at
   the exact clean revision, then runs `./scripts/ci-all` and validates checked
   evidence, systemd units, closed-stdin/SIGTERM behavior, authenticated MCP
   bridge framing, generated artifacts, and dependency policy;
3. builds exactly `riffdbd`, `riffdb`, and `riffdb-mcp` with the Git revision
   embedded in two independent Cargo target directories and compares every
   binary byte-for-byte, then bootstraps the release server and verifies its
   reported semantic version, Git revision, Rust version, feature set, storage
   format, contract IR version, and MCP protocol baseline;
4. produces deterministic CycloneDX 1.5 SBOMs and dependency trees from the
   product, nested budget-comparison, and isolated Fjall-comparison lockfiles;
5. assembles documentation, the installed-binary demo, the tracked budget
   comparison sources, service units, release notes, known limitations, and
   the final `locally_verified` requirement report with only POC-010 promoted
   from the demo report;
6. installs the staged bundle into a private root, compares every required
   asset and mode, checks the extracted layout, and runs the installed demo;
7. writes and verifies SHA-256 checksums; and
8. creates the release tarball twice, compares it byte-for-byte, and only then
   atomically publishes the private staging directory as `target/wp200/release`.

No `locally_verified` report is published under `target/wp200/release/` after a
failed run. The private staging directory is removed on normal failure. Outputs
are local artifacts until a maintainer signs the POC requirements. The source
and bundle are offered under `MIT OR Apache-2.0` and include the canonical
ownership notice and both license texts.

Copyright © 2026 Kevin O'Shea and O'Shea & Sons, LLC.

The systemd check combines `systemd-analyze verify`, exact hardening-directive
checks, direct daemon lifecycle execution, and the bridge's newline-delimited
socket framing. An unprivileged release job does not launch the units in the
host manager; installation verification on the target host remains required
for manager-enforced sandboxing and Unix-socket ownership.

The CycloneDX files conservatively inventory each locked resolved workspace.
The adjacent dependency text files narrow each normal/build graph. The budget
comparison source in the binary bundle retains repository-relative development
dependencies; run it from the matching full source revision rather than
treating it as a standalone installed product. The Fjall workspace remains an
engine-review harness and is not linked into or shipped as a product binary.

## Human Sign-Off

The final review must record:

- source revision and clean-tree proof;
- the three acceptance command results;
- POC-001 through POC-010 result report;
- benchmark hardware/methodology and raw artifacts;
- dependency, advisory, license, and SBOM review;
- backup/restore rewind acknowledgment;
- security posture and known limitations; and
- one decision: proceed to alpha, revise and repeat a gate, or stop.

## Deployable Application Alpha gate

The final installed gate is intentionally a coordinator, not another semantic
implementation:

```bash
./scripts/deployable-alpha-acceptance \
  --all-domains --all-languages --remote
```

It preflights the entire closed phase inventory before running anything, then
executes application-binding and durable-format checks, retained performance
verification, Compose and Kubernetes deployment checks, four-language driver
and adapter conformance, bulk/query/workflow/row-policy behavior, symbolic
export/reimport, destructive recovery, retained 72-hour endurance evidence,
installed bootstrap, and the sealed independent-agent evaluation. Missing
component scripts or evidence fail before an expensive partial gate.

Each phase receipt stores only its symbolic name, exit status, elapsed time,
and a SHA-256 of the reviewed argument array. A failed run remains under
`target/deployable-alpha-gate/`; only a complete pass publishes
`release/evidence/deployable-application-alpha-v1.json`. The coordinator cannot
waive a phase, shorten endurance, substitute a diagnostic benchmark, or print
command arguments into the receipt.

The retained WP-552 comparator corpus is verified separately with:

```bash
./scripts/check-alpha-performance-evidence --verify
```

That verifier checks both content hashes and the semantic 90-second,
three-repetition, idle-host, safe-app durability, stability, correctness, and
concurrency matrix. It does not rerun the benchmark during routine release
verification.
