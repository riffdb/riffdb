# Product Conformance and Ecosystem Qualification

RiffDB owns its protocol contracts, expected behavior, and product/SDK
conformance checks. Adapter implementations and their framework tests belong
in their own repositories or an external playground. The main RiffDB build
never imports those adapter packages or installs them to run release CI.

The announcement review still covers all five tracks:

| Track | Required evidence | Owner |
| --- | --- | --- |
| Configuration | Named causes, repairs, redaction, human/v1/v2 output | RiffDB |
| Readiness | Every authorized blocker; no role-provisioning bypass | RiffDB |
| Compiler | Exact source locations, safe expected/found details, compatible v1 spans | RiffDB |
| Operations | Offline versions, rejected reload, bounded JSON logs, correlated traces and metrics | RiffDB |
| Ecosystem integration | Real framework workflows, todo sharing/organization, replay, isolation and restart | Consuming projects |

## Core artifact conformance

Obtain a product bundle and signed first-party driver distribution for the same
version, revision, and target. Verify release signatures and checksums first.
The archive must contain exactly `riffdb`, `riffdbd`, `riffdb-driverd`, and
`riffdb-mcp` under `bin/`. The driver distribution must have clean-source
metadata and carry the same CLI and driver-host bytes as the product archive.
Missing or mismatched artifacts refuse qualification.

Build the candidate image with the tracked `release/container/Dockerfile` and
the archive's `riffdb` / `riffdbd` bytes. Label it with
`org.opencontainers.image.revision=<exact revision>`. Qualification runs those
image binaries with networking disabled and compares versions and digests.

Use Linux, the pinned Rust toolchain, and the existing first-party SDK package
smoke prerequisites. With absolute paths to verified artifacts:

```bash
umask 077
qa="$(mktemp -d /tmp/riffdb-qa.XXXXXX)"
./scripts/announcement-readiness \
  --bundle-root "$bundle" --version "$version" --revision "$revision" \
  --driver-distribution "$drivers" \
  --allowed-signers "$signers" --signer-identity riffdb-release \
  --container-engine podman --container-image "$image" \
  --qualify-core --run-root "$qa/core" --report "$qa/core-result.json"
```

Use `--container-engine docker` for Docker. A pass reports `core_qualified:
true`. Checks cover exact artifact identity, offline versions, representative
configuration/compiler diagnostics, the installed product demo/MCP bridge, and
native/TypeScript/Python/Go/Rust package arrival. SDK smoke compilation validates
first-party delivered source crates; it supplies no replacement executable to
the running product tests.

## Implementation-neutral behavior contract

`fixtures/announcement-readiness/behavior-contract-v1.json` defines expected
bounded CRUD, idempotent retry, stale atomic rejection, authority isolation and
retained-state restart observations. Implementations exercise those behaviors
in their own projects. This keeps the dependency directed from consumers to
the core contract.

An external result names `contract_version`, `product_revision`, and a `cases`
object containing exactly the contract's case IDs with boolean pass results.
The producer's source, execution and artifact provenance require review.
`--behavior-evidence <FILE>` validates bounded receipt completeness and exact
revision binding without loading or invoking the implementation.

The combined Better Auth/OpenFGA/MLflow/todo rehearsal is external. It owns its
framework pins, package imports, application session logic, candidate adapter
profile migration, and real HTTP workflows. It consumes installed RiffDB
artifacts, the core behavior contract, and first-party conformance tools.
Its evidence must prove fresh installation, retained restart, sharing,
organization, stale atomic rejection, idempotent retry, authorization isolation,
readiness/reload failures and Collector correlation. No external AI provider or
manual role-binding readiness workaround is allowed.

`collector-v1.json` beside the behavior contract pins the official Collector
archive and binary. Generic `--fetch-collector --collector-binary <NEW_PATH>`
verifies those digests for consuming conformance projects. Core operational
semantics and signal isolation also have first-party runtime tests.

## Independent publication gate

After publication, run the anonymous check with a fresh private directory:

```bash
./scripts/announcement-readiness \
  --bundle-root "$bundle" --version "$version" --revision "$revision" \
  --probe-public --run-root "$qa/public" \
  --container-engine podman --report "$qa/public-result.json"
```

It downloads archives and `SHA256SUMS` without GitHub credentials and pulls the
image with an empty registry credential store. It checks executable inventory,
product/driver revisions, archive checksums, and image/source/binary identity.
A failure exits nonzero and blocks announcement independently of local tests.

Review the core, external ecosystem, and public receipts together. Neither a
core product pass nor an unexecuted external receipt establishes full ecosystem
readiness. Keep raw child output, credentials, sessions and failure tracebacks
in private runtime directories; publish only curated value-free evidence.

For custom server audiences, the operator bootstrap preset's `riffdb-cli` /
`riffdb-mcp` audiences do not apply. Use an explicit bootstrap request with the
configured audience. The standard rehearsal uses `riffdb-cli` and CLI-generated
application driver configuration.
