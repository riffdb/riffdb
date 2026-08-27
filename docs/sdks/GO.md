# Go applications

RiffDB Go applications use a generated, typed facade over one retained local
session to `riffdb-driverd`:

```text
generated Go operations
        |
        v
riffdb.dev/application
        |
        v
private Unix socket
        |
        v
riffdb-driverd -> verified remote RiffDB
```

The Go process never receives the remote endpoint, TLS configuration, or
capability credential. The Rust host owns connection pooling, authentication,
retries, uncertainty resolution, read-after-commit behavior, reactive cursor
resumption, and overload. The Go runtime owns only bounded local framing,
language values, context cancellation, and typed facade assembly.

Create an offline-buildable starter with:

```bash
riffdb new orders --language go
cd orders
GOPROXY=off go test ./...
```

The generated repository contains:

```text
go.mod
main.go
generated/go/client.go
third_party/riffdb-application/
riffdb.application.json
riffdb.application.lock.json
```

`generated/go/client.go` contains exact operation and schema identities,
typed query parameters/results, closed command outcomes, per-item batch
results, exact decimals/money, reactive iterators, lease operations, and live
query cursors. `context.Context` cancellation propagates to the Rust host; a
cancelled command is never reported as failed when its outcome is uncertain.

Eligible covering named queries use the negotiated driver V2 compact result.
The generated method validates its exact compiler-owned positional layout and
constructs the same typed result directly, without a per-row map or exposed
ordinal. The runtime retains legacy named-record decoding for compatible
servers and rejects malformed or mixed response shapes.

The starter connects with the driver socket plus the host's public handshake
identity. Those hashes are substitution checks, not authority. Do not put a
credential, remote endpoint, raw method name, numeric compiler ID, field mask,
or Protobuf value in Go application code.

For a repository created with `riffdb init --generator go`, author `main.go`
against the generated client and `riffdb.dev/application`, then run the whole
local stack with:

```bash
riffdb dev --seed --run
```

The development workflow starts `riffdb-driverd` and passes the private socket
plus the exact compiler-owned handshake identity as the program's ten positional
arguments. Their order is a closed development-runner ABI:

```text
SOCKET MANIFEST_HASH CATALOG_HASH DATABASE ROLE ROLE_HASH REMOTE_HASH LINEAGE VERSION BUNDLE_HASH
```

`VERSION` is the ninth value after the program name. The generated
`riffdb new --language go` starter demonstrates the complete checked decoding;
copy that connection prelude when authoring an application from `riffdb init`.
Do not guess or reorder the values, and do not derive, persist, or replace them.
They are substitution evidence used only to open the generated application
session. The workflow supports `go.mod` as a first-class runner manifest and
runs with the caller's selected offline module source.

A V7 Go-only application does not declare or retain an MCP artifact. Before
starting the driver, the development workflow derives its operation catalog
once from the exact application source and V8 lock in a private temporary
directory. The catalog remains compiler-checked and role-bounded; it is not a
hidden generated application surface and disappears when the workflow exits.

A Go library module can keep its executable in one dedicated package:

```bash
riffdb dev --seed --run --go-runner-package cmd/server
```

For a detected Go repository, the default remains `.`; omitting the option does
not classify a TypeScript, Rust, or Python repository as Go. The option accepts only one canonical,
repository-relative directory inside the root module and requires that package
to declare `package main`. Absolute paths, parent traversal, symlink escapes,
nested modules, non-main packages, and flag-like executable selection fail
before the driver starts or credentials exist. RiffDB invokes `go run` directly
with the checked package and prints it in the bounded runner summary. This is
development process configuration only: it changes no contract, query module,
role, application lock, plan, or production identity.

Current limitation: the Go/driver-host path is Linux-only in the alpha. The Go
runtime has no pure-Go remote fallback.
