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

The starter connects with the driver socket plus the host's public handshake
identity. Those hashes are substitution checks, not authority. Do not put a
credential, remote endpoint, raw method name, numeric compiler ID, field mask,
or Protobuf value in Go application code.

Current limitation: the Go/driver-host path is Linux-only in the alpha. The Go
runtime has no pure-Go remote fallback.
