# Python Application Driver

RiffDB's Python package is `riffdb-application`; application code imports
`riffdb_application`. Wheels use the stable `cp313-abi3` interface, so
installing a wheel does not require Rust. Building the source distribution
requires Rust 1.97.0.

The alpha release matrix is deliberately finite:

| Python | Platform | Architecture | Status |
| --- | --- | --- | --- |
| CPython 3.13 or 3.14 | manylinux 2.28 (glibc) | x86_64 | Supported alpha wheel |
| CPython 3.13 or 3.14 | manylinux 2.28 (glibc) | aarch64 | Supported alpha wheel |
| CPython | musllinux | x86_64 or aarch64 | Unsupported |
| CPython | macOS | x86_64 or arm64 | Unsupported |
| CPython | Windows | x86_64 or arm64 | Unsupported |
| PyPy | Any | Any | Unsupported |

Unsupported combinations fail installation; there is no pure-Python transport
fallback. The machine-readable source of truth, including the reason for each
unsupported family, is
`release/python/platform-matrix-v1.json` in the source distribution repository.

The package is application-only. It exposes generated named queries and typed
commands through `SyncApplicationTransport` and `AsyncApplicationTransport`.
It does not expose Protobuf stubs, kernel requests, administrative RPCs, SQL,
or the private native module. Both transports delegate gRPC status validation,
request identity, bounded retry, and uncertain-outcome behavior to the stable
Rust application client.

The wheel's in-process PyO3 transport and the local-socket driver use one
first-party Rust protocol core. That core alone admits values, applies nesting,
collection, string, and frame bounds, constructs typed query and command
requests, and classifies public failures. Python keeps its self-contained wheel
and does not require `riffdb-driverd`; it also cannot accept a value the socket
driver would reject. One collection is limited to 4,096 values, one string to
262,144 UTF-8 bytes, and enum names use the closed generated-symbol alphabet.
Compiled operation schemas may impose stricter limits.

Generated named queries that the compiler proves fully covered negotiate a
compact result from the native transport. The generated synchronous and
asynchronous decoders validate the exact plan-owned entity, field order,
cardinality, row width, enum values, and bounds before constructing the same
dataclasses directly. No ordinal or physical-index choice is exposed to Python,
and legacy named-record results remain accepted.

`AsyncApplicationTransport` also backs Application Source V5 generated event
iterators and live named-query iterators. Generated delivery types retain the
attempt-specific acknowledgement evidence, and live cursors are returned as
standard padded Base64 for persistence. See [Reactive Application
Clients](reactive/CLIENTS.md).

Generated synchronous and asynchronous command batch methods accept
`CommandBatchOptions(concurrency=...)` from 1 through 384 and at most 4,096
inputs. Each item remains an ordinary independently authorized and idempotent
command; the collection is not one transaction.

## Create an application

Build or obtain the matching RiffDB wheel, then create a source-layout project:

```bash
export RIFFDB_APPLICATION_WHEEL=/path/to/riffdb_application-0.1.0-cp313-abi3-linux_x86_64.whl
riffdb new my-app --language python
cd my-app
uv sync --locked
PYTHONPATH=src uv run --locked python -m unittest discover -s tests
```

Installed release bundles place the architecture-matching wheel under
`public/python`; the environment variable is only needed by a source-tree CLI
that has no installed bundle. The scaffold copies the wheel into `vendor/`,
pins its SHA-256 in `uv.lock`, and performs no registry lookup during
`uv sync --locked`.

Run the generated example after deploying and binding its application role:

```bash
PYTHONPATH=src uv run --locked python -m my_app \
  http://127.0.0.1:50051 \
  .riffdb/development/credentials/MyAppApplication.credential \
  my_database
```

`riffdb dev --run` supplies those same three values as both positional
arguments and the runner-owned `RIFFDB_ENDPOINT`, `RIFFDB_CREDENTIAL_FILE`, and
`RIFFDB_DATABASE` environment. A runner that accepts both forms must reject a
mismatch. Go and TypeScript do not receive these variables because their
application process is confined to the private driver-host protocol.

Credentials are Rust-owned, redacted, nonextractable, non-pickleable objects.
Use `BearerCredential.from_protected_file()` for a mode-0600 Linux credential.
Use `CallMetadata.with_database(DatabaseAlias("my_database"))` to select one
database before authentication.

For an authenticated remote listener, configure explicit private-CA trust;
Python never falls back to native roots or a trust-all mode:

```python
from riffdb_application import SyncApplicationTransport, VerifiedTlsConfig

tls = VerifiedTlsConfig(
    endpoint="https://riffdb.internal:7443",
    trust_root="/run/secrets/riffdb-ca.pem",
    server_name="riffdb.internal",
    pool_connections=4,
    streams_per_connection=64,
)
with SyncApplicationTransport.connect_verified_tls(tls, metadata) as transport:
    client = MyAppClient(transport, AttemptBudget(3))
```

The asynchronous transport exposes the same verified-TLS constructor. Pool
connections are bounded from 1 through 16 and streams per connection from 1
through 256 before native transport work begins.

## Existing applications

Application source v1 remains exact and does not gain Python implicitly. Preview
the explicit source-only migration:

```bash
riffdb application migrate --to v2
```

Write only the source manifest after review:

```bash
riffdb application migrate --to v2 --write
riffdb application lock --write
riffdb application lock --check
```

The migration command never writes a lock or generated artifact and never
contacts a server. Source v2 requires `generation.python`; lock v2 contains one
Python artifact path and digest. Normal lock review remains the only operation
that publishes compiler-derived identities and generated bytes.

## Direct installation

Install a released wheel with either tool:

```bash
python3 -m pip install ./riffdb_application-0.1.0-cp313-abi3-manylinux_2_28_x86_64.whl
uv pip install ./riffdb_application-0.1.0-cp313-abi3-manylinux_2_28_x86_64.whl
```

Source installation is conventional but compiles the bundled Rust dependency
graph:

```bash
RUSTUP_TOOLCHAIN=1.97.0 python3 -m pip install ./riffdb_application-0.1.0.tar.gz
```

Publishing to PyPI is intentionally a separate maintainer action. The POC
release pipeline produces and verifies artifacts but holds no upload token.

Maintainers build both wheels and the self-contained source distribution with:

```bash
scripts/build-python-distribution dist/python
```

The build also writes `riffdb-python-artifacts-v1.json`, a deterministic receipt
containing the platform-matrix digest and the name, SHA-256, and byte length of
each artifact. The sdist contains only the pinned Rust application-client
closure, required persistent Protobuf fixtures, licenses, and Python package;
it does not copy the repository or depend on a checkout. The release checks
install the wheel with Rust absent from `PATH` and install the sdist with Cargo
network access disabled.

That command defaults to `manylinux_2_28`. Local development hosts that are not
manylinux build environments can set `RIFFDB_PYTHON_COMPATIBILITY=linux` for a
non-release smoke artifact. The pinned GitHub workflow builds and installs the
x86_64 and aarch64 `cp313-abi3` wheels on CPython 3.13 and 3.14; it never
publishes them. Maintainers can run the same focused checks with:

```bash
./scripts/build-python-distributions --check
./scripts/test-python-platform-matrix
```
