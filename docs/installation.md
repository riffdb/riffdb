# Installation

RiffDB ships three self-contained binaries on Linux (`x86_64-unknown-linux-gnu`
and `aarch64-unknown-linux-gnu`) and Apple Silicon macOS
(`aarch64-apple-darwin`):

- **`riffdb`** — project initialization, contract compiler, SDK code generator,
  local `riffdb dev` supervisor, and administrative CLI.
- **`riffdbd`** — the authoritative database server and optional hosted MCP
  endpoint.
- **`riffdb-mcp`** — the stdio Model Context Protocol bridge for AI agents.

For application-language driver packages (`@riffdb/client`,
`riffdb-application`, `riffdb.dev/application`, `riffdb-client-rust`), see
[Driver and CLI Packages](getting-started/PACKAGE-INSTALLATION.md). For
machine-wide systemd deployments, multi-database administration, and manual key
provisioning, see
[Production Systemd Deployment and Administration](operations/PRODUCTION-DEPLOYMENT.md).

## Option 1: Install From a Prebuilt Release Archive (Fastest)

Download the verified release archive for your platform
(`x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, or
`aarch64-apple-darwin`), verify its SHA-256 checksum (`sha256sum -c` on Linux or
`shasum -a 256 -c` on macOS), and place the three binaries on your `PATH`:

```bash
shasum -a 256 -c riffdb-0.2.1-<target>.tar.gz.sha256
tar -xzf riffdb-0.2.1-<target>.tar.gz
cd riffdb-0.2.1-<target>
shasum -a 256 -c SHA256SUMS

mkdir -p "$HOME/.local/bin"
install -m 0755 bin/riffdb bin/riffdbd bin/riffdb-mcp "$HOME/.local/bin/"
export PATH="$HOME/.local/bin:$PATH"
```

Verify the installed binaries:

```bash
riffdb --version
```

With `riffdb` and `riffdbd` in `$HOME/.local/bin`, you can immediately run
`riffdb init --starter`, `riffdb generate`, and `riffdb dev` in any project
without configuring systemd or provisioning digest keys manually.

## Option 2: Run With Docker (Persistent Container Pilot)

> [!TIP]
> **First time evaluating RiffDB locally?** Use the native `riffdb` + `riffdbd`
> binaries from **Option 1** and run `riffdb dev`. It automatically provisions
> an isolated ephemeral database, digest keys, and role credentials in
> `.riffdb/dev/` with zero container volume setup.

The release container image (`ghcr.io/riffdb/riffdb:0.2.1`, defined in
`release/container/Dockerfile`) runs as non-root user `65532:65532` and executes
`riffdbd --config /etc/riffdb/riffdbd.toml`. Because `riffdbd` fails closed when
configuration, mode-`0600` digest keys, or TLS certificates are absent, mounting
an empty `/etc/riffdb` directory will exit immediately.

To start a persistent local container pilot with verified direct TLS,
auto-provisioned X.509 certificates (`riffdb tls issue`), and durable host
directories:

```bash
export RIFFDB_PILOT_DIR="$HOME/.local/share/riffdb-container-pilot"
mkdir -p "$RIFFDB_PILOT_DIR"/{cfg,secrets,data,backups}

# 1. Generate mode-0600 capability and idempotency HMAC digest keys
printf 'riffdb-capability-digest-keys-v1\n1:%s\n' "$(openssl rand -hex 32)" \
  > "$RIFFDB_PILOT_DIR/secrets/capability.keys"
printf 'riffdb-idempotency-digest-keys-v1\n1:%s\n' "$(openssl rand -hex 32)" \
  > "$RIFFDB_PILOT_DIR/secrets/idempotency.keys"
chmod 0600 "$RIFFDB_PILOT_DIR/secrets/capability.keys" \
  "$RIFFDB_PILOT_DIR/secrets/idempotency.keys"

# 2. Issue a local Root CA and server TLS certificate for 127.0.0.1 / localhost
riffdb tls issue \
  --out-dir "$RIFFDB_PILOT_DIR/secrets/tls" \
  --san-dns localhost \
  --san-dns riffdbd \
  --san-ip 127.0.0.1

# 3. Write /etc/riffdb/riffdbd.toml (matching release/container/riffdbd.toml)
cat > "$RIFFDB_PILOT_DIR/cfg/riffdbd.toml" <<'EOF'
[server]
database = "/var/lib/riffdb/data/riffdb.redb"
environment = "container-pilot"
audience = "riffdb-container-application"
capability_keys = "/run/secrets/capability.keys"
idempotency_keys = "/run/secrets/idempotency.keys"

[server.application_listener]
mode = "direct_tls"
listen = "0.0.0.0:7443"
public_endpoint = "https://127.0.0.1:7443"
certificate_chain = "/run/secrets/tls/server-chain.pem"
private_key = "/run/secrets/tls/server-key.pem"

[maintenance]
backup_root = "/var/lib/riffdb/backups"
projections_root = "/var/lib/riffdb/data/projections"
EOF

# 4. Start the container with read-only config/secrets and writable data/backups
docker run -d --name riffdb-pilot \
  -p 127.0.0.1:7443:7443 \
  -v "$RIFFDB_PILOT_DIR/cfg/riffdbd.toml:/etc/riffdb/riffdbd.toml:ro" \
  -v "$RIFFDB_PILOT_DIR/secrets:/run/secrets:ro" \
  -v "$RIFFDB_PILOT_DIR/data:/var/lib/riffdb/data" \
  -v "$RIFFDB_PILOT_DIR/backups:/var/lib/riffdb/backups" \
  ghcr.io/riffdb/riffdb:0.2.1
```

Verify container liveness over TLS using the generated CA certificate:

```bash
riffdb \
  --endpoint https://127.0.0.1:7443 \
  --tls-trust-root "$RIFFDB_PILOT_DIR/secrets/tls/ca-cert.pem" \
  server health
```

> [!IMPORTANT]
> **TLS client path and Server Name rules:**
> 1. `--tls-trust-root` (and `client.tls_trust_root` in TOML) **must be an absolute, normalized path** (for example `"$PWD/.runtime/ca.pem"`, never a relative path like `.runtime/ca.pem`).
> 2. `--tls-server-name` (and `client.tls_server_name`), when supplied, **must match the host in `--endpoint` / `client.endpoint`**. Setting `--endpoint https://127.0.0.1:7443` together with `--tls-server-name localhost` or `--tls-server-name riffdbd` fails closed with `configuration_invalid: client configuration is invalid`. Omit `--tls-server-name` so `riffdb` derives `127.0.0.1` from the endpoint automatically (verified against the certificate's `IP:127.0.0.1` SAN), or use `--endpoint https://localhost:7443 --tls-server-name localhost`.
> 3. Unauthenticated `riffdb server health` is a process-liveness check (`status: "pre_bootstrap"`, `lifecycle: "process_alive"`, exit code `0`) and must omit `--database`. To check per-database readiness (`status: "ready"`, `readiness: true`), bootstrap an operator credential inside the container and pass `--credential-file` and `--database`.

- **State & cleanup**: Authoritative state (`riffdb.redb`) and derived
  projections (`projections/`) live in `$RIFFDB_PILOT_DIR/data`. To stop and
  remove the pilot completely, run `docker rm -f riffdb-pilot && rm -rf
  "$RIFFDB_PILOT_DIR"`.
- **Transition to a shared team pilot**: See `release/container/compose.yaml`
  for the multi-container Docker Compose deployment and
  [Helm Operations](operations/HELM-OPERATIONS.md) for Kubernetes StatefulSets,
  TLS secrets, and readiness probes.

## Option 3: Install Language CLI & Driver Packages

Each target ecosystem provides a CLI wrapper and a runtime application driver:

| Ecosystem | Runtime Requirement | CLI Package | Application Driver Package |
|---|---|---|---|
| TypeScript / Node.js | Node.js 20+ | `npm install --save-dev @riffdb/cli` | `npm install @riffdb/client` |
| Python (`uv` / `pip`) | **CPython 3.13 or 3.14** (`cp313-abi3`) | `pip install riffdb-cli` | `pip install riffdb-application` |
| Go | Go 1.24+ | `riffdb` release binary | `go get riffdb.dev/application@v0.2.1` |
| Rust | Rust 1.98.1 | `riffdb` release binary | `cargo add riffdb-client-rust@0.2.1` |

> [!NOTE]
> **Evaluating from a Python 3.11 or 3.12 host?** The native `riffdb-application`
> and `riffdb-driver` wheels use the `cp313-abi3` ABI (`CPython 3.13` or
> `3.14`). You can evaluate RiffDB in an isolated interpreter without upgrading
> your system Python using `uv`:
> ```bash
> uv venv --python 3.13 .venv && source .venv/bin/activate
> ```

See [Driver and CLI Packages](getting-started/PACKAGE-INSTALLATION.md) for
offline wheelhouses, local development registries, and version lockstep rules.

## Source Checkout Convenience Install

From a RiffDB repository checkout on Linux (with Rust 1.98.1, systemd, `jq`, and
`uuidgen`), you can build and install a persistent per-user systemd service in
two commands:

```bash
cargo riffdb install --user
cargo riffdb bootstrap --user
```

To also register the local MCP developer server with Codex during bootstrap:

```bash
cargo riffdb bootstrap --user --register-codex
```

What these two commands do:

1. `cargo riffdb install --user` builds `riffdbd`, `riffdb`, and `riffdb-mcp`,
   installs them into `$HOME/.local/bin`, provisions mode-`0600` capability and
   idempotency digest keys under `${XDG_CONFIG_HOME:-$HOME/.config}/riffdb/`, and
   starts the user `riffdbd.service` on `127.0.0.1:7443`.
2. `cargo riffdb bootstrap --user` performs the one-time operator capability
   bootstrap and writes mode-`0600` client and MCP credentials to
   `${XDG_CONFIG_HOME:-$HOME/.config}/riffdb/`.

Check your running user service at any time with:

```bash
riffdb --config "$HOME/.config/riffdb/client.toml" server health
```

Or build the three release binaries directly without installing a background
service:

```bash
cargo build --locked --release \
  -p riffdb-server --bin riffdbd \
  -p riffdb-cli --bin riffdb \
  -p riffdb-mcp-stdio --bin riffdb-mcp
```

## Verify Your Setup in 30 Seconds

Once `riffdb` and `riffdbd` are on your `PATH`, test the full compile, codegen,
and ephemeral server loop in a scratch directory:

```bash
mkdir /tmp/hello-riffdb && cd /tmp/hello-riffdb
riffdb init hello-riffdb --generator typescript --starter
riffdb generate
riffdb dev --exec 'riffdb --endpoint "$RIFFDB_ENDPOINT" --credential-file "$RIFFDB_CREDENTIAL_FILE" server health'
```

Next step: follow [Your First Application](getting-started/FIRST-APPLICATION.md)
to build a complete application from scratch.

## License

RiffDB is available under either the MIT License or the Apache License, Version
2.0, at your option. A verified release bundle includes the ownership notice
and both license texts.

Copyright © 2026 Kevin O'Shea and O'Shea & Sons, LLC.


## Verify the installed build offline

Run `riffdb --version` and `riffdbd --version` (or `-V`). Each prints exactly one
`<binary> <workspace-version>` line to stdout, empty stderr, and exits zero.
These standalone flags run before configuration, credentials, storage, network,
logging/tracing subscribers, or runtime initialization, so a broken configuration
or absent server cannot prevent build identification. Mixing a version flag with
other arguments exits 2 without acting on those arguments. Unknown daemon flags
and known flags missing their value now report distinct configuration errors.
