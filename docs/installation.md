# Installation

RiffDB ships three self-contained binaries on Linux (`x86_64` and `aarch64`):

- **`riffdb`** — project initialization, contract compiler, SDK code generator,
  local `riffdb dev` supervisor, and administrative CLI.
- **`riffdbd`** — the authoritative database server and optional hosted MCP
  endpoint.
- **`riffdb-mcp`** — the stdio Model Context Protocol bridge for AI agents.

For application-language driver packages (`@riffdb/application`,
`riffdb-application`, `riffdb.dev/application`, `riffdb-client-rust`), see
[Driver and CLI Packages](getting-started/PACKAGE-INSTALLATION.md). For
machine-wide systemd deployments, multi-database administration, and manual key
provisioning, see
[Production Systemd Deployment and Administration](operations/PRODUCTION-DEPLOYMENT.md).

## Option 1: Install From a Prebuilt Release Archive (Fastest)

Download the verified Linux release archive (`x86_64-unknown-linux-gnu` or
`aarch64-unknown-linux-gnu`), verify its SHA-256 checksum, and place the three
binaries on your `PATH`:

```bash
sha256sum -c riffdb-0.1.0-<target>.tar.gz.sha256
tar -xzf riffdb-0.1.0-<target>.tar.gz
cd riffdb-0.1.0-<target>
sha256sum -c SHA256SUMS

mkdir -p "$HOME/.local/bin"
install -m 0755 bin/riffdb bin/riffdbd bin/riffdb-mcp "$HOME/.local/bin/"
export PATH="$HOME/.local/bin:$PATH"
```

Verify the installed binaries and run the self-contained release smoke test:

```bash
riffdb --version
./demo --assert
```

With `riffdb` and `riffdbd` in `$HOME/.local/bin`, you can immediately run
`riffdb init --starter`, `riffdb generate`, and `riffdb dev` in any project
without configuring systemd or provisioning digest keys manually.

## Option 2: Run With Docker

Run an isolated RiffDB container with persistent volume mounts for configuration
and storage:

```bash
docker run --rm -it \
  -p 127.0.0.1:7443:7443 \
  -v riffdb-data:/var/lib/riffdb \
  -v riffdb-config:/etc/riffdb \
  ghcr.io/riffdb/riffdb:0.1.0
```

For Kubernetes StatefulSets, TLS secrets, and readiness probes, see
[Helm Operations](operations/HELM-OPERATIONS.md).

## Option 3: Install Language CLI & Driver Packages

Each target ecosystem provides a CLI wrapper and a runtime application driver:

| Ecosystem | CLI Package | Application Driver Package |
|---|---|---|
| TypeScript / Node.js | `npm install --save-dev @riffdb/cli` | `npm install @riffdb/application` |
| Python (`uv` / `pip`) | `pip install riffdb-cli` | `pip install riffdb-application` |
| Go | `riffdb` release binary | `go get riffdb.dev/application@v0.1.0` |
| Rust | `riffdb` release binary | `cargo add riffdb-client-rust@0.1.0` |

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
