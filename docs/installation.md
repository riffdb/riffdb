# Installation

RiffDB's POC deployment target is Linux. Protected key and bearer loaders
depend on Linux `/proc/self/status` and exact owner/mode checks. Public
interfaces are loopback-only and do not include TLS.

RiffDB is available under either the MIT License or the Apache License, Version
2.0, at your option. A verified release bundle includes both license texts.

## Install a Verified Release Bundle

Use this procedure for an extracted archive produced by
`scripts/release-poc --verify`. Keep the archive, its adjacent checksum, and the
extracted directory together until verification completes:

```bash
sha256sum -c riffdb-0.1.0-<target>.tar.gz.sha256
tar -xzf riffdb-0.1.0-<target>.tar.gz
cd riffdb-0.1.0-<target>
sha256sum -c SHA256SUMS
```

The extracted directory is the bundle root. Install exactly its three product
binaries and service definitions:

```bash
sudo install -o root -g root -m 0755 \
  bin/riffdbd bin/riffdb bin/riffdb-mcp /usr/bin/
sudo install -o root -g root -m 0644 sysusers.d/riffdb.conf \
  /usr/lib/sysusers.d/riffdb.conf
sudo install -o root -g root -m 0644 tmpfiles.d/riffdb.conf \
  /usr/lib/tmpfiles.d/riffdb.conf
sudo systemd-sysusers
sudo systemd-tmpfiles --create /usr/lib/tmpfiles.d/riffdb.conf
sudo install -o root -g riffdb -m 0640 \
  release/config/riffdbd.toml.example /etc/riffdb/riffdbd.toml
sudo install -o root -g root -m 0644 systemd/riffdbd.service \
  systemd/riffdb-mcp.socket systemd/riffdb-mcp@.service \
  /usr/lib/systemd/system/
```

Install the reviewed operational documentation, examples, and bundle demo
without changing their relative asset layout:

```bash
sudo install -o root -g root -m 0755 -d \
  /usr/share/doc/riffdb/docs \
  /usr/share/doc/riffdb/release/config \
  /usr/share/doc/riffdb/examples/contracts
sudo install -o root -g root -m 0644 \
  README.md LICENSE-MIT LICENSE-APACHE /usr/share/doc/riffdb/
sudo install -o root -g root -m 0755 demo /usr/share/doc/riffdb/demo
sudo install -o root -g root -m 0644 docs/*.md \
  /usr/share/doc/riffdb/docs/
sudo install -o root -g root -m 0644 \
  release/README.md release/RELEASE_NOTES.md \
  /usr/share/doc/riffdb/release/
sudo install -o root -g root -m 0644 release/config/* \
  /usr/share/doc/riffdb/release/config/
sudo install -o root -g root -m 0644 examples/contracts/budget.riff \
  /usr/share/doc/riffdb/examples/contracts/budget.riff
```

The release verifier installs the staged bundle into a private root, compares
every required asset and mode, checks the extracted layout, and runs the demo
from the private installed layout before creating the archive.

## Build and Install From Source

Install rustup, let `rust-toolchain.toml` select Rust 1.97.0, and build the
three product binaries:

```bash
cargo build --locked --release \
  -p riffdb-server --bin riffdbd \
  -p riffdb-cli --bin riffdb \
  -p riffdb-mcp-stdio --bin riffdb-mcp
```

Install the binaries and service definitions from the source tree:

```bash
sudo install -o root -g root -m 0755 \
  target/release/riffdbd target/release/riffdb target/release/riffdb-mcp \
  /usr/bin/
sudo install -o root -g root -m 0644 release/sysusers.d/riffdb.conf \
  /usr/lib/sysusers.d/riffdb.conf
sudo install -o root -g root -m 0644 release/tmpfiles.d/riffdb.conf \
  /usr/lib/tmpfiles.d/riffdb.conf
sudo systemd-sysusers
sudo systemd-tmpfiles --create /usr/lib/tmpfiles.d/riffdb.conf
sudo install -o root -g riffdb -m 0640 release/config/riffdbd.toml.example \
  /etc/riffdb/riffdbd.toml
sudo install -o root -g root -m 0644 \
  release/systemd/riffdbd.service \
  release/systemd/riffdb-mcp.socket \
  release/systemd/riffdb-mcp@.service \
  /usr/lib/systemd/system/
```

Install the documentation assets using the same installed layout as the
release bundle:

```bash
sudo install -o root -g root -m 0755 -d \
  /usr/share/doc/riffdb/docs \
  /usr/share/doc/riffdb/release/config \
  /usr/share/doc/riffdb/examples/contracts
sudo install -o root -g root -m 0644 \
  README.md LICENSE-MIT LICENSE-APACHE /usr/share/doc/riffdb/
sudo install -o root -g root -m 0755 release/demo \
  /usr/share/doc/riffdb/demo
sudo install -o root -g root -m 0644 docs/*.md \
  /usr/share/doc/riffdb/docs/
sudo install -o root -g root -m 0644 \
  release/README.md release/RELEASE_NOTES.md \
  /usr/share/doc/riffdb/release/
sudo install -o root -g root -m 0644 release/config/* \
  /usr/share/doc/riffdb/release/config/
sudo install -o root -g root -m 0644 contracts/examples/budget.riff \
  /usr/share/doc/riffdb/examples/contracts/budget.riff
```

The remaining examples use the installed asset root:

```bash
asset_root=/usr/share/doc/riffdb
```

You may instead set `asset_root` to an extracted release-bundle root.

## Provision Digest Keys

The service requires two distinct protected key documents. Each file must be a
regular file owned by the `riffdb` effective user with no group or other mode
bits. The first entry is the current write key; up to seven later entries are
readable retired keys.

The following uses OpenSSL only as an operator-side random-byte source:

```bash
sudo -u riffdb sh -eu -c '
umask 077
capability=$(openssl rand -hex 32)
idempotency=$(openssl rand -hex 32)
test "$capability" != "$idempotency"
printf "riffdb-capability-digest-keys-v1\n1:%s\n" "$capability" \
  > /var/lib/riffdb/capability.keys.new
printf "riffdb-idempotency-digest-keys-v1\n1:%s\n" "$idempotency" \
  > /var/lib/riffdb/idempotency.keys.new
'
sudo install -o riffdb -g riffdb -m 0600 \
  /var/lib/riffdb/capability.keys.new /etc/riffdb/capability.keys
sudo install -o riffdb -g riffdb -m 0600 \
  /var/lib/riffdb/idempotency.keys.new /etc/riffdb/idempotency.keys
sudo rm /var/lib/riffdb/capability.keys.new \
  /var/lib/riffdb/idempotency.keys.new
```

Do not reuse material between the documents. Back them up through a separate
secret-management procedure. `/etc/riffdb` is administrator-owned, while the
key files remain owned by the `riffdb` effective user because the protected
loader requires exact owner matching. The database backup root deliberately
does not contain them.

## Start `riffdbd`

`riffdbd.service` selects the strict `/etc/riffdb/riffdbd.toml` configuration.
Review its state, backup, key, loopback, audience, and optional hosted-MCP
settings before enabling it. Unknown or malformed configuration fails startup.

The RiffDB service account must be the only writer to
`/var/lib/riffdb/data`. Never start two `riffdbd` processes against the same
database and never grant a backup tool, CLI user, or bridge direct write access
to the data directory. The packaged unit and tmpfiles rules enforce this
ownership boundary for the standard layout.

Validate the unit syntax, reload systemd, and start the service:

```bash
systemd-analyze verify /usr/lib/systemd/system/riffdbd.service
sudo systemctl daemon-reload
sudo systemctl enable --now riffdbd.service
sudo journalctl -u riffdbd.service -n 50 --no-pager
```

`Type=exec` means systemd's active state is a process state, not RiffDB
readiness. The server writes a bounded line beginning with
`riffdbd-ready-v1<TAB>` after its startup proof. Once bootstrap exists, use the
authenticated `riffdb server health` operation for database readiness.

The unit closes stdin. `riffdbd` ignores clean stdin EOF and stops on SIGTERM
through its bounded graceful drain. `TimeoutStopSec=80s` exceeds the server's
35-second transport drain plus runtime shutdown allowance.

## Bootstrap the First Operator

Create private operator directories and a request from the installed example:

```bash
umask 077
mkdir -p "$HOME/.config/riffdb" "$HOME/.local/state/riffdb"
install -m 0600 \
  "$asset_root/release/config/bootstrap-operator.json.example" \
  "$HOME/.config/riffdb/bootstrap-operator.json"
```

Generate and durably retain both the one-time bootstrap document and its bearer
presentation while submitting the bootstrap request:

```bash
riffdb --endpoint http://127.0.0.1:7443 --output json \
  capability bootstrap \
  --request "$HOME/.config/riffdb/bootstrap-operator.json" \
  --generate "$HOME/.local/state/riffdb/bootstrap.credential" \
  --bearer-output "$HOME/.config/riffdb/operator.credential"
```

If the response is uncertain, retry with `--bootstrap-file` and the retained
bootstrap document. Do not generate a second identity:

```bash
riffdb --endpoint http://127.0.0.1:7443 --output json \
  capability bootstrap \
  --request "$HOME/.config/riffdb/bootstrap-operator.json" \
  --bootstrap-file "$HOME/.local/state/riffdb/bootstrap.credential" \
  --bearer-output "$HOME/.config/riffdb/operator-recovered.credential"
```

Bearer output paths are create-only. Choose a new absent output path for an
uncertain retry. After a terminal bootstrap result and successful authenticated
health call, securely retire the bootstrap document. Retain the normal
operator credential at mode `0600`.

Install client configuration:

```bash
install -m 0600 "$asset_root/release/config/client.toml.example" \
  "$HOME/.config/riffdb/client.toml"
sed -i "s|/home/operator|$HOME|" "$HOME/.config/riffdb/client.toml"
riffdb --config "$HOME/.config/riffdb/client.toml" server health
```

The CLI performs no implicit configuration discovery. Every later example
therefore passes `--config` explicitly.

Deploy the example contract through the public application service:

```bash
contract_path="$asset_root/examples/contracts/budget.riff"
riffdb --config "$HOME/.config/riffdb/client.toml" \
  contract validate "$contract_path"
riffdb --config "$HOME/.config/riffdb/client.toml" \
  contract deploy "$contract_path"
```

## Run the Installed-Binary Demo

The packaged demo starts a disposable server with closed stdin, bootstraps it,
deploys the bundled contract, commits `CreateBudget` and `AllocateBudget`,
queries the resulting commit record, and stops the server with SIGTERM. It uses
only the three installed binaries and packaged assets; it does not invoke
Cargo, repository tests, or Git:

```bash
"$asset_root/demo" --assert
```

This is an installation smoke, not the complete POC-exit evidence run. The
revision-specific acceptance demo remains `./scripts/demo --assert` in the
matching full source checkout.

## Optional MCP Bridge

The normal deployment model is for an MCP client to spawn:

```bash
riffdb-mcp --config /path/to/mcp.toml
```

This gives the client a direct stdio child and is the most widely supported
form. The bridge is still an ordinary public gRPC client and has no storage
access.

For clients that can speak MCP stdio framing over a local Unix stream, the
release includes an optional socket-activated systemd pair. Install the MCP
configuration and a normal restricted capability:

```bash
sudo install -o riffdb-mcp -g riffdb-mcp -m 0600 \
  /secure/staging/riffdb-mcp.credential \
  /var/lib/riffdb-mcp/credential
sudo install -o root -g riffdb-mcp -m 0640 \
  "$asset_root/release/config/mcp.toml.example" \
  /etc/riffdb-mcp/config.toml
sudo systemctl enable --now riffdb-mcp.socket
```

The socket is `/run/riffdb-mcp.sock`, mode `0660`, group `riffdb-mcp`. Adding a
user to that group gives the user the effective authority of the bridge's
shared capability. Use a narrowly scoped agent capability, review group
membership, and prefer one spawned bridge per client when principals must
remain separate. Never give the bridge access to `/var/lib/riffdb`.
