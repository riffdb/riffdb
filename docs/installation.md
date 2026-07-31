# Installation

RiffDB's POC deployment target is Linux. Protected key and bearer loaders
depend on Linux `/proc/self/status` and exact owner/mode checks. Public
interfaces are loopback-only and do not include TLS.

RiffDB is available under either the MIT License or the Apache License, Version
2.0, at your option. A verified release bundle includes both license texts.

## Source Checkout Convenience Install

Run the convenience commands from the repository root so Cargo reads
`.cargo/config.toml` and its `riffdb` alias. Rust 1.97.0, Linux, systemd, Git,
`jq`, a util-linux `uuidgen` with UUIDv7 support, and the ordinary utilities
checked by the installer are required.

Use a clean, reviewed checkout for an attributable installation. When the
checkout is clean, the installer embeds its exact Git revision. A dirty
checkout is allowed for development, but its binaries are labeled
`development-unversioned` and are not release evidence or attributable to a
specific revision.

For a service owned by the current user:

```bash
cargo riffdb install --user
cargo riffdb bootstrap --user
```

To register the generic MCP developer capability with Codex during bootstrap:

```bash
cargo riffdb bootstrap --user --register-codex
```

To create several isolated databases under one service, provide every alias on
the first install and then bootstrap them separately:

```bash
cargo riffdb install --user --database ea --database orders
cargo riffdb bootstrap --user --database ea
cargo riffdb bootstrap --user --database orders
```

To add a database to an existing installer-owned user or system installation,
use the offline migration command:

```bash
cargo riffdb install --user --add-database ea
cargo riffdb bootstrap --user --database ea
```

The command stops an active service, preserves a legacy database as alias
`default`, moves the legacy backup contents to `backups/default`, creates the
new sibling root `backups/ea`, atomically publishes named configuration, and
starts the service again. Its private phase journal makes the operation
resumable after process or machine interruption. `--no-start` requires the
service to already be inactive and leaves it inactive. The command accepts
only an exact installer-generated configuration; it refuses customized TOML
without changing it.

`--database` is repeatable on install, accepts the canonical
`[a-z][a-z0-9_-]{0,63}` alias grammar, and is bounded to 32 unique aliases.
Repeating installation with explicit aliases requires the exact retained alias
list. Only `--add-database` changes an existing installer-owned registry. The
installer never edits custom server configuration.

Named bootstrap writes separate private files such as `client-ea.toml`,
`operator-ea.credential`, `mcp-ea.toml`, and `mcp-ea.credential`. With
`--register-codex`, it registers `riffdb_ea`. The unsuffixed filenames and
registration name remain the compatibility form for alias `default`.

For a machine-wide service, select the same scope in both phases:

```bash
cargo riffdb install --system
cargo riffdb bootstrap --system
```

System scope is an administrator-facing POC path. Repository automation
validates the shared release service assets but does not execute this source
installer's privileged account, `/etc`, or `/usr/local` flow. Perform the first
privileged installation and service-start acceptance on the intended
disposable or staging host, review every `sudo` prompt, and require
authenticated RiffDB Health to report the exact deployment-required state
before creating the first application.

Always invoke these as the intended unprivileged operator. Never use
`sudo cargo riffdb ...`; the installer rejects a root caller. The system
installer builds all three binaries as the operator and invokes `sudo`
internally to inspect protected destination state, publish root-owned files,
create the two service accounts and their directories, verify the unit, and
control the system service.
System bootstrap still runs as the operator and uses the public loopback API.
The helper is mutable source-checkout code and is not suitable for a narrow
`sudoers` allowlist. System installation assumes the operator already has
ordinary administrative authorization for the displayed `sudo` operations.
Privileged publication opens only root-owned destination paths; the
unprivileged process opens and streams each checkout or build source.

### Installed Paths

| Asset | `--user` | `--system` |
|---|---|---|
| Product binaries | `$HOME/.local/bin/{riffdbd,riffdb,riffdb-mcp}` | `/usr/local/bin/{riffdbd,riffdb,riffdb-mcp}` |
| Server configuration and digest keys | `${XDG_CONFIG_HOME:-$HOME/.config}/riffdb/` | `/etc/riffdb/` |
| Database and backups | `${XDG_DATA_HOME:-$HOME/.local/share}/riffdb/data/riffdb.redb` and `.../backups/`, or `<alias>.redb` and `backups/<alias>/` | `/var/lib/riffdb/data/riffdb.redb` and `/var/lib/riffdb/backups/`, or `<alias>.redb` and `backups/<alias>/` |
| Service unit | `${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/riffdbd.service` | `/etc/systemd/system/riffdbd.service` |
| Service state/working directory | `${XDG_STATE_HOME:-$HOME/.local/state}/riffdb/` | `/var/lib/riffdb/` |
| Service accounts | Current user | `riffdb` and `riffdb-mcp` |
| MCP service roots | Operator XDG files described below | `/etc/riffdb-mcp/` and `/var/lib/riffdb-mcp/` |
| Account/directory definitions | Not applicable | `/usr/local/lib/sysusers.d/riffdb.conf` and `/usr/local/lib/tmpfiles.d/riffdb.conf` |

Bootstrap always writes the current operator's private client files beneath
`${XDG_CONFIG_HOME:-$HOME/.config}/riffdb/` and recovery state beneath
`${XDG_STATE_HOME:-$HOME/.local/state}/riffdb/`, including for a `--system`
database. In particular, `client.toml`, `operator.credential`, `mcp.toml`, and
`mcp.credential` are user-owned mode-`0600` files. The same private state root
retains `operator-capability.id` and `mcp-capability.id` for deliberate
server-side revocation. System scope does not turn an operator credential into
a root-owned secret.

User binaries follow the XDG-recommended `~/.local/bin` convention. Add that
directory to `PATH` for direct `riffdb` and `riffdb-mcp` invocations when the
login environment does not already include it. The `cargo riffdb bootstrap`
command uses the installed absolute paths and does not depend on that `PATH`
entry.

### Start Control and User Managers

Installation normally verifies, enables, and starts the selected service. To
publish and verify files without reloading, enabling, starting, or restarting
the service:

```bash
cargo riffdb install --user --no-start
cargo riffdb install --system --no-start
```

Bootstrap requires the selected service to be running and reachable. After a
user `--no-start` installation, start it deliberately with:

```bash
systemctl --user daemon-reload
systemctl --user enable --now riffdbd.service
```

Every user-scope installation requires a valid existing `XDG_RUNTIME_DIR`
owned by the current user with mode `0700`; `systemd-analyze --user verify`
uses it even with `--no-start`. Automatic user-service start or restart also
requires a functioning per-user systemd manager, normally supplied by a PAM
login session. `install --user --no-start` verifies the unit without contacting
the live user bus. The installer does not call `loginctl`, enable lingering,
or fabricate a runtime directory or user bus. On a headless or restricted SSH
host, arrange the runtime directory, user manager, and linger policy
explicitly, retain `--no-start` until the manager exists, or use the system
scope. For system `--no-start`, an administrator later runs:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now riffdbd.service
```

### Explicit Bootstrap Semantics

Install creates filesystem state and, unless `--no-start` is selected, starts
an unbootstrapped server. It does not silently create database authority. The
separate bootstrap command:

1. submits the one-time first-human bootstrap through loopback gRPC;
2. durably retains uncertainty-recovery material before submission;
3. writes private operator CLI configuration and credential files;
4. leaves the active contract catalog empty;
5. creates a separate generic developer capability and config for
   `riffdb-mcp`, limited to contract validation, contract catalog reads,
   contract deployment, and authenticated health; and
6. with `--register-codex`, runs `codex mcp add riffdb -- ...` only after the
   database and MCP credential are ready.

These are authoritative database operations, not local file setup. A failure
after owner bootstrap may leave the database successfully bootstrapped even
when later MCP-capability creation or Codex registration failed. Rerun the same
bootstrap command: it preserves retained bootstrap material and pending
capability identity so uncertainty is resolved rather than assigned a new
identity. It refuses to replace changed credential/config files or an existing
Codex MCP entry named `riffdb`. Codex currently exposes replacement-capable
`mcp add`, not an atomic create-if-absent operation, so use
`--register-codex` only while no other process is changing that user's Codex
MCP configuration.

The MCP developer can author, validate, and deploy an application contract
without installation choosing an application. Because application permissions
name exact contract lineage and stable IDs, the pre-deployment capability does
not grant wildcard command, entity, index, projection, commit, provenance, or
outbox access. After deployment, use the administrator CLI config to bind a
compiled application role or issue an exact scoped capability, then configure
the application or a separate MCP identity with that credential.

For an exact locked application, the canonical installed workflow is:

```bash
riffdb application lock --write
riffdb application generate --locked
riffdb application deploy \
  --provision-role EaApplication \
  --tenant organization_acme \
  --seed
```

Deployment refuses unlocked or drifted sources. Contract and query deployment
are idempotently resumable, role creation is explicit, and seed files run as
ordinary idempotent commands under the resulting application credential.
Private lock- and database-bound progress, the application credential, and
generated application-only `client.toml` and `mcp.toml` live beneath
`.riffdb/deployments/<database>/`; this state records identities but is never
an authority source. Explicit `--replace-expired-credential` revokes the
retained old capability before creating its replacement.

Bootstrap defaults to `http://127.0.0.1:7443`. To select a deliberately
reconfigured loopback listener, set an exact endpoint for that invocation:

```bash
RIFFDB_ENDPOINT=http://127.0.0.1:7553 \
  cargo riffdb bootstrap --user
```

Treat that value as an authority selection: verify it identifies the intended
unbootstrapped database, and do not leave a stale override in the environment.
User and system installs both default to port `7443`, so choose one default
scope. Running both on one host requires distinct server listen ports and
separate operator XDG config/state roots; otherwise their service listeners and
private client filenames collide.

The generated owner capability requests 30 days and the MCP developer
capability requests less than 30 days. The POC does not renew either
automatically. Create replacement capabilities through the authenticated
public service before the current administrative capability expires.

### Rerun and Update Limits

Re-running install validates ownership, modes, non-symlink paths, and existing
key documents, then republishes the three binaries. It preserves existing
digest keys, server configuration, service-unit files, and system
sysusers/tmpfiles definitions byte-for-byte rather than treating them as
upgrade templates. During first-time key creation it publishes a private,
create-only marker before either key. A rerun creates a missing typed
counterpart only when that exact marker is still present, server configuration
and the service unit have not been published, and the data, backup, and state
directories remain empty. Partial key state without the marker, or any missing
key beside later-phase configuration, unit, data, backup, or state evidence,
fails closed as possible key loss. It refuses linked or malformed key state,
unsafe file types, unexpected ownership, or modes.

Each binary replacement is atomic on its destination filesystem, but the
three-binary update is not one transaction. If installation is interrupted
during publication, rerun the same install command before starting or
restarting the service.

Consequently, the convenience command is not a general configuration
migration, key-rotation, uninstall, or compatibility-aware upgrade tool.

### Pre-Alpha Database Reset

A reset is an offline, destructive operator action for disposable pre-alpha
data. It is not a schema migration, is never performed by contract deployment,
and has no runtime API. Prefer an additive successor whenever the compatibility
rules permit it.

Resetting one alias creates a new database identity. Its old operator, MCP, and
application credentials no longer authorize the new database. Retained
bootstrap material, capability IDs, generated client configuration, and
application deployment progress for that alias must be archived with the old
database and recreated.

For an installer-owned user database named `ea`, first close every client that
could restart the service, then stop the one server process that owns all
configured aliases:

```bash
systemctl --user stop riffdbd.service
systemctl --user is-active riffdbd.service
```

Require the second command to report `inactive`. Set the XDG roots, create a
private timestamped archive, and move the selected database plus its
database-bound local authority and recovery files into it:

```bash
config_root="${XDG_CONFIG_HOME:-$HOME/.config}/riffdb"
data_root="${XDG_DATA_HOME:-$HOME/.local/share}/riffdb"
state_root="${XDG_STATE_HOME:-$HOME/.local/state}/riffdb"
reset_archive="$data_root/manual-reset/ea-$(date -u +%Y%m%dT%H%M%SZ)"
install -d -m 0700 "$reset_archive/config" "$reset_archive/state"

mv -- "$data_root/data/ea.redb" "$reset_archive/ea.redb"
for path in \
  "$config_root/operator-ea.credential" \
  "$config_root/client-ea.toml" \
  "$config_root/mcp-ea.credential" \
  "$config_root/mcp-ea.toml" \
  "$state_root/bootstrap-ea.credential" \
  "$state_root/bootstrap-local-owner-ea.json" \
  "$state_root/mcp-local-developer-ea.json" \
  "$state_root/operator-capability-ea.id" \
  "$state_root/mcp-capability-ea.pending-id" \
  "$state_root/mcp-capability-ea.id"
do
  if [[ -e "$path" || -L "$path" ]]; then
    case "$path" in
      "$config_root"/*) mv -- "$path" "$reset_archive/config/" ;;
      "$state_root"/*) mv -- "$path" "$reset_archive/state/" ;;
    esac
  fi
done
```

Before executing this procedure, confirm the selected `[databases.ea].path` in
the active `riffdbd.toml`; custom installations may use another path. Backups
under `backups/ea` are deliberately retained. Do not move another alias, the
shared capability or idempotency digest keys, or the server configuration.

Start the service, bootstrap only the new empty alias, and deploy a complete
genesis application:

```bash
systemctl --user start riffdbd.service
cargo riffdb bootstrap --user --database ea

riffdb application lock --write
riffdb application generate --locked
riffdb --config "$config_root/client-ea.toml" application deploy \
  --provision-role EaApplication \
  --tenant organization_acme \
  --seed
```

Archive or remove the application's old `.riffdb/deployments/ea` directory
before the final command so the application does not reuse database-bound
deployment identities. If Codex or another MCP host retained the old
`riffdb_ea` registration, replace it only after the new bootstrap has produced
the new `mcp-ea.toml`. A reset is complete only after authenticated health,
active-contract inspection, and an application query succeed against alias
`ea`.
Review [Upgrade and Removal](upgrade-removal.md) and compatibility policy
before changing revisions. Use `--no-start` when replacing binaries should not
immediately restart the existing service.

`--from-binaries DIR` is an advanced staging/test input. `DIR` must contain
exactly named executable regular files `riffdbd`, `riffdb`, and `riffdb-mcp`.
This mode skips the locked Cargo build and performs no revision, checksum,
signature, or provenance verification; use only a directory whose complete
contents were independently verified. Normal source installs should omit it.

After bootstrap succeeds, the convenience flow is complete. The procedures
below are advanced alternatives; do not manually reprovision keys or overwrite
configuration created by the convenience installer.

## Advanced: Install a Verified Release Bundle

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

## Advanced: Manual Build and Install From Source

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
health call, retain the exact owner capability ID before securely retiring the
bootstrap document:

```bash
mapfile -t bootstrap_lines \
  < "$HOME/.local/state/riffdb/bootstrap.credential"
[[ "${#bootstrap_lines[@]}" -eq 3 \
   && "${bootstrap_lines[0]}" == "riffdb-bootstrap-credential-v1" \
   && "${bootstrap_lines[1]}" =~ ^capability-id:([0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})$ ]] \
  || exit 1
operator_capability_id="${BASH_REMATCH[1]}"
operator_id_file="$HOME/.local/state/riffdb/operator-capability.id"
[[ ! -e "$operator_id_file" && ! -L "$operator_id_file" ]] || exit 1
(set -o noclobber; printf '%s\n' "$operator_capability_id" > "$operator_id_file") \
  || exit 1
chmod 0600 "$operator_id_file"
```

Retain every later capability ID through the same private operational
inventory; the POC has no operator-facing capability list. Retain the normal
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
