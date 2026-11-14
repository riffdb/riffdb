# Production Systemd Deployment and Administration

For local developer installation and quickstart options, start with
[Installation](../installation.md). This runbook covers machine-wide systemd
deployments, multi-database service administration, manual release-bundle
installation, digest-key provisioning, operator capability bootstrap recovery,
and offline local database resets.

RiffDB's supported deployment target is Linux. Protected key and bearer loaders
depend on Linux `/proc/self/status` and exact owner/mode checks. Controlled
remote application deployments can opt into verified direct TLS through explicit
server configuration (see
[Remote and Local Application Ingress](REMOTE-INGRESS.md)).

## System-Scope Convenience Install

From a clean, reviewed source checkout on a Linux systemd host, an operator can
install and bootstrap a machine-wide service:

```bash
cargo riffdb install --system
cargo riffdb bootstrap --system
```

System scope is an administrator-facing installation path. Perform the first
privileged installation and service-start acceptance on the intended staging or
production host, review every `sudo` prompt, and require authenticated RiffDB
Health to report the exact deployment-required state before deploying an
application.

Always invoke these commands as the intended unprivileged operator. Never use
`sudo cargo riffdb ...`; the installer rejects a root caller. The system
installer builds all three binaries as the operator and invokes `sudo`
internally to inspect protected destination state, publish root-owned files,
create the `riffdb` and `riffdb-mcp` service accounts and their directories,
verify the unit, and control the system service. System bootstrap still runs as
the operator and uses the public loopback API. Re-running `cargo riffdb install`
stages and backs up `riffdbd`, `riffdb`, and `riffdb-mcp` inside
`.riffdb-binary-rollback-v1` on the destination filesystem while preserving
existing keys, configuration, and unit files so an interrupted publication rolls
back cleanly.

### Installed Paths (`--user` vs `--system`)

| Asset | `--user` | `--system` |
|---|---|---|
| Product binaries | `$HOME/.local/bin/{riffdbd,riffdb,riffdb-mcp}` | `/usr/local/bin/{riffdbd,riffdb,riffdb-mcp}` |
| Server configuration and digest keys | `${XDG_CONFIG_HOME:-$HOME/.config}/riffdb/` | `/etc/riffdb/` |
| Database and backups | `${XDG_DATA_HOME:-$HOME/.local/share}/riffdb/data/riffdb.redb` and `.../backups/`, or `<alias>.redb` and `backups/<alias>/` | `/var/lib/riffdb/data/riffdb.redb` and `/var/lib/riffdb/backups/`, or `<alias>.redb` and `backups/<alias>/` |
| Service unit | `${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/riffdbd.service` | `/etc/systemd/system/riffdbd.service` |
| Service state/working directory | `${XDG_STATE_HOME:-$HOME/.local/state}/riffdb/` | `/var/lib/riffdb/` |
| Service accounts | Current user | `riffdb` and `riffdb-mcp` |
| MCP service roots | Operator XDG files | `/etc/riffdb-mcp/` and `/var/lib/riffdb-mcp/` |
| Account/directory definitions | Not applicable | `/usr/local/lib/sysusers.d/riffdb.conf` and `/usr/local/lib/tmpfiles.d/riffdb.conf` |

Bootstrap always writes the current operator's private client files beneath
`${XDG_CONFIG_HOME:-$HOME/.config}/riffdb/` and recovery state beneath
`${XDG_STATE_HOME:-$HOME/.local/state}/riffdb/`, including for a `--system`
database. In particular, `client.toml`, `operator.credential`, `mcp.toml`, and
`mcp.credential` are user-owned mode-`0600` files. The same private state root
retains `operator-capability.id` and `mcp-capability.id` for deliberate
server-side revocation.

### Start Control and User Managers

Installation normally verifies, enables, and starts the selected service. To
publish and verify files without reloading, enabling, starting, or restarting
the service:

```bash
cargo riffdb install --user --no-start
cargo riffdb install --system --no-start
```

After a `--no-start` installation, start the service deliberately before
running bootstrap:

```bash
# User service
systemctl --user daemon-reload
systemctl --user enable --now riffdbd.service

# System service
sudo systemctl daemon-reload
sudo systemctl enable --now riffdbd.service
```

Every user-scope installation requires a valid existing `XDG_RUNTIME_DIR`
owned by the current user with mode `0700`. On a headless or restricted SSH
host without a per-user systemd manager, arrange the runtime directory and
linger policy explicitly, retain `--no-start` until the manager exists, or use
`--system` scope.

### Multi-Database Service Provisioning

To create several isolated databases under one service, provide every alias on
the first install and bootstrap each alias separately:

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

`--add-database` stops an active service, preserves a legacy database as alias
`default`, moves legacy backup contents to `backups/default`, creates the new
sibling root `backups/ea`, atomically publishes named configuration, and starts
the service again. `--database` accepts the canonical `[a-z][a-z0-9_-]{0,63}`
alias grammar and is bounded to 32 unique installer-managed aliases (`riffdbd`
itself supports up to 512 `[databases.<alias>]` tables with online configuration
reload and optional `idle_hibernate_seconds`; see
[Multiple Databases](MULTIPLE-DATABASES.md)).

### Explicit Bootstrap and Application Deployment Semantics

Install creates filesystem state and starts an unbootstrapped server; it does
not silently create database authority. The separate bootstrap command:

1. submits the one-time first-human bootstrap through loopback gRPC;
2. durably retains uncertainty-recovery material before submission;
3. writes private operator CLI configuration and credential files;
4. leaves the active contract catalog empty;
5. creates a separate generic developer capability and config for
   `riffdb-mcp`, limited to contract validation, contract catalog reads,
   contract deployment, and authenticated health; and
6. with `--register-codex`, runs `codex mcp add riffdb -- ...` only after the
   database and MCP credential are ready.

For an exact locked application, the canonical deployed workflow against an
installed service is:

```bash
riffdb application lock --write
riffdb application generate --locked
riffdb application deploy \
  --provision-role EaApplication \
  --tenant organization_acme \
  --seed
```

When deploying a successor application lock where a prior application role
exists, supply both the role and the explicit replacement acknowledgement so
RiffDB proves the new role credential before revoking the predecessor:

```bash
riffdb application deploy \
  --provision-role EaApplication \
  --replace-role-credential \
  --seed
```

The generated owner capability requests 30 days and the MCP developer
capability requests less than 30 days. Renew administrative credentials before
expiration with:

```bash
cargo riffdb bootstrap --user --renew
# or rotate a specific capability:
riffdb capability rotate <CAPABILITY_UUIDV7> --credential-output <PATH> --revoke-old
```

### Rerun and Binary Rollback Limits

Re-running `cargo riffdb install` validates ownership, modes, non-symlink paths,
and existing key documents, then republishes the three binaries while preserving
existing digest keys, server configuration, and service-unit files byte-for-byte.
Each binary replacement is staged on its destination filesystem and backed up in
`.riffdb-binary-rollback-v1` until all three binaries are published and unit
verification succeeds. See [Upgrade, Reset, and Removal](../upgrade-removal.md)
for removal and upgrade procedures.

## Manual System Installation From a Verified Release Bundle

Verify the release archive and its checksum manifest before extracting binaries:

```bash
sha256sum -c riffdb-0.2.5-<target>.tar.gz.sha256
tar -xzf riffdb-0.2.5-<target>.tar.gz
cd riffdb-0.2.5-<target>
sha256sum -c SHA256SUMS
```

Install the three product binaries, system account definitions, configuration
template, and systemd units:

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

Install the bundled documentation, configuration examples, and smoke demo:

```bash
sudo install -o root -g root -m 0755 -d \
  /usr/share/doc/riffdb/docs \
  /usr/share/doc/riffdb/release/config \
  /usr/share/doc/riffdb/examples/contracts
sudo install -o root -g root -m 0644 \
  README.md COPYRIGHT LICENSE-MIT LICENSE-APACHE /usr/share/doc/riffdb/
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

## Manual Provisioning of Digest Keys and First Operator

When installing systemd units manually rather than through `cargo riffdb install`,
provision two distinct mode-`0600` digest key documents owned by the `riffdb`
service account:

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

Validate the unit syntax, reload systemd, and start `riffdbd`:

```bash
systemd-analyze verify /usr/lib/systemd/system/riffdbd.service
sudo systemctl daemon-reload
sudo systemctl enable --now riffdbd.service
sudo journalctl -u riffdbd.service -n 50 --no-pager
```

Then bootstrap the first operator credential:

```bash
asset_root=/usr/share/doc/riffdb
umask 077
mkdir -p "$HOME/.config/riffdb" "$HOME/.local/state/riffdb"
install -m 0600 \
  "$asset_root/release/config/bootstrap-operator.json.example" \
  "$HOME/.config/riffdb/bootstrap-operator.json"

riffdb --endpoint http://127.0.0.1:7443 --output json \
  capability bootstrap \
  --request "$HOME/.config/riffdb/bootstrap-operator.json" \
  --generate "$HOME/.local/state/riffdb/bootstrap.credential" \
  --bearer-output "$HOME/.config/riffdb/operator.credential"
```

If the bootstrap response is uncertain, retry with `--bootstrap-file` and the
retained bootstrap document rather than generating a second identity:

```bash
riffdb --endpoint http://127.0.0.1:7443 --output json \
  capability bootstrap \
  --request "$HOME/.config/riffdb/bootstrap-operator.json" \
  --bootstrap-file "$HOME/.local/state/riffdb/bootstrap.credential" \
  --bearer-output "$HOME/.config/riffdb/operator-recovered.credential"
```

Install the operator client configuration and verify authenticated health:

```bash
install -m 0600 "$asset_root/release/config/client.toml.example" \
  "$HOME/.config/riffdb/client.toml"
sed -i "s|/home/operator|$HOME|" "$HOME/.config/riffdb/client.toml"
riffdb --config "$HOME/.config/riffdb/client.toml" server health
```

## Disposable Local Database Reset

A reset is an offline, destructive operator action for disposable local
development data. For an ephemeral `riffdb dev` workspace, run
`riffdb dev --reset`. For an installer-owned user service database named `ea`,
stop the service first:

```bash
systemctl --user stop riffdbd.service
systemctl --user is-active riffdbd.service
```

Once `inactive`, move the selected database and its database-bound local
credentials and state into a private timestamped archive:

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

Restart the service and re-bootstrap the empty alias:

```bash
systemctl --user start riffdbd.service
cargo riffdb bootstrap --user --database ea
```
