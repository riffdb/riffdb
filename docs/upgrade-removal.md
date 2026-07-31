# Upgrade and Removal

## Upgrade

The POC has no general in-place upgrade or downgrade guarantee. Treat every
binary change as a compatibility review.

1. Read `release/RELEASE_NOTES.md`, `docs/compatibility.md`, and the accepted
   ADRs for storage, durable envelope, public protocol, contract IR, and MCP.
2. Run `./scripts/ci-all`, `./scripts/demo --assert`, and
   `./scripts/release-poc --verify` at the exact source revision.
3. Quiesce application writes, close client-spawned MCP bridges, and stop the
   optional MCP socket and instances when installed. Keep only the
   administrative path needed for backup available.
4. Create an offline backup and poll its caller-stable operation ID to a
   terminal success. Do not readmit application traffic after the backup.
5. Stop `riffdbd` with SIGTERM and confirm a clean unit exit.
6. Preserve the old binaries, key documents, release bundle, SBOM, and
   checksums outside the database and backup roots.
7. Install all three binaries from one verified bundle.
8. Start `riffdbd` and require the complete startup validation and authenticated
   health result before admitting application traffic.
9. Re-run the budget smoke, inspect projection/outbox derived health, and only
   then restart MCP bridges and application clients.

For the optional system bridge, stop activation before draining instances:

```bash
sudo systemctl stop riffdb-mcp.socket
sudo systemctl stop 'riffdb-mcp@*.service'
```

Restart the socket only after authenticated database health succeeds.

Do not rotate digest keys in the same maintenance window as a binary or storage
upgrade. A retired key must remain in the readable list until no authoritative
or idempotency record needs it.

Downgrade is unsupported. Restoring a pre-upgrade backup is a destructive
history rewind, not a downgrade mechanism. It invalidates the destroyed suffix
even though `DatabaseId` is preserved.

### Source Convenience Upgrade

For a source convenience installation, perform the compatibility review and
steps 1 through 6 above from a clean, reviewed checkout. In particular, close
every Codex or other client-spawned `riffdb-mcp` process before publication; a
running process does not change merely because its executable path is
replaced. After `./scripts/release-poc --verify` succeeds, `target/release`
contains the exact three binaries that the verifier compared, exercised, and
placed in the verified bundle. With the selected service stopped and its old
assets preserved, publish those same binaries without starting it:

```bash
verified_bin_dir="$PWD/target/release"

# User scope
cargo riffdb install --user \
  --from-binaries "$verified_bin_dir" \
  --no-start
systemctl --user start riffdbd.service

# System scope
cargo riffdb install --system \
  --from-binaries "$verified_bin_dir" \
  --no-start
sudo systemctl start riffdbd.service
```

Require authenticated RiffDB Health and rerun the budget smoke before
restarting MCP clients or readmitting traffic. The convenience installer
preserves existing configuration, keys, unit, sysusers, and tmpfiles content.
If a reviewed revision requires any of those files to change, this is not a
supported automatic upgrade: stage and review a package-specific migration
instead of forcing the installer to overwrite them.

## Removal

Removal is intentionally not part of `cargo riffdb install`. Before deleting
any state, configuration, digest key, or credential, decide whether a retained
backup is intended to be restorable and verify it. Capability revocation is
part of the authoritative snapshot: revoking a capability after a backup does
not revoke the record inside that backup.

### Source Convenience Capability Revocation

The convenience bootstrap retains the operator and MCP capability IDs in the
operator's private XDG state root. Treat the owner and MCP capabilities as
independent: owner bootstrap can succeed before a deterministic capability
request or health failure prevents MCP creation. Resolve each identity whose
retained state shows that submission may have begun. Rerun the same
`cargo riffdb bootstrap` invocation when it can resolve that uncertainty;
correct a deterministic pre-MCP failure before retrying. Do not delete
`bootstrap.credential`, `mcp-capability.pending-id`, credential candidates, or
related recovery state while a server-side outcome is uncertain.

When `mcp-capability.id` exists and bootstrap is terminal, revoke that
generic MCP developer capability while the server is running and the operator
credential is still valid:

```bash
config_home="${XDG_CONFIG_HOME:-$HOME/.config}"
state_home="${XDG_STATE_HOME:-$HOME/.local/state}"
riffdb_bin="$HOME/.local/bin/riffdb" # Use /usr/local/bin/riffdb for --system.
"$riffdb_bin" \
  --config "$config_home/riffdb/client.toml" \
  capability revoke \
  "$(<"$state_home/riffdb/mcp-capability.id")" \
  --reason requested
```

If both `mcp-capability.id` and `mcp-capability.pending-id` are absent in
intact bootstrap state, and it is certain MCP creation was never submitted,
skip only MCP revocation. Continue to preserve or revoke the independently
created owner capability as described below. If an expected identity file was
lost or its submission outcome is uncertain, stop; local credential deletion
is not server-side revocation.

Revocation is a durable database operation. Removing a Codex entry, bearer
file, or service unit is not a substitute. Quiesce application writes after
unwanted capabilities are revoked, then create and verify the final retained
backup while one deliberately retained administrative capability and its
bearer are still usable. Keep application writes quiesced from that snapshot
through shutdown.

If the database state may be used again, retain that operator capability or
create and verify another administrative capability before removing local
files. For permanent retirement only, revoke the operator capability after
the final backup and immediately before shutdown:

```bash
config_home="${XDG_CONFIG_HOME:-$HOME/.config}"
state_home="${XDG_STATE_HOME:-$HOME/.local/state}"
riffdb_bin="$HOME/.local/bin/riffdb" # Use /usr/local/bin/riffdb for --system.
"$riffdb_bin" \
  --config "$config_home/riffdb/client.toml" \
  capability revoke \
  "$(<"$state_home/riffdb/operator-capability.id")" \
  --reason requested
```

Restoring the final backup restores its retained operator authority even when
the live database revoked that operator after the snapshot. Protect the
retained bearer as active and revoke or replace it immediately after any
restore.

### Source Convenience User Scope

If bootstrap registered the MCP server with Codex, remove that client entry
after server-side revocation and before deleting local files:

```bash
codex mcp remove riffdb
```

Close the active Codex session and any other client-spawned `riffdb-mcp`
process. Removing the client entry prevents future launches but does not
terminate an already-running stdio bridge.

When a live user manager exists, stop and disable the user service:

```bash
systemctl --user disable --now riffdbd.service
```

If the service was installed with `--no-start` and no user manager exists,
skip that command only after independently confirming that no `riffdbd`
process is using the database. An unavailable user bus is not evidence that
the process is stopped.

Remove only the installed binaries and unit:

```bash
config_home="${XDG_CONFIG_HOME:-$HOME/.config}"
rm -f -- "$HOME/.local/bin/riffdbd"
rm -f -- "$HOME/.local/bin/riffdb"
rm -f -- "$HOME/.local/bin/riffdb-mcp"
rm -f -- "$config_home/systemd/user/riffdbd.service"
```

Run `systemctl --user daemon-reload` immediately when a manager is available,
or at the beginning of the next manager-backed login session.

The installer deliberately leaves the database, backups, keys, server/client
configuration, credentials, and recovery state. Archive and verify these roots
before any explicit deletion:

- `${XDG_DATA_HOME:-$HOME/.local/share}/riffdb`
- `${XDG_CONFIG_HOME:-$HOME/.config}/riffdb`
- `${XDG_STATE_HOME:-$HOME/.local/state}/riffdb`

After the irreversible decision to destroy them:

```bash
config_home="${XDG_CONFIG_HOME:-$HOME/.config}"
data_home="${XDG_DATA_HOME:-$HOME/.local/share}"
state_home="${XDG_STATE_HOME:-$HOME/.local/state}"
rm -rf -- "$data_home/riffdb"
rm -rf -- "$config_home/riffdb"
rm -rf -- "$state_home/riffdb"
```

### Source Convenience System Scope

If used, remove the user-owned Codex entry as the operator after server-side
revocation:

```bash
codex mcp remove riffdb
```

Close that Codex session and every other client-spawned `riffdb-mcp` process
before deleting binaries; removing the entry does not terminate an existing
stdio bridge. Then stop the system service:

```bash
sudo systemctl disable --now riffdbd.service
```

Remove the source convenience paths and reload systemd:

```bash
sudo rm -f -- /usr/local/bin/riffdbd
sudo rm -f -- /usr/local/bin/riffdb
sudo rm -f -- /usr/local/bin/riffdb-mcp
sudo rm -f -- /etc/systemd/system/riffdbd.service
sudo rm -f -- /usr/local/lib/sysusers.d/riffdb.conf
sudo rm -f -- /usr/local/lib/tmpfiles.d/riffdb.conf
sudo systemctl daemon-reload
```

The database, backups, digest keys, server configuration, `riffdb` and
`riffdb-mcp` accounts, and both accounts' tmpfiles-managed roots remain. The
operator's `client.toml`, `operator.credential`, `mcp.toml`, `mcp.credential`,
capability IDs, and recovery files also remain under that operator's XDG
config/state roots. Archive and verify all of them before deletion. Only after
that explicit decision:

```bash
sudo rm -rf -- /var/lib/riffdb /var/lib/riffdb-mcp
sudo rm -rf -- /etc/riffdb /etc/riffdb-mcp
sudo userdel riffdb-mcp
sudo userdel riffdb
```

Remove the operator's XDG files separately after confirming they are not also
used for a user-scope database.

### Verified Bundle or Manual System Scope

The convenience capability-ID filenames and `/usr/local/bin` command do not
apply to this scope. Revoke capabilities before shutdown with
`/usr/bin/riffdb` and the IDs deliberately retained when those capabilities
were issued. If an ID was not retained, the POC has no operator-facing
capability inventory command; deleting its bearer presentation still does not
revoke the server-side record.

Before shutdown, quiesce application writes and client-spawned MCP bridges,
revoke unwanted capabilities, and create and verify the final retained backup
while one deliberately retained administrator remains active in that
snapshot. Keep writes quiesced through shutdown. For permanent retirement
only, the live database's last operator may be revoked after the backup; the
backup still contains that operator authority, so protect its retained bearer
as active and revoke or replace it immediately after any restore.

Stop and disable the optional bridge first, then the database:

```bash
sudo systemctl disable --now riffdb-mcp.socket
sudo systemctl stop 'riffdb-mcp@*.service'
sudo systemctl disable --now riffdbd.service
```

Archive or intentionally destroy the following before removing packages:

- `/var/lib/riffdb/data`
- `/var/lib/riffdb/backups`
- `/etc/riffdb`
- `/var/lib/riffdb-mcp`
- `/etc/riffdb-mcp`
- operator and agent credential files
- release checksums, SBOM, configuration, and accepted ADR revision

Remove binaries and unit files:

```bash
sudo rm -f /usr/bin/riffdbd /usr/bin/riffdb /usr/bin/riffdb-mcp
sudo rm -f /usr/lib/systemd/system/riffdbd.service
sudo rm -f /usr/lib/systemd/system/riffdb-mcp.socket
sudo rm -f /usr/lib/systemd/system/riffdb-mcp@.service
sudo rm -f /usr/lib/sysusers.d/riffdb.conf
sudo rm -f /usr/lib/tmpfiles.d/riffdb.conf
sudo rm -rf /usr/share/doc/riffdb
sudo systemctl daemon-reload
```

State, configuration, and the two service accounts are deliberately not
deleted by unit removal. After independent backup verification and explicit
authorization, remove `/var/lib/riffdb`, `/var/lib/riffdb-mcp`, `/etc/riffdb`,
and `/etc/riffdb-mcp` separately, then remove the `riffdb-mcp` and `riffdb`
accounts. Destruction of digest keys can make retained databases and
idempotency records unreadable; destruction of capability presentations does
not revoke their server-side records.

Only after that irreversible decision, remove retained data, configuration,
and accounts:

```bash
sudo rm -rf -- /var/lib/riffdb /var/lib/riffdb-mcp
sudo rm -rf -- /etc/riffdb /etc/riffdb-mcp
sudo userdel riffdb-mcp
sudo userdel riffdb
```
