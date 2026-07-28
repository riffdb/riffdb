# Upgrade and Removal

## Upgrade

The POC has no general in-place upgrade or downgrade guarantee. Treat every
binary change as a compatibility review.

1. Read `release/RELEASE_NOTES.md`, `docs/compatibility.md`, and the accepted
   ADRs for storage, durable envelope, public protocol, contract IR, and MCP.
2. Run `./scripts/ci-all`, `./scripts/demo --assert`, and
   `./scripts/release-poc --verify` at the exact source revision.
3. Create an offline backup and poll its caller-stable operation ID to a
   terminal success.
4. Stop `riffdbd` with SIGTERM and confirm a clean unit exit.
5. Preserve the old binaries, key documents, release bundle, SBOM, and
   checksums outside the database and backup roots.
6. Install all three binaries from one verified bundle.
7. Start `riffdbd` and require the complete startup validation and authenticated
   health result before admitting application traffic.
8. Re-run the budget smoke and inspect projection/outbox derived health.

Do not rotate digest keys in the same maintenance window as a binary or storage
upgrade. A retired key must remain in the readable list until no authoritative
or idempotency record needs it.

Downgrade is unsupported. Restoring a pre-upgrade backup is a destructive
history rewind, not a downgrade mechanism. It invalidates the destroyed suffix
even though `DatabaseId` is preserved.

## Removal

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
