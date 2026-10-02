# Durable local outbox archive

A primary database can deliver its committed events to one protected local
archive. This POC destination runs in a server-owned worker after command
acknowledgement. Applications continue to emit events through compiled commands;
they cannot select destinations or write archive records.

Precreate an absolute directory owned by the server's effective user with mode
`0700`. Every path component must be a real directory, without symlinks. The
directory must be disjoint from database, backup, projection, replication,
credential, and other destination paths. It contains only `archive.redb`; that
file must be a private regular file with exactly one hard link.

For a single database, add this restart-applied server configuration:

```toml
[maintenance.outbox]
kind = "durable_local"
directory = "/var/lib/riffdb/outbox"
quota_encoded_bytes = 1073741824
```

For named databases use `[databases.<alias>.outbox]` instead. Unknown options,
relative/overlapping paths, zero quotas, and destination configuration on a
follower reject configuration. Omitting this section preserves the existing
no-destination behavior: intents remain pending and outbox health degrades.

The worker validates the archive in bounded pages before dispatch. Startup
validation, unavailability, and exhausted capacity degrade outbox health while
primary command durability remains intact. Known destination refusal pauses
before claiming a new delivery. Repair availability or permissions in place, or
increase the quota and restart. A changed archive binding, incompatible format,
or corrupt content stops delivery and requires operator investigation.

Delivery is at least once. Acceptance requires an Immediate, two-phase archive
transaction storing the exact event and its quota charge atomically. A crash
between acceptance and source completion can replay the event; equal retries
preserve one record, and different bytes under the same identity are refused.
Database identity, history incarnation, and event identity form the key.
Failures discovered after a claim use the existing bounded retry and dead-letter
rules. Dead letters remain undelivered and continue to fence source retention.

Quota charges include canonical keys and complete encoded records. Allocated
engine blocks and cache memory are additional resources. There is no automatic
archive deletion. Preserve the archive across restart and account for this
intentionally retained destination separately from reclaimable source history.

## Offline inspection

Stop the owning server before inspection; the archive requires exclusive engine
custody. Supply the expected database identity as 32 hexadecimal characters:

```bash
riffdb --output json outbox archive inspect \
  --directory /var/lib/riffdb/outbox \
  --database-id 00000000002a71018101010101010101 \
  --max-pages 16
```

The command reports format, database binding, record count, encoded bytes,
physical allocated bytes, pages examined, and integrity. Each page examines at
most 256 records and 4 MiB. `--max-pages` is 1 through 256, default 1. Integrity
is `verified` only after the complete scan matches durable accounting;
`incomplete` means the supplied bound ended first. Increase the bound for a
larger archive. Counts alone are not integrity evidence. The validated library
reader can continue bounded pages for campaign reconciliation of larger archives.

Inspection refuses a missing archive and never creates destination records.
Errors contain no filesystem paths or event payloads. There is no application
RPC or MCP inspection method. This implementation requires Linux; it refuses
unsupported platforms.
