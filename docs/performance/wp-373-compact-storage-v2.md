# WP-373 — compact durable storage format V2

WP-373 replaces the per-row Protobuf `StoredEnvelope` written by current
databases with a fixed 16-byte header:

```text
RDB2 | format=2 | closed record tag | schema revision
     | payload length | CRC-32C | canonical Protobuf payload
```

The tag and revision are nonzero and resolved through the generated closed
registry. A V2 database retains the digest of that exact registry in
`record_registry/v2`; a missing, unknown, or substituted binding fails closed
before identity probing or public readiness.

The immutable V1 framing, schema hashes, and checked-in wire vectors remain
readable and byte-identical. A dormant open migrates V1 rows in bounded pages
of at most 500 rows or 4 MiB of inspected plus replacement bytes. Each page is
durable and restartable. The format metadata stays V1 while mixed framing can
exist; the registry digest and V2 format value are published together only
after every authoritative table has been validated and transcoded. A restart
therefore resumes from durable rows without a separate migration marker.

Derived outbox-status and projection rows retain their existing recovery rule:
valid legacy rows are transcoded, while malformed rebuildable rows do not
prevent authoritative activation and remain classified by derived recovery.

## Representative framing measurement

The 26 immutable semantic V1 vectors occupy 6,075 bytes in V1 envelopes and
4,233 bytes in V2 records. V2 removes 1,842 bytes, or 30.3%, from this
representative set. The saving is framing-only: canonical semantic payloads,
checksums, schema validation, key/value validation, and recovery reciprocity
are unchanged.

## Compatibility

- V1 databases migrate in place before operational ports are exposed.
- V1 fixture bytes and legacy schema digests never change.
- V2 databases reject legacy authoritative rows, unknown tags or revisions,
  registry mismatch, malformed lengths, checksum mismatch, and noncanonical
  payloads.
- Current encoders write only V2 framing.
