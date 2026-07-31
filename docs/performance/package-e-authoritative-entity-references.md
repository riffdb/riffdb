# Package E — Authoritative entity references

Package E replaces embedded entity post-images in commit records with
`CommittedEntityReferenceV2` values: entity target, committed version, and a
domain-separated hash of the complete post-image (`riffdb.entity-record/v1`).
Entity rows remain the sole durable owner of post-image field bytes. Contract
attribution and schema binding are not carried on the reference — they are
derivable from the commit's executable plan — and the expected pre-state is a
total function of the committed version.

## Compatibility

The branch-base registry digest (pre-Package-E current) is frozen as
`PRE_ENTITY_REFERENCE_REGISTRY_DIGEST`:

```text
e953d2c49f74e028c1aec71eedf62314623ab95cf533a2d636cc6d700842324f
```

Only databases at known pre-migration digests, including that exact value, may
enter the migration chain. The AuditRequestIndex step now publishes
`PRE_ENTITY_REFERENCE` rather than `current`. The new `EntityReference` step
transcodes commit rows in bounded, crash-restartable pages (no ENTITIES join)
and then publishes `current`.

## Byte evidence

Measured on the checked codec sample (one small entity mutation + one event),
complete compact-envelope sizes:

| Durable row | Pre-Package-E (V2 commit) | Package E (V3 commit) |
| --- | ---: | ---: |
| Entity post-image row | 126 | 126 |
| Commit | 481 | 421 |
| Commit savings | | **60 bytes** |

The entity row is unchanged. The commit shrinks by the embedded post-image
payload (minus the fixed-size reference). Savings grow with field-byte size
because the unprunable commit log no longer duplicates those bytes.

### Command-growth re-measure (`./scripts/benchmark-command-growth --checked`)

No thresholds were adjusted. Selected file-size observations from this branch:

| Retained commands before window | `file_bytes` after 128-command window |
| ---: | ---: |
| 0 | 225280 |
| 1024 | 1085440 |
| 4096 | 4747264 |
| 16384 | 14344192 |
| 32768 | 32272384 |
| 65536 | 72609792 |

PERF gates reported by the checked run: PERF-003, PERF-004, PERF-006, PERF-008,
PERF-009, PERF-012, PERF-013 all passed.

## Startup history check

`entity_history_matches` no longer scans the full commit log per entity.
On the first ENTITIES structural row, a single forward COMMITS pass builds a
`BTreeMap` of version/hash chains (via `decode_commit_entity_references`, zero
EVENTS reads). Each entity inspect is then O(1). Chain violations set
`intact = false` and continue so corrupt entity A cannot mask entity B.
