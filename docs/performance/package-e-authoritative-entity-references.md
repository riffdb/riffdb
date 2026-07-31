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
transcodes commit rows in bounded, crash-restartable pages (no ENTITIES join and
no EVENTS join) and then publishes `current`.

## Byte evidence (honest)

### Per-commit wire-vector reduction (what this change actually saves)

Measured on the checked codec sample (one entity mutation + one event), complete
compact-envelope sizes:

| Durable row | Pre-Package-E (V2 commit) | Package E (V3 commit) |
| --- | ---: | ---: |
| Entity post-image row | 126 | 126 |
| Commit | **481** | **421** |
| Commit savings | | **−60 bytes** |

The entity row is unchanged. The commit shrinks by the embedded post-image
payload (minus the fixed-size reference). Savings grow with field-byte size
because the unprunable commit log no longer duplicates those bytes.

### Command-growth re-measure (unchanged by construction)

`./scripts/benchmark-command-growth --checked` runs an **audit-only** workload
(`service_audit_started_failed_pair`) with **zero entity mutations**. Therefore
it cannot exhibit the commit-log reduction above: file-byte curves are expected
to match the pre-E baseline for this harness, and PERF-013 is measured with zero
entity rows. The checked run remains useful only as a regression gate that the
change did not break audit-path growth or PERF thresholds (PERF-003/004/006/008/
009/012/013 passed; no thresholds were adjusted).

Selected file-size observations from this branch (audit workload):

| Retained commands before window | `file_bytes` after 128-command window |
| ---: | ---: |
| 0 | 225280 |
| 1024 | 1085440 |
| 4096 | 4747264 |
| 16384 | 14344192 |
| 32768 | 32272384 |
| 65536 | 72609792 |

### Application-baseline end-to-end numbers (placeholder)

End-to-end write-byte reduction under real entity-mutating commands is measured
by the app-baseline / concurrency-sweep harnesses at integration, not by
command-growth. **Numbers are not fabricated here.**

| Workload | Pre-E bytes/cmd | Post-E bytes/cmd | Delta | Source run |
| --- | ---: | ---: | ---: | --- |
| _TBD at integration_ | — | — | — | app-baseline JSONL |

## Startup history check

On ENTITIES phase entry, a single forward COMMITS pass builds a bounded
`BTreeMap` of version/hash chains via `decode_commit_entity_references` (zero
EVENTS reads). Each entity inspect is O(1). Chain violations set `intact =
false` and continue so corrupt entity A cannot mask entity B. Unconsumed and
overflow orphan targets report as separate `CrossLinkMismatch` findings without
rewriting intact entities' verdicts.
