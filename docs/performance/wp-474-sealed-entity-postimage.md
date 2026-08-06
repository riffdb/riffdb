# WP-474: Sealed entity post-image materialization

WP-474 removes a duplicate canonical-record materialization from the ordinary
command path. It changes only immutable in-process ownership. Durable schemas,
record bytes, hashes, keys, transaction membership, ordering, authorization,
acknowledgement, uncertainty, recovery, and public interfaces are unchanged.

## Sealed handoff

Deterministic evaluation already creates each `EntityPostImage` from a checked
`CanonicalRecord`. The prior implementation encoded that record to enforce the
document bound, retained only the resulting length, then deep-cloned and
encoded the same record again when assigning the commit-time entity version.

An `EntityPostImage` now retains two inseparable immutable values:

- the checked canonical field record; and
- the exact canonical bytes produced by the existing bounded encoder.

Both use shared `Arc` ownership. The command graph's narrow
`StoredEntityRecordV1::from_checked_post_image` constructor accepts only the
sealed post-image, the assigned entity version, and the exact durable schema
binding. It rechecks the contract-version binding and shares the sealed values;
callers cannot provide detached encoded bytes.

The general `StoredEntityRecordV1::new` constructor remains unchanged for
durable decoding, recovery, migration, fixtures, and independently supplied
semantic values. Those paths still encode and validate the complete canonical
record themselves.

## Evidence

The retained full public-path report is
`target/app-baseline/wp474-sealed-entity-postimage.json`. Three full seed runs
completed in approximately 2.102, 2.151, and 2.155 seconds. The immediately
preceding three-run paired control on the retained WP-473 source completed in
approximately 2.166, 2.215, and 2.236 seconds.

| Metric | Paired WP-473 control | WP-474 |
|---|---:|---:|
| Full 19,220-command seed range | 2.166–2.236 s | 2.102–2.155 s |
| Validation/encoding/staging | 0.807–0.825 s | 0.780–0.797 s |
| Representative `create_comment` p50 | 1.823 ms | 1.690 ms |

The comparison is same-host development evidence, not a portable performance
claim. It exists to enforce the package's retain-or-revert gate.

## Safety coverage

Automated tests prove that the sealed constructor and full constructor produce
equal semantic records and identical durable envelopes, that the stored record
and evaluated post-image share the same immutable field value, and that a
mismatched schema binding still fails closed. Command, redb, concurrency,
lost-response, storage-recovery, generated-artifact, requirement-coverage, and
handbook gates retain the external/recovered validation path.
