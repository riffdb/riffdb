# WP-459: Consumed staged command graphs

The complete `AtomicCommandRecordSet` is required while a backend verifies and
physically stages every entity, index, event, outbox, provenance, idempotency,
and commit record. After those writes have been applied to the private
transaction, the backend needs only the checked terminal outcome and ordered
event identities until commit.

WP-459 makes that ownership boundary explicit. Memory and redb consume the
complete graph immediately after staging and retain a minimal immutable
summary. This releases the plan, mutation, event-payload, outbox, provenance,
and commit graphs earlier. The terminal outcome moves into the pre-validated
`CommittedBatchV1` rather than being deep-cloned by each backend. Grouped
commit integrity checks compare the returned outcomes directly with the
retained expected outcomes instead of cloning another comparison vector.

All complete graph and canonical encoding checks still happen before the first
physical write for a command. The complete committed-batch value is still
constructed and validated before the durable engine commit. Transaction-local
reads use the retained outcome sequence as their observed frontier. Durable
bytes, table writes, authorization, FIFO ordering, uncertainty fencing,
idempotent replay, and all-or-absent recovery are unchanged.

The retain-or-revert gate compares a full public TicketDesk seed at generated
batch concurrency 128 with WP-458. The change is retained only when total seed,
validation/encoding/staging, and representative unary mutation latency do not
materially regress.

## Evidence

The retained A/B used the exact WP-458 commit (`c388954b`) and the WP-459
working tree alternately on the same host, with a warm release build and
`RIFFDB_SEED_CONCURRENCY=128`. The host was under unrelated I/O load, so these
absolute results are not replacements for the quieter retained baseline; they
are useful as a paired regression check because both revisions experienced the
same elevated flush cost.

| Revision | Full seed | validation / encoding / staging | completion groups |
|---|---:|---:|---:|
| WP-458 exact baseline | 6.188 s | 1.332 s | 344 |
| WP-459 alternating run | 5.977 s | 1.272 s | 342 |

WP-459 reduced the paired total by 3.4% and the targeted stage by 4.5%. The
earlier quiet-host WP-459 repetitions completed in 3.50--3.65 s, with the
representative unary mutation at 1.72--1.92 ms; that overlaps the retained
WP-458 unary point of 1.74 ms. The ownership change therefore passes the
retain-or-revert gate, but it is deliberately recorded as a small CPU and
allocation improvement rather than a new throughput milestone. Durable flush
time remains the dominant full-seed cost.
