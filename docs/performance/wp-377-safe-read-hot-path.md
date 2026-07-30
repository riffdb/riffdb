# WP-377 safe read hot path

Status: implementation and measured evidence.

## Finding

The earlier diagnosis that named RiffQL was reparsed for every request and that
all redb reads were serialized by a process-global mutex was stale:

- query modules already retain the parsed `Document` and checked
  `QueryAccessProgramV1`;
- operational storage uses an `RwLock`, so read transactions take shared
  access.

Two live costs remained. The cached module and both retained query artifacts
were deep-cloned into each named execution, and compact durable records were
wire-preflighted, Protobuf-decoded, re-encoded, then Protobuf-decoded again by
the semantic codec.

## Product rule

An optimization may remove repeated representation work, but it may not remove
an authorization safe point, a durable integrity check, or semantic
reconstruction.

- Validated modules, parsed documents, and checked access programs are shared
  through immutable `Arc` ownership under their exact module and contract
  identities.
- Named execution retains shared artifacts. Ad hoc execution still compiles
  caller source and then enters the same executor with immutable ownership.
- Every existing initial, pre-execution, and post-snapshot authorization check
  remains in place.
- Compact V2 reads verify the registered record identity, size, CRC-32C,
  record-specific wire bounds, Prost decoding, and canonical re-encoding once.
- Historical schema identities and legacy V1 framing retain the complete
  registry compatibility validator as a fail-closed fallback.
- Storage semantic constructors still validate every domain invariant after
  decoding.

## Same-run evidence

The first full TicketDesk run with shared query artifacts and the typed durable
decoder recorded:

| Scenario | Before p50 | After p50 | Reduction |
| --- | ---: | ---: | ---: |
| `point_get_ticket` | about 0.37 ms | 0.195 ms | about 47% |
| `point_get_user` | about 0.37 ms | 0.188 ms | about 49% |
| `list_tickets_by_project_status` | about 0.43 ms | 0.268 ms | about 38% |
| `list_open_tickets_for_assignee` | about 0.46 ms | 0.293 ms | about 36% |
| `list_comments_for_ticket` | about 0.40 ms | 0.218 ms | about 46% |
| `list_project_members` | about 0.40 ms | 0.214 ms | about 47% |
| `ticket_detail_page` | about 0.42 ms | 0.281 ms | about 33% |

The run used the same public gRPC, named-query, authorization, and redb paths as
the application baseline. Absolute ratios against in-process warm prepared
PostgreSQL remain roughly 1.5x to 4x and are retained for WP-379 rather than
being hidden by a changed benchmark.

## Safety evidence

Automated coverage freezes shared pointer identity for cloned named-query
handles, strict compact-header and checksum rejection, alternate payload
encoding rejection, all registered semantic record round trips, malformed
semantic rejection, and process-level bootstrap/restart recovery.

No authorization checkpoint, field-visibility rule, query bound, partition
proof, snapshot rule, persistent schema, or public result shape changed.
