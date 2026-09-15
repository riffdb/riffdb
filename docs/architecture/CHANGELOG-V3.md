# Authoritative changelog V3 substrate

WP-772 implements ADR-0186's storage substrate and ADR-0207's frozen
compatibility boundary. Its [verification and fixture review](WP-772-VERIFICATION.md)
are complete as of 2026-09-14.
WP-746 adds the production publication observer and administrative tail RPC.
Follower storage construction, staged transfer/publication, continuous frame
apply, local acknowledgement, daemon activation and the bootstrap crash proofs
are implemented and verified through full CI. The
[follower lifecycle amendment](WP-746-FOLLOWER-LIFECYCLE-REVIEW.md) is accepted;
attached state cannot be reopened as a source or allocate local lifecycle receipts.
Archives remain WP-749. Internal values grant no application mutation, format
selection, bootstrap installation or pruning authority. Application export is
supported and unchanged.
The [WP-746 verification note](WP-746-VERIFICATION.md) maps the implementation,
crash proofs, compatibility changes and closure checks.

## Administrative tail stream (WP-746)

`riffdb.v1.ReplicationService.StreamChangelog` negotiates exact database,
incarnation, leadership epoch, catalog digest and receipt position/hash/frontiers.
It accepts only V3 and the existing 32-MiB/256-transition ceilings. Each emitted
frame contains one complete receipt. A connection's frame chain starts at its
requested history hash; later frames bind the preceding frame checksum. Receipt
history supplies continuity across reconnections. Emission is not a receiver
acknowledgement and does not release any retention hold.

WP-747 adds an optional `source_head` observation to frame responses. It names
the physical transaction head and both frontiers in the same immutable source
publication used for emission. Durable V3 frame bytes are unchanged; bootstrap
and refusal responses cannot carry this field. Head metadata has a separate
128-byte ceiling; the existing frame and bootstrap page ceilings stay intact.
Older responses without it mean
unknown source progress, never zero lag. The receiver rejects a report behind
its accompanying frame, an inconsistent frontier at an equal position, or a
regression from an earlier report. It publishes the observation only with the
frame's completed local prefix after durability, replay and acknowledgement.
Source-head telemetry never supplies read freshness or advances local progress.
The primary's observational reader visits only the bounded source-hold table
(at most 4,096 entries) in that same publication. It reports the follower count
and oldest durable follower acknowledgement, excluding archive and bootstrap
holds; it neither scans application history nor changes any hold. The typed
statistics calculation uses separate application and administration sequence
distances. Primary lag is measured from its head to the oldest acknowledgement;
follower lag is measured from the reported source head to its completed applied
frontier. Unknown source progress or absent followers has no reported lag.
Control-only receipts do not add logical lag, and an inconsistent or negative
frontier distance refuses instead of being clamped to zero.
Authenticated Health and Statistics expose these observations on both roles,
including the follower's actual optional durable acknowledgement from the same
immutable read pin as its applied prefix. Recovery does not infer an
acknowledgement from application. The additive wire metadata is bounded to 512
bytes, and MCP/CLI preserve wide counters as decimal strings. See
[replication health and statistics](../operations/REMOTE-INGRESS.md#replication-health-and-statistics).
The real TLS stream-kill test parks both delivery and workload at explicit
barriers, verifies positive primary lag against its oldest acknowledgement, and
checks follower counters against its completed prefix before each of three
crashes. Separate schema/presentation tests cover unknown progress, zero lag,
the full `u64` range, malformed observations and metadata bounds. Operational
reads use the existing bounded worker-admission wait; a deterministic
capacity-held test proves that path without sleeps.
The full follower freshness/provider gate remains WP-747 work in progress.

The service requires an explicit administrative `ReplicateChangelog` grant,
rechecks current authority before each release, and is exposed only through
direct TLS or a protected local socket. Cleartext loopback and proxy headers
cannot enable it. Foreign lineage, stale epoch, pruned history, unsupported
format/catalog/bounds, invalid position and corrupt history have terminal typed
refusals. The source caps concurrent streams at 16, idle waits at 30 seconds,
and source lifetimes at 15 minutes; the request deadline may close it earlier.

The same wire RPC now defines three mutually exclusive request phases: an exact
tail position, bootstrap begin/resume, or bootstrap attachment. Begin supplies a
nonzero hold ID; resume echoes the original manifest and the last durably staged
page. Attachment supplies the manifest and exact durable acknowledgement after
receiver publication and validation. These are transport values: storage must
still prove the manifest, lineage and retained fence before acting on them.

An exact tail request may also carry `follower_hold_id`: 16 bytes, not all zero,
identifying the follower hold established by attachment. In this case `after`
is a claim that the receiver has already persisted its local acknowledgement.
The source validates the exact retained position and advances only that existing
hold, under the same administrative authority. Missing holds, backwards claims,
foreign lineage and stale epochs refuse without changing the hold. Retrying an
identical claim writes no receipt. Omitting the ID keeps tail resume read-only;
bootstrap and attachment requests cannot carry it. This additive request field
uses the existing RPC and hold encoding. Peers without its implementation ignore
the unknown field and retain the earlier, conservative fence.

Bootstrap responses are one manifest followed by pages, then stream end. A new
attachment request starts the tail. Manifests are capped at 512 bytes, pages at
32 MiB + 512 bytes, and total encoded responses at 32 MiB + 1024 bytes. Clients
must allow that response ceiling in their transport receive limit; the codec
still independently enforces every item bound. Public preflight rejects mixed
phases, missing acknowledgements, duplicate nested singular fields and malformed
positions. Unknown public fields remain ignored and are never relayed, as
required by ADR-0006. The service rejects wrong-phase items and repeated manifests, and
rechecks current authority before and after each manifest/page wait.

The production source connects these phases to bounded held-artifact jobs.
Negotiation checks the current source lineage, catalog, format and bounds before
creating an artifact. Resume revalidates the complete artifact and compares its
exact manifest before releasing pages after the acknowledged ordinal. Source
lineage and retention are checked around each item read. Attachment first proves
the exact retained successor cursor, then durably transfers the hold and cleans
up the artifact. A lost attachment response can be retried after cleanup.

TLS tests exercise phase framing with injected item custody, including a maximum
size page and revocation while a manifest or page is withheld. Separate real
storage tests exercise the production source adapter's bootstrap, page resume,
attachment and retained successors. A real-source TLS test also validates the
complete transcript, resumes at EOF and checks the first retained successor
after attachment through the listener and administrative service.

Publication retains one newest pinned snapshot and coalesces notifications.
Receipt reads and framing run outside the writer gate, including direct,
source-control and lifecycle publications that advance neither dual frontier.
The daemon composes this source stream with staged bootstrap, exact receiver
resume and follower service admission. The process proofs below exercise that
workflow with the app-baseline workload.

The storage adapter exposes a separate dormant follower handle. It requires
current-format attached state, refuses source journals and source lifecycle
certificates, and runs the existing structural and catalog validation before
releasing one non-cloneable applier through the internal
`ChangelogFollowerApplyPortV3` storage-api contract. The source activation handoff rejects a
follower handle. Follower startup preserves validated-prefix evidence and
rebuilds transient indexes without writing a local lifecycle receipt.

Each bounded V3 frame commits through one hardened Immediate transaction.
The applier verifies lineage, epoch, frame and receipt chains, every exact
pre-image, and each receipt's physical dual frontier. A failure aborts the
frame and fences that applier. Exact terminal receipt retries are read-only,
including after reopen: the existing durable follower position includes the
canonical receipt hash, which also binds every preceding receipt in the frame. Local acknowledgement records only the
existing follower metadata after durability; it does not allocate a changelog
sequence or claim delivery to the source. Shutdown consumes the synchronous
applier without a source CLEAN transaction.

Follower engines retain no source-only receipt-history rows, holds, or lifecycle
certificate. Their existing follower metadata binds the exact applied receipt
hash and shared tail. An empty source history remains corrupt on a source-mode
database; the unchanged authoritative and catalog validators still run on a
follower. Tests use isolated attached fixtures with empty source-only tables.
They prove frame rollback, process-crash recovery, exact retry, acknowledgement
isolation, and unchanged startup validation. The daemon tests below add staged
bootstrap, every namespace under the app-baseline workload, and service write
refusals to this component evidence.

## Bounded bootstrap transfer (WP-746)

`ReplicationBootstrapPageCursorV3` reads the complete authoritative cursor at
one exact V3 fence. Each page contains rows from one namespace, with at most
256 rows and 32 MiB + 512 bytes; the extra framing allowance accommodates the
largest accepted complete row. Namespace ends are explicit, including empty
namespaces. The source cursor must also reach its exact end before it can
release a manifest. A stage is capped at 1,048,576 pages and one TiB of framed
page bytes.

The external `replication_bootstrap_receipt/v1` manifest embeds the existing V3
history envelope and binds the source hold identity, exact page/row/byte counts,
and final page hash. Pages use `RDBRBP01`; manifests use `RDBRBR01`. Every page
binds the same fence, ordinal and preceding page hash. The transcript rejects
missing, repeated, reordered, foreign and corrupt pages, cross-page key disorder,
and any omitted namespace end. These codecs do not grant permission to prune,
install or serve a database. Database envelopes and V3 frame bytes are unchanged.

The reviewed byte fixtures are `fixtures/replication/bootstrap-pages-v1.hex`
`fixtures/replication/bootstrap-manifest-v1.hex`, and
`fixtures/replication/bootstrap-progress-v1.hex`; regenerate/check them with
`./scripts/generate-replication-bootstrap-fixtures [--check]`.

The V1 progress receipt (`RDBRBS01`) embeds the expected manifest and binds the
last page hash, namespace, prior key, and exact observed counts. The offline
`RedbBootstrapStage` commits each page and its progress together with Immediate
durability. Reopening checks the receipt against the exact last durable page;
repeating that page requires identical stored bytes and performs no write.
A failure requires reopening the stage. Complete transfer verification rereads
all pages, including pages preceding a resumed boundary. The stage retains an
exclusive engine lock and a pinned private directory; symlinks and replaced
paths and unexpected stage tables are refused. Private staging currently
requires Unix permission enforcement; other platforms refuse it. These artifacts
remain offline and cannot grant readiness.
Source preparation materializes one published cursor before registering its
exact bootstrap hold at the existing source control barrier. Only the resulting
held artifact exposes a manifest or pages to replication composition. If history
was pruned during preparation, registration refuses before release. A completed
artifact can be reopened after a source crash and its hold registration retried
without allocating a duplicate receipt. An interrupted build before the final
manifest has released no bytes and is refused as incomplete; a new unpublished
job must start from a new pin. Dropping the artifact never removes its hold.
Source construction and complete-artifact verification now have move-only,
bounded owners: each advance copies or rereads at most one page. Exact cursor
EOF and the durable manifest release the source snapshot before artifact
verification starts. Cancellation and failures release non-durable handles;
failed owners cannot resume or expose a manifest. Synchronous offline helpers
use these same steps.

Server composition provides four concurrent source jobs with a fifteen-minute
operation deadline, retained through held-artifact reads. Each storage step runs
outside the async runtime. Cancelling or timing out its waiter retains capacity
until that actual step finishes and drops its resources. Deterministic tests
cover that ownership ordering, and real-file tests cover admission, bounded
construction, complete verification, and exact held-artifact reopen.

The source-side attachment operation atomically replaces the bootstrap hold
with a follower acknowledgement at the identical fence, using the existing
source-control receipt. It never advances the retention floor. Exact retries
allocate no receipt and do not require reopening the artifact. An old artifact
cannot recreate its bootstrap hold while the follower hold exists. Wrong
positions, lineage, epoch and conflicting holds are refused. Process tests kill
the source around insertion, removal, receipt staging and commit, with
reclamation between retries; every recovery retains the fence and its exact
successor stream. This is source-side evidence; authenticated receiver
publication and acknowledgement must still precede that operation.

Each source reserves a private sibling directory named by appending
`.riffreplication` to its database path. Its persistent inventory contains at
most four artifacts, including abandoned builds, plus one exclusion lock. IDs
select only fixed hexadecimal child names; peers cannot supply paths. Every
live build or held artifact retains the repository lock. Under file exclusion,
admission can reclaim an abandoned artifact only after proving that no source
hold protects its ID. Four protected artifacts refuse another admission.
Attachment cleanup follows the durable handoff and never removes its follower
fence. It unlinks only the known private artifact, refuses unclassified files
and substituted paths, and syncs the directory changes. Repeated process-crash
tests cover each cleanup edge and exact retries. Interrupted or abandoned held
artifacts require successful attachment or audited hold retirement; cancellation
alone does not authorize their deletion.

The daemon reconnect orchestration is described below. Audited follower-hold
retirement and its operator ceremony belong to WP-748.

`RedbBootstrapMaterializer` now constructs a separate private `follower.redb`.
Each bounded page and its copy progress share one Immediate transaction. A
restart reads the durable transfer boundary and construction checkpoint without
rescanning previously copied authority. A construction lock remains held through
validation. At seal, one transaction installs the existing lineage-shared roots
and attached follower state and removes the temporary progress table. Source
history, holds, and lifecycle certificates remain absent. No new retained
serving-database metadata domain is added.

Completion compares every authoritative row and namespace end against every
transferred page and verifies the exact manifest. It respects valid page splits
rather than repacking them. Sealed-crash recovery repeats this full comparison.
The candidate then uses the existing complete structural and catalog scrub in
follower mode, which refuses malformed authority and missing required derived
state. Tests construct fresh files with and without a validated-prefix
checkpoint and reopen them through normal follower validation.

The private candidate also accepts bounded projection replay results from the
existing pure evaluator. It writes only derived rows and apply markers in one
Immediate transaction, requires the exact source control, and never changes the
source frontier or allocates a generation. A durable marker provides local
resume progress; an identical retry is read-only. A failed write refuses further
use of that owner until reopen. Tests cover both process-crash edges, rejected
gaps and stale controls, and independent event replay including filtered events,
empty commits and aggregate updates. The existing semantic validator detects
well-formed derived rows with incorrect measures.

The server's private rebuild worker now reads the candidate's real historical
evidence through the unchanged catalog driver. It binds the resulting lineage
proof to that exact construction session, enumerates every retained published
and candidate generation, and replays one commit at a time without advancing
source controls. Each completed generation must pass independent semantic
validation. Only exact control EOF followed by the complete structural/catalog
scrub produces its sealed rebuilt-candidate result. Cancellation is checked
between bounded evidence and replay reads; reopening rebuilds the catalog proof.

The real-catalog composition tests cover multiple retained generations,
cancellation, reopen and complete scrub. The gRPC budget restart test also
bootstraps the actual nonempty command graph, including outcomes, provenance,
audits, events and entities, reopens during projection replay, and passes the
complete follower scrub. Process-crash tests cover the derived storage boundary.

The sealed rebuild result can now publish a same-filesystem hard link to the
configured follower file. The private candidate remains available for crash
retries. Publication restricts the database inode to mode `0600`, retains its
actual engine lock, durably removes the old format marker, installs the database
link, then publishes the marker last and syncs the parent. An absent marker
withholds startup after a partial replacement. Existing targets must be empty
or closed followers of the same database; same-lineage positions cannot regress,
and incarnation and epoch cannot decrease. Rejected primary files remain
byte-exact. Classifying an existing target uses redb's read-only engine through
the retained descriptor on Linux, excluding writers without writable-open
bookkeeping; other platforms refuse that replacement path.

Every subsequent follower open independently pins the database and marker,
opens redb through that retained descriptor, and syncs both the database and
parent directory under the engine lock before returning a store. It therefore
does not rely on a previous publisher's uncertain final sync. Injected sync
failure and substituted-path tests prove refusal and release of local handles.

After publication, server composition releases the lock and runs the same
structural/catalog evidence driver and retained-metadata proof join as source
startup. It performs no initialization, index migration or source activation.
Only an exact match to the published manifest releases the sole follower
applier. The gRPC budget test now continues through this gate, persists a local
acknowledgement, replaces the source hold, and applies the actual registration
and attachment receipts at the exact successor. Publication tests repeatedly
kill and resume every link/marker/sync boundary for both fresh and existing
targets, and cover engine exclusion, substituted paths and refused rollback.

Receiver composition now has one shared construction slot and a 15-minute job
lifetime across transfer, materialization, semantic replay, publication and
startup. Storage work runs on blocking workers, and its actual owner retains the
slot through completion even if the async waiter is cancelled or times out.
Cancelled operations fuse the handle; resume reads the existing durable boundary.
Cancellation reaches page comparisons, catalog/structural evidence reads,
projection validation and transient-index reconstruction. An engine call or
publication durability step may finish after cancellation; the job then returns
no applier and reports no acknowledgement. Private files remain for exact retry.

The real gRPC budget test resumes its interrupted candidate through this async
receiver owner, including final startup and source attachment. Deterministic
barrier tests also cancel a real page append after its durable commit, prove
that the file lock and slot stay held until the storage step finishes, and then
resume the exact persisted page. Early publication and changed page retries
refuse without publishing or losing the previous durable progress.

The receiver connection reserves that same construction slot before contacting
a source. A new connection requires the manifest first; a restart opens the
local transfer under engine exclusion and derives its exact manifest and page
ordinal from durable progress. Configured lineage is checked before a resume
contacts the peer. Each response persists one next page before requesting
another; wrong phases, repeated pages, changed manifests and early EOF refuse.
Even a complete page count requires source EOF before the transfer can reach the
materializer. Cancelling or timing out a network wait drops the stream and local
transfer owner; a fresh connection must recover the durable boundary.

The outbound `VerifiedReplicationPeer` uses the existing checked gRPC client
with explicit-CA, exact-name TLS verification. It accepts no arbitrary channel,
cleartext URL or trust bypass. Trust-file loading reuses the server's protected
file identity checks, with a 256-KiB/64-certificate ceiling. It admits one active
stream, bounds responses to 32 MiB + 1024 bytes, and retains a 15-minute stream
deadline. Credentials and peer failure details remain redacted. Wire conversion remains in `riffdb-api-grpc`, using the existing checked
Protobuf client feature. Server composition retains TLS and credential custody;
no dependency version or durable encoding changes.

A real TLS test drives this peer and the receiver connection through complete
transfer, semantic reconstruction, publication, local durable acknowledgement,
source attachment, and the first durable follower apply. Separate tests cover
local-page resume, cancellation, deadlines, malformed response sequences and
trust rejection before credential release. The separate daemon process tests
below cover repeated reconnect and crash recovery.

`publish_and_follow` carries the same receiver slot directly from validated
publication into a move-only continuous receiver. Each `advance` receives at
most one frame, applies its exact successor in blocking storage work, persists
the follower-local acknowledgement, and only then returns progress. Requests
derive their lineage and complete position from local durable state. Initial
attachment requires the original manifest fence; later reports name the existing
follower hold. Duplicate, corrupt or wrong-phase frames fuse the receiver.

A receiver reports progress after 32 frames or a connection end. Receipts that
only update replication holds do not trigger a report by themselves, preventing
idle acknowledgement loops. Network streams retain a 15-minute deadline; expiry
closes the stream and reconnects without releasing the applier or discarding an
uncertain claim. Each blocking storage step has its own bounded lifetime.
Transient peer unavailability returns its typed refusal while retaining the
validated applier for exact reconnect, including when the listener's shorter
request limit ends a stream. Cancellation, malformed frames, terminal source
refusals and storage errors drop the active receiver; in-flight storage retains
the sole writer and admission slot until it actually finishes.
`reopen_follower` runs complete unchanged startup validation, checks configured
lineage, and retries from durable local state. The saved manifest permits an
exact retry when the initial attachment reply was lost.

Real storage tests cover lost replies, idle reconnects, corrupt frames, network
deadlines and cancellation after a committed apply. Isolated exact V3 control
receipts cover report batching and uncertain-claim retry. The TLS test also
reopens the validated follower into this continuous receiver. These checks do
not replace the complete authoritative workload and process-crash exit gate.
Managed receiver jobs now use a fixed private repository, reserved as the
`.riffreceiver` sibling of the database and separate from `.riffreplication`.
The inventory contains `inventory.lock`, `transfer`, `candidate`, and at most
the corresponding `transfer.creating` and `candidate.creating` directories.
The root lock follows active stages, construction jobs and tail custody, even
when the factory or async waiter is dropped.

Initial transfer and candidate creation commit their first progress record under
the `.creating` name, close their handles, then rename into the resumable name
and sync the parent. No page acknowledgement or candidate progress escapes
before this publication. After a crash, recovery removes only the closed list of
unpublished files under actual file exclusion; it never opens an engine to clean
up, recursively deletes an unknown tree, or discards published progress. Unknown
entries, symlinks, substituted paths, live file locks and ambiguous simultaneous
initial/resumable names refuse. A retry also syncs an already absent temporary
name to finish uncertain deletion. The existing manifest and progress encodings
are unchanged.

`connect_managed` derives the stored manifest and last durable page before
contacting the peer; `materialize_managed` chooses creation or resume inside the
same inventory. Configured jobs cannot use arbitrary transfer or candidate paths.
Process tests repeatedly kill 15 initial creation boundaries and retry interrupted
file/directory cleanup. The composed receiver test continues from initial-create
recovery through page resume, complete rebuild/publication and tail apply.
After the first valid frame or successful EOF confirms an attachment/report,
the managed receiver retires its bootstrap scratch in a blocking job under the
same custody. The live follower retains the database and format-marker
descriptors pinned before engine open. Cleanup verifies that candidate links
refer to those objects and that their directory differs from the live parent;
it never reopens the candidate engine after tail apply. Both scratch inventories
and their locks are checked before any unlink. Candidate database and marker
links are removed before the construction lock, with each removal synced;
the complete transfer manifest remains until the candidate directory is gone.
Transfer cleanup acquires the actual transfer-file lock. Unknown entries,
substitution, or another construction owner refuse without deleting live data.

Cleanup retries finish directory syncs even for already absent entries. A crash
requires validated live-follower reopen before resuming retirement; partially
retired scratch cannot be used to rebuild or replace that follower. Process
reopen recovers the original manifest from complete transfer evidence, while an
empty or absent transfer after retirement selects the existing follower hold.
Lost attachment replies keep both scratch directories for this retry. Process
tests interrupt ten unlink/sync boundaries, run complete structural/catalog
validation again, and retry cleanup while preserving the exact durable position.
The storage tests also verify unchanged live bytes, retained engine exclusion,
and refusal of live or scratch substitution. The incremental derived maintenance,
daemon serving composition and app-baseline process proofs are described below.

Follower frame apply now maintains the in-memory command, event-route and outbox
indexes incrementally. It processes each receipt's complete final state, including
segment replacements/splits, retention deletes, standalone compatibility rows and
outbox status transitions. Work follows changed keys and affected bounded segments;
it does not rescan retained history or clone the full cache. An index update is
published only after the frame is durable. While that update is in progress,
cache-dependent reads fail closed as unavailable; pinned authoritative snapshots
remain usable. No index lock spans the durable flush. A failed update fences the
applier, and reopen rebuilds from the durable prefix before retry.

Tests compare all six command-manifest index kinds, event routes and outbox
membership against a complete rebuild through insertion, splitting and retirement.
They also cover atomic rollback, a deterministic reader/writer publication schedule,
and process crashes before and after durability and index publication.

The sole follower applier also owns a bounded projection replay persistence port,
sharing the bootstrap candidate's row and marker validation. It persists only
`ProjectionState` and `ProjectionApplied`; replicated projection controls,
receipts, allocators and applied/acknowledged positions remain unchanged. An exact
retry checks the stored apply hash and performs no durable write. A gap, stale
control, wrong row predecessor or changed retry rolls back and fences the owner.
Process tests interrupt before and after the derived commit, then check atomic
rows/markers, exact retry and unchanged bytes in every non-rebuildable namespace.
Follower reopen now restores lagging local projections before the complete
startup validation pass. A restricted recovery phase retains the original engine
and namespace locks, validates the historical catalog, and replays only the local
rows and markers. It uses the same bounded pure-engine replay and independent
generation validation as bootstrap. It cannot apply source frames, acknowledge
positions or release application ports. The same locked engine then enters the
unchanged structural/catalog validator and proof join; required replay sources
that are missing or invalid still prevent activation.

Real-command tests apply a budget creation and allocation plus source projection
controls without local markers. They verify that ordinary validation detects the
lag, recovery restores the exact allocation amount, and full follower startup
then succeeds. Process crashes after the first replay commit and after rebuild
resume without double application or advancing source history.

The continuous receiver now replays affected projection generations after each
source frame is durable and before acknowledging it or receiving its successor.
It takes the final controls from that bounded frame, resolves their checked plans,
and applies one exact next application commit at a time through the same pure
engine and derived-only persistence port. Unchanged generations are not scanned.
A checked catalog lineage is cached across ordinary frames and reloaded through
bounded catalog reads when catalog records change. Cancellation or replay failure
closes receiver custody without acknowledging the incomplete prefix; validated
reopen performs the recovery described above. Control deletion is refused because
the accepted projection lifecycle retains generation-allocation authority.
Real-command tests check the receiver before startup recovery, exact allocation
rows, cache reuse and rebuilding a candidate alongside a caught-up published
generation.

A receiver can publish an immutable read view of its completed prefix. Initial
publication follows unchanged startup validation; later views are captured only
after frame durability, local projection replay and acknowledgement. The view
binds one exact V3 history, semantic storage snapshot and checked active catalog.
Readers clone that same root, never the applier or a current mutable root. A
captured projection observation checks its local apply-marker frontier against
the source control and refuses an unreplayed frontier. An older pin remains
historical even after replay completes.

Cancellation, terminal refusal and close withdraw the live read publication.
Transient network failures that restore the same applier preserve its last
completed view. Notifications coalesce through one watch slot; consumers use
their request deadline and drain retained pins before engine shutdown. Capability
authentication and current policy use the same production adapters against this
read-only source. Every authorization observes the live publication; historical
pins supply no current-view generation that could bypass full reauthorization.
Tests replicate real capability creation and revocation, retain an older pin,
and verify denial after revocation and withdrawal. The follower service graph now
consumes the same startup proof that released the applier, including lifecycle
and allocator facts. It composes the shared catalog, authoritative-read,
projection and query owners against immutable snapshots. Exact query-module
reads retain their pointer/audit/module cross-link checks without a history scan.
Projection notifications follow completed publication and touch the frame's
changed controls; catalog changes refresh the bounded registry. Cancellation and
close retire registrations so waiting requests can drain.

Follower exact-text, exact-predicate, tokenized-text and long-pattern providers
reuse the primary query engines through a snapshot-only storage adapter. The
adapter rechecks live publication on each new pin and cannot issue authoritative
mutations. Their worker writes only local derived checkpoints and is joined
during shutdown. Tokenized rebuilds scan one bounded entity partition from one
immutable snapshot, with the exact catalog identity and application frontier
checked before scanning. They retain the existing candidate bound and policy
admission before constructing posting state. Columnar/vector composition and
the complete follower freshness matrix remain open in WP-747. The
[accepted follower columnar amendment](WP-747-FOLLOWER-COLUMNAR-REVIEW.md),
recorded in `1d910c83`, permits independently validated disposable V2 views
from completed follower authority. It permits no local authoritative control
write or reuse of a source checksum for different local bytes. This design is
accepted; follower columnar serving is not yet implemented.

The shared V2 builder now consumes a narrow immutable entity-page reader. A
follower can supply its completed `RedbOwnedSnapshot` directly, without an
application-export owner or primary writer ports. The existing primary export
snapshot adapts to that same reader; entity-type selection, continuation, row
and byte limits, and independent V2 logical validation are shared. Pinned
columnar-control inventory reads decode the complete at-most-256 set and refuse
corruption or excess rows. Tests retain old pins across writes, compare pages
with the primary export snapshot, check byte-bound continuation and prove that
these reads acquire no writer admission.

Follower service startup now validates its complete configured scalar and
compiler-declared vector source set before opening derived artifacts. Source
resolution and alias checks are shared with primary registration. Each required
replicated control must match the checked specification and history at the same
completed pin; missing controls, mismatched targets or replay limits, duplicate
sources, excessive inventories and frontiers beyond the applied head refuse.
A valid source artifact failure does not certify or forbid an independently
validated follower artifact. Admission performs no control write and opens no
columnar artifact.

A storage-owned disposable build helper now provides an exclusively locked
`follower-columnar/build` area, separate from primary selection paths. A mutable
build lease allows one builder at a time. Cleanup checks cumulative file/byte
ceilings and the fixed directory depth before deleting through retained directory
handles. Restart discards abandoned material on the next build; it never adopts
that material as a view. Process-exit, path-substitution, symlink and excess-size
checks cover this helper. Snapshot-only builds use format-derived file and byte
ceilings; final partition lanes reject rows beyond manifest capacity before
appending bytes, including on builds that ultimately fail validation.

The existing V2 validation result owns its decoded query snapshot in memory.
A new full-streaming-build test confirms that scalar query results, partition
isolation and the exact frontier survive removal of all build files. This allows
the follower worker to finish validation, release file handles and discard its
one build area before installing the immutable snapshot. The service now owns
one worker for scalar and vector sources. Startup/status remain cold; concurrent
first demand coalesces into one activation and returns typed Building. The worker
rechecks the completed catalog/control inventory, builds from one immutable pin,
and binds the validated memory view to this process and a local view ordinal.
Source control generation numbers and checksums never identify this view.

Active sources rebuild when the local applied frontier advances. Scalar reads
report the validated local frontier. The vector adapter shares the primary's
bounded evidence, model, row-policy and freshness checks, but V2 construction
currently rejects vector fields. They remain rowless; the separate
[canonical-vector proposal](WP-747-V2-VECTOR-REVIEW.md) requires acceptance before
that encoding gap can be resolved. Failed sources remain rowless
and degrade projection health. Shutdown closes demand and wakes waiters before
draining the worker; publication withdrawal refuses new observations even while
an older snapshot remains alive. Restart begins cold and rebuilds on demand.
WP-747 remains open for full dataful scalar/vector TLS parity, freshness,
namespace and failure-campaign coverage.

`follower_exact_providers_match_primary_after_tail_and_restart` deploys all four
compiled provider families over TLS, checks matching results and application
heads after bootstrap, a replicated write and follower restart, and excludes a
matching row from another partition. Its namespace oracle then verifies that
the follower still equals a complete source prefix; derived rebuilds create no
local authoritative records.

A supervised continuous receiver now owns the validated applier through shutdown.
Transient network failures retry only while that same receiver retains live
custody. Reconnects and exact EOF use exponential backoff from 250 milliseconds
to 30 seconds, reset after applied progress. Terminal refusals stop the worker.
Shutdown interrupts network waits and backoff, closes any retained follower, and
waits for the actual storage admission permit to return before reporting
completion. Dropping the supervisor requests the same drain in its owned task.
It does not infer engine release from a timeout or write source lifecycle state.
The [managed daemon](../configuration.md#follower-mode) now activates per-alias
application routes and configured hosted MCP before its process-ready receipt.
Follower routing accepts source-driven capability/catalog transitions and never
enters local bootstrap. Runtime failure and read-publication withdrawal close
admission. Shutdown drains transport, receiver custody and admitted service jobs,
then joins the bounded read workers. WP-747 freshness/provider coverage and the
broader WP-750 campaign remain unfinished.

`tests/replication_follower.rs` runs separate primary and follower `riffdbd`
processes over verified TLS. It creates the replication capability through the
public administration API, deploys the app-baseline TicketDesk contract, and
checks follower catalog reads, Health, typed write refusal, clean restart and
restart after a process kill. A retained TLS connection observes a subsequent
source capability revocation without reconnecting. This service activation test
does not replace the complete namespace oracle or the workload crash campaign.

`follower_applies_exact_prefix_byte_faithfully` now drives the complete
app-baseline smoke seed and its four transactional write probes through the
generated TicketDesk client. Every command is replayed with the same input to
check its original outcome and commit identity. The test validates the stopped
follower through the full startup ceremony, then compares its exact
lineage/hash/frontier position against `riffdb-testkit`'s independent canonical
namespace model. The model starts from a complete source baseline and applies
strictly successive source receipts, checking every prior value before changing
any map. It also compares against the later source snapshot, so neither side is
used as a substitute for modeled transitions. All replicated namespaces,
including empty ones, are mandatory; population and byte budgets are fixed.
Follower Health, Statistics, an authenticated denied read and a refused write
are included before the comparison, which detects local authoritative changes
absent from the source history.

`replication_stream_resumes_gap_free_after_repeated_kills` uses a bounded TLS
relay to hold an actual frame containing a new workload commit before delivery.
It kills the follower at three explicit barriers, fully validates each recovered
database, and checks the next resume request against its exact durable position.
All three interrupted prefixes and the final prefix must match the independent
source transition model. The relay forwards the original credential to the real
primary over verified TLS and never changes source bytes or grants authority.

`bootstrap_to_tail_fence_is_gap_free_across_crash` interrupts the attachment
request after local bootstrap publication. It kills both the source and receiver
twice, advances the source workload beyond the original fence, and requires the
recovered bootstrap image to remain byte-identical before final attachment. A
foreign-incarnation tail request is refused, and the completed follower must
match the source model through the final workload commit. These protocol barriers
use explicit notifications, not sleeps. Storage-level tests separately crash at
bootstrap page persistence/publication, applier receipt/root/commit boundaries,
transient-index publication, local acknowledgement, and scratch retirement.

The API-neutral service now supports a follower composition with no primary
command, control-plane, idempotency-inspection or service-audit executors. A
closed admission check rejects command, administration, installation/reimport,
maintenance, migration and export operations with the typed follower-mode error
before their operation futures run. This includes read-only command invocation
and outcome resolution. Ordinary standard reads keep current authorization;
a policy-required durable audit causes a follower-mode refusal before data is
released. Tests retain a real source coordinator as a canary and verify zero
writer/audit admission for these follower requests.

The [accepted follower audit amendment](WP-746-FOLLOWER-AUDIT-REVIEW.md), recorded
in ADR-0178 and SPEC §13.5, permits authorized Health/Statistics and authenticated
read denials to use bounded redacted operational telemetry. The service retains
current authorization and result shaping, including reauthorization after waits.
Denied reads release no data and do not fence the applier as a failed primary
audit coordinator. No local durable service audit is produced for these follower
operations; telemetry is not replication or recovery evidence. Policy-obligated
standard reads still refuse before releasing data, including obligations newly added
at a read reauthorization safe point. Applications requiring durable
audit of reads or denials must use the primary; primary audit behavior is unchanged.
The all-namespace workload comparison checks that these service operations add
no locally originated authoritative records, including after repeated crashes.

### Recovery matrix registration

The shared `riffdb-testkit::failpoint::RECOVERY_SCENARIOS` inventory registers
three replication cases. The existing simulator guard classifies each by name;
these native process tests do not claim simulated schedule exploration.

| Case | Executable evidence | Boundary |
| --- | --- | --- |
| `replication.applier.crash` | `follower_process_crashes_preserve_whole_frames_and_exact_retry_positions` | Receipt, root and durable-commit process exits; whole-frame recovery and read-only exact retry |
| `replication.bootstrap.crash` | `bootstrap_to_tail_fence_is_gap_free_across_crash` | Repeated source/receiver crashes at exact bootstrap attachment, with complete namespace comparison |
| `replication.stream.kill-riffdbd` | `replication_stream_resumes_gap_free_after_repeated_kills` | Three held-frame process kills, exact resume and four independently checked durable prefixes |

The existing bootstrap page/publication, derived-index, acknowledgement and
scratch-retirement process tests cover their narrower storage boundaries. The
WP-190 report still names its original 18 executed cases; its inventory count
includes these three additional cases, whose commands run in workspace tests.

## Authority inventory and published readers

`riffdb-storage-api::AuthoritativeStateCatalogV1` owns all 62 table/metadata
domains, including 52 replicated-authoritative domains. The generated
`fixtures/replication/authoritative-state-catalog-v1.txt` is a reviewed artifact,
not configuration. Seven V3 control domains are installed by validated activation,
not by constructing codecs. Unknown tables, namespace tags and metadata keys
refuse; metadata classification uses exact keys, never a table-wide default.

Only projection rows and apply markers are rebuildable under ADR-0017's complete
historical-plan/contiguous-log owner. Missing rebuild inputs cannot authorize
readiness. Mixed projection-frontier state, delivery state, locators,
validated-prefix evidence and vector/columnar controls remain authoritative.

`PublishedDurableSnapshot::authoritative_state_v3` returns one bounded row or
exact namespace end per call, including empty namespaces, at one V3 fence.
Callers cannot select partial inventory. Journaled tables and metadata use the
original immutable overlay's overwrite/tombstone semantics; other rows use the
same checkpoint. Borrowed bounds precede copying; errors are redacted and fused.

`PublishedDurableSnapshot::changelog_receipts_v3` reads exact successors through
that pin's tail. Lineage, epoch, position, hash and frontier are checked; pruned
positions return typed `history_pruned`. Missing rows cannot be skipped, and
inactive/legacy snapshots refuse without fallback. Old pins retain original
bytes through overwrite, deletion, checkpoint and reclamation. Neither cursor
owns a writer, gate, journal-I/O lane or durability decision.

## Frozen formats and bounds

The independent nonzero `ChangelogTransactionSequence` orders physical
transactions, not application commands. Its checked `Next(nonzero) | Exhausted`
allocator never wraps. `StoredChangelogTransactionAllocatorV3` is compact tag
68, revision 1, with an 11-byte maximum Protobuf payload. Journal preconditions
hash its complete canonical envelope; bare counters, zero, missing state and
another allocator identity refuse.

Catalog and leadership roots use tags 69/70, revision 1. Catalog accepts only
the owned inventory digest; leadership is nonzero with checked advancement.
History/follower roots use tags 71/72, revision 1, with 281/215-byte maximum
payloads. History binds lineage, anchor, tail and minimum resume with exact
receipt hashes/frontiers. Follower state is detached or attached; acknowledgement
cannot outrun applied state. Decoding grants no ancestry or readiness proof.

Source holds use tag 73, revision 1: follower acknowledgement, archive
acknowledgement or bootstrap; a nonzero opaque 16-byte ID; and exact lineage,
position, hash and dual frontier. The canonical 17-byte key repeats kind/ID.
Payloads are at most 163 bytes and the combined count is at most 4,096.
IDs are neither NodeIds nor capabilities. Full validation checks every hold
against the same retained chain; constructing one grants no authority.

Receipts carry strictly ordered namespace/key mutations, complete put values
and exact absent/prior-hash preconditions. Deletes retain the prior-value hash.
Repeated writes fold to the original precondition/final value; cancellation
retains bounded observed-state evidence. A precondition or bounds failure
poisons the candidate. Control rows never describe themselves recursively;
control-only receipts have empty mutations and exact attribution.

ADR-0186 Amendment 1 adds `CommandAdmission` tag 32 and
`CommandExecutionFailure` tag 33 before first durable use. Both carry nonempty
exact authoritative mutations. Admission advances neither frontier; failure
never advances application and advances administration only for audit in that
same transaction. Existing advancing-group checks remain strict.

V3 is exactly `riffdb.changelog-frame/v3`, numeric 3, `RDBCLF03`/`RDBCLE03`.
Its 33 source-count slots yield a 298-byte header; its footer is 48 bytes.
Admission reserves the whole wrapper and length prefix. The 32 MiB and
256-transition ceilings are independent hard limits; receipts never split.
Unknown tags/versions, wrong counts/frontiers, noncanonical order, broken
ancestry, truncation and trailing bytes refuse. Diagnostics omit keys, values,
payload-derived hashes and population counts.

V1/V2 decoders and original encodings remain compatibility-only. Legacy emitter
construction/derivation lives in `tests/storage_recovery/changelog_compatibility.rs`,
not production exports. Topology has exactly ordered V1/V2/V3 readers, only V3
writable/current/active, V1/V2 read-only, empty candidates/retired, and
`single_current`. Readability never grants production selection, fallback,
translation or partial-authority replication.

## Transaction and checkpoint durability

Same-lineage authoritative transactions retain one exact receipt in their
existing durability boundary. Direct commands, admission/failure, audited
controls, offline holds, pruning and migration use mutation-time capture in the
original Immediate transaction. Sealing checks actual allocator/frontier
postimages and stages receipt, allocator and tail atomically. Stale inputs,
unknown tables and overflow refuse; no-op captures allocate nothing. Commit
uncertainty retains existing writer fencing.

Standard journal admission proves a bounded receipt and stages its checked
allocation before submitting the original Journal V1 frame. Immutable source
bindings retain original mutations, not latest-row reads or another frame
decode. Logical subgroups in one physical epoch produce one net receipt while
preserving each command's outcome, events, provenance and idempotency identity.
Journal V1 bytes/tags and command acknowledgement/flush dependencies are unchanged.
Writer-private changes cannot publish a receipt cursor.

Checkpoint workers materialize identical source receipts in the existing
durable transaction before source reclamation. Direct barriers drain the suffix
into the caller's same transaction before capturing its successor; abort leaves
the source intact. Recovery validates retained history and exact original
overlaps before replaying missing successors. Gaps, duplicates, partial roots
and disagreement refuse without reclaiming the source. Empty extents retain
their bounded-root/header no-op check. Composite rebase drops only the covered
source prefix, preserving newer sources and old pins; destruction is iterative
on the production stack.

Validated-prefix certificates check physical AUDIT bounds from the same pin.
Command-segment audits can make the V3 logical frontier larger without changing
certificate bytes or audit placement. Optional-proof fallback remains: after
full validation, invalid proof replacement requires its exact preimage; an
absent singleton can be inserted with bounded net head changes. Valid-prior
parent/monotonicity checks and authoritative corruption refusal remain strict.
Head hashing streams unchanged SHA-256 v1 framing with bounded memory; it is
not a constant-time population scan.

## Activation, lifecycle, migration and restore

Fresh initialization/legacy normalization retain the exact pre-V3 registry.
Inactive startup completes structural and catalog validation without CLEAN or
prefix shortcuts. The dormant handoff owns the exclusive lease across their
join; cancellation releases it without writing even if readers retain the
database handle. One hardened transaction installs current registry, complete
V3 roots/control tables and the sequence-one activation receipt through the
existing durable publication owner. Application/admin allocators are preserved;
older receipts are never synthesized.

Current-registry claims require complete V3 roots; erasure cannot authorize
legacy repair or make an active database inactive. Dormant reopen validates
the catalogued layout without writing. Full startup streams retained history
and every hold before lifecycle writes. Valid CLEAN takes bounded root checks;
missing, malformed or exhausted evidence selects full validation or refusal.
Each DIRTY activation/clean consumption and final CLEAN close includes its
empty, nonrecursive receipt in the existing Immediate transaction. Lifecycle,
allocator, tail and receipt are atomic; no write follows CLEAN. Lifecycle V1
bytes and binding streams remain unchanged.

Index rewrites, generation repair, marker insertion and same-lineage contract
migration keep their transaction boundaries. Private migration witnesses bind
the original V3 prefix and validate its complete successor chain. Only tail
and allocator may advance; original receipt, lineage, anchor, minimum resume
and all other immutable bytes remain checked. Resume reuses the predecessor
witness rather than adopting a newer stage baseline.

Offline authoritative pruning receipts each original checkpoint-invalidation
and bounded delete/tombstone/watermark transaction. Tombstone hashing uses the
same current rows in streaming passes; oversized plans refuse. Watermark
stamps validate before equal retries and preserve the chain-root digest.
Same-lineage pruning does not reset lineage.

Destructive restore reanchors active V3 in its existing incarnation-stamp
transaction: preserve database identity, leadership and dual frontier; advance
incarnation; clear source history/holds; detach follower state; remove lifecycle;
install one empty sequence-one RestoreAnchor. Watermarks are rebound without
inventing a digest. Equal retries validate without writes; backwards incarnation
refuses. Old-lineage resume fails and receivers must bootstrap after the anchor.

## Source holds and history retention

Controls are crate-private, with no application exports or active scheduler.
Registration checks exact retained fences; equal retries allocate nothing.
Follower/archive acknowledgements advance monotonically; bootstrap fences
cannot move or release here. WP-746 owns authorized remote durability evidence,
durable tail attachment and audited abort.

Reclamation needs two observed known-durable checkpoints, using both publication
identity and successful-commit epoch. The floor cannot pass the earlier tail or
any registered follower/archive/bootstrap fence. An existing drained exclusive
barrier encloses at most 256 receipt deletions, exact minimum resume, allocator
and an empty HistoryReclamation receipt. Surviving receipts and authoritative
rows are unchanged. Post-commit observation prevents self-stimulation; lost
observations delay pruning safely. No command-path hook/flush is added. CLEAN,
writer fencing, invalid holds and exhaustion refuse; cancellation aborts and
releases the lease.

## Proofs and fixture review

- `replication_inventory_classifies_every_storage_namespace`: populated physical
  catalog, exact ends and reviewed classification.
- `successor_changelog_receipts_survive_checkpoint_and_recovery`: real direct
  groups, journal overwrite/delete, complete-authority replay, unchanged command
  graphs, identical checkpoint bytes and repeated recovery; also executes the
  real command allocator/journal process-crash matrix.
- `changelog_history_reclamation_respects_checkpoint_and_fences`: real journaled
  audits, exact materialization, every hold kind, unchanged full authority,
  old/new pins and seven hold/reclamation crash edges with repeated reopen.
- `changelog_v3_is_only_production_replication_identity`: fixed format digests,
  round trips, resealed downgrade refusal and no legacy production consumers.

Additional process tests cover actual activation, lifecycle, checkpoint workers,
journal recovery, captured controls, optional-proof fallback, migration, restore
and watermarks. Refusal tests cover interior corruption, missing roots,
substituted holds, exhaustion, the 4,096-hold ceiling, concurrent registration
and cancellation. Integrated recovery and full CI remain required alongside
scoped acceptance. Isolated codec fixtures do not prove application semantics
or an activated replication protocol.

Simulation keeps all historical coordinates and predicates. Before/after replay
attributes one restored window to `630e1a42` and six moved windows to `a29312ff`.
Appended witnesses `0x51C2C30A`, `0x51C2C404` and `0x51C2C500` each reproduced
identical reports through twelve reruns. No oracle, generator, bound, accepted
outcome or retirement history was weakened.

The maintainer approved the catalog and initial V1/V2/V3 vectors, then the three
source-hold vectors on 2026-09-14. Amendment 1 subsequently added admission,
execution-failure, audited-failure and command-lifecycle vectors and regenerated
`changelog-frame-v3.hex` for 33 source slots. These synthetic vectors freeze
encoding, not application-record validity. The maintainer approved all five final
vectors in session on 2026-09-14; their exact hashes are in the verification report.
V1/V2 and unrelated vectors remain byte-identical. Regenerate with
`./scripts/generate-changelog-fixtures`; its `--check` runs in generated-artifact
acceptance. Fixture review does not accept a new ADR.

## Archive consumer foundation (WP-749, incomplete)

The engine-neutral archive consumer validates complete V3 frames before passing
bytes to a sink. It checks database, history and leadership identity, receipt
ancestry, exact dual frontiers, frame-chain continuity and existing frame bounds.
Its descriptor carries a checksum of the full stored bytes. The consumer retains
at most one uncertain frame and advances archive-local progress only after the
sink confirms durability. An exact retry preserves pending bytes; another
submission while a frame is uncertain returns typed resync without skipping it.
A reconnect resets only frame chaining at the confirmed receipt position.

The consumer has no database writer, source-acknowledgement callback or command
admission dependency. Tests cover failures before and after sink persistence,
exact retry, overflow, reconnect, malformed frames, gaps, frontier mismatch and
redacted diagnostics. This is an internal foundation: operator configuration and
offline archive restore remain unfinished. The bounded external manifest codec
and filesystem sink are described below.
No archive CLI availability or wall-clock recovery promise is implied.

### External archive manifest V1

ADR-0178 admits the additive `archive-manifest/v1` external format. Its linked
records are each exactly 386 bytes and describe one complete existing V3 frame.
They contain the database, history incarnation, leadership epoch, current catalog
digest, original full-backup manifest digest and receipt fence, exact before and
covered receipt points, full frame byte length and digest, preceding manifest
digest, and an explicit encryption declaration. Receipt points retain the
physical transaction sequence, history hash and both application/administration
frontiers. The first record has no predecessor and begins exactly at the backup
fence. Every successor preserves that backup binding and links to the complete
preceding manifest bytes. This permits bounded sequential verification without
retaining the whole archive in memory.

The canonical encoding is big-endian: `RDBARM01`, version u16 `1`, database UUID
(16 bytes), incarnation and leadership (u64 each), catalog and backup digests
(32 bytes each), encryption and predecessor-presence tags (u8 each), predecessor
digest (32 bytes; all zero when absent), three receipt points (58 bytes each),
frame length (u64), frame digest (32 bytes), then SHA-256 of all preceding bytes
(32 bytes). Each receipt point uses the existing canonical 18-byte dual frontier
following its u64 sequence and 32-byte history hash. The linked manifest and
frame digests cover their complete stored bytes, including checksum footers.
Unknown versions, tags, noncanonical absence, invalid ranges, lengths, checksums
and trailing bytes refuse before use. A frame remains subject to full V3 decoding
and exact descriptor comparison. No V3 or BackupManifestV1 bytes change.

Encryption tag `0` declares operator-permitted unencrypted storage; tag `1`
declares operator-managed sink encryption. The descriptor performs no encryption
and attests no external encryption enforcement. Sink composition must honor the
configured posture before releasing archive bytes. Manifest decoding
alone neither establishes backup validity nor permits restore: the repository
must select an exact terminal record and verify the complete suffix before any
database replacement. Archive configuration and restore are still unfinished;
no new recovery-granularity promise is made by this codec.

Regenerate the four deterministic first/successor vectors for both encryption
postures with `./scripts/generate-archive-manifest-fixtures`; `--check` verifies
them without writing. The generator participates in `check-generated`. These
new format vectors require the package's human fixture review before closure.

### Filesystem archive custody

The storage backend now provides an exclusively locked private filesystem
repository. It receives only already validated frames and independently verified
backup binding inputs; it has no database writer or source-acknowledgement port.
Immutable frame and manifest files are addressed by the first covered physical
receipt sequence. Their complete bytes and directory entries become durable
before `CURRENT` is atomically replaced and its directory synced. `CURRENT`
contains the same existing manifest encoding, with no second record format.

An uncertain write keeps consumer progress unchanged. Retry compares the exact
pending bytes, resolves a possibly completed selector rename and synchronizes
the selected pair before confirming durability. Reopen walks from the original
backup fence to the exact selected terminal manifest, validating every linked
descriptor and complete frame with one frame in memory. Missing, corrupt,
reordered or foreign records refuse; directory ordering never selects a head.
Cancellation is checked between frames and grants no recovered progress.

An exclusively owned repository also exposes a bounded reader of that selected
prefix. Each step checks the pinned selector, validates the next linked manifest
and complete frame, and returns that one frame. A changed selector, lost custody
or corrupt pair terminates the reader with a typed error; subsequent steps yield
nothing. Reading grants no source acknowledgement or database mutation authority.
The caller checks cancellation between frames and releases each frame before
requesting the next.

Only after successful prefix validation may recovery remove the three fixed
staging names and the exact next unconfirmed frame/manifest pair. All five names
are size-checked before cleanup; unrelated entries remain untouched. Symlinks,
nonprivate roots and changed lock identities refuse. Process-exit tests cover
each staged-file, immutable-pair and selector boundary; independent uncertain-I/O
tests require exact retry at those same nine boundaries. This repository does
not implement encryption, retention policy, object-store access or the offline
restore ceremony; WP-749 remains open for that composition and its full proof.
