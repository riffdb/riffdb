# Schema-Bound Columnar Control

RiffDB resolves each configured scalar projection name and each compiler-declared
vector projection through the active checked contract bundle at startup. The
name remains the application-facing lookup value; it is not a durable key and
never becomes a directory component. A scalar source is contract lineage plus
its existing physical definition fingerprint. A vector source retains contract
lineage, entity ID, and vector field ID.

The complete columnar specification separately binds primary-key order and
codec facts, all projected and organization-scope types, provider descriptors,
policy mode, replay limits, and vector production settings. A name-only change
therefore preserves both source and specification. A changed scalar physical
fingerprint creates a new source. Other semantic changes under the same source
allocate a disjoint rebuild and make the predecessor unservable.

Startup admits at most 256 distinct sources as one set. Empty, path-shaped,
duplicate names and duplicate aliases for one source are rejected before any
columnar control or projection file is touched. Every absent source receives a
durable `BeforeFirst` retention fence before command writers and projection
workers start. The worker then rebuilds V1 from one authoritative snapshot plus
the retained contiguous tail. Only a fully synced, reopened, checksummed
immutable manifest can advance the fence or publish.

The common durable control is the sole selector. V1 manifests live below a
schema-hash directory and use `MANIFEST-V1-<sha256>` names; opening requires the
exact length and checksum recorded by control. Directory enumeration, a legacy
`MANIFEST`, and name-derived directories cannot choose state. A crash before
the control update leaves an ignored orphan. A crash after it reopens the exact
selected artifact.

The predecessor vector-control record (durable tag 65) remains readable only
for bounded structural validation through the epoch-1 compatibility window.
It is not writable and contributes no selection, retention, health, recovery,
metrics, or migration input. Its values and old directories are never copied or
translated into common control; WP-757 removes that predecessor family.
