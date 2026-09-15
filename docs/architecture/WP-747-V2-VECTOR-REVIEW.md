# WP-747 — exact canonical vectors in existing V2 byte lanes

Status: accepted 2026-09-15. The maintainer approved the exact text in session:
"Approve exact text". Standalone acceptance commit `1b3a97fc` records it in SPEC
§4.10 and ADR-0160/0190/0192/0209. The follower-only amendment
in [WP-747-FOLLOWER-COLUMNAR-REVIEW.md](WP-747-FOLLOWER-COLUMNAR-REVIEW.md) is already
accepted and is not being reopened.

## Conflict demonstrated

SPEC §4.10 and accepted ADR-0178 require follower vector views. The accepted
follower amendment requires the existing V2 builder and complete independent
logical validation. ADR-0209 makes V2 the sole current layout for primary and
follower columnar sources. Before this amendment, `generation_v2::logical_type` rejected every
`ValueTypeTag::Vector` as `UnsupportedDefinition`, including an empty vector
source. The streaming adapter reported this as `Invalid` before creating any
V2 generation. `SegmentV2LogicalType` retains its existing scalar physical types.

The real-bootstrap WP-747 lifecycle test demonstrated this refusal after scalar
materialization succeeded. The owned scalar worker/restart test passed in the
same run. No source control or source checksum was changed. The vector-success
attempt is recorded in `/tmp/wp-747-runtime-green5.log`; that pre-amendment lifecycle test retained a rowless-refusal assertion.

ADR-0160 Decision §3 permits offset-plus-bytes lanes for bounded canonical values
but explicitly says: “General-purpose compression, native-endian values,
floating point, lossy encoding, CPU-feature-dependent bytes, and
application-supplied codecs are excluded.” Canonical vectors contain finite
IEEE-754 binary32 components. Treating those bytes as an implicit exception would
choose an interpretation of an accepted exclusion without human review.

## Exact accepted amendment

The following qualifies ADR-0160 Decision §3 and the corresponding V2 construction
and validation requirements of ADR-0190, ADR-0192 and ADR-0209. It applies to
primary and follower V2 materialization; their distinct publication authority
rules remain intact.

> **Canonical vector lowering in V2.** The floating-point exclusion does not
> exclude lossless storage of an already checked `CanonicalValue::Vector` through
> the existing canonical-value codec. Only a field whose checked registered
> definition is `Vector` or `Optional<Vector>` may use this lowering. Its schema,
> source identity, definition fingerprint, dimension and columnar specification
> remain vector identities. A non-null vector cell is encoded as the exact
> existing canonical-value bytes, including the Vector discriminant and
> dimension, within an existing Segment V2 Bytes lane. Null retains the existing
> validity representation. Ordinary Bytes fields retain their existing meaning.
>
> This adds no logical-type tag, physical-encoding tag, registry version, segment
> version, manifest/root version, source identity, durable control or public
> selector. The existing deterministic Bytes encoding selection and all lane,
> segment, row, partition, work and output bounds apply. There is no raw native
> float lane, alternate vector codec, lossy conversion, new scalar floating-point
> type or unbounded population path.
>
> Before any V2 view is installed, the generation owner checks the exact
> schema-derived physical lane type, decodes the entire bounded canonical vector
> payload, checks its discriminant and declared dimension, requires canonical
> re-encoding equality, and restores the typed vector cell. Trailing bytes,
> non-finite components, noncanonical negative zero, wrong types or dimensions,
> malformed validity, bounds failures and logical mismatches refuse the whole
> generation. Full root/member validation and independent equality compare the
> restored typed rows against the authoritative input, including vector values.
>
> Byte-lane ordering, dictionaries and statistics never prove vector ordering,
> distance, similarity, eligibility or pruning. Vector columns supply no scalar
> zone-map or dictionary pruning claim; existing nearest-query and scalar-filter
> semantics operate on the restored typed view. Current model, evidence,
> authorization, partition isolation and freshness checks remain mandatory.
>
> Previously valid V2 scalar artifacts remain byte-exact. Older code encountering
> a vector definition continues to refuse it; it cannot interpret the new view as
> a scalar fallback. Primary views still require exact durable-control selection.
> Follower views remain independently validated disposable material and never
> claim the source artifact checksum or originate authoritative control writes.

## Proof required before package closure

- Golden canonical vector payloads, including nullable cells and dimension limits,
  round-trip through existing V2 Bytes encodings without scalar fixture changes.
- Wrong physical type, wrong canonical tag/dimension, negative zero, NaN/infinity,
  trailing data and row/lane limits fail before installing a view.
- Complete V2 typed logical equality detects vector-value corruption; vector
  columns cannot contribute byte-order pruning evidence.
- Primary and follower projected-nearest results match at the same frontier,
  including model/policy/staleness refusals, source advancement and restart.
- The complete namespace oracle remains byte-exact after follower materialization,
  failures and refusals; primary selection and crash tests pass.

## Acceptance mechanics

The maintainer's words/date and exact normative text are recorded in standalone
acceptance commit `1b3a97fc`. The shared V2 generation builder now lowers checked
vectors into existing Bytes lanes, restores typed cells after complete canonical
validation, and removes vector columns from installed pruning evidence. Scalar
bytes remain unchanged. WP-747 remains open for the complete primary/follower
parity, freshness, restart and authoritative-namespace campaign.
