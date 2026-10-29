# WP-825: explicit native catalog and source inheritance transitions

Package: WP-825. Tier: guarantee. Accepted decision: ADR-0326.
Status: exact decision and SPEC/WP-825 amendments accepted in session on
2026-10-16. Implementation is authorized; integrated activation is not yet qualified.

The recent audit found an existing integration gap: active history has one
catalog, and the existing restore anchor preserves that catalog while resetting
source-local history. Those owners cannot implement ADR-0325's upgrade and
ADR-0324's inherited checkpoint validity unchanged. ADR-0325 also says to
preserve retained history through every supported transition, while ADR-0186
requires source-local history reset/exclusion during destructive recovery and
bootstrap. ADR-0326 resolves that conflict explicitly.

## Concrete choice

Ordinary upgrade keeps the existing incarnation, epoch and physical hash chain.
One audit fact marks the V2-to-V3 cut; old receipts retain their original bytes
and catalog interpretation until ordinary fenced reclamation. Live old holds
and jobs must retire through their existing owners first. New V3 replication
starts at the cut or a later bootstrap fence. No generic catalog-span engine,
history copy or new catalog namespace is introduced.

Restore and promotion instead retain their existing new-incarnation semantics.
A separate authoritative audit fact binds inherited source references, original
origin, exact predecessor scalar bytes and the new anchor. This lets native
validation reconstruct exact attribution after the original receipt is pruned,
without treating an older incarnation number or a derived cache as authority.
Restore keeps sidecar removal/rebuild; promotion preserves valid local state.

The accepted decision explicitly preserves legacy historical control-fact interpretation
through a checked streaming commitment. This attestation grants no current
writer, fence, promotion or checkpoint authority, and does not invent missing
legacy restore records. New checkpoint inheritance always needs its own proof.

## Accepted changes

- Two new bounded Audit roles: catalog transition <=2 KiB complete envelope;
  source inheritance <=16 KiB complete envelope. Existing source7 schemas,
  catalog digests and legacy record bytes remain unchanged.
- A NEW maintenance admission limit of 128 distinct required inherited source
  references. Every required surviving root/base must fit; otherwise publication
  refuses with predecessor intact. No valid base is silently discarded.
- V3 promotion allocates inheritance/start/control/success atomically: four
  administration entries, with actual typed result and success links. Legacy
  V2-catalog promotion retains its three-entry bytes. The independently framed
  external attempt/receipt is unchanged.
- V3 restore attributes the inherited-source fact, actual allocator reset and
  every authoritative stamp change. Legacy V2 empty anchors remain unchanged.
- Explicit amendment of ADR-0325 Decision 13: ordinary upgrade preserves original
  source history until reclamation; destructive restore/promotion and bootstrap
  retain ADR-0186's source-local reset/exclusion while preserving authoritative
  historical bindings, origins and transition evidence.

No workload size, pacing, source/provider memory ceiling, scan/write limit,
authorization boundary or 45/60-second recovery gate changes. No new dependency,
unsafe code, public bypass or independent background timer is proposed.

## Evidence and implementation gate

C3D qualified the existing source codec foundation and the additive fencing
codecs. Fencing qualification retains 23 executions/22 distinct semantic tests
plus all-target/all-feature Clippy. Receipt:
`54ef95a0ed5f47ff8d82035574a7fc61c194d09ee01fb2ec2e4198bf06fdc0dc`.
This proves those components, not the complete transition.

The bounded private staging backend also passed its one expected RED, five
GREEN semantic tests, Clippy and scoped guards. Receipt:
`7bb98e89797ac1162c167c324df7a389dde94ea1764f122088555da189ec1d18`.
Complete staging memory/disk admission and evaluation remain unfinished.

Final ADR form/index/package checks passed on C3D. Receipt:
`ed6a2e81b5b38beb6afc73c0d7dff718ca0c75409b598c239987f270c0973728`.
Acceptance-plan
printing is not acceptance execution. No full CI, integrated transition proof,
eight-hour rehearsal or 24-hour qualification is claimed here.

Implementation must supply focused failing-test repairs for the cut,
origin/witness codecs, native startup/scrub, retention, stream/bootstrap,
restore/promotion reconstruction and process-crash reconciliation. Concrete
schema/hash/wire/compatibility fixture diffs require separate human review.
Update affected operational/compatibility handbook pages with implementation.

The standalone acceptance commit is `1c1be3d7`. Its Decision text and reviewed
SPEC/package amendments are unchanged. Accepted form, generated index and
package brief passed on C3D; receipt:
`5e45c95256f18c2bb58c9110a54b80382a3743b7231f537678ba1e88a8ee3071`.

The next native integration check executed all 33 focused semantic tests
successfully, including the modified recovery oracles. Its final Clippy check
found an item-ordering lint, so the whole check is not a qualification pass.
That move-only correction is checked separately without repeating those tests.
Receipt: `dab0917fe67b500642d6e0e1fbc543a3ff1d96dbf6eaebf95abefc4600bb3c95`.

A genuine retained-command diagnostic reproduced a separate database gap:
the existing captured receipt pin does not fence the complete Original prefix
against application pruning. Its one expected semantic failure was observed
on C3D; this is regression evidence, not a passing implementation. Receipt:
`7f5e226635ca37da9076ccc317a521a8ba8c82e0363ea7bbf4d7b324a1404e44`.

Handbook pages are updated with the eventual implementation. This decision
review adds no available operator command, configuration or compatibility claim.
