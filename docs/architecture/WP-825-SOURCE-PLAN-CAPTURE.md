# WP-825 checked source-plan capture

The authoritative replay worker must retain the exact checked projection plan
and its historical writer lineage while commands and compatible deployments
continue. A logical command-prefix pin alone does not own that compiled graph.
Cloning a plan before admitting its allocations would also bypass ADR-0316's
aggregate source-worker allowance.

The catalog now provides a fields-private, read-only capture that borrows its
original immutable catalog owner, retains the resolved plan and checked group
schema, and reserves their capacity under the caller's actual source ledger.
It admits graph-inspection scratch first, then the complete retained catalog
and incremental plan/schema clones before construction. An explicit destruction
guard keeps the original owner borrowed until the capture physically drops;
cloned data drops before the reservation. Existing provider-owned shared
allocations retain their original lifecycle owner and are never reassigned to
Source64. The source reservation conservatively covers retention overlap.

Read-only accessors expose the checked plan, schema and original active pointer.
Exact ledger affinity uses the accounting owner's pointer, not a database ID.
The native worker must still bind that pointer to its captured engine catalog,
retain the actual command-prefix pin, and recheck publication authority. This
pure capture grants no source locator, database writer or publication right.
Ordinary catalog resolution preserves its existing selection and capacity rules.

Four focused semantic checks passed on the qualification host: plan/schema
parity and capacity release; refusal before cloning under pressure; original
provider charge through the final independent identity reference; and latest
exact-plan selection across a validated compatible lineage. Three passes are
retained from receipt
`d03af48f684196f47edf2d69cd6b6f0d6f5218d052aabb17b08483efd849c81b`;
the corrected final-reference proof and catalog/types/storage-API/contract-IR
all-target, all-feature Clippy passed in receipt
`32cd56c398830029c4335ab6747388fb68bc0456c6c746fada706bf79a035f7a`.
Scope, size and panic guards passed separately in receipt
`d902b2cf5f07c3556cd7018937051963e8c22c20f8edc7f38803dc38b44d8165`.
The scope registration precedes implementation in its own commit.

Package: WP-825. Tier: guarantee under accepted ADR-0316. Behavior changed:
internal checked-plan custody and prior allocation admission. Compatibility:
no durable encoding, fixture or application operation changes. Handbook: no
update is needed for these hidden first-party ownership helpers. Hazards and
follow-ups: native capture, admitted evaluation, staging and publication still
need composition; combined acceptance, full CI and the rehearsals remain pending.
