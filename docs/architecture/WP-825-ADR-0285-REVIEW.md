# ADR-0285 review evidence

The maintainer accepted ADR-0285 in session on 2026-10-11: "I accept". Its body
is unchanged; the exact proposed record SHA-256 was `03b5dde4c68bca8d53c827bbe285deb2e22f0e30534ce88e3530b3a52193b371`.
No implementation of its new custody model has begun.

## Native correctness evidence

The committed hydration correction refuses repair before a later complete scrub
can authorize repaired bytes. The subsequent final sealing/stamping correction
at `170867f6` passes all seven scoped acceptance checks on the required 8-vCPU,
32-GiB Linux host: 1,368 native tests pass, with two existing skips. It refuses
repair at final sealing and independently at sealed-stage stamping, and checks
all three existing artifact seals before the controlled incarnation write.

A separate native regression on that exact corrected source now reproduces the
remaining ordinary-open gap. The server's staged authorization path uses the
same ordinary source engine construction exercised by this test. The test starts
with a genuine current backup and completely validated private stage, opens it
through native source recovery, runs another complete physical/structural/catalog
scrub, and attempts final sealing.

| Native control | Source open | Bytes relative to validated stage | Later complete scrub | Final seal |
| --- | --- | --- | --- | --- |
| Healthy stage | Succeeds | Changed | Succeeds | Succeeds |
| Stage requiring allocator repair | Repairs and succeeds | Changed | Succeeds | Succeeds |

The repaired case fails the intended refusal assertion. The original backup
primary remains byte-identical and the configured target remains absent. This
proves the ability to obtain a seal after an earlier repair, not an actual public
target replacement or a bearer-authorization result. Earlier direct final-seal
and stamping regressions cover their own boundaries separately.

Checking only whether the final open requests repair misses the earlier repair.
Rejecting every ordinary-open byte change would also reject the healthy control.
The proposed native lifecycle instead keeps required preparation before final
complete validation, retains exact custody afterward, and obtains the staged
authorization snapshot without an ordinary writable source activation. All
existing storage/catalog, retained metadata, bootstrap and allocator joins and
fresh bearer checks remain required.

## Performance and qualification limits

The preserved populated 100-artifact backup materializes in 100.1 seconds on the
hydration-corrected native source, with all original artifact hashes unchanged.
That component alone exceeds the unchanged 60-second whole public action limit.
An earlier nested profile measures verification-copy capture at 15.4 seconds.
Eliminating that extra copy is an architectural opportunity, not evidence that
the final whole action will meet its deadline.

The earlier genuine normal-window p95 of 101.766 ms remains unresolved. Final
combined-source full CI, four-language conformance, four frozen ordinary release
binaries and the actual eight-hour all-seven-fault campaign remain required.
No eight-hour or 24-hour campaign has started.

## Review boundary

ADR-0269 explicitly requires review when file-lock ownership changes. ADR-0285
also refines ADR-0050's staged startup construction. Exact-text human acceptance
was required before implementing that ownership change and is now recorded. The proposal keeps the
full validation predicates, public interfaces, formats, native transaction and
synchronization boundaries, compatibility, resource limits and workload gates.

## Implementation readiness: actual backend locks

Inspection of the pinned engine implementation confirms that backend `close`
actively releases recorded range locks and its legacy whole-file lock. The Linux
range-lock implementation uses open-file-description locks. Keeping a cloned
file descriptor alive is therefore insufficient evidence that engine close or
a range unlock preserved exclusion. Shared range acquisition must also be
accounted for; it cannot silently downgrade an enclosing custody lock.

The implementation must prove native exclusion through every engine ownership
transition and through final closed-byte sealing, using the actual backend lock
methods. Cross-process tests must attempt an independent ordinary engine open at
the deterministic transition hooks; in-process type checks and descriptor counts
cannot substitute. A retained proof must be invalidated on uncertain custody.
This inspection does not implement a new lock protocol or change ADR-0285's
proposed text. Feasibility and exclusion remain implementation exit conditions.

Further inspection of the actual redb 4.3.0 dependency establishes a contractual
constraint as well: `StorageBackend::close` must release every lock acquired
through its lock methods. Suppressing close or unlock to retain descriptor
custody would violate that contract. An independently opened guard can instead
conflict with the engine's own native range locks or legacy whole-file lock;
acquiring one alongside an ordinary backend is not a proven handoff protocol.

One candidate requiring implementation proof is an externally owned native
exclusive guard with a single bounded engine lease. The guard would be acquired
outside the per-engine backend's lock methods; backend close must release all
of that engine's leases, while the independently owned custody guard remains.
Shared requests must preserve the stronger exclusion, range queries must retain
their documented meaning, and closed engines must lose access. Ordinary,
read-only and legacy cross-process open attempts must remain excluded through
every handoff and final sealing. This is a feasibility candidate, not an
implemented or qualified protocol. Descriptor survival alone remains insufficient.

The existing server `validate_ready_join` checks matching catalog/database/open
session, active catalog, retained metadata, bootstrap lifecycle and both sequence
allocators before operational activation. The private authorization path must
retain those same joins while granting only its immutable authorization reader.
Complete offline validation currently discards its catalog proof; retaining a
receipt without that proof cannot replace the join. Required native compatibility
preparation belongs before the final complete validation.

## Actual staged-server caller and narrower repair refusal

A separate regression now calls the actual server staged-validation and
authorization helper with a bounded invalid credential. A healthy stage reaches
the expected credential refusal. A stage requiring allocator repair also
reaches that refusal, failing the required corruption-before-authentication
assertion. Both original backup bytes and configured target bytes remain
unchanged. This proves the earlier repair boundary in the actual caller; it does
not prove successful bearer authorization or target publication.

A narrower implementation correction is being tested independently: refuse
engine repair at the existing private-stage startup open while sharing the same
complete startup joins and activation. It preserves fresh native exclusive
ownership and retains no earlier validation proof. That repair-refusal change
does not implement this proposed custody redesign. Reusing a validation proof
or changing file-lock ownership still requires exact acceptance of the record.


## Fresh corrected-source attribution

Fresh ordinary preparation of `8d5e6172` passes every specified rate and normal-
window limit with complete state and archive reconciliation. Its unchanged
whole public restore still fails: creation takes 19.34 seconds, empty recovery
admission takes 1.05 seconds, and restore exhausts the remaining 28.23 seconds.

A separate owned-copy native profiler measures core staging at 38.92 seconds,
including history hydration at 12.92 seconds and retention-watermark processing
at 9.56 seconds. Complete private verification takes 64.14 seconds, including
33.96 seconds in its validation session. Materialization including verification
is 104.17 seconds before authorization, publication and startup complete. Values
are inclusive; nested timers cannot be summed. Its finite diagnostic observer
stops before post-publication validation completes, retaining an unknown final
outcome. Public polling is intentionally closed by ADR-0050 during recovery;
its exact typed unavailable responses are unknown, never success evidence.

This supports reconsidering the duplicate private-copy lifecycle. It does not
prove that this proposal alone will satisfy the unchanged 60-second whole-action
limit: core staging already exceeds the budget remaining after backup and
handoff. Latest-main claim groups may reduce future history volume, but that
requires fresh combined-source measurement. No copy elimination, proof reuse,
lock-protocol change or qualification-limit change has been implemented.
The ADR's exact proposed text and SHA-256 remain unchanged.
