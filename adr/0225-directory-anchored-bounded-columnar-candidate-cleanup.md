---
adr: "0225"
title: Directory-Anchored Bounded Columnar Candidate Cleanup
status: accepted
tier: guarantee
date: 2026-09-12
accepted: "2026-09-21"
acceptance: >-
  Agent acceptance on 2026-09-21 under the maintainer's explicit in-session
  delegation: "you have permission to approve adrs without a human to accomplish
  this work", specifically including ADR-0225 acceptance provenance. The agent
  reviewed and reaccepted the exact Decision text from branch
  codex/wp757-integration-safe. This is fresh delegated acceptance, not evidence
  of who accepted the record on 2026-09-12; that historical reference is unknown.
requires: [ADR-0209, ADR-0221]
amends: [ADR-0221]
supersedes: []
requirements: [REC-001, PRJ-004, PRJ-006, PRJ-008, PRJ-009, PRJ-010]
packages: [WP-757]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - The rustix version, default-feature posture, feature set or dependency graph would change.
  - Descriptor-relative operations would escape the exact authorized columnar collision implementation or become an application-facing filesystem API.
  - Cleanup would follow a symlink, resolve a descendant through an ambient pathname, enumerate the source parent, adopt artifact bytes, or touch another candidate or quarantine.
  - A bound, reference proof, directory identity check, synchronization edge or unknown-outcome refusal would be omitted or weakened.
  - Atomic NOREPLACE would be replaced by ordinary rename, copy/delete, an absence precheck, unsafe code, direct FFI or an external process.
  - Control, transaction, retention, publication, acknowledgement, format or recovery semantics would change.
---
# ADR-0225: Directory-Anchored Bounded Columnar Candidate Cleanup

## Context

ADR-0209 Decision 8 requires aggregate-bounded inspection and removal of exact
unreferenced candidate quarantines, rejecting symlinks and path escapes. An
ambient pathname can name a different directory after inspection. Checking it
again before a pathname-based deletion does not bind the deletion to the
inspected directory; recursively deleting an ambient tree also cannot enforce
the frozen two-directory and aggregate ceilings during removal.

ADR-0221 authorizes the existing pinned rustix dependency only for the exact
no-replace rename. That approval does not authorize its descriptor-relative
directory traversal and unlink operations. The current aggregate preflight and
rename primitive therefore do not constitute a complete retirement gate. This
record extends only the implementation mechanism for the existing guarantee.

## Decision

1. ADR-0221 Decision 3 is amended only to permit the additional safe rustix
   operations listed below inside ADR-0209 Decision 10's existing columnar
   collision implementation. The exact direct pin remains rustix `=1.1.4`,
   `default-features = false`, features `fs` and `std` only. No Cargo manifest,
   lockfile, version, checksum, transitive dependency or feature change is needed
   or authorized by this record.

2. In addition to the existing `renameat_with(..., RenameFlags::NOREPLACE)`, the
   implementation may use `rustix::fs::openat`, `fstat`, `statat`, `Dir::read_from`,
   `Dir::rewind`, directory iteration and `unlinkat`. Explicit `openat` directory
   opens are read-only and require `DIRECTORY`, `NOFOLLOW` and `CLOEXEC`; `statat` uses
   `SYMLINK_NOFOLLOW`. Unlink uses no flags for a direct regular-file member and
   `REMOVEDIR` only for an admitted empty scratch or quarantine directory.
   `std::fs::File::sync_all` continues to synchronize retained directory handles.
   Safe owned-descriptor conversion is permitted; unsafe code and direct FFI
   are not. No other dependency operation or purpose is added.

3. The implementation pins the validated source parent and derives the exact
   final or `.tmp` candidate `P` and its current-incarnation quarantine `Q`
   exactly as ADR-0209 requires. Directory opens, metadata inspection, member
   enumeration and deletion below that parent are relative to retained
   directory handles, not re-resolved ambient descendant paths. Directory
   identity/kind is checked against the retained handle before it is consumed
   by a later phase. A substituted, missing, symlinked or otherwise mismatching
   required directory closes the gate; no replacement tree is selected by
   pathname. The source parent is never enumerated.

4. Only `P`, `Q` and their optional exact `.rebuild-scratch-v1` child can be
   inspected. Only the already-authorized unreferenced `Q` and its admitted
   regular-file members and scratch child can be removed. Every enumerated
   member name is one direct component; traversal components, separators,
   symlinks, special files, extra directories and escapes refuse. Iterator
   entries for `.` and `..` are not traversed. No arbitrary caller path,
   content adoption, relabelling, parent scan, sibling cleanup or unselected-
   published-generation reclamation is added to this mechanism.

5. ADR-0209's exact arithmetic and platform-conversion preflight runs before
   construction, traversal or mutation. Inspection and removal both charge
   the unchanged aggregate file and byte ceilings and admit at most the root
   plus its exact scratch child. Removal begins only after complete bounded
   validation and the existing exact control/captured-view reference proof;
   inspection alone grants no deletion authority. Traversal retains bounded
   directory handles and counters, not an inventory proportional to the tree.
   A changed tree or a deletion, iteration, identity, bound or synchronization
   failure leaves construction and publication closed. Partial quarantine
   removal is not inferred to have completed and repeats through the same
   reference-fenced protocol on retry.

6. Existing exclusive database ownership, the sole worker, capture fencing and
   exact durable-control rereads remain required. An unprepared Candidate's
   reservation is not an artifact witness; selected/prepared control pointers
   and live captured views remain protected. This record adds no reference
   exemption, lock-order change, transaction callback, control transition,
   public API or recovery inference. ADR-0209's quarantine removal, parent sync,
   no-replace `P` to `Q` rename and parent sync sequence is unchanged and repeats
   for both exact candidate paths before every unprepared construction.

7. ADR-0221's destination-race refusal, exact NOREPLACE syscall and lack of any
   fallback remain mandatory. Directory-handle operations do not authorize
   ordinary rename, recursive unbounded removal, `/proc` or other descriptor-
   pathname emulation, copy/delete, external processes or platform-specific
   application branches. An unsupported operation or uncertain outcome closes
   the gate; it never selects or reconstructs a candidate by inference.

8. OBL-0209-4 retains its exact proof name
   `epoch_two_unprepared_v2_retires_exact_candidate_paths`. It must exercise
   the actual integrated control/reference/cleanup/construction path, not only
   these primitives. Deterministic directory and scratch substitution tests
   must prove that unrelated replacement/outside bytes are not traversed or
   removed and the gate stays closed. Existing destination-race, aggregate
   bounds, captured-view protection and every remove/rename/sync process-crash
   edge remain required. No new obligation replaces or discharges that proof.

9. After exact human acceptance, a separate governance commit adds ADR-0225 to
   WP-757's required ADRs and names this exact mechanism amendment in its
   deliverable and review triggers. Requirements, dependencies, allowed paths,
   bounds, acceptance commands and all existing obligations stay unchanged.
   Before that acceptance and reconciliation, this proposal changes only its
   own record and the generated ADR index; no production implementation,
   database, package closure, public behavior or external state changes.

## Options considered

1. **Extend the existing pinned dependency narrowly:** chosen; directory
   capabilities confine the already-required bounded cleanup without another
   package, feature, authority owner or fallback.
2. **Precheck ambient paths, then delete by pathname:** rejected; repeated
   path checks do not bind later descendant operations to the inspected object.
3. **Add a general filesystem capability dependency:** rejected; this collision
   protocol needs only the exact operations above from the existing dependency.

## Consequences

- The implementation can retain directory authority across bounded inspection,
  removal and synchronization while rejecting namespace substitution.
- More descriptor identity, race and crash cases become explicit implementation
  evidence; accepting this record does not prove any of them.
- All pending direct-V2 worker, V1 retirement and WP-757 epoch obligations remain.

## Standing design tests

- **Interface safety:** applications, agents, operators and transports cannot
  supply paths, descriptors, layout, cleanup limits, fallback or deletion
  authority. Only the server-owned closed retirement path uses this machinery.
- **Scale:** exact ADR-0209 file/byte/directory ceilings remain unchanged, with
  checked arithmetic before traversal and bounded retained traversal state.

## Checks

- Architecture checks confine these dependency operations to the authorized
  collision path and reject ambient descendant deletion and fallback code.
- OBL-0209-4 covers descriptor/scratch substitution, no outside-byte mutation,
  exact reference fences, aggregate ceilings and all crash/retry edges.
- Cargo metadata and lockfile comparison prove no dependency or feature change.
- ADR index, requirement, obligation, size, panic, allowed-path and scoped
  acceptance checks remain required; the record body remains at most 200 lines.
