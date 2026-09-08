---
adr: "0221"
title: Atomic No-Replace Columnar Candidate Retirement
status: proposed
tier: guarantee
date: 2026-09-08
accepted: null
requires: [ADR-0200, ADR-0209]
amends: [ADR-0209]
supersedes: []
requirements: [REC-001, PRJ-004, PRJ-006, PRJ-008, PRJ-009, PRJ-010]
packages: [WP-757]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - The rustix version, default-feature posture, or exact fs/std feature set would change.
  - The dependency would be used outside ADR-0209's existing authorized columnar collision paths or expose a public interface.
  - Atomic NOREPLACE would be replaced by std::fs::rename, an absence precheck, unsafe code, direct FFI, an external process, copy/delete, or another fallback.
  - Destination-exists, unsupported-platform, syscall, path, descriptor, or synchronization failure would not fail closed without changing P or Q.
  - Any collision bound, reference proof, path derivation, cleanup, storage format, control transition, transaction, publication, or acknowledgement semantic would change.
---
# ADR-0221: Atomic No-Replace Columnar Candidate Retirement

## Context

ADR-0209 Decision 8 requires each exact candidate directory `P` to be renamed
atomically without replacement to an absent quarantine directory `Q`. An
absence check followed by `std::fs::rename` cannot prove that guarantee: a
concurrent destination can appear between the check and rename, and ordinary
rename may replace a file, symlink, or empty directory. Treating that window as
impossible would weaken OBL-0209-4's explicit race and unknown-outcome refusal.

The workspace lock already contains `rustix` 1.1.4 and another first-party
crate pins it with only `fs` and `std`. Its safe filesystem API exposes the
operating-system no-replacement rename primitive, but `riffdb-columnar` cannot
use a transitive dependency and ADR-0209 Decision 10 does not authorize its
`Cargo.toml` or the resulting lockfile dependency-edge update. Implementation
must stop until those exact paths and that exact purpose are accepted.

## Decision

1. ADR-0209 Decision 10 is amended only to add
   `crates/riffdb-columnar/Cargo.toml` and `Cargo.lock` to WP-757 production
   authority for Decisions 2 through 6 below. No other path, package,
   dependency, feature, or purpose is authorized.

2. `riffdb-columnar` may add exactly this direct dependency:
   `rustix = { version = "=1.1.4", default-features = false, features =
   ["fs", "std"] }`. `Cargo.lock` may change only as Cargo's deterministic
   record of that direct `riffdb-columnar` dependency edge; the already-locked
   rustix package, version, checksum, and transitive graph do not rotate.

3. The dependency is used only inside ADR-0209's already-authorized collision
   implementation to rename the exact validated final or `.tmp` candidate
   basename `P` to its exact quarantine basename `Q` in the same already-opened
   parent directory. The operation is exactly
   `rustix::fs::renameat_with(..., RenameFlags::NOREPLACE)`. It adds no path
   scan, caller-selected name, ambient destination, adoption, relabelling,
   copy, or deletion authority.

4. `P` and `Q` retain ADR-0209's exact derivation, expected-directory-kind
   checks, control and captured-view reference proof, aggregate bounds,
   quarantine removal, and parent synchronization protocol. A destination
   created before or during the rename produces a closed destination-exists
   result and leaves both paths unchanged. Invalid descriptors or names,
   unsupported syscall/platform behavior, I/O failure, and any unknown result
   also fail closed without constructing, adopting, deleting, or inferring a
   candidate. Successful rename is followed by the already-required parent
   `sync_all` before construction can proceed.

5. No `std::fs::rename` or absence-precheck substitution is a compatible
   fallback. No unsafe block, direct libc or kernel FFI, external command,
   build script, platform-specific application branch, or broader rustix
   feature is authorized. A target unable to provide the safe no-replacement
   operation refuses the collision gate rather than weakening it.

6. This amendment changes no durable or wire byte, storage key or table,
   public interface, application behavior, control state or transition,
   transaction ordering, generation identity, collision ceiling, retention,
   publication, acknowledgement, or recovery inference. It authorizes the
   mechanism required to implement the already-accepted guarantee, not a new
   guarantee or filesystem authority.

7. OBL-0209-4 remains the sole obligation and keeps its exact proof
   `epoch_two_unprepared_v2_retires_exact_candidate_paths` and exact `says`
   text. No duplicate obligation or narrower replacement proof is added.

8. This proposal changes only this record and the generated ADR index. It
   changes no accepted ADR, work package, governance rule, manifest,
   dependency, lockfile, implementation, test, runtime behavior, artifact, or
   external state before exact human acceptance.

## Options considered

1. **Use `std::fs::rename` after checking Q is absent:** rejected because the
   check and rename are not one operation and an intervening Q may be replaced.
2. **Call the platform API through local FFI:** rejected because first-party
   crates forbid unsafe code and a local binding would duplicate mature
   platform handling.
3. **Copy, link, reserve, or move through another directory:** rejected because
   none is the accepted atomic exact `P`-to-`Q` directory rename and each adds
   new crash states or path authority.
4. **Add the pinned safe rustix filesystem surface:** chosen because it exposes
   the exact no-replacement primitive already required by ADR-0209 without a
   new package version or broad dependency feature set.

## Consequences

- WP-757 can make a destination race a proved refusal rather than a destructive
  replacement window.
- `riffdb-columnar` gains one direct pinned dependency edge and the lockfile
  records that ownership explicitly.
- All collision bounds, reference proofs, crash edges, abandoned-material
  changes, and direct-V2 control work remain implementation tasks under
  ADR-0209; this record proves none of them by itself.

## Standing design tests

- **Interface safety:** no application, agent, operator, transport,
  configuration, or caller can request a rename, choose P or Q, select a
  platform fallback, or weaken no-replacement behavior.
- **Scale:** the syscall adds no scan or retained collection; ADR-0209's exact
  aggregate file, byte, directory, replay, lane, and scratch ceilings remain
  unchanged and are computed before traversal or mutation.

## Checks

- `epoch_two_unprepared_v2_retires_exact_candidate_paths` remains the exact
  OBL-0209-4 proof and must include a deterministic destination-race refusal
  showing byte-exact P and Q preservation.
- Architecture evidence rejects `std::fs::rename`, unsafe/FFI, external-command,
  and fallback implementations in the collision path and confines the rustix
  use to the exact authorized module.
- Cargo metadata and the lockfile diff prove the exact direct pin, disabled
  default features, `fs`/`std` only, and no package/checksum/transitive rotation.
- Scoped WP-757 acceptance, `cargo deny check`, file-size, panic, requirement,
  ADR-obligation, and allowed-path checks remain required for implementation.
