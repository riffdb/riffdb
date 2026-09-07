---
adr: "0208"
title: Exact-Empty Command Authority Avoids Command-Audit Index Activation
status: proposed
tier: guarantee
date: 2026-09-06
accepted: null
requires: [ADR-0156, ADR-0165, ADR-0197, ADR-0205, ADR-0206]
amends: [ADR-0156, ADR-0205, ADR-0206]
supersedes: []
requirements: [OUT-001, OUT-002, TXN-042, REC-004, PERF-019]
packages: [WP-705]
obligations:
  - id: OBL-0208-1
    package: WP-705
    proof: production_grouped_command_arms_fresh_locator_coverage_without_manual_arm
    says: An exact command-empty clean start prepares and executes two audited production Group commands through shared ports while retained operational ports prove zero transient rebuilds and both novel-key inspections prove zero fallback scans.
  - id: OBL-0208-2
    package: WP-705
    proof: exact_empty_command_authority_avoids_command_audit_index_activation
    says: Architecture checks freeze the same-read-transaction exact-empty command-tail branch, complete physical administration validation, and unchanged derived-index validation for every nonempty history.
review_triggers:
  - Physical-only administration validation would use a certificate, allocator, cached frontier, different transaction, caller claim, or anything except exact `read_commit_tail(transaction) == Ok(None)`.
  - A nonempty, retained, uncertain, malformed, or failed command-tail observation would skip `indexed_command_audit`, warm or rebuild less strictly, or stop failing closed.
  - Administration allocation, stream continuity, physical key or record decoding, audit identity, command-owned audit validation, or corruption classification would change.
  - The production proof would prepare outside the reopened live ports, discard the ports needed for rebuild observations, bypass the real Group coordinator or its operational inspector, manually arm, or omit either command, rebuild assertion, or novel miss.
  - WP-705 would touch another path, or any public surface, durable byte, transaction, acknowledgement, publication, audit-cause, threshold, restart, bound, or evidence rule would change.
---
# ADR-0208: Exact-Empty Command Authority Avoids Command-Audit Index Activation

## Context

ADR-0206 requires the WP-705 production proof to prepare two audited commands
on ports reopened by verified bounded fast start while transient indexes remain
`Dormant`. Preparation first resolves the executable plan. That catalog read
validates the administration stream, and its per-record join unconditionally
calls `indexed_command_audit`; the call activates the command-derived index even
when the application command history is exactly empty. The proof is therefore
masked before idempotency inspection or the authorized arming edge runs.

An administration record can be command-owned only when it is atomic inside an
application command segment. The same read transaction proving that the exact
command tail is `None` therefore proves that every administration record must
be physical. This permits a narrower validation branch without a non-audited
test command, a test hook, an idempotency lookup reorder, or a weakened audit
join.

## Decision

1. ADR-0205 Decision 9 and ADR-0206 Decision 1 are amended only to add
   `crates/riffdb-storage-redb/src/administration.rs` to WP-705 implementation
   authority. The existing seven paths and their exact purposes remain
   unchanged. The added path is solely for Decisions 2 through 4; another path
   or purpose requires a separately accepted amendment.

2. `validate_administration_stream_readonly` reads
   `read_commit_tail(transaction)` exactly once before its administration loop,
   using the same already-open read transaction from which it reads the
   administration allocator and `AUDIT`. Only `Ok(None)` supplies
   `ExactNoCommandAuthority`. It is not inferred from a clean-close certificate,
   allocator, cached frontier, transient state, table count, startup result, or
   caller evidence. `Some`, including a retained-only watermark, does not
   supply it; any read, decode, retention, or corruption error propagates.

3. With that exact authority, `read_administration_record_readonly` does not
   call `indexed_command_audit`. It still reads the exact `AUDIT` key, decodes
   the complete physical record against the same transaction, and requires its
   administration sequence to match. A missing, malformed, wrong-sequence, or
   locator-shaped record fails under the existing corruption algebra. The
   validator still walks every sequence from first through the allocator-owned
   end and retains its overflow and continuity checks.

4. Without that authority, the existing physical/derived join remains exact:
   `indexed_command_audit` may activate the transient index, command-owned
   records remain resolvable, equal physical/derived records may agree, and a
   missing or conflicting pair fails closed. Tail-only validation, write-side
   administration validation, catalog selection, and query/reactive module
   validation otherwise do not change. ADR-0156 Decision 5 is narrowed only by
   this exact local proof that no command-derived audit can exist.

5. The production semantic proof creates an exact command-empty database,
   completes clean close, and reopens through verified bounded fast start. It
   retains `RedbOperationalPorts`, asserts fast-start classification and zero
   transient rebuilds, and prepares both audited commands against those same
   live ports. A small test-local constructor starts the public production
   coordinator with `CoordinatorDurability::Group` on
   `ports.shared_ports()`; it adds no production hook or scheduling control.

6. After each command completion the retained ports still report zero rebuilds.
   After each completion, a distinct exact-scope novel key is inspected through
   `command_idempotency_inspector`; it returns
   `CommandIdempotencyPlanSelection::Absent`, fallback-history scans remain
   zero, and rebuilds remain zero. The proof does not call repository admission,
   a storage batch, an arming method, a transient builder, or state-changing
   test control. Removing the authorized arming must make the first novel miss
   take the counted fallback while rebuilds remain zero.

7. The architecture proof freezes the one exact tail read before iteration,
   same-transaction binding, `None`-only branch, physical decode and sequence
   validation, unchanged nonempty derived join, error propagation, and absence
   of transient activation on the exact-empty branch. Existing nonempty
   clean-start catalog and command-audit tests continue proving that
   command-owned records warm and resolve and conflicting evidence is refused.

8. ADR-0197 coverage construction, locator and capsule validation, result
   algebra, private/public witnesses, publication, rebase, failure, restart,
   and bounds remain unchanged. ADR-0205 audit-cause preservation and threshold
   rules and ADR-0206 logical-table access remain unchanged. This record changes
   no transaction, mutation, durable or journal byte, ordering, acknowledgement,
   allocator, frontier, retention, startup gate, public interface, or evidence
   eligibility rule.

## Options considered

1. **Reorder operational idempotency lookup:** rejected; executable-plan
   resolution activates the index earlier, so lookup precedence cannot make the
   proof discriminating and would widen unrelated behavior.
2. **Add a non-audited command fixture:** rejected; WP-608 intentionally made
   production-shaped commands audited, and bypassing that lifecycle would prove
   a path RiffDB does not ship.
3. **Trust physical audits for every catalog read:** rejected; nonempty command
   histories contain command-owned audits only inside command segments and
   require the existing derived join.
4. **Use exact empty command authority:** chosen; it proves the derived half is
   impossible before skipping it and leaves every other history unchanged.

## Consequences

- An application-command-empty bounded start can resolve its catalog and
  prepare audited commands without surrendering dormant-index readiness.
- The exact-empty path adds one bounded command-tail probe before the existing
  administration walk; nonempty histories keep their current activation cost.
- General administration-stream acceleration, idempotency lookup precedence,
  non-audited command support, and changes to derived audit storage remain
  deferred.

## Standing design tests

- **Interface safety:** applications, agents, operators, and transports cannot
  select, forge, observe, or bypass this branch. It is derived privately from
  one authoritative transaction and adds no public method, option, or hook.
- **Scale:** exact emptiness reads only the bounded command tail and retention
  witness once. It retains no population state and does not add a second
  administration walk; nonempty behavior is unchanged.

## Checks

- `production_grouped_command_arms_fresh_locator_coverage_without_manual_arm`
  proves the retained-port/shared-coordinator lifecycle, two audited commands,
  zero rebuilds, and two covered operational misses.
- `exact_empty_command_authority_avoids_command_audit_index_activation` freezes
  exact authority, validation order, and the unchanged nonempty path.
- Existing `clean_close_fast_startup_warms_under_a_live_catalog_read`,
  `clean_close_fast_startup_resolves_command_owned_audit_members`, locator
  corruption, administration continuity, ADR-0197 state-machine, ADR-0205
  audit-threshold, and ADR-0206 arming proofs pass before evidence is eligible.
