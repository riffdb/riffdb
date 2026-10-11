# WP-825 latest-main identifier reconciliation

Main revision `2f25216a` adds the accepted bounded native outbox claim-group
decision as ADR-0284 and closes WP-832. The WP-825 branch already uses ADR-0284
for the separately accepted bootstrap source-custody decision. Both decisions
must survive the merge.

The concrete proposal moves only the bootstrap decision to
`adr/0288-bounded-native-bootstrap-source-custody-validation.md`,
updates its three obligation identifiers and WP-825 requirement/path metadata,
and repairs the existing implementation-review link. The historical acceptance
metadata, requirement identifiers, proof functions, review triggers and every
byte of its Decision section remain unchanged. The prior ADR-0284 source path
stays in WP-825's allowed paths for the rename deletion; the ADR-0288 source
path is added for the new filename. Historic review filenames remain stable.

The accepted outbox decision from main remains ADR-0284 and is unchanged.
ADR-0285 is the separately accepted private restore-custody decision, whose
implementation remains pending. ADR-0287 remains proposed and unimplemented.
This reconciliation changes no
runtime semantics, public interface, format, fixture, authorization, ordering,
validation rule or deadline.

Human review is requested because AGENTS.md requires review of accepted
guarantee records and additions to package paths. The renumbering is prepared
for review and has not been merged into the campaign branch. Index generation,
main-merge conflict resolution and combined-source acceptance will run on C3D
after identifier review.

## Full-branch scope verification

The proposed integration registers the already reviewed WP-825 path scope on
pinned main before merging the campaign branch. The registration changes only
WP-825's `allowed_paths` field; every other main package field stays unchanged.
The concrete registration is prepared, uncommitted, in the sibling
`riffdb-wp825-main2f-scope-registration` worktree. It includes the new ADR-0288
path and retains the old bootstrap ADR-0284 path for the rename deletion.

This ordering is needed because `check-allowed-paths` reads scope at the range's
base and rejects self-widening. Using the campaign branch as that base would also
include unrelated incoming WP-832 changes. Instead, the full branch diff against
main must have exactly the same path list as the diff against the registration.
The final CI plan must cover that complete list, add the registered package scope
check, and run full acceptance once. This changes no qualification requirement.

Neither the registration nor the ADR rename has been committed or merged into
the campaign branch. Human identifier/path review remains pending.

## Acceptance

The maintainer accepted this identifier and path reconciliation in session on
2026-10-11: "I accept". The bootstrap Decision section remains byte-identical.
The same response accepts the separate exact ADR-0285 proposal; its record path
is registered alongside this scope and its body is preserved unchanged.

## Native claim-group integration consequence

The original automatic Rust merge gives claim groups a route through the broad
server storage cell, while existing dispatcher transitions use the independently
owned native outbox port. That would retain the broad cell during native writer
or checkpoint admission. The restricted port also inherits the default absence
of claim-group support instead of forwarding the accepted native capability.

Two C3D regressions reproduce the defects before the correction: a real native
claim group through the owned dispatcher port lacks support, and a production
server claim group cannot finish while an explicit barrier holds the broad cell.
The latter releases the barrier and joins both threads before asserting; its
watchdog grants no timing or workload qualification. The correction forwards the
same native atomic claim operation through the existing dispatcher owner and
preserves shared poison refusal, exact event authority, FIFO admission, durable
receipts and read-only authority. No fixture, encoding or transaction changes.

The corrected source passes five native claim-group checks, including actual
process-crash redelivery, and five production server outbox checks. C3D also
passes workspace formatting and the three-file native scope check. These
focused results do not replace combined-source full CI or measured recovery.
