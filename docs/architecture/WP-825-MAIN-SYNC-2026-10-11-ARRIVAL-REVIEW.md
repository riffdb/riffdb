# WP-825 offered-load main synchronization and identifier review

Local main revision `13160dfa` accepts the workload-arrival correction as
ADR-0285. The campaign branch uses that number for the independently accepted
private restore-custody guarantee. Both accepted records must survive the merge.

The concrete proposal moves the restore record to ADR-0289 at
`adr/0289-owned-private-restore-validation-custody.md`.
Only its front-matter identifier and title identifier change. Every byte of its
Decision section, original acceptance, requirements, obligations, review triggers
and remaining body stays unchanged. The Decision section SHA-256 is
`6d2ce44a3523a6a76db00a6259011f58bb0864357113a714d03a6bb49bc3484c`.

WP-825's required records retain the restore decision under ADR-0289 and add
main's accepted ADR-0285. Its allowed paths add the new restore filename and keep
the old filename for the rename deletion. Active implementation references follow
ADR-0289. Historical acceptance and review filenames remain stable; the original
restore review links to this reconciliation. The generated ADR index will be
regenerated on C3D after integration, retaining both decisions. No fixture changes
are proposed. ADR-0287 remains proposed and unimplemented.

The arriving main changes spread each second's unchanged 100 commands and
400 reads over independently seeded, reproducible offsets. Operations count
against their offered window and latency starts at their own offer time. These
are main's already accepted semantics. Prior burst-shaped latency measurements
cannot qualify or compare with the new arrival shape; preparation and campaign
must use fresh final combined-source binaries. All existing recovery deadlines,
resource ceilings, durations, fault paths and free-space gates remain mandatory.

AGENTS.md requires human review of accepted guarantee records and changes to a
package's declared paths. Review is requested for this identifier and path
reconciliation only. It is prepared in an isolated worktree and has not been
merged into the campaign branch. The restore Decision text needs no semantic
change. Main's accepted ADR-0285 is unchanged.

PR note: Package WP-825 (open); Tier guarantee. Behavior: retain both accepted
records during main synchronization and use main's accepted offered-load shape.
Checks: byte equality of the restore Decision section was verified during
preparation; generated index, scope, handbook, native workload tests and combined
source acceptance will run on C3D after integration. Compatibility: no encoding,
wire field, fixture, authorization, ordering or public selector changes.
Updated handbook: existing backup/restore references follow the new identifier;
main updates the sustainable Ticketdesk architecture page. Hazards and follow-ups:
owned restore-custody integration, final combined-source CI, fresh preparation,
whole-action recovery and the actual rehearsal remain required. Neither package
is complete and neither long campaign has started.

## Acceptance

The maintainer approved the identifier-only reconciliation in session on
2026-10-11: "Approve the identifier-only reconciliation". This includes the
restore record rename, active references and package path additions. The
original restore Decision and acceptance text are unchanged. Main integration
and C3D checks follow this acceptance; campaign qualification remains open.


## Merge implementation and native checks

The probe merge retains WP-825's shared monotonic epoch and terminal hold while
using main's seeded arrival schedule, per-offer window attribution, bounded
settling and explicit late completions. Each offset waits against that same
campaign clock, including after process replacement; terminal totals are copied
before releasing the metrics mutex and entering the asynchronous hold. Existing
resume-confirmation tests now use the offered-window metrics interface.

Before this merge, native copied-artifact checks reproduced all four identity
replacement failures and passed all five corrected cases. Native backup, engine
and archive regressions, 59 architecture checks, process-crash recovery, Clippy,
package scope, handbook and formatting passed on the required host. The same
verified files are included here; active restore references follow ADR-0289.
Merged-source probe and lifecycle checks remain required before this merge is
qualified. Neither these checks nor the earlier scoped native acceptance qualify
the still-unfinished complete private restore factory or any actual campaign.
