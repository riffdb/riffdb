# WP-825 main synchronization: identifier reconciliation

Main now owns accepted ADR-0298 for the CI test battery under nextest plus
doctests. The accepted owned-backup decision on WP-825 therefore moves from
ADR-0298 to ADR-0299. Its Decision section and original acceptance metadata
remain byte-for-byte unchanged. Only the front-matter identifier and heading
change in the record itself.

The concrete proposed record is `adr/0299-native-restore-borrows-sealed-history-from-one-owned-backup.md`.
The implementation applies this rename and updates WP-825's required ADR and
allowed path, source comments and owned-backup review references. Historical
acceptance quotations and measured source revisions remain unchanged. The ADR
index is regenerated after the merge. Main's new ADR-0298, SPEC 1.31 and CI
nextest/doctest behavior remain intact. Reservations for records already present
on WP-825 are removed, preserving abandoned identifiers.

This is an identifier and registration reconciliation. No storage, custody,
recovery clock, public interface, fixture or acceptance gate changes.

Review is requested because AGENTS.md requires human acceptance and review of
guarantee records and review when a package needs paths outside its scope.
The new ADR filename must be added to WP-825's allowed paths before use.

The maintainer approved this identifier-only reconciliation in session on
2026-10-13. The original Decision and acceptance metadata remain unchanged.

C3D regenerated the index and passed the index and ADR-form checks. Checked
receipt: `620dff2c4ec0aaaaf01e744a3a6a9454227ecd6236e93470b0daa8d45f4f6f65`.
An initial isolated check omitted the historical ADR-form baseline; that
preparation failure remains preserved and was corrected by including the
unchanged baseline. It was not a repository form failure.

PR note: Package WP-825; Tier guarantee. Behavior integrates accepted upstream
CI changes and reconciles an identifier; database behavior and compatibility
remain unchanged. Checks: required-host index generation/check and ADR form;
full merged-source CI remains required. Handbook: CONTRIBUTING inherits main's
nextest/doctest instructions, SUMMARY links this review, and the owned-backup
review follows the reconciled identifier. Hazards: the latest shipping restore
still fails whole60, and campaign qualification remains outstanding.
