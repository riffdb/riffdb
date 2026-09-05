---
adr: "0202"
title: Bounded Helm Upgrade Rehearsal and Published Evidence Split
status: proposed
tier: surface
date: 2026-09-05
accepted: null
requires: [ADR-0055, ADR-0056, ADR-0105, ADR-0110, ADR-0112, ADR-0124]
amends: []
supersedes: []
requirements: [NET-010, NET-012, APE-002]
packages: [WP-727, WP-783]
obligations:
  - id: OBL-0202-1
    package: WP-727
    proof: scripts/check-helm-operator-package
    says: The chart's default render has no proof Jobs; its opt-in render has exactly one application and one operator Job using separate external Secrets, the chart-bound image, protected mounts, no host network or loopback, and no installation authority, while authenticated readiness, global bounds, retained backups, and the complete runbook remain structural and load-bearing.
  - id: OBL-0202-2
    package: WP-727
    proof: scripts/check-helm-upgrade-rehearsal
    says: A bounded offline rehearsal packages two explicitly non-release chart identities, preserves release/namespace/workload/PVC/Secret identity across install, upgrade, and rendered rollback, moves only declared values, and proves image.tag defaults to each appVersion without claiming Kubernetes execution, durable compatibility, or publication.
  - id: OBL-0202-3
    package: WP-783
    proof: scripts/check-published-helm-upgrade-evidence
    says: A strict validator cross-checks two distinct releases, chart archives, source revisions, and appVersion images against immutable signed non-draft WP-728 publication attestations and independently signed/provenanced cluster evidence from one reviewed harness/workflow; actual separate-pod stable-application access, payload-free liveness, controlled-design-partner-network restriction, preflight, backup, upgrade, readiness, proxy, rotation, drain, and safe rollback/refusal all have terminal result digests, while truthful common runbook and durable-manifest digests may match.
review_triggers:
  - The chart would create a Secret, capability, credential, application installation, host-network path, loopback shortcut, unbounded listener or workload setting, or unsafe storage action.
  - The local rehearsal would use a published-looking identity, contact a cluster or registry, mutate a database, or satisfy any published-release evidence field.
  - Published evidence would omit stable-application access from a separate pod, payload-free liveness, an allowed-partner/refused-untrusted network-boundary probe, durable preflight, backup, or safe rollback/refusal.
  - A hand-authored receipt, URL, mutable tag, draft release, repository-local claim, unsigned WP-728 publication record, or cluster result without a reviewed harness/workflow identity, external run anchor, terminal-result digests, signature, and provenance could satisfy evidence.
  - Distinctness would not cover release/chart versions, archives, source revisions, and intended appVersion images, or would incorrectly require truthful shared runbook or durable-manifest digests to differ.
  - Helm rollback would be presented as a durable database downgrade, or existing protocol, authorization, command, storage, durable-format, backup, or release-version semantics would change.
---
# ADR-0202: Bounded Helm Upgrade Rehearsal and Published Evidence Split

## Context

WP-727 already has a chart, golden render, and release-container startup proof,
but its advertised opt-in proof Jobs are not rendered and its operator runbook is
absent. Its remaining exit gate also requires an upgrade between two released
chart versions. No tagged RiffDB revision contains the chart: `poc-v0.1.0`
predates it. WP-728 owns publication and depends on WP-727, so making WP-727 wait
for two published charts creates a cycle; relabelling local packages as released
would create false evidence.

## Decision

1. WP-727 owns the repository-local operator surface. The chart gains exactly
   two opt-in, health-only proof Jobs: one stable-application principal and one
   operator principal. They run in separate pods, use the chart's appVersion-
   bound image, and consume separately named pre-created Secrets through a
   protected memory volume. The default render contains neither Job. The chart
   creates no Secret, bearer, capability, installation plan, or campaign and
   grants no application or MCP installation authority.

2. Chart traffic always uses service DNS through the configured TLS listener or
   pass-through proxy, never host networking, host ports, `localhost`, or
   `127.0.0.1`. Authenticated readiness names a protected credential file. The
   existing finite admission, connection, stream, query, command, probe, drain,
   resource, and volume bounds remain explicit. Backup storage retains its keep
   policy and secret bytes never enter rendered YAML, logs, or evidence.

3. WP-727 adds `scripts/check-helm-operator-package` and keeps the golden render
   load-bearing. The checker validates both default and opt-in renders, exact Job
   roles, external Secret references, protected mounts, image/version binding,
   no-loopback rules, bounded settings, authenticated probes, retained backups,
   and exact runbook operation markers. It runs from WP acceptance and `ci-all`;
   token or prose presence alone is not proof.

4. WP-727 also adds a bounded local Helm lifecycle rehearsal. In temporary
   storage it packages the reviewed chart twice under reserved
   `0.0.0-wp727.local-*` chart/app identities, renders install, upgrade, and
   rollback with frozen bounded values, and compares parsed object identity and
   the declared mutable fields. Those identities and archives are never placed
   under `release/`, tagged, signed, uploaded, or described as published. The
   rehearsal contacts no registry, cluster, or database and proves no process,
   storage-format, backup, release, or rollback execution.

5. The operator runbook covers install, controlled-network restriction,
   authenticated readiness, drain, verified backup, closed-database
   `riffdb storage preflight`, only the manifest-authorized upgrade action,
   chart upgrade, certificate and successor-then-revoke credential rotation,
   restore, and rollback. Helm rollback is never called a database downgrade.
   Before candidate storage access it may restore deployment objects; afterward
   the operator follows the durable manifest, retries forward, or performs the
   documented destructive restore from a verified pre-upgrade backup. An
   unsupported downgrade remains refused.

6. WP-727 may close when these repository-local mechanics and existing release-
   container checks pass. It does not depend on WP-728 or WP-783 and makes no
   published-chart upgrade claim. WP-728 may then publish its signed release
   artifacts without a dependency cycle.

7. WP-783 separately depends on both WP-727 and WP-728 and stays open until two
   distinct chart versions genuinely exist as immutable published artifacts.
   The receipt is an index, not authority. The checker verifies WP-728's signed,
   non-draft immutable publication attestation for each archive and a separately
   signed/provenanced cluster attestation produced by one reviewed harness and
   workflow, bound to their content hashes, external run identity, and terminal
   result digests. The cluster run includes separate-pod stable-application
   access through the proxy, closed payload-free liveness, authenticated
   readiness, an allowed-partner/refused-untrusted boundary probe, and every
   operation in OBL-0202-3. Publication and cluster evidence cross-bind exact
   inputs and results using WP-728's accepted verifier; hand-authored receipt
   fields or URLs grant nothing.

   Source and target must differ in release SemVer, chart SemVer, archive hash,
   source revision, appVersion, and appVersion-resolved image digest. Truthful
   common inputs may remain equal, including the exact runbook and durable-
   manifest digests. The bounded receipt contains no Secret, bearer, application
   data, database path, hostname, tenant, principal, or schema value. A local
   render, dry run, mutable tag, draft release, unsigned archive, or one release
   under two names cannot close WP-783.

8. This allocation changes deployment documentation, chart templates, and
   evidence custody only. It changes no application operation, MCP tool,
   installation authority, protocol, authorization, command, storage, durable
   format, backup semantics, release identity, or compatibility edge.

## Standing design tests

- **Interface safety:** the chart assembles existing safe binaries and external
  Secrets; it cannot mint authority or expose force, reset, raw mutation,
  downgrade, or installation through an application or MCP surface.
- **Scale:** every render, object set, Job count, values profile, rehearsal, and
  receipt is finite; runtime work retains existing global bounds and the alpha
  remains restricted to controlled design-partner networks.

## Checks

- `scripts/check-helm-operator-package` proves the chart and runbook boundary.
- `scripts/check-helm-upgrade-rehearsal` proves only local Helm mechanics.
- `scripts/check-published-helm-upgrade-evidence` accepts only the later genuine
  two-version execution receipt.
