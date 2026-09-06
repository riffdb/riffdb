# Helm Operations

The RiffDB Helm chart is the supported Kubernetes packaging surface for the
proof of concept. It assembles existing safe binaries and references Secrets
that an operator provisions separately. It does not create credentials,
capabilities, application installations, or any other authority.

The chart is pre-alpha and is restricted to a controlled design-partner network.
Network policy, ingress allowlists, certificate issuance, Secret
provisioning, backup export, and storage-class behavior remain the operator's
responsibility.

## Install

Create the target namespace and the external server Secret before installing.
The Secret must contain `server-chain.pem`, `server-key.pem`,
`capability.keys`, `idempotency.keys`, `riffdb-ca.pem`, and
`readiness.credential`, all from the same reviewed deployment. Keep Secret
values out of values files, rendered manifests, shell history, and logs.
The application and operator proof Secrets, when enabled, must be nonempty and
pairwise distinct from each other and from the server Secret. The chart refuses
an overlapping name and projects only `credential` and `ca.pem` into each proof
pod.

Review a pinned values file and render it before applying anything:

```bash
helm lint release/helm/riffdb
helm template riffdb release/helm/riffdb \
  --namespace riffdb-alpha --values /absolute/path/to/reviewed-values.yaml
helm upgrade --install riffdb release/helm/riffdb \
  --namespace riffdb-alpha --values /absolute/path/to/reviewed-values.yaml
```

An empty `image.tag` deliberately resolves to the chart `appVersion`. A
nonempty tag must equal that exact `appVersion`; the chart refuses any other
value. Never use a mutable tag. The data claim belongs to the StatefulSet and
the backup claim has Helm's keep policy, so neither is disposable release
state.

## Controlled network

Permit only reviewed design-partner sources to reach the TLS listener or the
pass-through proxy. Every chart-owned probe and proof Job uses the exact
`<release>.<namespace>.svc` identity; there is no values override for another
endpoint, host networking, or a loopback shortcut. Refuse public or untrusted
ingress at infrastructure boundaries. RiffDB authentication and authorization
remain mandatory inside that restricted network; network location is not
authority.

## Authenticated readiness

Liveness is the closed, payload-free pre-bootstrap health check and carries no
credential or database selector. Readiness uses `readiness.credential` copied
from the server Secret into the size-bounded memory-backed protected volume.
Its exec gate exits successfully only for the canonical typed `ready` response;
authenticated `not_ready`, `degraded`, malformed, or failed responses keep the
pod out of Service routing. A TLS-only or process-alive result does not satisfy
readiness.

The optional proof Jobs are disabled by default. When explicitly enabled, the
operator must first provision distinct `applicationSecretName` and
`operatorSecretName` Secrets, each containing `credential` and `ca.pem`. The
Jobs execute only `server health`, from separate pods, through service DNS.
They do not install an application or mint authority.

## Drain

Stop new application, MCP, and administrative submissions at ingress, then
terminate the StatefulSet pod normally. `riffdbd` stops admission and performs
its bounded internal drain on SIGTERM. Require a clean container exit before
closed-database work; never use a forced deletion as an upgrade step.

## Verified backup

Before the drain, create a named backup through the authenticated public
maintenance API and retain its operation ID:

```bash
riffdb --config /run/operator/client.toml backup create before-upgrade
riffdb --config /run/operator/client.toml backup operation OPERATION_UUIDV7
```

Proceed only after terminal success. Preserve the immutable backup and verify
its manifest as described in [Backup and Restore](../backup-restore.md). Do not
readmit writes after taking the upgrade backup. Copy retained backups outside
the cluster failure domain according to local recovery policy.

## Storage preflight

With every RiffDB pod stopped and the database volume mounted exclusively into
a controlled maintenance environment, run the target binary's source-free
decision:

```bash
riffdb storage preflight --database-path /var/lib/riffdb/data/riffdb.redb
```

`ready` permits target startup. `upgrade_required` permits only the exact
manifest-authorized `riffdb storage upgrade` action printed by the release,
using the verified pre-upgrade backup. Any other result is a refusal. Do not
edit a marker, receipt, database, or journal to bypass it.

## Chart upgrade

Pin and verify the target chart archive and image, render the target with the
reviewed values, compare the objects and Secret/PVC references, take the
verified backup, drain, and complete Storage preflight. Only then run:

```bash
helm upgrade riffdb /absolute/path/to/riffdb-TARGET.tgz \
  --namespace riffdb-alpha --values /absolute/path/to/reviewed-values.yaml
```

Require complete startup validation, authenticated readiness, and the bounded
application smoke before reopening ingress. The repository-local
`scripts/check-helm-upgrade-rehearsal` packages reserved local identities and
renders object transitions only; it does not prove or claim published release
artifacts, Kubernetes execution, durable compatibility, or database rollback.

## Certificate rotation

Issue a successor certificate for the exact configured service DNS name. Update
the external server Secret atomically with the successor chain/key and the
needed trust overlap, then perform a bounded rolling restart. Prove TLS and
authenticated readiness through service DNS before retiring the predecessor
certificate or trust root. Never place certificate private keys in chart
values.

## Credential rotation

Create a least-authority successor capability through the existing authorized
capability surface and write its bearer only to the appropriate external
Secret. Use the **successor, prove, switch, revoke** sequence: prove the
successor from its separate workload, switch the Secret consumer, confirm
authenticated readiness or application health, and only then revoke the
predecessor. Rotate application, operator, and readiness principals
independently; the chart never creates their authority.

## Restore

Restoration is a destructive history replacement, not a chart rollback. Stop
all database pods, select a previously verified immutable backup, and follow
[Backup and Restore](../backup-restore.md). For a nonempty target the public
operation requires `--confirm-replace-current-database`. Require full startup
validation and authenticated readiness before reopening ingress.

## Deployment rollback

Before the candidate binary has accessed storage, `helm rollback` may restore
the predecessor deployment objects. After candidate storage access, do not
assume that object rollback makes the database compatible. Follow the durable
format manifest: retry forward with the target release, or deliberately perform
the documented destructive Restore from the verified pre-upgrade backup.

## Durable downgrade refusal

Helm history is not a database-format history. An unsupported downgrade remains
refused even if Kubernetes can render or start an older container. Never force,
reset, hand-edit, or delete durable format evidence. If no manifest-authorized
edge exists, preserve the stopped volume and backup and request human review.
