# RiffDB Helm chart

Deploys `riffdbd`, its TLS pass-through proxy, and the storage an operator has
to reason about. Rendered output is committed under `rendered/` and checked by
`./scripts/check-helm-render`.

## Why this exists alongside `release/kubernetes/riffdb.yaml`

That manifest is a retained **historical acceptance fixture**. It hardcodes
`riffdb:remote-alpha`, pins one namespace, states no resource requests or
limits, and bundles two proof Jobs with the workload. It is not the current
NET-010 proof and is not something an operator installs or upgrades. The chart
and its parsed-object checks own the repository-local packaging proof.

This chart deploys the same workload with the parts an operator must set
exposed as values, and makes the proof Jobs opt-in
(`acceptanceJobs.enabled`, default `false`).

## What is deliberately not configurable

`docs/known-limitations.md` records that `riffdbd` exposes only the documented
POC configuration fields, with no network metrics listener, storage-engine
selector, general tuning surface, or secret value in TOML. The chart adds no
knob beyond that surface. In particular:

- **No secret values are templated.** Certificate chain, private key,
  capability keys, idempotency keys, readiness credential, and probe trust
  root come from a Kubernetes Secret named by `server.secretName`. The chart
  never renders their contents, and an init container copies them into a
  memory-backed volume with the restricted modes `riffdbd` requires.
- **TLS terminates at `riffdbd`, not at the proxy.** The proxy is pass-through,
  so no private key reaches it.
- **`server.replicas` is not horizontal scaling.** `riffdbd` is a single-writer
  engine; a second replica is a second server against a second volume, not a
  shard or a replica set.

## Version binding

`image.tag` is empty by default and resolves to the chart's `appVersion`, so a
chart release cannot silently deploy a different RiffDB release than it
declares. WP-728 binds both to the tag it publishes.

The proxy image is digest-pinned, matching the acceptance manifest.

## Storage

`server.persistence.data` becomes the StatefulSet's volume claim template.
`server.persistence.backups` is a separate claim carrying
`helm.sh/resource-policy: keep`, because backups must outlive the release —
uninstalling a release that deleted its own backups would make
restore-after-disk-loss untestable, and END-001 requires that drill.

Set `storageClassName` explicitly in production. The default empty value takes
the cluster default, which is convenient for a first install and wrong for
anything whose durability you care about.

## Resource requests

The acceptance manifest states none. This chart sets modest requests and a
memory limit, because an unbounded database pod is the first thing evicted
under node pressure. Raise them against your own workload — the defaults are a
floor that lets a small cluster schedule the pod, not a sizing recommendation.

No CPU limit is set. Throttling a database's CPU produces latency behaviour
that looks like a RiffDB problem and is not one.

## Rendering and review

```sh
helm lint release/helm/riffdb
./scripts/check-helm-render            # verify against the committed golden
./scripts/check-helm-render --write    # after reviewing a deliberate change
```

The golden exists so a values or template edit is reviewable as a manifest
diff. Reviewing the template alone reviews the source of a program whose output
an operator runs; the golden reviews the output.

`./scripts/check-helm-operator-package` parses both the default and opt-in
renders. The opt-in render contains exactly one stable-application health Job
and one operator health Job, each using its own pre-created external Secret,
service DNS, the chart-bound image, and a protected memory volume. No Job or
proof Secret reference appears in the default render.

## Local lifecycle rehearsal

`./scripts/check-helm-upgrade-rehearsal` packages the chart under two reserved
`0.0.0-wp727.local-*` identities in temporary storage. It renders install,
upgrade, and rollback with frozen values, parses their objects, and proves
stable release, namespace, workload, PVC, and Secret identity while only the
declared image, CPU request, proxy replica, and derived metadata move.

This is deliberately offline, non-release evidence. It contacts no registry,
cluster, or database and makes no publication, execution, storage compatibility,
or durable rollback claim. Genuine two-published-version execution evidence is
owned by WP-783.

## Process-level startup boundary

`helm lint` and `helm template` prove the chart renders. They prove nothing
about whether an API server accepts the objects or a cluster starts the pods.
WP-727 makes no controlled-cluster execution claim. The bounded
`scripts/check-container-startup` check instead starts the release image with a
read-only root filesystem, uid 65532, and only the documented volumes, and
requires the server's ready marker.

That process-level check found a defect the render checks could not.

**`projections_root` defaulted under a read-only path.** It falls back to
`<cwd>/projections`, the image's `WORKDIR` is `/var/lib/riffdb`, and the pod
mounts only `data` and `backups` under `readOnlyRootFilesystem: true`. The
exact-text runtime could not open, and process graph construction refused. The
chart now sets `projections_root` inside the data volume.

`release/container/compose.yaml` and `release/kubernetes/riffdb.yaml` have the
same shape — `read_only: true`, only `data` and `backups` mounted, no
`projections_root` configured — so this is not a chart-only problem. Both need
the same fix, and `remote-kubernetes-render-check` cannot catch it because it
parses YAML rather than starting anything.

The probes also need `tls_trust_root` and `tls_server_name`, which the first
draft of this chart omitted. Without them the probe fails
`configuration_invalid` and the pod never reports ready while the server is
listening perfectly well.

## Operator procedure

The [Helm operations runbook](../../docs/operations/HELM-OPERATIONS.md) covers
install, controlled-network restriction, authenticated readiness, drain,
verified backup, closed-database storage preflight, chart upgrade, certificate
and capability rotation, restore, safe deployment rollback, and durable
downgrade refusal.
