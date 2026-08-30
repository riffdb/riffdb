# RiffDB Helm chart

Deploys `riffdbd`, its TLS pass-through proxy, and the storage an operator has
to reason about. Rendered output is committed under `rendered/` and checked by
`./scripts/check-helm-render`.

## Why this exists alongside `release/kubernetes/riffdb.yaml`

That manifest is an **acceptance fixture**. It hardcodes `riffdb:remote-alpha`,
pins one namespace, states no resource requests or limits, and bundles two
proof Jobs with the workload. It proves NET-010's properties; it is not
something an operator installs and upgrades.

This chart deploys the same workload with the parts an operator must set
exposed as values, and makes the proof Jobs opt-in
(`acceptanceJobs.enabled`, default `false`).

## What is deliberately not configurable

`docs/known-limitations.md` records that `riffdbd` exposes only the documented
POC configuration fields, with no network metrics listener, storage-engine
selector, general tuning surface, or secret value in TOML. The chart adds no
knob beyond that surface. In particular:

- **No secret values are templated.** Certificate chain, private key,
  capability keys, and idempotency keys come from a Kubernetes Secret named by
  `server.secretName`. The chart never renders their contents, and an init
  container copies them into a memory-backed volume with the restricted modes
  `riffdbd` requires.
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

## Not yet covered

WP-727 also requires a documented upgrade between two released chart versions
including the durable-format compatibility check, a documented rollback, and an
operator runbook exercised against the chart rather than hand-written YAML.
Those need two published chart versions to exist, which depends on WP-728.
