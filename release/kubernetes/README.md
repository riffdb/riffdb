# Kubernetes remote alpha

`riffdb.yaml` is an intentionally closed deployment shape: one stateful RiffDB
pod, one TCP pass-through proxy, and separate application/operator proof jobs.
Only the RiffDB pod mounts data, backup storage, digest keys, or the TLS private
key. The proof jobs receive disjoint client Secrets and no database volume.

Before applying it, build and publish the `riffdb:remote-alpha` image, replace
that example tag with the exact published digest in every workload, and
create these Secrets in `riffdb-alpha`:

- `riffdb-server-secrets`: `server-chain.pem`, `server-key.pem`,
  `riffdb-ca.pem`, `capability.keys`, `idempotency.keys`, and the separately
  issued least-authority `readiness.credential`;
- `riffdb-application-client`: `application.toml`,
  `application.credential`, and `riffdb-ca.pem`;
- `riffdb-operator-client`: `operator.toml`, `operator.credential`, and
  `riffdb-ca.pem`.

The certificate DNS SAN must include both `riffdbd.riffdb-alpha.svc` and
`127.0.0.1`. Bootstrap remains local-only: after liveness starts, execute the
checked `capability bootstrap` request inside the `riffdbd` container against
`https://127.0.0.1:7443`, writing its bearer only to
`/run/riffdb-bootstrap`. Transfer that file through a protected operator
ceremony, use it over the TLS Service to issue the least-authority readiness
credential, update `riffdb-server-secrets`, and restart the pod. The Service
publishes the not-yet-ready address for those post-bootstrap authenticated
operations; protected application traffic must not be routed until readiness
succeeds. The init containers
copy Kubernetes Secret projections into a memory-backed owner-only directory,
because RiffDB rejects a group-readable private key or bearer file.

Replace Secrets by versioned rollout and atomic projected-volume publication.
Credential replacement still uses RiffDB's role-exact rotation command; a
Kubernetes Secret update is not revocation. Keep `terminationGracePeriodSeconds`
greater than the configured RiffDB drain bound.
