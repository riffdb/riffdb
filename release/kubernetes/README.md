# Historical Kubernetes remote-alpha fixture

`riffdb.yaml` is retained as the historical manifest fixture accepted with the
first remote-alpha transport proof. It is not the current NET-010 proof, a
supported operator installation surface, or controlled-cluster evidence. Use
the bounded chart under `release/helm/riffdb`, its committed golden render, and
`scripts/check-helm-operator-package` for current Kubernetes packaging.

The fixture records one stateful RiffDB pod, one TCP pass-through proxy, and
separate application/operator proof Jobs. Only the RiffDB pod mounts data,
backup storage, digest keys, or the TLS private key. The proof Jobs receive
disjoint client Secrets and no database volume. Do not apply it as an operator
procedure.

Its frozen Secret inventory is:

- `riffdb-server-secrets`: `server-chain.pem`, `server-key.pem`,
  `riffdb-ca.pem`, `capability.keys`, `idempotency.keys`, and the separately
  issued least-authority `readiness.credential`;
- `riffdb-application-client`: `application.toml`,
  `application.credential`, and `riffdb-ca.pem`;
- `riffdb-operator-client`: `operator.toml`, `operator.credential`, and
  `riffdb-ca.pem`.

The historical certificate included both `riffdbd.riffdb-alpha.svc` and the
literal-loopback bootstrap identity. The current chart instead fixes all
chart-owned traffic to its release Service DNS name, requires separately
provisioned authority, disables ambient service-account tokens, and refuses
unsafe values before rendering. Follow the
[Helm operations runbook](../../docs/operations/HELM-OPERATIONS.md).
