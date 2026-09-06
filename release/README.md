# Release Layout

`scripts/release-poc --verify` builds a local release bundle from a clean,
accepted revision. It assembles and verifies the bundle in a private staging
directory and publishes `target/wp200/release` only after every release check
and the byte-for-byte archive comparison passes. The bundle contains exactly
three product binaries, a self-contained installed-binary demo, documentation,
systemd assets, examples, a CycloneDX SBOM, dependency inventory, requirement
evidence, and checksums. Before archiving, the verifier installs the staged
bundle into a private root, compares the required assets and modes, and runs
the installed-binary demo from that private layout.

The tracked `examples/budget-comparison` workspace is included without its
build output, together with its own locked dependency inventory and CycloneDX
SBOM. It retains repository-relative development dependencies and is reference
evidence in the binary bundle; execute it from the matching full source
revision named by `REVISION`. It remains evidence source, not a fourth product
binary. The isolated Fjall comparison also has its own dependency inventory and
SBOM; Fjall is not linked into any product binary.

Checked-in files under this directory are inputs and candidate evidence. They
are not a binary release and do not attest that POC exit passed.

`release/container/` contains the checked Compose remote-alpha shape, while
`release/helm/riffdb/` is the current supported Kubernetes packaging surface.
Their application workloads receive only generated client configuration, an
application credential, and the explicit TLS trust root; they never mount
RiffDB data, backup media, digest keys, TLS private keys, or an operator
credential. Use `scripts/remote-compose-acceptance`,
`scripts/check-helm-operator-package`, and
`scripts/check-helm-upgrade-rehearsal` rather than treating a successful YAML
parse as deployment evidence. `release/kubernetes/riffdb.yaml` and its render
checker are retained historical fixtures, not the current NET-010 proof or an
operator installation surface.

Every assembled release carries `release/durable-format-manifest-v1.json` and
its exact `release/compatibility/` fixture inventory and upgrade table. The
release verifier rejects a missing or stale format statement. This remains a
pre-alpha compatibility contract: downgrade is unsupported, and a breaking
epoch requires the declared application export/reimport ceremony rather than a
physical backup restore or destructive reset.

For a same-epoch mismatch, stop the service and run `riffdb storage preflight`.
Only a release-table edge may proceed through the backup-bound, checksummed,
restartable `riffdb storage upgrade` command. Release tooling exposes no force,
ignore, reset, best-effort decoder, or downgrade path.

The matching full source checkout also provides
`cargo riffdb install --user|--system` and the separate authoritative
`cargo riffdb bootstrap --user|--system [--register-codex]` convenience flow.
Those Cargo commands are source-checkout tooling and are not embedded in the
binary archive. Archive users follow `docs/installation.md`'s verified-bundle
procedure. The source installer must run as an unprivileged operator, never
under `sudo`; system scope performs protected destination inspection,
publication, account setup, and service management through `sudo` internally.
Because that helper is mutable checkout code, do not grant it a narrow
command-specific `sudoers` exception.

RiffDB is offered under either the MIT License or the Apache License, Version
2.0, at your option. The release verifier requires matching workspace metadata
plus the root ownership notice and both license texts before it can publish a
bundle.

Copyright © 2026 Kevin O'Shea and O'Shea & Sons, LLC.
