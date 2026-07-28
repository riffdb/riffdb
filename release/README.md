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

RiffDB is offered under either the MIT License or the Apache License, Version
2.0, at your option. The release verifier requires matching workspace metadata
and both root license texts before it can publish a bundle.
