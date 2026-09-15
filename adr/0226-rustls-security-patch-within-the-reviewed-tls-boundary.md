---
adr: "0226"
title: Rustls Security Patch Within The Reviewed TLS Boundary
status: accepted
tier: guarantee
date: "2026-09-14"
accepted: "2026-09-14"
acceptance: "maintainer, in session, 2026-09-14"
requires: [ADR-0105]
amends: [ADR-0105]
supersedes: []
requirements: [NET-001, NET-002, NET-003, NET-005]
packages: [WP-772]
obligations:
  - id: OBL-0226-1
    package: WP-772
    proof: lockfile_has_one_exact_ring_tls_stack_and_no_alternative_or_compression_stack
    says: The exact patched rustls and webpki versions retain one ring-backed TLS closure,
      with no alternative provider, compression stack or unrelated dependency drift.
  - id: OBL-0226-2
    package: WP-772
    proof: scripts/ci-all
    says: The patched dependency closure passes the unmodified advisory gate and all
      repository checks before WP-772 closes or merges.
review_triggers:
  - Any package other than rustls or rustls-webpki would change version, checksum or dependency edges.
  - A different provider, enabled feature, native dependency or first-party unsafe code would be introduced.
  - Certificate, peer-name, ALPN, transport bounds, credentials or public error guarantees would weaken.
  - A security advisory would be ignored, downgraded or bypassed to permit closure.
---
# ADR-0226: Rustls Security Patch Within The Reviewed TLS Boundary

## Context

WP-772 full CI at `b6760385` fails `cargo deny check` on
RUSTSEC-2026-0285 for `rustls 0.23.43`, also pinned on main. The server proof
`lockfile_has_one_exact_ring_tls_stack_and_no_alternative_or_compression_stack`
requires that exact version. ADR-0105 and NET-005 require an explicitly reviewed,
version-pinned TLS closure; silently changing this architecture pin is not
storage-substrate implementation discretion. This is the failing gate underlying
the D-001 exception, not a new changelog design decision.

The [upstream advisory](https://github.com/rustls/rustls/security/advisories/GHSA-2mjx-qc3c-rqvc)
published 2026-09-14 affects 0.23.13 through 0.23.44 and identifies 0.23.45 as
patched. Complete pending TLS 1.3 handshake messages were accepted across a key
change instead of requiring record alignment. The transcript remains
authenticated; this is not evidence that an on-path attacker can forge a
completed handshake. The published 0.23.45 manifest additionally requires
`rustls-webpki >=0.103.14`, incompatible with the current exact 0.103.13 pins.

## Decision

1. Amend only ADR-0105's exact reviewed TLS dependency closure: advance
   `rustls 0.23.43 -> 0.23.45` and
   `rustls-webpki 0.103.13 -> 0.103.14`. Keep rustls transitive; do not add a
   first-party direct rustls dependency. Advance both existing webpki pins in
   the server and Rust client manifests, preserving their exact feature lists
   and default-feature exclusions. Update the corresponding exact architecture
   assertions, never remove or generalize them.

2. Resolve exactly one copy of each package. All other versions, checksums and
   dependency edges remain unchanged, including `ring 0.17.14`,
   `rustls-pki-types 1.15.1`, `tokio-rustls 0.26.4`, `tonic 0.14.6` and
   `base64 0.22.1`. Keep the existing ring-backed feature closure; no AWS-LC,
   OpenSSL, native-tls, compression, post-quantum provider or new native build
   requirement is authorized. Unexpected resolution or feature drift stops
   implementation for review rather than widening this permission.

3. Freeze the published crates.io archive SHA-256 checksums:
   - rustls 0.23.45:
     `0d41d731c7d2f962d1ccc364cec258de3c0e93b38c2fb3ba97ac74513048d634`;
   - rustls-webpki 0.103.14:
     `0527518605e68109d875e248ea259b6758801cf165e4b2c2733ae3b51f12535a`.
   The downloaded manifests preserve Rust 1.71 minimum and the existing
   Apache-2.0/ISC/MIT rustls and ISC webpki licenses. The rustls build script
   is byte-identical; webpki still declares no build script. Rustls retains
   `forbid(unsafe_code)`; this grants no first-party unsafe exception and does
   not expand the existing ring native-code permission.

4. The source comparison is not a claim of a one-line patch. Rustls changes
   the handshake deframer's alignment check to reject any pending message,
   complete or partial, at a key boundary. It also changes other handshake
   code and wraps consumed ring private-key input in the existing
   `zeroize::Zeroizing`. The webpki ring algorithm implementation is unchanged;
   its CRL iteration change preserves early error propagation. Both releases
   change disabled AWS-LC code/dependency requirements, and webpki moves ML-DSA
   exports behind its AWS-LC feature. Those features stay disabled and confer
   no new RiffDB algorithm or provider option. Full resolved graph, feature
   and transport regression checks remain mandatory after implementation.

5. Keep all accepted trust and interface guarantees: configured CA and exact
   peer-name verification, certificate validity, ALPN, bounded input/handshake/
   connection/drain behavior, redacted errors, capability authentication and
   current authorization. There is no cleartext fallback, caller-selectable
   TLS version/provider/cipher, trust-all mode or new listener profile.
   Command ordering, durability, acknowledgement, Journal V1, V3 receipts and
   supported application export do not change.

6. This proposal changes only ADR files and their generated index. It does not
   accept itself, update a pin, waive an advisory, close WP-772 or merge its
   worktree. The maintainer accepts the exact record in a separate commit with
   an `acceptance:` reference. After acceptance, a separate governance commit
   adds this record, its requirements, and the exact manifest/lockfile/test
   paths to WP-772 before its narrowly scoped dependency-fix commit. Record
   implementation choices in `Decisions:` and `decisions_taken:`.
   No other work package is inserted into the directed replication sequence.

7. Keep `deny.toml`, advisory severity and CI commands unchanged. Passing
   `cargo deny check` and `cargo audit` is required, not inferred from the
   version number. Rerun scoped acceptance, exact server/client architecture
   tests, transport verification/refusal tests, `scripts/recovery_full` and
   complete `scripts/ci-all`. This amendment does not approve the outstanding
   V3 codec fixture batch or substitute for its human review.

## Options considered

1. **Retain 0.23.43 or ignore the advisory:** rejected; it leaves the security
   gate failing and knowingly preserves the protocol-validation defect.
2. **Update rustls alone:** rejected; its published webpki requirement cannot
   satisfy the exact 0.103.13 application pins.
3. **The two exact patches within the existing ring closure:** chosen; it
   addresses the finding without changing provider or application authority.
4. **Broad dependency refresh or provider replacement:** deferred; neither is
   necessary or authorized by this amendment.

## Consequences

- The accepted target becomes an exact patched TLS closure, not a general
  automatic security-update exception.
- Transport implementation bytes change and require regression evidence; no
  claim of byte-identical TLS traffic or measured performance is made.
- WP-772 remains unmerged until all gates and outstanding fixture review pass.

## Standing design tests

- **Interface safety:** no application surface or configuration option is
  added. Malformed handshake acceptance is tightened; verification, capability
  authority, boundedness and redaction cannot be opted out of.
- **Scale:** there is no storage scan, state rewrite or co-location assumption.
  Existing connection and handshake limits remain in force. No throughput or
  latency improvement is claimed merely because the dependency is patched.

## Checks

- The two obligation proofs above, plus
  `production_transport_features_are_exact_default_disabled_and_confined`,
  `cryptography_is_nameable_only_by_reviewed_transport_manifests_and_server_source`,
  and client `reviewed_transport_and_entropy_graph_remains_exact`.
- Compare before/after lockfile package inventories and `cargo tree -e features`;
  the only version/checksum changes permitted are the two listed packages.
- Source/manifests/checksums were inspected while drafting. The patched runtime
  has not yet been built or tested in this workspace; those are acceptance-gated
  implementation checks, not evidence claimed by this proposal.
