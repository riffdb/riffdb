# Versioning and Retirement

RiffDB does not have one global format version. A storage envelope, contract
bundle, gRPC package, MCP baseline, application lock, generated catalog, and
simulation trace solve different compatibility problems. Advancing one does
not imply that any other should advance.

The release-level map is
`release/version-topology-v1.json`. It records each release-significant
domain's owner, source assertion, complete readable and writable identities,
writer policy, compatibility rule, lifecycle, evidence, and last change
classification. `./scripts/check-version-topology` verifies that map and
cross-checks every durable reader/writer window against
`release/durable-format-manifest-v1.json`. The map is a review and release
control; it is not a runtime decoder, a negotiated super-version, or permission
to accept unsupported input.

## What changes require

Classify a version-affecting change at the boundary that owns it:

| Classification | Use it when | Required result |
|---|---|---|
| `no_format_change` | Implementation changes but canonical bytes and public meaning do not | Retain the identity and prove the existing fixtures |
| `additive_same_identity` | The accepted owner contract explicitly permits unknown/additive behavior | Add fixtures proving old and new peers retain their declared behavior |
| `new_domain_identity` | The boundary needs a new representation or meaning | Add the identity, reader/writer window, lifecycle, fixtures, and release note |
| `writer_transition` | A durable release in the same alpha epoch changes its writer | Keep declared readers and provide the exact backup-required, receipted, one-way upgrade |
| `breaking_epoch` | Existing durable state cannot be preserved by a supported same-epoch edge | Use the full backup and symbolic export/reimport ceremony; never reset in place |

Pre-1.0 SemVer permits a declared breaking minor release for public source or
API surfaces. It does not permit a silent break. The exact generated
application lock remains the application compatibility identity, while package
versions communicate release selection.

When a version changes, update the owning constant or schema, its frozen
fixtures, the topology record and classification, affected generated artifacts,
and release notes in one reviewed change. A durable change also updates the
durable-format manifest and release-pair evidence. Run:

```bash
./scripts/check-version-topology
./scripts/check-generated
./scripts/handbook check
```

## Reader, writer, and lifecycle rules

Readable and writable identities are explicit lists, not numeric ranges. A
smaller number is not evidence of compatibility. Writers use one of four
policies:

- `single_current`: all new artifacts use one identity;
- `least_sufficient`: use the oldest registered identity that represents the
  requested semantics without loss;
- `multi_current`: distinct closed artifact classes intentionally use distinct
  identities; or
- `external_baseline`: RiffDB implements one externally owned protocol
  identity.

An identity moves forward through `active`, `read_only`,
`retirement_candidate`, and `retired`. A newer identity does not automatically
deprecate an older decoder. WP-612 retires no decoder: every existing reader
stays active or read-only.

Before moving an identity to `retirement_candidate` or `retired`, one reviewed
change must prove all of the following:

1. no release in the supported window writes it;
2. at least one previously published release carried it as read-only and
   announced its future removal;
3. every supported upgrade, backup, application migration, or export/reimport
   route either consumes it or refuses before mutation;
4. frozen fixtures retain the last-readable behavior and new fixtures prove
   the typed refusal;
5. release notes name affected artifacts, the last reader, replacement,
   operator action, downtime, backup, and downgrade posture;
6. published tags, fields, enum values, hashes, and symbolic identities remain
   reserved forever; and
7. the topology, owning registry, generated artifacts, and release checks
   change atomically.

`retired` means the current binary deliberately refuses the identity. It never
means the historical fixture or identifier reservation can be deleted. An
urgent security retirement may skip the prior-release notice only through a
separate accepted ADR that identifies the threat and safe recovery path.

## Choose the correct transition

These mechanisms are intentionally different:

- **Application migration** changes one symbolic application from an exact
  parent contract to an exact successor through a checked migration bundle. It
  does not rewrite the database physical format.
- **Physical format upgrade** moves a database only along an exact
  durable-manifest release edge. For the current alpha, that is the
  backup-required one-way writer-0 to writer-1 transition.
- **Symbolic export/reimport** is the portability path across a breaking alpha
  epoch. It exports public symbolic state and replays compiled behavior into an
  empty new epoch; it is not a best-effort byte decoder.
- **Decoder retirement** removes current read support only after the evidence
  ceremony above. It neither migrates data nor authorizes a reset.

Unsupported input always fails at the boundary that owns it. There is no force,
ignore, reset, downgrade, raw-import, or best-effort option that can opt out of
these guarantees.
