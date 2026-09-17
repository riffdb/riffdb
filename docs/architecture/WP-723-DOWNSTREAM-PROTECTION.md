# WP-723 adapter compatibility ownership

Package: WP-723. Tier: internal. WP-569 is complete in the inherited main history.

## Decision and behavior

The maintainer directed on 2026-09-17 that RiffDB must not depend on sample
adapter implementations. Core builds, tests, releases, and merge protection
therefore require no external adapter repository. This supersedes WP-723's
original blocking downstream objective. First-party SDK, compiler, runtime,
and installed-package conformance remain core responsibilities.

All four repositories are public under the `riffdb` organization. Pending
adapter implementation work was committed before publication. Each adapter
now owns `.github/workflows/riffdb-compatibility.yml` and its exact upstream
pin in `.github/riffdb-revision`:

- [OpenFGA](https://github.com/riffdb/riffdb-openfga/commit/4224898)
- [Better Auth](https://github.com/riffdb/riffdb-better-auth/commit/d952cee),
  including its materialized administrative profile
- [MLflow](https://github.com/riffdb/riffdb-mlflow/commit/f608a36)

Push and pull-request checks build the pinned RiffDB CLI and check application
sources, locks, and generated artifacts. Scheduled checks track upstream main;
manual dispatch permits an explicit revision. Failures belong to the adapter
repository and cannot block RiffDB. These checks prove application compilation,
not complete framework behavior or published driver release conformance.

Core-owned adapter pins, fetch/check scripts, the downstream CI job, and
acceptance hooks are removed. GitHub main protection retains the existing core
checks, strict up-to-date enforcement, and administrator enforcement, with no
Downstream adapters requirement. [CONTRIBUTING.md](../CONTRIBUTING.md#external-adapter-compatibility)
documents adapter upgrades. The old AGENTS.md checker reference is superseded
by the maintainer's explicit direction and is no longer an executable gate.
The sealed, inert WP-579 definition also retains historical downstream wording;
it must be reconciled with this direction when that frozen package is reopened.

## CI corrections

The first hosted runs exposed independent stale packaging and tooling checks:

- The Python sdist omitted the shared driver host and operation registry.
  Its staged sources and exact archive inventory now cover the current closure.
  Runtime checks use a private scratch directory; hosted wheel builds create
  the artifact directory before the container writes its files.
- Installed Go package tests expected driver protocol 3 despite the accepted
  version topology requiring 4. The current artifact passes; an artifact
  altered back to protocol 3 is rejected by the regression test.
- Native registry acceptance uses portable grep for its literal checks.
- Generated handbook diagrams install Graphviz in their CI job.
- Bootstrap checks require the exact six-component fresh-primary health state,
  including healthy replication with zero registered followers. Unknown,
  missing, duplicated, or unhealthy required components remain failures.
- The fuzz lockfile adopts the main workspace's patched h2 0.4.16 and rustls
  0.23.45, and reconciles existing manifest dependencies. Dependency policy and
  security audit remain enforced; no advisory is ignored.
- Core workflows run on pull requests, main pushes, tags, and manual dispatch,
  avoiding duplicate branch-push and pull-request runs for the same change.

## Validation and limits

Local verification includes Python wheel/sdist inventory, reproducibility and
offline installation, all 17 Python runtime tests and strict type checks;
real-process source bootstrap smoke; positive and negative Go package arrival;
workflow syntax; fuzz dependency policy and security audit; and package acceptance.
Hosted results and final closure are recorded after the corrected head runs.

The adapter commits preserve their existing framework-test limitations:
Better Auth's dev.16 registry packages were unavailable locally, and MLflow's
full upstream conformance target remains unimplemented. Neither limitation is
misrepresented by the application compilation checks.

The [original implementation](https://github.com/riffdb/riffdb/commit/91d13095f1e8eccc08d9da1ffb5048f4f7a6257e)
retains historical evidence for the superseded downstream gate.
