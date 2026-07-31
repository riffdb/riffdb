# Contributing to the Handbook

`docs/SUMMARY.md` is the public handbook inventory. Public behavior must be
documented from the same pull request that changes it.

## Source of truth

| Subject | Authoritative source | Handbook responsibility |
|---|---|---|
| Normative semantics | `SPEC.md` and Accepted ADRs | Explain implemented behavior without weakening it |
| Work sequencing | `work_packages.yaml` | Keep historical package status out of primary user guidance |
| CLI | Clap definitions in `riffdb-cli` | Regenerate `docs/reference/CLI.md` |
| Architecture visuals | `diagrams/*.dot` | Regenerate checked SVGs |
| Public Rust API | `riffdb-client-rust` | Build Rustdoc into the site artifact |
| Contract and query syntax | Grammar, compiler, fixtures | Update language pages and runnable examples together |
| Installation and configuration | Installer scripts and config parsers | Keep commands, paths, defaults, and security notes exact |

Do not hand-edit `docs/reference/CLI.md` or `docs/assets/*.svg`. Regenerate with:

```bash
./scripts/generate-cli-reference --write
./scripts/generate-handbook-diagrams --write
```

## Local checks

Install the versions in `scripts/tool-versions`, then run:

```bash
./scripts/handbook check
```

The check validates source inventory, generated freshness, snippets, internal
links and fragments, external runtime assets, the mdBook build, and public
Rustdoc. Use `./scripts/handbook serve` for a local preview.

## Writing rules

- State the POC boundary when a reader could mistake a feature for production
  readiness.
- Use current public commands and symbolic names; do not teach internal IDs or
  storage access.
- Give images meaningful alternative text and keep tables usable on narrow
  screens.
- Link to one detailed source rather than copying long instructions into
  several pages.
- Mark non-executable illustrative code explicitly. Tested snippets must remain
  deterministic and require no external service unless the page says so.
- Put internal histories and work-package evidence in an excluded directory,
  not in the public learning path.

Every pull request records `Documentation impact:`. Explain the updated pages or
give a concrete reason the change cannot affect users, operators, application
authors, public interfaces, or compatibility.
