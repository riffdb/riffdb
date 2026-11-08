---
adr: "0340"
title: Operational bootstrap presets, external deployment state, live driver credential reload, and specific file-permission diagnostics
status: accepted
tier: surface
date: 2026-11-08
accepted: 2026-11-08
acceptance: 'maintainer, in session, 2026-11-08: "yes, go ahead and do both, commit any WP/ADR to main immediately to claim the IDs, and then build in a worktree, merge when finished"'
requires: ["0121", "0124", "0304", "0305", "0308", "0328", "0329", "0330"]
amends: ["0304", "0305", "0328", "0329"]
supersedes: []
requirements: ["DX-006", "DX-007", "DRV-001", "DRV-003", "DRV-005", "VER-001"]
packages: ["WP-913"]
obligations:
  - id: OBL-0340-1
    package: WP-913
    description: "riffdb-driver-host DriverRuntime reloads protected bearer credentials from credential_file when the on-disk file changes so atomic role credential rotation takes effect without restarting riffdb-driverd"
    proof: live_credential_file_rotation_reloads_bearer_token_without_driver_restart
  - id: OBL-0340-2
    package: WP-913
    description: "riffdb application deploy supports --deployment-dir (and RIFFDB_DEPLOYMENT_DIR), --credential-output, --driver-config-output, and --driver-env-output to materialize deployment state, credentials, and driver-host configs outside the source checkout"
    proof: application_deploy_external_deployment_dir_and_driver_outputs
  - id: OBL-0340-3
    package: WP-913
    description: "riffdb capability bootstrap supports --operator with optional --principal and --lifetime-seconds and an automatic temporary request file when --generate is omitted"
    proof: capability_bootstrap_operator_preset_writes_bearer_without_manual_json
review_triggers:
  - "External deployment directory, driver config output, or live credential reload weakens 0600/0700 file mode, symlink refusal, or effective-UID ownership checks."
  - "riffdb capability bootstrap --operator bypasses offline single-process database custody or grants tenant/application data-plane scopes outside the standard operator control-plane permissions."
---
# ADR-0340: Operational bootstrap presets, external deployment state, live driver credential reload, and specific file-permission diagnostics

## Context

Deploying multi-database containerized applications and ecosystem adapters (`@riffdb/openfga`, `@riffdb/better-auth`, `riffdb-mlflow`, and custom Next.js/TypeScript services) on RiffDB `0.2.0` surfaced five operational friction points:

1. **Expired credentials require manual deletion and driver-host restarts:** `riffdb application deploy --provision-role <Role>` defaults `--lifetime-seconds` to `28800` (8 hours), and `riffdb-driverd` (`crates/riffdb-driver-host`) reads `credential_file` once at startup and caches `CallMetadata` in memory. Even when an operator rotates the credential on disk via `riffdb application deploy --replace-role-credential` or `riffdb capability rotate`, a running `riffdb-driverd` continues sending the expired bearer token until restarted.
2. **Deploying writes credentials and state inside source checkouts:** `riffdb application deploy` hardcodes `<app>/.riffdb/deployments/<database>/` for `state.json`, `application.credential`, `application-client.json`, and `mcp-client.json`. In multi-service or read-only checkouts, operators must copy credentials out to a runtime directory and `.gitignore` `.riffdb/`. Furthermore, launching `riffdb-driverd` and `@riffdb/client` requires assembling 10 exact hash and role environment variables (`RIFFDB_DRIVER_SOCKET`, `RIFFDB_MANIFEST_HASH`, `RIFFDB_CATALOG_HASH`, `RIFFDB_ROLE_HASH`, `RIFFDB_REMOTE_HASH`, `RIFFDB_BUNDLE_HASH`, etc.) from `riffdb.lock` and generated catalogs.
3. **Container bootstrap ceremony per database:** `riffdb capability bootstrap` requires either an existing JSON request file or `--generate <PATH>` followed by authoring a 20-line `CreateCapabilityRequest` JSON listing all 11 operator control-plane permissions per database.
4. **Opaque client configuration and file-permission errors:** When a credential or TLS trust root path is relative, symlinked, or mode `0644` instead of `0600` (or when `tls_server_name` is missing/mismatched), the CLI collapses the specific cause into `configuration_invalid: client configuration is invalid`.
5. **Handbook and package drift:** Several handbook pages still reference `@riffdb/application` instead of `@riffdb/client`, client READMEs still show `0.1.0`, `docs/SUMMARY.md` links internal maintainer evidence (`docs/integrations/ADAPTER-EVIDENCE.md`), and the handbook does not clearly distinguish when to use `riffdb dev`, `riffdb push`, and `riffdb application deploy`.

## Decision

1. **Live `credential_file` reload in `riffdb-driverd` (`crates/riffdb-driver-host`):**
   - `DriverRuntime` retains the canonical validated `credential_path: PathBuf` from `credential_file` and re-reads the bearer token via `load_protected_bearer_credential` whenever the file's `(dev, ino, mtime, size)` metadata changes (falling back to the last valid loaded token only if an atomic replacement is not needed, while failing closed if the file permissions or ownership become unsafe).
   - Rotating a role credential in place (`riffdb application deploy --provision-role <Role> --replace-role-credential` or `riffdb capability rotate`) immediately updates subsequent RPC calls from a running `riffdb-driverd` without restarting the driver host or application.
2. **External deployment state and turnkey `riffdb-driverd` / `.env` emission on `riffdb application deploy` (`crates/riffdb-cli`):**
   - `riffdb application deploy` accepts `--deployment-dir <ABS_PATH>` (or `RIFFDB_DEPLOYMENT_DIR`) to store per-database deployment state (`<deployment_dir>/<database>/state.json`, `application.credential`, `application-client.json`, `mcp-client.json`) outside the application source checkout.
   - `riffdb application deploy` accepts `--credential-output <ABS_PATH>` to write the mode-`0600` role bearer token directly to an explicit runtime path.
   - `riffdb application deploy` accepts `--driver-config-output <ABS_PATH>` and `--driver-env-output <ABS_PATH>` (with optional `--driver-socket <ABS_PATH>`) to write a ready-to-run mode-`0600` `driverd.toml` and `.env` handshake file containing all exact lock, manifest, catalog, role, and remote hashes required by `riffdb-driverd` and `@riffdb/client`.
3. **Single-command operator bootstrap preset (`riffdb capability bootstrap --operator`):**
   - `riffdb capability bootstrap` gains `--operator`, `--principal <ID>` (default `"operator"`), and `--lifetime-seconds <SECONDS>` (default `2592000` / 30 days). When `--operator` is passed without `--request` or `--generate`, the command synthesizes the standard 11-permission operator control-plane request in a protected temporary file and writes the resulting bearer token to `--bearer-output <ABS_PATH>`.
4. **Actionable file-permission, path, and TLS diagnostics (`crates/riffdb-cli`, `crates/riffdb-client-rust`):**
   - Client and CLI configuration/credential errors preserve the specific failure class (relative path vs. symlink vs. file permissions not `0600`/`0400` vs. ownership mismatch vs. `tls_server_name` configuration) in the safe diagnostic message without leaking secret contents or raw filesystem paths when forbidden.
5. **Handbook and version synchronization (`docs/`, `clients/`, `scripts/bump-version`):**
   - Replace `@riffdb/application` with `@riffdb/client`, remove `docs/integrations/ADAPTER-EVIDENCE.md` from `docs/SUMMARY.md`, clean up internal WP/ADR citations in `docs/contracts/LANGUAGE.md`, document `riffdb dev` vs `riffdb push` vs `riffdb application deploy` and rootless Podman `keep-id` permissions, and wire client/distribution README versions into `scripts/bump-version`.

## Standing design tests

- **Interface safety:** External deployment directories, emitted `driverd.toml`/`.env` files, `--operator` bootstrap tokens, and live-reloaded driver credentials enforce the same `0600`/`0700` file-mode, symlink-refusal, and effective-UID ownership checks as existing paths.
- **Scale:** Live credential reload performs one `stat` (`symlink_metadata`) per driver operation and re-reads the bounded (`<= 4 KiB`) credential file only when `(dev, ino, mtime, len)` changes.

## Checks

- Unit and integration tests in `crates/riffdb-driver-host` and `crates/riffdb-cli`.
- `./scripts/check-adr-form`, `./scripts/generate-cli-reference --check`, and `./scripts/handbook check`.
