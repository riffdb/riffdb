---
adr: "0330"
title: Native Apple Silicon macOS Platform and Release Support
status: accepted
tier: guarantee
date: 2026-11-04
accepted: 2026-11-04
acceptance: "User approved native Apple Silicon macOS (aarch64-apple-darwin) platform and binary release plan"
requires: ["0009", "0124", "0328", "0329"]
amends: ["0009", "0328"]
supersedes: []
requirements: ["SEC-001", "SEC-002", "SEC-003", "STO-001", "STO-010", "DRV-014", "AFC-001", "VER-001", "VER-004", "DX-004", "DX-014", "DX-046"]
packages: ["WP-883", "WP-884", "WP-885"]
obligations:
  - id: OBL-0330-1
    package: WP-883
    description: "Protected secret and bearer credential loaders in riffdb-auth and riffdb-client-rust and directory custody in riffdb-outbox and riffdb-server enforce pre-open/post-open inode identity, regular-file, link-count, effective-user ownership, and 0600/0700 mode checks on both Linux and Apple Silicon macOS using platform-correct O_NOFOLLOW | O_NONBLOCK flags"
    proof: linux_bounds_and_open_flags_are_frozen
  - id: OBL-0330-2
    package: WP-884
    description: "release/distribution/manifest-v1.json, scripts/build-driver-distributions, release/distribution/install-riffdb, scripts/publish-release-artifacts, and .github/workflows/release.yml include aarch64-apple-darwin for native macOS server/CLI/MCP release bundles, @riffdb/cli-darwin-arm64 npm packages, and macOS arm64 Python wheels"
    proof: scripts/check-driver-distribution
  - id: OBL-0330-3
    package: WP-885
    description: "docs/installation.md, docs/getting-started/PACKAGE-INSTALLATION.md, docs/known-limitations.md, and docs/compatibility.md document native Apple Silicon macOS support and pass ./scripts/handbook check"
    proof: scripts/handbook
review_triggers:
  - "Protected file or custody loading on macOS weakens symlink refusal, pre/post-open (dev, ino) identity checks, effective-UID ownership verification, or 0600/0700 mode enforcement."
  - "A non-Apple-Silicon (x86_64-apple-darwin) or unsigned macOS artifact is added to the release distribution without an explicit ADR amendment."
---
# ADR-0330: Native Apple Silicon macOS Platform and Release Support

## Context

RiffDB `0.1.0` shipped with Linux (`x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`) as the only supported platform for `riffdbd`, `riffdb`, `riffdb-mcp`, protected credential/secret loading (`crates/riffdb-auth/src/protected_file.rs`, `crates/riffdb-client-rust/src/credential_file.rs`), directory-anchored custody (`crates/riffdb-outbox/src/local_archive_custody.rs`, `crates/riffdb-server/src/exact_provider_checkpoint_custody.rs`), `riffdb-storage-redb` maintenance primitives, and binary/package distributions (`release/distribution/manifest-v1.json`). While RiffDB's core storage, networking, and runtime dependencies (`redb`, `tokio`, `tonic`, `cap-std`, `rustix`) are portable Unix crates, Linux-specific `#![cfg(target_os = "linux")]` gates, hardcoded Linux `O_NOFOLLOW | O_NONBLOCK` octal constants (`0o00400000 | 0o00004000` vs Darwin `0x0100 | 0x0004`), `/proc/self/status` UID lookups, and Linux-only release matrices prevented developers on Apple Silicon Macs (`aarch64-apple-darwin`) from installing and running RiffDB natively.

Amending `ADR-0009` and `ADR-0328` is required so that Apple Silicon macOS (`aarch64-apple-darwin`) is a first-class supported platform across all RiffDB binaries, protected file/custody boundaries, storage maintenance paths, and signed binary/package releases while preserving every fail-closed filesystem and capability guarantee.

## Decision

1. **Protected File and Directory Custody on Linux and Apple Silicon macOS (`WP-883`):**
   - `crates/riffdb-auth/src/protected_file.rs`, `crates/riffdb-client-rust/src/credential_file.rs`, `crates/riffdb-outbox/src/local_archive_custody.rs`, and `crates/riffdb-server/src/exact_provider_checkpoint_custody.rs` support `#[cfg(any(target_os = "linux", target_os = "macos"))]`.
   - Open flags use platform-exact `O_NOFOLLOW | O_NONBLOCK` constants (`0o00400000 | 0o00004000` on Linux; `0x0100 | 0x0004` on macOS) so symlinks and blocking FIFOs are refused at the kernel `open`/`openat` boundary on both platforms.
   - Effective user identity (`euid`) continues to be parsed from `/proc/self/status` on Linux (preserving existing Linux procfs fault-injection tests) and is obtained via safe `rustix::process::geteuid().as_raw()` on macOS. Pre-open/post-open `(dev, ino)` identity equality, regular-file verification, hard-link count checks, owner equality (`uid == euid`), and private mode (`mode & 0o077 == 0`) remain identical and fail-closed on both platforms.
2. **Storage Engine, Maintenance, and Test Harness Portability (`WP-883`):**
   - In `crates/riffdb-storage-redb`: page-cache eviction hints (`rustix::fs::fadvise`) remain gated to `#[cfg(all(target_os = "linux", not(test)))]`; atomic non-overwriting renames (`rustix::fs::renameat_with(..., RenameFlags::NOREPLACE)`), retained-descriptor read-only opens (`open_read_only_maintenance_file` / `/dev/fd/<fd>`), and offline source maintenance/verification are enabled on both Linux and macOS.
   - `crates/riffdb-bench-root` classifies macOS filesystems via `rustix::fs::statvfs` when Linux `statfs`/`/proc/self/mounts` are absent, and `crates/riffdb-testkit` uses safe `rustix::process::test_kill_process` (`kill(pid, 0)`) on macOS when `/proc/<pid>` is absent and keeps Unix domain socket paths within Darwin's 104-byte `SUN_LEN` bound.
3. **Apple Silicon (`aarch64-apple-darwin`) Binary and Package Release Matrix (`WP-884`):**
   - `release/distribution/manifest-v1.json` adds `aarch64-apple-darwin` (`os: "darwin"`, `arch: "arm64"`, `goarch: "arm64"`, `python_tag: "macosx_11_0_arm64"`). Intel macOS (`x86_64-apple-darwin`) is intentionally excluded.
   - `scripts/build-driver-distributions` emits `@riffdb/cli-darwin-arm64` (`"os": ["darwin"]`, `"cpu": ["arm64"]`), registers `"darwin-arm64": "@riffdb/cli-darwin-arm64"` in `@riffdb/cli`'s launcher and `optionalDependencies`, and packages macOS arm64 `riffdb_application` (`cp313-abi3-macosx_11_0_arm64.whl`) and `riffdb_cli` (`py3-none-macosx_11_0_arm64.whl`) wheels.
   - `release/distribution/install-riffdb` accepts `aarch64-apple-darwin` and supports `shasum -a 256` alongside `sha256sum`.
   - `.github/workflows/release.yml` and `scripts/publish-release-artifacts` build, verify, sign, and publish the `aarch64-apple-darwin` driver distribution and native `riffdb-<version>-aarch64-apple-darwin.tar.gz` server/CLI/MCP release archive on `macos-15` runners.
4. **Handbook and Platform Documentation (`WP-885`):**
   - `docs/installation.md`, `docs/getting-started/PACKAGE-INSTALLATION.md`, `docs/known-limitations.md`, and `docs/compatibility.md` document native Apple Silicon macOS (`aarch64-apple-darwin`) support while noting that automated `systemd` service unit management remains specific to Linux hosts.

## Options considered

1. **Support both `aarch64-apple-darwin` and `x86_64-apple-darwin`:** Rejected. Apple has completed its transition to Apple Silicon; targeting `aarch64-apple-darwin` exclusively avoids legacy Intel CI/packaging overhead.
2. **Support only client SDKs on macOS while keeping `riffdbd` Linux-only:** Rejected. `riffdb dev` and local integration testing require running `riffdbd`, `riffdb`, and `riffdb-mcp` natively on the developer's machine without Docker or a Linux VM.

## Consequences

- Developers on Apple Silicon Macs can install and run `riffdb`, `riffdbd`, `riffdb-mcp`, and `riffdb dev`, plus all four language SDKs, natively via `npx @riffdb/cli`, `pip install`, `cargo install`, or prebuilt release tarballs.
- Linux `systemd` service unit management (`cargo riffdb install --user|--system`) remains Linux-specific; macOS runs `riffdbd` directly or via `riffdb dev`.

## Standing design tests

- **Interface safety:** Protected secret and credential files on macOS enforce the same symlink refusal, `(dev, ino)` race detection, owner-match, and `0600`/`0700` permission checks as Linux.
- **Scale:** All file size bounds, path component ceilings (`<= 32` components, `<= 4096` bytes), and release artifact verification limits (`<= 256 MiB`) remain unchanged.

## Checks

- `cargo test -p riffdb-auth -p riffdb-client-rust -p riffdb-outbox -p riffdb-server -p riffdb-storage-redb`
- `./scripts/check-driver-distribution`
- `./scripts/publish-release-artifacts --self-test`
- `./scripts/handbook check`
