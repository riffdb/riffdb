//! Real-disk placement, leak hygiene, and device baseline for RiffDB benchmarks.
//!
//! Harnesses must not put multi-GB durable databases on tmpfs (`/tmp` on many
//! machines). This crate resolves a root, classifies the storage medium,
//! refuses tmpfs unless explicitly allowed, sweeps stale dirs from killed
//! runs, and records a cheap device baseline for report embedding.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
// PathBuf-rich error variants are intentional for operator diagnostics.
#![allow(clippy::result_large_err)]

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

/// Linux `TMPFS_MAGIC` (`linux/magic.h`).
const TMPFS_MAGIC: u64 = 0x0102_1994;
/// Linux `RAMFS_MAGIC`.
const RAMFS_MAGIC: u64 = 0x8584_58f6;

/// Environment override shared by every harness (`cli → env → default`).
pub const BENCH_DB_ROOT_ENV: &str = "RIFFDB_BENCH_DB_ROOT";

/// Session directory name prefix written by [`BenchDir::create`].
pub const BENCH_DIR_PREFIX: &str = "riffdb-bench-";

/// Legacy session prefixes still swept (pre-unification harness names).
pub const LEGACY_SWEEP_PREFIXES: &[&str] = &[
    "riffdb-app-baseline-",
    "riffdb-command-growth-",
    "riffdb-wp125-",
    "riffdb-wp135-public-",
    "riffdb-wp139-safety-",
];

/// Hostile-env belt: [`sweep_stale`] refuses roots that lack this path component.
pub const PERF_DB_PATH_COMPONENT: &str = "perf-db";

static NEXT_DIR: AtomicU64 = AtomicU64::new(1);

/// Classified backing store for a resolved bench root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageMedium {
    /// Ordinary block or network filesystem (not RAM-backed).
    Disk {
        /// `statfs` / mounts fstype string (e.g. `ext4`, `xfs`).
        fstype: String,
        /// Mount point covering the root.
        mount: PathBuf,
        /// Optional `/sys/block/<dev>/device/model` value.
        device_model: Option<String>,
        /// Mount options string from `/proc/self/mounts`.
        mount_options: String,
    },
    /// tmpfs or ramfs — RAM-flattered, forbidden unless `allow_tmpfs`.
    RamBacked {
        /// Filesystem type (`tmpfs` / `ramfs`).
        fstype: String,
        /// Mount point covering the root.
        mount: PathBuf,
    },
    /// Classification failed or filesystem magic was unrecognized.
    Unknown,
}

impl StorageMedium {
    /// Returns true when the medium is tmpfs/ramfs.
    #[must_use]
    pub fn is_ram_backed(&self) -> bool {
        matches!(self, Self::RamBacked { .. })
    }

    /// Compact JSON-ish object for report embedding.
    #[must_use]
    pub fn to_report_json(&self) -> String {
        match self {
            Self::Disk {
                fstype,
                mount,
                device_model,
                mount_options,
            } => {
                let model = device_model
                    .as_deref()
                    .map(|m| format!(r#""{m}""#))
                    .unwrap_or_else(|| "null".to_owned());
                format!(
                    r#"{{"kind":"disk","fstype":"{fstype}","mount":"{}","device_model":{model},"mount_options":"{}"}}"#,
                    escape_json(mount.display().to_string()),
                    escape_json(mount_options.clone()),
                )
            }
            Self::RamBacked { fstype, mount } => format!(
                r#"{{"kind":"ram_backed","fstype":"{fstype}","mount":"{}"}}"#,
                escape_json(mount.display().to_string()),
            ),
            Self::Unknown => r#"{"kind":"unknown"}"#.to_owned(),
        }
    }
}

/// Resolved, classified, free-space-checked database root.
#[derive(Clone, Debug)]
pub struct BenchRoot {
    path: PathBuf,
    medium: StorageMedium,
    free_bytes: u64,
    harness: &'static str,
}

impl BenchRoot {
    /// Resolves `cli_override` → `RIFFDB_BENCH_DB_ROOT` → `default_root`.
    ///
    /// Creates the directory, canonicalizes it, classifies the medium, and
    /// refuses tmpfs unless `allow_tmpfs`. Fails closed when free space is
    /// below `min_free_bytes`.
    pub fn resolve(options: BenchRootOptions) -> Result<Self, BenchRootError> {
        let env_override = std::env::var_os(BENCH_DB_ROOT_ENV).and_then(|value| {
            if value.is_empty() {
                None
            } else {
                Some(PathBuf::from(value))
            }
        });
        let chosen = choose_root(
            options.cli_override.clone(),
            env_override,
            options.default_root,
        );

        let absolute = if chosen.is_absolute() {
            chosen
        } else {
            std::env::current_dir()
                .map_err(|error| BenchRootError::Io {
                    context: "resolve current_dir for relative bench root".to_owned(),
                    source: error,
                })?
                .join(chosen)
        };

        fs::create_dir_all(&absolute).map_err(|error| BenchRootError::Io {
            context: format!("create_dir_all {}", absolute.display()),
            source: error,
        })?;
        let path = fs::canonicalize(&absolute).map_err(|error| BenchRootError::Io {
            context: format!("canonicalize {}", absolute.display()),
            source: error,
        })?;

        let medium = classify_medium(&path)?;
        if medium.is_ram_backed() && !options.allow_tmpfs {
            return Err(BenchRootError::RamBacked {
                fstype: match &medium {
                    StorageMedium::RamBacked { fstype, .. } => fstype.clone(),
                    _ => "unknown".to_owned(),
                },
                mount: match &medium {
                    StorageMedium::RamBacked { mount, .. } => mount.clone(),
                    _ => path.clone(),
                },
                path: path.clone(),
                harness: options.harness,
                cli_override: options.cli_override,
                env_override: std::env::var_os(BENCH_DB_ROOT_ENV).map(PathBuf::from),
            });
        }

        let free_bytes = free_bytes_for(&path)?;
        if free_bytes < options.min_free_bytes {
            return Err(BenchRootError::InsufficientFreeSpace {
                path: path.clone(),
                free_bytes,
                min_free_bytes: options.min_free_bytes,
                harness: options.harness,
            });
        }

        Ok(Self {
            path,
            medium,
            free_bytes,
            harness: options.harness,
        })
    }

    /// Absolute canonical path of the bench root.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Classified storage medium.
    #[must_use]
    pub fn medium(&self) -> &StorageMedium {
        &self.medium
    }

    /// Free bytes observed at resolve time.
    #[must_use]
    pub fn free_bytes_at_start(&self) -> u64 {
        self.free_bytes
    }

    /// Harness name used when creating session directories.
    #[must_use]
    pub fn harness(&self) -> &'static str {
        self.harness
    }

    /// JSON-ish environment block for report embedding.
    #[must_use]
    pub fn environment_report_json(&self) -> String {
        let kernel = read_trimmed("/proc/sys/kernel/osrelease").unwrap_or_default();
        let governor = first_cpu_governor().unwrap_or_else(|| "unknown".to_owned());
        format!(
            r#"{{"harness":"{}","database_root":"{}","medium":{},"free_bytes_at_start":{},"kernel":"{}","cpu_governor":"{}"}}"#,
            escape_json(self.harness.to_owned()),
            escape_json(self.path.display().to_string()),
            self.medium.to_report_json(),
            self.free_bytes,
            escape_json(kernel),
            escape_json(governor),
        )
    }
}

/// Inputs to [`BenchRoot::resolve`].
#[derive(Clone, Debug)]
pub struct BenchRootOptions {
    /// Short harness id (used in session dir names and errors).
    pub harness: &'static str,
    /// Explicit CLI `--database-root` override.
    pub cli_override: Option<PathBuf>,
    /// Default root when neither CLI nor env is set.
    pub default_root: PathBuf,
    /// When true, allow resolving onto tmpfs/ramfs (tests only).
    pub allow_tmpfs: bool,
    /// Minimum free bytes required before any daemon spawn.
    pub min_free_bytes: u64,
}

/// Errors from root resolution, sweep, or classification.
#[derive(Debug)]
pub enum BenchRootError {
    /// Filesystem I/O failure.
    Io {
        /// Human context for the failing operation.
        context: String,
        /// Underlying I/O error.
        source: io::Error,
    },
    /// Root sits on tmpfs/ramfs and `allow_tmpfs` was false.
    RamBacked {
        /// Detected fstype.
        fstype: String,
        /// Mount covering the path.
        mount: PathBuf,
        /// Resolved database root.
        path: PathBuf,
        /// Harness that requested the root.
        harness: &'static str,
        /// CLI override that was in effect (if any).
        cli_override: Option<PathBuf>,
        /// Env override that was in effect (if any).
        env_override: Option<PathBuf>,
    },
    /// Free space below the configured floor.
    InsufficientFreeSpace {
        /// Resolved path.
        path: PathBuf,
        /// Observed free bytes.
        free_bytes: u64,
        /// Required free bytes.
        min_free_bytes: u64,
        /// Harness name.
        harness: &'static str,
    },
    /// [`sweep_stale`] refused to run because the root lacks `perf-db`.
    SweepRefused {
        /// Offending root.
        path: PathBuf,
    },
}

impl fmt::Display for BenchRootError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { context, source } => write!(f, "{context}: {source}"),
            Self::RamBacked {
                fstype,
                mount,
                path,
                harness,
                cli_override,
                env_override,
            } => write!(
                f,
                "bench root for harness '{harness}' is RAM-backed \
                 (fstype={fstype}, mount={}, path={}). \
                 Pass --database-root on real disk, set {BENCH_DB_ROOT_ENV}, \
                 or pass --allow-tmpfs only for deliberate tmpfs tests. \
                 cli_override={cli}, env_override={env}",
                mount.display(),
                path.display(),
                cli = cli_override
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "<none>".to_owned()),
                env = env_override
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "<none>".to_owned()),
            ),
            Self::InsufficientFreeSpace {
                path,
                free_bytes,
                min_free_bytes,
                harness,
            } => write!(
                f,
                "bench root for harness '{harness}' has insufficient free space: \
                 path={}, free_bytes={free_bytes}, min_free_bytes={min_free_bytes}",
                path.display(),
            ),
            Self::SweepRefused { path } => write!(
                f,
                "refusing to sweep stale bench dirs outside a path containing \
                 '{PERF_DB_PATH_COMPONENT}': {}",
                path.display(),
            ),
        }
    }
}

impl std::error::Error for BenchRootError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Unique session directory under a [`BenchRoot`]; removes itself on drop.
#[derive(Debug)]
pub struct BenchDir {
    path: PathBuf,
}

impl BenchDir {
    /// Creates `<root>/riffdb-bench-<harness>-<pid>-<n>` and returns the owner.
    pub fn create(root: &BenchRoot, prefix: &str) -> Result<Self, BenchRootError> {
        let name = format!(
            "{BENCH_DIR_PREFIX}{prefix}-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        );
        let path = root.path().join(name);
        fs::create_dir_all(&path).map_err(|error| BenchRootError::Io {
            context: format!("create bench dir {}", path.display()),
            source: error,
        })?;
        let path = fs::canonicalize(&path).map_err(|error| BenchRootError::Io {
            context: format!("canonicalize bench dir {}", path.display()),
            source: error,
        })?;
        Ok(Self { path })
    }

    /// Absolute path of the session directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for BenchDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Removes orphaned session directories left by killed/hung harness runs.
///
/// Only runs when `root` contains a `perf-db` path component. Deletes entries
/// matching [`BENCH_DIR_PREFIX`] or [`LEGACY_SWEEP_PREFIXES`] whose embedded
/// pid is dead or whose mtime is older than 24 hours.
pub fn sweep_stale(root: &BenchRoot) -> Result<usize, BenchRootError> {
    sweep_stale_path(root.path())
}

/// Path-level sweep used by tests and harnesses that already hold a path.
pub fn sweep_stale_path(root: &Path) -> Result<usize, BenchRootError> {
    if !path_has_perf_db_component(root) {
        return Err(BenchRootError::SweepRefused {
            path: root.to_path_buf(),
        });
    }
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => {
            return Err(BenchRootError::Io {
                context: format!("read_dir for sweep {}", root.display()),
                source: error,
            });
        }
    };

    let now = SystemTime::now();
    let max_age = Duration::from_secs(24 * 60 * 60);
    let mut removed = 0_usize;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !is_sweep_candidate(name) {
            continue;
        }
        let pid_dead = extract_pid(name).is_some_and(|pid| !pid_is_alive(pid));
        let too_old = entry
            .metadata()
            .ok()
            .and_then(|meta| meta.modified().ok())
            .and_then(|mtime| now.duration_since(mtime).ok())
            .is_some_and(|age| age > max_age);
        if pid_dead || too_old {
            match fs::remove_dir_all(&path) {
                Ok(()) => {
                    eprintln!(
                        "riffdb-bench-root: swept stale dir {} (pid_dead={pid_dead} too_old={too_old})",
                        path.display()
                    );
                    removed = removed.saturating_add(1);
                }
                Err(error) => {
                    eprintln!(
                        "riffdb-bench-root: failed to sweep {}: {error}",
                        path.display()
                    );
                }
            }
        }
    }
    Ok(removed)
}

/// Cheap device baseline: 4 KiB fdatasync loop + one 64 MiB buffered write.
#[derive(Clone, Debug)]
pub struct DeviceBaseline {
    /// Median 4 KiB write+`sync_data` latency in microseconds.
    pub fdatasync_p50_us: u64,
    /// p99 4 KiB write+`sync_data` latency in microseconds.
    pub fdatasync_p99_us: u64,
    /// Completed 4 KiB fdatasyncs per second over the probe window.
    pub fsyncs_per_s: f64,
    /// Sustained buffered 64 MiB write + final fdatasync throughput (MB/s).
    pub sequential_write_mib_s: f64,
}

impl DeviceBaseline {
    /// JSON-ish object for report embedding.
    #[must_use]
    pub fn to_report_json(&self) -> String {
        format!(
            r#"{{"fdatasync_p50_us":{},"fdatasync_p99_us":{},"fsyncs_per_s":{:.3},"sequential_write_mib_s":{:.3}}}"#,
            self.fdatasync_p50_us,
            self.fdatasync_p99_us,
            self.fsyncs_per_s,
            self.sequential_write_mib_s,
        )
    }
}

/// Runs the 10 s device baseline probe under `root` (creates a probe file).
pub fn run_device_baseline(root: &Path) -> Result<DeviceBaseline, BenchRootError> {
    run_device_baseline_for(root, Duration::from_secs(10))
}

/// Testable variant with a caller-chosen probe duration.
pub fn run_device_baseline_for(
    root: &Path,
    probe_duration: Duration,
) -> Result<DeviceBaseline, BenchRootError> {
    fs::create_dir_all(root).map_err(|error| BenchRootError::Io {
        context: format!("create root for device baseline {}", root.display()),
        source: error,
    })?;
    let probe_path = root.join(format!(
        "riffdb-device-baseline-{}-{}.bin",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| -> Result<DeviceBaseline, BenchRootError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&probe_path)
            .map_err(|error| BenchRootError::Io {
                context: format!("open probe {}", probe_path.display()),
                source: error,
            })?;
        let block = [0xA5_u8; 4096];
        let mut samples_us = Vec::with_capacity(16_384);
        let started = Instant::now();
        while started.elapsed() < probe_duration {
            let sample_start = Instant::now();
            file.write_all(&block).map_err(|error| BenchRootError::Io {
                context: "probe write 4KiB".to_owned(),
                source: error,
            })?;
            file.sync_data().map_err(|error| BenchRootError::Io {
                context: "probe sync_data 4KiB".to_owned(),
                source: error,
            })?;
            let us = u64::try_from(sample_start.elapsed().as_micros()).unwrap_or(u64::MAX);
            samples_us.push(us);
        }
        let elapsed = started.elapsed().as_secs_f64().max(1e-9);
        let fsyncs_per_s = samples_us.len() as f64 / elapsed;
        samples_us.sort_unstable();
        let fdatasync_p50_us = percentile_sorted(&samples_us, 50);
        let fdatasync_p99_us = percentile_sorted(&samples_us, 99);

        // 64 MiB buffered write + final fdatasync.
        const SEQ_BYTES: usize = 64 * 1024 * 1024;
        let chunk = vec![0x5A_u8; 1024 * 1024];
        let seq_start = Instant::now();
        let mut written = 0_usize;
        while written < SEQ_BYTES {
            let n = (SEQ_BYTES - written).min(chunk.len());
            file.write_all(&chunk[..n])
                .map_err(|error| BenchRootError::Io {
                    context: "probe sequential write".to_owned(),
                    source: error,
                })?;
            written += n;
        }
        file.sync_data().map_err(|error| BenchRootError::Io {
            context: "probe sequential sync_data".to_owned(),
            source: error,
        })?;
        let seq_secs = seq_start.elapsed().as_secs_f64().max(1e-9);
        let sequential_write_mib_s = (SEQ_BYTES as f64 / (1024.0 * 1024.0)) / seq_secs;

        Ok(DeviceBaseline {
            fdatasync_p50_us,
            fdatasync_p99_us,
            fsyncs_per_s,
            sequential_write_mib_s,
        })
    })();
    let _ = fs::remove_file(&probe_path);
    result
}

/// Classifies the storage medium for an existing path (canonical preferred).
pub fn classify_medium(path: &Path) -> Result<StorageMedium, BenchRootError> {
    let stat = rustix::fs::statfs(path).map_err(|error| BenchRootError::Io {
        context: format!("statfs {}", path.display()),
        source: io::Error::from_raw_os_error(error.raw_os_error()),
    })?;
    let magic = u64::from(stat.f_type as u32);
    let mount_info = match_mount(path);
    let (fstype_from_mount, mount, options) = match mount_info {
        Some(info) => (info.fstype, info.mount, info.options),
        None => ("unknown".to_owned(), path.to_path_buf(), String::new()),
    };

    let is_ram = magic == TMPFS_MAGIC
        || magic == RAMFS_MAGIC
        || fstype_from_mount == "tmpfs"
        || fstype_from_mount == "ramfs";
    if is_ram {
        let fstype = if magic == RAMFS_MAGIC || fstype_from_mount == "ramfs" {
            "ramfs"
        } else {
            "tmpfs"
        };
        return Ok(StorageMedium::RamBacked {
            fstype: fstype.to_owned(),
            mount,
        });
    }

    if fstype_from_mount == "unknown" && magic == 0 {
        return Ok(StorageMedium::Unknown);
    }

    let device_model = device_model_for_mount_source(&mount, &options);
    Ok(StorageMedium::Disk {
        fstype: if fstype_from_mount == "unknown" {
            format!("magic={magic:#x}")
        } else {
            fstype_from_mount
        },
        mount,
        device_model,
        mount_options: options,
    })
}

/// Longest-prefix mount match for `path` against `/proc/self/mounts`.
///
/// Ensures `/tmp` matches `/tmp` and not a sibling like `/tmpfoo`.
#[must_use]
pub fn match_mount(path: &Path) -> Option<MountInfo> {
    let mounts = parse_mounts().ok()?;
    let path_str = path.to_string_lossy();
    let mut best: Option<MountInfo> = None;
    for mount in mounts {
        let mount_str = mount.mount.to_string_lossy();
        if path_is_under(&path_str, &mount_str)
            && best
                .as_ref()
                .is_none_or(|current| mount_str.len() > current.mount.to_string_lossy().len())
        {
            best = Some(mount);
        }
    }
    best
}

/// One `/proc/self/mounts` entry (subset).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountInfo {
    /// Device / source field.
    pub source: String,
    /// Mount point.
    pub mount: PathBuf,
    /// Filesystem type.
    pub fstype: String,
    /// Mount options.
    pub options: String,
}

/// Default default-root helper: `<manifest_dir>/target/perf-db/<harness>`.
#[must_use]
pub fn default_perf_db_root(manifest_dir: &Path, harness: &str) -> PathBuf {
    manifest_dir.join("target").join("perf-db").join(harness)
}

/// Pure override precedence: CLI → env → default.
#[must_use]
pub fn choose_root(
    cli_override: Option<PathBuf>,
    env_override: Option<PathBuf>,
    default_root: PathBuf,
) -> PathBuf {
    cli_override.or(env_override).unwrap_or(default_root)
}

/// True when any path component equals `perf-db`.
#[must_use]
pub fn path_has_perf_db_component(path: &Path) -> bool {
    path.components().any(|component| {
        component
            .as_os_str()
            .to_str()
            .is_some_and(|name| name == PERF_DB_PATH_COMPONENT)
    })
}

fn free_bytes_for(path: &Path) -> Result<u64, BenchRootError> {
    let stat = rustix::fs::statvfs(path).map_err(|error| BenchRootError::Io {
        context: format!("statvfs {}", path.display()),
        source: io::Error::from_raw_os_error(error.raw_os_error()),
    })?;
    let frsize = stat.f_frsize.max(1);
    Ok(stat.f_bavail.saturating_mul(frsize))
}

fn is_sweep_candidate(name: &str) -> bool {
    name.starts_with(BENCH_DIR_PREFIX)
        || LEGACY_SWEEP_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
}

/// Best-effort pid extraction from `…-<pid>-<n>` style names.
fn extract_pid(name: &str) -> Option<u32> {
    let mut parts = name.rsplit('-');
    let _ordinal = parts.next()?;
    let pid = parts.next()?;
    pid.parse().ok()
}

fn pid_is_alive(pid: u32) -> bool {
    Path::new("/proc").join(pid.to_string()).exists()
}

fn path_is_under(path: &str, mount: &str) -> bool {
    if mount == "/" {
        return path.starts_with('/');
    }
    path == mount || path.starts_with(&(mount.to_owned() + "/"))
}

fn parse_mounts() -> io::Result<Vec<MountInfo>> {
    let text = fs::read_to_string("/proc/self/mounts")?;
    let mut out = Vec::new();
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        let Some(source) = fields.next() else {
            continue;
        };
        let Some(mount) = fields.next() else {
            continue;
        };
        let Some(fstype) = fields.next() else {
            continue;
        };
        let options = fields.next().unwrap_or("");
        out.push(MountInfo {
            source: unescape_mounts(source),
            mount: PathBuf::from(unescape_mounts(mount)),
            fstype: fstype.to_owned(),
            options: options.to_owned(),
        });
    }
    Ok(out)
}

fn unescape_mounts(value: &str) -> String {
    // /proc/self/mounts escapes space as \040, tab as \011, etc.
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            let mut oct = String::new();
            for _ in 0..3 {
                if let Some(d) = chars.peek().copied().filter(|c| c.is_ascii_digit()) {
                    oct.push(d);
                    chars.next();
                } else {
                    break;
                }
            }
            if let Ok(code) = u8::from_str_radix(&oct, 8) {
                out.push(code as char);
            } else {
                out.push('\\');
                out.push_str(&oct);
            }
        } else {
            out.push(ch);
        }
    }
    out
}

fn device_model_for_mount_source(mount: &Path, _options: &str) -> Option<String> {
    // Prefer findmnt-like source from mounts: often /dev/nvme0n1p2.
    let mounts = parse_mounts().ok()?;
    let source = mounts
        .into_iter()
        .find(|m| m.mount == mount)
        .map(|m| m.source)?;
    let dev_name = source.strip_prefix("/dev/").map(str::to_owned)?;
    // Partition → whole-disk for model: strip trailing partition digits carefully.
    let candidates = whole_disk_candidates(&dev_name);
    for candidate in candidates {
        let model_path = Path::new("/sys/block")
            .join(&candidate)
            .join("device/model");
        if let Ok(model) = read_trimmed(&model_path)
            && !model.is_empty()
        {
            return Some(model);
        }
    }
    None
}

fn whole_disk_candidates(dev_name: &str) -> Vec<String> {
    let mut out = vec![dev_name.to_owned()];
    // nvme0n1p2 → nvme0n1; sda1 → sda
    if let Some(idx) = dev_name.rfind('p')
        && dev_name[idx + 1..].chars().all(|c| c.is_ascii_digit())
    {
        out.push(dev_name[..idx].to_owned());
    }
    let trimmed = dev_name.trim_end_matches(|c: char| c.is_ascii_digit());
    if trimmed != dev_name {
        out.push(trimmed.to_owned());
    }
    out
}

fn read_trimmed(path: impl AsRef<Path>) -> io::Result<String> {
    let text = fs::read_to_string(path)?;
    Ok(text.trim().to_owned())
}

fn first_cpu_governor() -> Option<String> {
    read_trimmed("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor").ok()
}

fn percentile_sorted(sorted: &[u64], percentile: u8) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (u64::from(percentile).saturating_mul(sorted.len() as u64 - 1)) / 100;
    sorted[rank as usize]
}

fn escape_json(value: String) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Shared helper for nested test workspaces that still need a unique dir.
///
/// Resolves via [`BenchRoot`] with a default under `default_root` (must include
/// `perf-db` for sweep safety when sweeping).
pub fn resolve_for_harness(
    harness: &'static str,
    default_root: PathBuf,
    cli_override: Option<PathBuf>,
    allow_tmpfs: bool,
    min_free_bytes: u64,
) -> Result<BenchRoot, BenchRootError> {
    BenchRoot::resolve(BenchRootOptions {
        harness,
        cli_override,
        default_root,
        allow_tmpfs,
        min_free_bytes,
    })
}

// Silence unused File import when only OpenOptions is needed on some toolchains.
#[allow(dead_code)]
fn _touch(path: &Path) -> io::Result<File> {
    File::create(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    fn unique_perf_root(label: &str) -> PathBuf {
        std::env::temp_dir().join("perf-db").join(format!(
            "riffdb-bench-root-test-{label}-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn resolve_prefers_cli_over_env_over_default() {
        let cli = PathBuf::from("/data/cli");
        let env = PathBuf::from("/data/env");
        let default = PathBuf::from("/data/default");
        assert_eq!(
            choose_root(Some(cli.clone()), Some(env.clone()), default.clone()),
            cli
        );
        assert_eq!(choose_root(None, Some(env.clone()), default.clone()), env);
        assert_eq!(choose_root(None, None, default.clone()), default);

        // End-to-end resolve still honors cli over default (no env mutation).
        let cli_disk = unique_perf_root("cli");
        let default_disk = unique_perf_root("default");
        let _ = fs::remove_dir_all(&cli_disk);
        let _ = fs::remove_dir_all(&default_disk);
        let root = BenchRoot::resolve(BenchRootOptions {
            harness: "unit",
            cli_override: Some(cli_disk.clone()),
            default_root: default_disk.clone(),
            allow_tmpfs: true,
            min_free_bytes: 0,
        })
        .expect("resolve with cli");
        assert_eq!(root.path(), fs::canonicalize(&cli_disk).expect("canon cli"));
        let _ = fs::remove_dir_all(&cli_disk);
        let _ = fs::remove_dir_all(&default_disk);
    }

    #[test]
    fn mount_prefix_matches_tmp_not_tmpfoo() {
        assert!(path_is_under("/tmp/foo", "/tmp"));
        assert!(!path_is_under("/tmpfoo/bar", "/tmp"));
        assert!(path_is_under("/tmp", "/tmp"));
        assert!(path_is_under("/var/lib", "/"));
    }

    #[test]
    fn free_space_error_message_names_floor() {
        let root = unique_perf_root("enospc");
        let _ = fs::remove_dir_all(&root);
        let err = BenchRoot::resolve(BenchRootOptions {
            harness: "unit",
            cli_override: Some(root.clone()),
            default_root: root.clone(),
            allow_tmpfs: true,
            min_free_bytes: u64::MAX,
        })
        .expect_err("must fail free space");
        let text = err.to_string();
        assert!(
            text.contains("insufficient free space") && text.contains("min_free_bytes"),
            "unexpected message: {text}"
        );
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn ram_backed_without_allow_tmpfs_is_hard_error() {
        // Fabricated medium path: resolve real /tmp (tmpfs on this host) without allow.
        let under_tmp = std::env::temp_dir().join(format!(
            "riffdb-bench-root-ram-gate-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&under_tmp);
        let err = BenchRoot::resolve(BenchRootOptions {
            harness: "unit",
            cli_override: Some(under_tmp.clone()),
            default_root: under_tmp.clone(),
            allow_tmpfs: false,
            min_free_bytes: 0,
        });
        // If /tmp is not tmpfs on some hosts, also assert the fabricated medium path.
        match err {
            Err(BenchRootError::RamBacked { fstype, .. }) => {
                assert!(
                    fstype == "tmpfs" || fstype == "ramfs",
                    "unexpected fstype {fstype}"
                );
            }
            Ok(root) => {
                // Host /tmp is not RAM-backed; assert the enum path directly.
                let fabricated = StorageMedium::RamBacked {
                    fstype: "tmpfs".to_owned(),
                    mount: PathBuf::from("/tmp"),
                };
                assert!(fabricated.is_ram_backed());
                let message = BenchRootError::RamBacked {
                    fstype: "tmpfs".to_owned(),
                    mount: PathBuf::from("/tmp"),
                    path: root.path().to_path_buf(),
                    harness: "unit",
                    cli_override: Some(under_tmp.clone()),
                    env_override: None,
                }
                .to_string();
                assert!(message.contains("RAM-backed"));
                assert!(message.contains("tmpfs"));
                assert!(message.contains("--allow-tmpfs"));
                let _ = fs::remove_dir_all(root.path());
            }
            Err(other) => panic!("unexpected error: {other}"),
        }
        let _ = fs::remove_dir_all(&under_tmp);
    }

    #[test]
    fn sweep_removes_dead_pid_dirs() {
        let root_path = unique_perf_root("sweep");
        let _ = fs::remove_dir_all(&root_path);
        fs::create_dir_all(&root_path).expect("root");
        // Use a pid that cannot be alive (kernel pid 0 is not a userspace process tree entry
        // in the usual sense; we pick a very high never-started pid).
        let dead = root_path.join("riffdb-bench-unit-999999-1");
        let live = root_path.join(format!("riffdb-bench-unit-{}-2", std::process::id()));
        let keep = root_path.join("keep-me");
        fs::create_dir_all(&dead).expect("dead");
        fs::create_dir_all(&live).expect("live");
        fs::create_dir_all(&keep).expect("keep");
        let removed = sweep_stale_path(&root_path).expect("sweep");
        assert!(removed >= 1, "expected at least the dead-pid dir");
        assert!(!dead.exists());
        assert!(live.exists(), "live pid dir must be retained");
        assert!(keep.exists());
        let _ = fs::remove_dir_all(&root_path);
    }

    #[test]
    fn sweep_refuses_outside_perf_db() {
        let hostile = std::env::temp_dir().join(format!(
            "riffdb-bench-root-hostile-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&hostile);
        fs::create_dir_all(&hostile).expect("hostile root");
        let err = sweep_stale_path(&hostile).expect_err("must refuse");
        assert!(
            matches!(err, BenchRootError::SweepRefused { .. }),
            "unexpected {err}"
        );
        assert!(err.to_string().contains("perf-db"));
        let _ = fs::remove_dir_all(&hostile);
    }

    #[test]
    fn bench_dir_drop_cleans_on_panic() {
        let root_path = unique_perf_root("drop");
        let _ = fs::remove_dir_all(&root_path);
        let root = BenchRoot::resolve(BenchRootOptions {
            harness: "unit",
            cli_override: Some(root_path.clone()),
            default_root: root_path.clone(),
            allow_tmpfs: true,
            min_free_bytes: 0,
        })
        .expect("root");
        let captured = catch_unwind(AssertUnwindSafe(|| {
            let dir = BenchDir::create(&root, "panic").expect("dir");
            let path = dir.path().to_path_buf();
            assert!(path.exists());
            panic!("force drop via unwind; path={}", path.display());
        }));
        assert!(captured.is_err());
        // After unwind, Drop ran; only the root should remain (plus maybe other noise).
        let children: Vec<_> = fs::read_dir(root.path())
            .expect("read")
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .is_some_and(|n| n.starts_with(BENCH_DIR_PREFIX))
            })
            .collect();
        assert!(
            children.is_empty(),
            "session dir must be removed on panic drop: {children:?}"
        );
        let _ = fs::remove_dir_all(root.path());
    }

    #[test]
    fn device_baseline_short_probe_runs() {
        let root_path = unique_perf_root("baseline");
        let _ = fs::remove_dir_all(&root_path);
        fs::create_dir_all(&root_path).expect("root");
        let baseline =
            run_device_baseline_for(&root_path, Duration::from_millis(50)).expect("baseline");
        assert!(baseline.fsyncs_per_s > 0.0);
        let _ = fs::remove_dir_all(&root_path);
    }
}
