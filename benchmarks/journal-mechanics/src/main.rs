#![forbid(unsafe_code)]

//! Same-device synchronous journal mechanics probe.

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

const EXTENT_BYTES: usize = 40 * 1024 * 1024;
const ZERO_CHUNK_BYTES: usize = 1024 * 1024;
const ALIGNMENT: usize = 4096;
const LOGICAL_FRAME_BYTES: [usize; 6] = [
    3000,
    64 * 1024,
    256 * 1024,
    1024 * 1024,
    4 * 1024 * 1024,
    16 * 1024 * 1024,
];

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("journal mechanics probe failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let root = probe_root()?;
    fs::create_dir_all(&root).map_err(|error| format!("create probe root: {error}"))?;
    let run_root = root.join(format!("run-{}", std::process::id()));
    fs::create_dir(&run_root).map_err(|error| format!("create run root: {error}"))?;

    println!(
        "{{\"schema\":\"riffdb.journal-mechanics/v2\",\"record_type\":\"configuration\",\"root\":\"{}\",\"extent_bytes\":{EXTENT_BYTES},\"frame_sizes\":[{}]}}",
        json_escape(&run_root.display().to_string()),
        LOGICAL_FRAME_BYTES
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );

    let mut append_best = u64::MAX;
    let mut prezero_best = u64::MAX;
    for logical_bytes in LOGICAL_FRAME_BYTES {
        for padded in [false, true] {
            let physical_bytes = if padded {
                align_up(logical_bytes, ALIGNMENT)?
            } else {
                logical_bytes
            };
            for mode in Mode::ALL {
                let sample = measure(&run_root, mode, logical_bytes, physical_bytes)?;
                if mode == Mode::AppendFdatasync {
                    append_best = append_best.min(sample.p50_us);
                }
                if matches!(
                    mode,
                    Mode::PrezeroPositionalFdatasync | Mode::PrezeroPositionalOdsync
                ) {
                    prezero_best = prezero_best.min(sample.p50_us);
                }
                println!(
                    "{{\"schema\":\"riffdb.journal-mechanics/v2\",\"record_type\":\"sample\",\"mode\":\"{}\",\"logical_bytes\":{logical_bytes},\"physical_bytes\":{physical_bytes},\"padded_4k\":{padded},\"warmup_samples\":{},\"measured_samples\":{},\"p50_us\":{},\"p95_us\":{},\"p99_us\":{},\"max_us\":{}}}",
                    mode.label(),
                    sample.warmup_samples,
                    sample.measured_samples,
                    sample.p50_us,
                    sample.p95_us,
                    sample.p99_us,
                    sample.max_us,
                );
            }
        }
    }

    let improvement_basis_points = append_best
        .checked_mul(10_000)
        .ok_or("improvement overflow")?
        / prezero_best.max(1);
    let threshold_met = improvement_basis_points >= 20_000;
    println!(
        "{{\"schema\":\"riffdb.journal-mechanics/v2\",\"record_type\":\"summary\",\"best_append_fdatasync_p50_us\":{append_best},\"best_prezero_positional_p50_us\":{prezero_best},\"improvement_basis_points\":{improvement_basis_points},\"required_basis_points\":20000,\"format_threshold_met\":{threshold_met}}}"
    );

    fs::remove_dir(&run_root).map_err(|error| format!("remove empty run root: {error}"))?;
    if threshold_met {
        Ok(())
    } else {
        Err("preallocated positional mechanics did not meet the required 2x threshold".to_owned())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    AppendFdatasync,
    SparsePositionalFdatasync,
    PrezeroPositionalFdatasync,
    PrezeroPositionalOdsync,
}

impl Mode {
    const ALL: [Self; 4] = [
        Self::AppendFdatasync,
        Self::SparsePositionalFdatasync,
        Self::PrezeroPositionalFdatasync,
        Self::PrezeroPositionalOdsync,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::AppendFdatasync => "append_fdatasync",
            Self::SparsePositionalFdatasync => "sparse_positional_fdatasync",
            Self::PrezeroPositionalFdatasync => "prezero_positional_fdatasync",
            Self::PrezeroPositionalOdsync => "prezero_positional_odsync",
        }
    }
}

struct Sample {
    warmup_samples: usize,
    measured_samples: usize,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
}

fn measure(
    root: &Path,
    mode: Mode,
    logical_bytes: usize,
    physical_bytes: usize,
) -> Result<Sample, String> {
    let path = root.join(format!(
        "{}-{logical_bytes}-{physical_bytes}.journal",
        mode.label()
    ));
    let mut file = Some(prepare_file(&path, mode)?);
    let mut payload = vec![0xA5; physical_bytes];
    payload[..8].copy_from_slice(b"RDBPROBE");
    let (warmup_samples, measured_samples) = sample_counts(logical_bytes);
    let samples = warmup_samples + measured_samples;
    let mut measured = Vec::with_capacity(measured_samples);
    let mut append_file = matches!(mode, Mode::AppendFdatasync)
        .then(|| file.take())
        .flatten();
    let positional_file = (!matches!(mode, Mode::AppendFdatasync))
        .then(|| file.take())
        .flatten();

    for index in 0..samples {
        let aligned_bytes = align_up(physical_bytes, ALIGNMENT)?;
        let extent_slots = EXTENT_BYTES
            .checked_div(aligned_bytes)
            .filter(|slots| *slots != 0)
            .ok_or("frame does not fit probe extent")?;
        let offset = (index % extent_slots)
            .checked_mul(aligned_bytes)
            .ok_or("offset overflow")?;
        if !matches!(mode, Mode::AppendFdatasync)
            && offset
                .checked_add(physical_bytes)
                .ok_or("extent overflow")?
                > EXTENT_BYTES
        {
            return Err("probe extent is too small for the configured sweep".to_owned());
        }
        payload[8..16].copy_from_slice(&(index as u64).to_le_bytes());
        let started = Instant::now();
        match mode {
            Mode::AppendFdatasync => {
                let target = append_file.as_mut().ok_or("missing append file")?;
                target
                    .write_all(&payload)
                    .and_then(|()| target.sync_data())
                    .map_err(|error| format!("append fence: {error}"))?;
            }
            Mode::SparsePositionalFdatasync | Mode::PrezeroPositionalFdatasync => {
                let target = positional_file.as_ref().ok_or("missing positional file")?;
                target
                    .write_all_at(&payload, offset as u64)
                    .and_then(|()| target.sync_data())
                    .map_err(|error| format!("positional fence: {error}"))?;
            }
            Mode::PrezeroPositionalOdsync => {
                positional_file
                    .as_ref()
                    .ok_or("missing O_DSYNC file")?
                    .write_all_at(&payload, offset as u64)
                    .map_err(|error| format!("O_DSYNC positional write: {error}"))?;
            }
        }
        let elapsed = started.elapsed().as_micros();
        if index >= warmup_samples {
            measured.push(u64::try_from(elapsed).map_err(|_| "duration overflow")?);
        }
    }

    measured.sort_unstable();
    let sample = Sample {
        warmup_samples,
        measured_samples,
        p50_us: percentile(&measured, 50),
        p95_us: percentile(&measured, 95),
        p99_us: percentile(&measured, 99),
        max_us: *measured.last().ok_or("empty measurement")?,
    };
    drop(append_file);
    drop(positional_file);
    fs::remove_file(&path).map_err(|error| format!("remove probe file: {error}"))?;
    Ok(sample)
}

const fn sample_counts(logical_bytes: usize) -> (usize, usize) {
    if logical_bytes <= 64 * 1024 {
        (8, 96)
    } else if logical_bytes <= 1024 * 1024 {
        (4, 32)
    } else if logical_bytes <= 4 * 1024 * 1024 {
        (2, 12)
    } else {
        (2, 4)
    }
}

fn prepare_file(path: &Path, mode: Mode) -> Result<File, String> {
    match mode {
        Mode::AppendFdatasync => OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(path)
            .map_err(|error| format!("open append file: {error}")),
        Mode::SparsePositionalFdatasync => {
            let file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(path)
                .map_err(|error| format!("open sparse file: {error}"))?;
            file.set_len(EXTENT_BYTES as u64)
                .and_then(|()| file.sync_data())
                .map_err(|error| format!("size sparse extent: {error}"))?;
            Ok(file)
        }
        Mode::PrezeroPositionalFdatasync | Mode::PrezeroPositionalOdsync => {
            let mut setup = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(path)
                .map_err(|error| format!("open prezero file: {error}"))?;
            let zeros = vec![0_u8; ZERO_CHUNK_BYTES];
            for _ in 0..(EXTENT_BYTES / ZERO_CHUNK_BYTES) {
                setup
                    .write_all(&zeros)
                    .map_err(|error| format!("zero extent: {error}"))?;
            }
            setup
                .sync_data()
                .map_err(|error| format!("sync zeroed extent: {error}"))?;
            drop(setup);
            let mut options = OpenOptions::new();
            options.read(true).write(true);
            if mode == Mode::PrezeroPositionalOdsync {
                options.custom_flags(libc::O_DSYNC);
            }
            options
                .open(path)
                .map_err(|error| format!("reopen prezero file: {error}"))
        }
    }
}

fn probe_root() -> Result<PathBuf, String> {
    if let Some(root) = env::var_os("RIFFDB_JOURNAL_MECHANICS_ROOT") {
        return Ok(PathBuf::from(root));
    }
    Ok(Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target/perf-db/journal-mechanics"))
}

fn align_up(value: usize, alignment: usize) -> Result<usize, String> {
    value
        .checked_add(alignment - 1)
        .map(|sum| sum / alignment * alignment)
        .ok_or_else(|| "alignment overflow".to_owned())
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = sorted.len().saturating_sub(1).saturating_mul(percentile) / 100;
    sorted[index]
}

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alignment_and_percentiles_are_bounded() {
        assert_eq!(align_up(3000, 4096), Ok(4096));
        assert_eq!(align_up(4096, 4096), Ok(4096));
        assert_eq!(percentile(&[1, 2, 3, 4], 50), 2);
        assert_eq!(percentile(&[1, 2, 3, 4], 99), 3);
        assert_eq!(sample_counts(3000), (8, 96));
        assert_eq!(sample_counts(256 * 1024), (4, 32));
        assert_eq!(sample_counts(4 * 1024 * 1024), (2, 12));
        assert_eq!(sample_counts(16 * 1024 * 1024), (2, 4));
    }
}
