//! Reject-first same-filesystem mechanics probe for ADR-0132.

#![forbid(unsafe_code)]

use std::env;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_ITERATIONS: usize = 128;
const GROUP_BYTES: [usize; 3] = [4 * 1024, 64 * 1024, 256 * 1024];
const DEPTHS: [usize; 3] = [1, 2, 4];
const ZERO_CHUNK_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy)]
enum Mode {
    SerialPerGroup,
    CoalescedReady,
    PipelinedCoalescedFence,
    ConcurrentSameFile,
    ConcurrentSeparateFiles,
}

impl Mode {
    const ALL: [Self; 5] = [
        Self::SerialPerGroup,
        Self::CoalescedReady,
        Self::PipelinedCoalescedFence,
        Self::ConcurrentSameFile,
        Self::ConcurrentSeparateFiles,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::SerialPerGroup => "serial_per_group",
            Self::CoalescedReady => "coalesced_ready",
            Self::PipelinedCoalescedFence => "pipelined_coalesced_fence",
            Self::ConcurrentSameFile => "concurrent_same_file",
            Self::ConcurrentSeparateFiles => "concurrent_separate_files",
        }
    }
}

struct Sample {
    mode: Mode,
    group_bytes: usize,
    depth: usize,
    operations: usize,
    wall: Duration,
    completion_us: Vec<u64>,
}

impl Sample {
    fn json(mut self) -> String {
        self.completion_us.sort_unstable();
        let p50 = percentile(&self.completion_us, 50);
        let p95 = percentile(&self.completion_us, 95);
        let p99 = percentile(&self.completion_us, 99);
        let wall_us = u64::try_from(self.wall.as_micros()).unwrap_or(u64::MAX);
        let operations_per_second = self.operations as f64 / self.wall.as_secs_f64().max(1e-9);
        format!(
            "{{\"mode\":\"{}\",\"group_bytes\":{},\"depth\":{},\"operations\":{},\"wall_us\":{},\"operations_per_second\":{:.3},\"completion_p50_us\":{},\"completion_p95_us\":{},\"completion_p99_us\":{}}}",
            self.mode.name(),
            self.group_bytes,
            self.depth,
            self.operations,
            wall_us,
            operations_per_second,
            p50,
            p95,
            p99,
        )
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let (root, iterations, output_path) = arguments()?;
    fs::create_dir_all(&root)?;
    let canonical = fs::canonicalize(&root)?;
    if canonical.starts_with("/tmp") {
        return Err("probe root must not be under /tmp".into());
    }
    let run = canonical.join(format!(
        "riffdb-journal-fence-pipeline-{}",
        std::process::id()
    ));
    fs::create_dir(&run)?;
    let result = run_probe(&run, iterations);
    let cleanup = fs::remove_dir_all(&run);
    match (result, cleanup) {
        (Ok(report), Ok(())) => {
            if let Some(path) = output_path.as_ref() {
                fs::write(path, report.as_bytes())?;
            }
            println!("{report}");
            Ok(())
        }
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
    }
}

fn arguments() -> Result<(PathBuf, usize, Option<PathBuf>), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let mut root = None;
    let mut iterations = DEFAULT_ITERATIONS;
    let mut output = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--root" => root = args.next().map(PathBuf::from),
            "--iterations" => {
                iterations = args
                    .next()
                    .ok_or("--iterations requires a value")?
                    .parse()?;
            }
            "--output" => output = args.next().map(PathBuf::from),
            _ => return Err(format!("unknown argument: {argument}").into()),
        }
    }
    let root = root.ok_or("usage: journal-fence-pipeline --root PATH [--iterations N]")?;
    if !(32..=4_096).contains(&iterations) {
        return Err("iterations must be in 32..=4096".into());
    }
    Ok((root, iterations, output))
}

fn run_probe(root: &Path, iterations: usize) -> Result<String, Box<dyn Error>> {
    let mut samples = Vec::new();
    for group_bytes in GROUP_BYTES {
        let operations = iterations
            .checked_mul(*DEPTHS.last().ok_or("depths are nonempty")?)
            .ok_or("operation count overflow")?;
        let file_bytes = operations
            .checked_mul(group_bytes)
            .ok_or("probe file size overflow")?;
        let shared_path = root.join(format!("shared-{group_bytes}.bin"));
        let shared = create_zeroed(&shared_path, file_bytes)?;
        let block = vec![0xA5; group_bytes];
        for depth in DEPTHS {
            for mode in Mode::ALL {
                let sample = match mode {
                    Mode::SerialPerGroup => run_serial(&shared, &block, iterations, depth, mode)?,
                    Mode::CoalescedReady => {
                        run_coalesced(&shared, &block, iterations, depth, mode)?
                    }
                    Mode::PipelinedCoalescedFence => {
                        run_pipelined(&shared, &block, iterations, depth, mode)?
                    }
                    Mode::ConcurrentSameFile => {
                        run_concurrent_same(&shared, &block, iterations, depth, mode)?
                    }
                    Mode::ConcurrentSeparateFiles => {
                        run_concurrent_separate(root, &block, iterations, depth, mode)?
                    }
                };
                samples.push(sample.json());
            }
        }
        drop(shared);
        fs::remove_file(shared_path)?;
    }
    Ok(format!(
        "{{\"schema\":\"riffdb-journal-fence-pipeline-probe-v1\",\"iterations\":{},\"samples\":[{}]}}",
        iterations,
        samples.join(",")
    ))
}

fn create_zeroed(path: &Path, bytes: usize) -> io::Result<File> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)?;
    let zero = vec![0_u8; ZERO_CHUNK_BYTES];
    let mut remaining = bytes;
    while remaining > 0 {
        let write = remaining.min(zero.len());
        file.write_all(&zero[..write])?;
        remaining -= write;
    }
    file.sync_data()?;
    Ok(file)
}

fn run_serial(
    file: &File,
    block: &[u8],
    iterations: usize,
    depth: usize,
    mode: Mode,
) -> io::Result<Sample> {
    let operations = iterations * depth;
    let mut completion_us = Vec::with_capacity(operations);
    let wall = Instant::now();
    for operation in 0..operations {
        let started = Instant::now();
        file.write_all_at(block, offset(operation, block.len())?)?;
        file.sync_data()?;
        completion_us.push(elapsed_us(started));
    }
    Ok(Sample {
        mode,
        group_bytes: block.len(),
        depth,
        operations,
        wall: wall.elapsed(),
        completion_us,
    })
}

fn run_coalesced(
    file: &File,
    block: &[u8],
    iterations: usize,
    depth: usize,
    mode: Mode,
) -> io::Result<Sample> {
    let operations = iterations * depth;
    let mut completion_us = Vec::with_capacity(operations);
    let wall = Instant::now();
    for iteration in 0..iterations {
        let started = Instant::now();
        for item in 0..depth {
            let operation = iteration * depth + item;
            file.write_all_at(block, offset(operation, block.len())?)?;
        }
        file.sync_data()?;
        let elapsed = elapsed_us(started);
        completion_us.extend(std::iter::repeat_n(elapsed, depth));
    }
    Ok(Sample {
        mode,
        group_bytes: block.len(),
        depth,
        operations,
        wall: wall.elapsed(),
        completion_us,
    })
}

fn run_pipelined(
    file: &File,
    block: &[u8],
    iterations: usize,
    depth: usize,
    mode: Mode,
) -> io::Result<Sample> {
    let operations = iterations * depth;
    let (submit, receive) = mpsc::sync_channel::<Instant>(depth);
    let (complete, completions) = mpsc::sync_channel::<io::Result<u64>>(depth);
    let fence_file = file.try_clone()?;
    let wall = Instant::now();
    thread::scope(|scope| -> io::Result<Sample> {
        let worker = scope.spawn(move || {
            while let Ok(first) = receive.recv() {
                let mut batch = Vec::with_capacity(depth);
                batch.push(first);
                while batch.len() < depth {
                    match receive.try_recv() {
                        Ok(submitted) => batch.push(submitted),
                        Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => break,
                    }
                }
                let result = fence_file.sync_data();
                for submitted in batch {
                    let completion =
                        result
                            .as_ref()
                            .map(|()| elapsed_us(submitted))
                            .map_err(|error| {
                                io::Error::new(error.kind(), "pipelined durability failed")
                            });
                    if complete.send(completion).is_err() {
                        return;
                    }
                }
            }
        });
        let mut completion_us = Vec::with_capacity(operations);
        for operation in 0..operations {
            file.write_all_at(block, offset(operation, block.len())?)?;
            submit
                .send(Instant::now())
                .map_err(|_| io::Error::other("pipeline fence worker stopped"))?;
            while completion_us.len() + depth <= operation + 1 {
                completion_us.push(
                    completions
                        .recv()
                        .map_err(|_| io::Error::other("pipeline completion stopped"))??,
                );
            }
        }
        drop(submit);
        while completion_us.len() < operations {
            completion_us.push(
                completions
                    .recv()
                    .map_err(|_| io::Error::other("pipeline completion stopped"))??,
            );
        }
        worker
            .join()
            .map_err(|_| io::Error::other("pipeline fence worker panicked"))?;
        Ok(Sample {
            mode,
            group_bytes: block.len(),
            depth,
            operations,
            wall: wall.elapsed(),
            completion_us,
        })
    })
}

fn run_concurrent_same(
    file: &File,
    block: &[u8],
    iterations: usize,
    depth: usize,
    mode: Mode,
) -> io::Result<Sample> {
    let files = (0..depth)
        .map(|_| file.try_clone())
        .collect::<io::Result<Vec<_>>>()?;
    run_concurrent_workers(files, block, iterations, mode)
}

fn run_concurrent_separate(
    root: &Path,
    block: &[u8],
    iterations: usize,
    depth: usize,
    mode: Mode,
) -> io::Result<Sample> {
    let bytes = iterations
        .checked_mul(depth)
        .and_then(|value| value.checked_mul(block.len()))
        .ok_or_else(|| io::Error::other("separate probe size overflow"))?;
    let mut paths = Vec::with_capacity(depth);
    let mut files = Vec::with_capacity(depth);
    for item in 0..depth {
        let path = root.join(format!("separate-{}-{depth}-{item}.bin", block.len()));
        files.push(create_zeroed(&path, bytes)?);
        paths.push(path);
    }
    let sample = run_concurrent_workers(files, block, iterations, mode)?;
    for path in paths {
        fs::remove_file(path)?;
    }
    Ok(sample)
}

fn run_concurrent_workers(
    files: Vec<File>,
    block: &[u8],
    iterations: usize,
    mode: Mode,
) -> io::Result<Sample> {
    let depth = files.len();
    let operations = iterations * depth;
    let barrier = Arc::new(Barrier::new(depth + 1));
    let (complete, completions) = mpsc::sync_channel::<io::Result<u64>>(depth);
    let wall = Instant::now();
    thread::scope(|scope| -> io::Result<Sample> {
        let mut submitters = Vec::with_capacity(depth);
        let mut workers = Vec::with_capacity(depth);
        for file in files {
            let (submit, receive) = mpsc::sync_channel::<u64>(1);
            submitters.push(submit);
            let worker_barrier = Arc::clone(&barrier);
            let worker_complete = complete.clone();
            workers.push(scope.spawn(move || {
                while let Ok(write_offset) = receive.recv() {
                    worker_barrier.wait();
                    let started = Instant::now();
                    let result = file
                        .write_all_at(block, write_offset)
                        .and_then(|()| file.sync_data())
                        .map(|()| elapsed_us(started));
                    if worker_complete.send(result).is_err() {
                        return;
                    }
                }
            }));
        }
        drop(complete);
        let mut completion_us = Vec::with_capacity(operations);
        for iteration in 0..iterations {
            for (item, submitter) in submitters.iter().enumerate() {
                let operation = iteration * depth + item;
                submitter
                    .send(offset(operation, block.len())?)
                    .map_err(|_| io::Error::other("concurrent worker stopped"))?;
            }
            barrier.wait();
            for _ in 0..depth {
                completion_us.push(
                    completions
                        .recv()
                        .map_err(|_| io::Error::other("concurrent completion stopped"))??,
                );
            }
        }
        drop(submitters);
        for worker in workers {
            worker
                .join()
                .map_err(|_| io::Error::other("concurrent worker panicked"))?;
        }
        Ok(Sample {
            mode,
            group_bytes: block.len(),
            depth,
            operations,
            wall: wall.elapsed(),
            completion_us,
        })
    })
}

fn offset(operation: usize, group_bytes: usize) -> io::Result<u64> {
    operation
        .checked_mul(group_bytes)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| io::Error::other("probe offset overflow"))
}

fn elapsed_us(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = sorted.len().saturating_mul(percentile).saturating_add(99) / 100;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}
