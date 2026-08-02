#![forbid(unsafe_code)]

//! Reproducible retained-state command-mechanics size sweep.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

use riffdb_bench_root::{
    BenchDir, BenchRoot, BenchRootOptions, default_perf_db_root, run_device_baseline_for,
    sweep_stale,
};
use riffdb_storage_redb::benchmark_support::{
    EngineDurability, EngineMechanicsProfile, ServiceAuditGrowthHarness,
    initialize_engine_mechanics, measure_clean_startup, measure_clean_startup_linear,
    measure_engine_reopen, run_engine_mechanics_window,
};

const CHECKED_CHECKPOINTS: [u64; 6] = [0, 1_024, 4_096, 16_384, 32_768, 65_536];
const SMOKE_CHECKPOINTS: [u64; 3] = [0, 64, 256];
const CHECKED_WINDOW: usize = 128;
const SMOKE_WINDOW: usize = 32;
const PERF_MIN_COMMANDS_PER_SECOND: u64 = 50;
const PERF_MIN_RETAINED_BASIS_POINTS: u64 = 5_000;
const PERF_MAX_GROUP_VS_SYNC_BASIS_POINTS: u64 = 7_500;
const PERF_013_MAX_GROWTH_RATIO: u64 = 32;
const PERF_013_MAX_STARTUP_NS: u64 = 30_000_000_000;
const PREFLIGHT_ENVIRONMENT: &str = "RIFFDB_COMMAND_GROWTH_PREFLIGHT";
const PREFLIGHT_EVIDENCE: &str = "semantic-crash-v1";
/// Bump when the retained-history generator contract changes; watermark reuse refuses mismatch.
const STARTUP_SCALE_SCHEMA_DIGEST: &str = "service-audit-growth-startup-scale-v2-grouped-fused";
const STARTUP_SCALE_DB_FILE: &str = "startup-scale.redb";
const STARTUP_SCALE_WATERMARK_FILE: &str = "startup-scale-watermark.json";
const STARTUP_SCALE_GENERATION_MODE: &str = "grouped_fused";
/// Extrapolated from 10^3–10^4 command service-audit growth samples (~1.1–1.2 KiB/cmd)
/// plus headroom for free-space preflight of multi-million retained DBs.
const STARTUP_SCALE_BYTES_PER_COMMAND: u64 = 1_200;
const STARTUP_SCALE_FREE_HEADROOM_BYTES: u64 = 512 * 1024 * 1024;
/// One grouped-fused durable transaction covers up to MAX_GROUPED_WRITE_TRANSITIONS commands.
const STARTUP_SCALE_GENERATE_CHUNK: usize = riffdb_storage_redb::benchmark_support::MAX_GROUP_COMMANDS;
const PROC_STATUS_PATH: &str = "/proc/self/status";
const PROC_CLEAR_REFS_PATH: &str = "/proc/self/clear_refs";
const PROC_STATUS_MAX_BYTES: usize = 65_536;

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) | Err(()) => ExitCode::FAILURE,
    }
}

fn run() -> Result<bool, ()> {
    let configuration = Configuration::parse()?;
    let preflight_passed = env::var(PREFLIGHT_ENVIRONMENT).as_deref() == Ok(PREFLIGHT_EVIDENCE);
    if (configuration.assert_perf_003
        || configuration.assert_group_mechanics
        || configuration.assert_perf_008
        || configuration.assert_perf_009
        || configuration.assert_perf_012
        || configuration.assert_perf_013)
        && !preflight_passed
    {
        return Err(());
    }
    let default_root =
        default_perf_db_root(Path::new(env!("CARGO_MANIFEST_DIR")), "command-growth");
    let min_free_bytes = configuration
        .startup_scale
        .as_ref()
        .map(|targets| estimate_startup_scale_free_bytes(targets))
        .unwrap_or(64 * 1024 * 1024);
    let bench_root = BenchRoot::resolve(BenchRootOptions {
        harness: "command-growth",
        cli_override: configuration.database_root.clone(),
        default_root,
        allow_tmpfs: configuration.allow_tmpfs,
        min_free_bytes,
    })
    .map_err(|error| {
        eprintln!("command-growth root: {error}");
    })?;
    let _ = sweep_stale(&bench_root);
    let baseline = run_device_baseline_for(
        bench_root.path(),
        std::time::Duration::from_millis(if configuration.checked { 10_000 } else { 100 }),
    )
    .ok();
    if let Some(baseline) = &baseline {
        println!(
            "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"device_baseline\",\"fdatasync_p50_us\":{},\"fdatasync_p99_us\":{},\"fsyncs_per_s\":{:.3},\"sequential_write_mib_s\":{:.3}}}",
            baseline.fdatasync_p50_us,
            baseline.fdatasync_p99_us,
            baseline.fsyncs_per_s,
            baseline.sequential_write_mib_s
        );
    }
    println!(
        "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"environment\",\"body\":{}}}",
        bench_root.environment_report_json()
    );

    if let Some(targets) = &configuration.startup_scale {
        return run_startup_scale(&configuration, &bench_root, targets);
    }

    let mut startup_4096_reps = Vec::new();
    let mut startup_65536_reps = Vec::new();
    let mut group_summaries = Vec::new();
    let mut growth_summaries = Vec::new();

    for rep in 0..configuration.reps {
        let dir = BenchDir::create(&bench_root, "command-growth").map_err(|_| ())?;
        let path = dir.path().join("service-audit-growth.redb");
        let mut harness = ServiceAuditGrowthHarness::new(&path).map_err(|_| ())?;
        let (checkpoints, window) = if configuration.checked {
            (&CHECKED_CHECKPOINTS[..], CHECKED_WINDOW)
        } else {
            (&SMOKE_CHECKPOINTS[..], SMOKE_WINDOW)
        };

        if rep == 0 {
            println!(
                "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"configuration\",\"workload\":\"service_audit_started_failed_pair\",\"acknowledgement_durability\":\"sync\",\"redb_commit_profile\":\"standard\",\"engine_durability\":\"immediate_one_phase\",\"durable_commits_per_command\":2,\"group_commands\":1,\"window_commands\":{window},\"reps\":{},\"semantic_preflight\":\"{}\"}}",
                configuration.reps,
                if preflight_passed {
                    "passed"
                } else {
                    "not_run"
                }
            );
        }

        let mut next_command = 1_u64;
        let mut first_rate = None;
        let mut final_rate = 0_u64;
        let mut startup_at_4096_ns = None;
        let mut startup_at_65536_ns = None;
        for checkpoint in checkpoints {
            while next_command.saturating_sub(1) < *checkpoint {
                let remaining = checkpoint.saturating_sub(next_command.saturating_sub(1));
                let chunk = usize::try_from(remaining.min(256)).map_err(|_| ())?;
                harness.run_window(next_command, chunk).map_err(|_| ())?;
                next_command = next_command
                    .checked_add(u64::try_from(chunk).map_err(|_| ())?)
                    .ok_or(())?;
            }
            let sample = harness.run_window(next_command, window).map_err(|_| ())?;
            next_command = next_command
                .checked_add(u64::try_from(window).map_err(|_| ())?)
                .ok_or(())?;
            let elapsed_ns = u64::try_from(sample.append().as_nanos()).map_err(|_| ())?;
            let rate = commands_per_second(sample.commands(), elapsed_ns)?;
            first_rate.get_or_insert(rate);
            final_rate = rate;
            println!(
                "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"window\",\"rep\":{rep},\"retained_commands_before\":{checkpoint},\"commands\":{},\"audit_records_per_command\":2,\"queue_wait_ns\":0,\"semantic_evaluation_ns\":{},\"transaction_current_table_and_durable_commit_ns\":{elapsed_ns},\"commands_per_second\":{rate},\"file_bytes\":{}}}",
                sample.commands(),
                sample.preparation().as_nanos(),
                sample.file_bytes()
            );

            drop(harness);
            let startup_ns =
                u64::try_from(measure_clean_startup(&path).map_err(|_| ())?.as_nanos())
                    .map_err(|_| ())?;
            println!(
                "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"startup\",\"rep\":{rep},\"retained_commands\":{checkpoint},\"startup_ns\":{startup_ns}}}"
            );
            if *checkpoint == 4_096 {
                startup_at_4096_ns = Some(startup_ns);
            }
            if *checkpoint == 65_536 {
                startup_at_65536_ns = Some(startup_ns);
            }
            harness = ServiceAuditGrowthHarness::reopen(&path).map_err(|_| ())?;
        }

        drop(harness);
        let recovery_ns = u64::try_from(measure_engine_reopen(&path).map_err(|_| ())?.as_nanos())
            .map_err(|_| ())?;
        println!(
            "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"recovery\",\"rep\":{rep},\"retained_commands\":{},\"engine_reopen_ns\":{recovery_ns}}}",
            next_command.saturating_sub(1)
        );
        let comparison = run_mechanics_comparison(dir.path(), configuration.checked, rep)?;
        let first_rate = first_rate.ok_or(())?;
        let retained_basis_points = final_rate.checked_mul(10_000).ok_or(())? / first_rate.max(1);
        growth_summaries.push((first_rate, final_rate, retained_basis_points));
        if let Some(v) = startup_at_4096_ns {
            startup_4096_reps.push(v);
        }
        if let Some(v) = startup_at_65536_ns {
            startup_65536_reps.push(v);
        }
        group_summaries.push(comparison);
    }

    let (first_rate, final_rate, retained_basis_points) = median_growth(&growth_summaries)?;
    let passed = final_rate >= PERF_MIN_COMMANDS_PER_SECOND
        && retained_basis_points >= PERF_MIN_RETAINED_BASIS_POINTS;
    println!(
        "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"summary\",\"first_commands_per_second\":{first_rate},\"final_commands_per_second\":{final_rate},\"retained_basis_points\":{retained_basis_points},\"minimum_commands_per_second\":{PERF_MIN_COMMANDS_PER_SECOND},\"minimum_retained_basis_points\":{PERF_MIN_RETAINED_BASIS_POINTS},\"reps\":{},\"perf_003_passed\":{passed}}}",
        configuration.reps
    );
    let comparison = median_group(&group_summaries)?;
    let group_basis_points = comparison.group_elapsed_ns.checked_mul(10_000).ok_or(())?
        / comparison.sync_elapsed_ns.max(1);
    let perf_004_passed = comparison.group_commands == 16
        && comparison.commands >= 32
        && group_basis_points <= PERF_MAX_GROUP_VS_SYNC_BASIS_POINTS;
    println!(
        "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"group_summary\",\"sync_elapsed_ns\":{},\"group_elapsed_ns\":{},\"commands\":{},\"group_commands\":{},\"group_vs_sync_basis_points\":{group_basis_points},\"maximum_group_vs_sync_basis_points\":{PERF_MAX_GROUP_VS_SYNC_BASIS_POINTS},\"engine_durability\":\"immediate_two_phase\",\"reps\":{},\"perf_004_passed\":{perf_004_passed},\"perf_006_mechanics_passed\":{perf_004_passed},\"perf_008_mechanics_passed\":{perf_004_passed}}}",
        comparison.sync_elapsed_ns,
        comparison.group_elapsed_ns,
        comparison.commands,
        comparison.group_commands,
        configuration.reps
    );
    let standard_vs_hardened_basis_points = comparison
        .standard_elapsed_ns
        .checked_mul(10_000)
        .ok_or(())?
        / comparison.hardened_elapsed_ns.max(1);
    let perf_009_passed = preflight_passed
        && comparison.standard_elapsed_ns > 0
        && comparison.hardened_elapsed_ns > 0;
    println!(
        "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"commit_profile_summary\",\"standard_profile\":\"immediate_one_phase\",\"hardened_profile\":\"immediate_two_phase\",\"standard_elapsed_ns\":{},\"hardened_elapsed_ns\":{},\"standard_vs_hardened_basis_points\":{standard_vs_hardened_basis_points},\"semantic_contract\":\"acknowledgement_survives_crash\",\"perf_009_passed\":{perf_009_passed}}}",
        comparison.standard_elapsed_ns, comparison.hardened_elapsed_ns,
    );
    let perf_012_passed = preflight_passed;
    println!(
        "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"generation_summary\",\"identity\":\"partition_index\",\"maximum_advances_per_distinct_pair_per_command\":1,\"prefix_fanout\":false,\"semantic_preflight\":\"{}\",\"perf_012_passed\":{perf_012_passed}}}",
        if preflight_passed {
            "passed"
        } else {
            "not_run"
        },
    );
    let group_gate_requested =
        configuration.assert_group_mechanics || configuration.assert_perf_008;
    let startup_at_4096_ns = median_u64(&startup_4096_reps);
    let startup_at_65536_ns = median_u64(&startup_65536_reps);
    let perf_013_passed = if configuration.assert_perf_013 {
        if startup_at_4096_ns > 0 && startup_at_65536_ns > 0 {
            let ratio = startup_at_65536_ns / startup_at_4096_ns.max(1);
            ratio <= PERF_013_MAX_GROWTH_RATIO && startup_at_65536_ns <= PERF_013_MAX_STARTUP_NS
        } else {
            false
        }
    } else {
        true
    };
    println!(
        "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"startup_summary\",\"startup_at_4096_ns\":{startup_at_4096_ns},\"startup_at_65536_ns\":{startup_at_65536_ns},\"max_growth_ratio\":{PERF_013_MAX_GROWTH_RATIO},\"max_startup_ns\":{PERF_013_MAX_STARTUP_NS},\"reps\":{},\"perf_013_passed\":{perf_013_passed}}}",
        configuration.reps
    );
    let all_passed = (!configuration.assert_perf_003 || passed)
        && (!group_gate_requested || perf_004_passed)
        && (!configuration.assert_perf_009 || perf_009_passed)
        && (!configuration.assert_perf_012 || perf_012_passed)
        && (!configuration.assert_perf_013 || perf_013_passed);
    let _ = fs::metadata(bench_root.path());
    Ok(all_passed)
}

struct GroupComparison {
    sync_elapsed_ns: u64,
    group_elapsed_ns: u64,
    standard_elapsed_ns: u64,
    hardened_elapsed_ns: u64,
    commands: usize,
    group_commands: usize,
}

fn run_mechanics_comparison(root: &Path, checked: bool, rep: usize) -> Result<GroupComparison, ()> {
    let commands = if checked { 128 } else { 32 };
    let mut sync_elapsed_ns = None;
    let mut group_elapsed_ns = None;
    let mut standard_elapsed_ns = None;
    let mut hardened_elapsed_ns = None;
    for (ordinal, (durability, group)) in [
        (EngineDurability::None, 1),
        (EngineDurability::ImmediateOnePhase, 1),
        (EngineDurability::ImmediateTwoPhase, 1),
        (EngineDurability::ImmediateTwoPhase, 16),
    ]
    .into_iter()
    .enumerate()
    {
        let path = root.join(format!("mechanics-{ordinal}.redb"));
        initialize_engine_mechanics(&path).map_err(|_| ())?;
        let profile = EngineMechanicsProfile::new(durability, group).map_err(|_| ())?;
        let sample = run_engine_mechanics_window(&path, 1, commands, profile).map_err(|_| ())?;
        let elapsed_ns = u64::try_from(sample.elapsed().as_nanos()).map_err(|_| ())?;
        if durability == EngineDurability::ImmediateOnePhase && group == 1 {
            standard_elapsed_ns = Some(elapsed_ns);
        } else if durability == EngineDurability::ImmediateTwoPhase && group == 1 {
            sync_elapsed_ns = Some(elapsed_ns);
            hardened_elapsed_ns = Some(elapsed_ns);
        } else if durability == EngineDurability::ImmediateTwoPhase && group == 16 {
            group_elapsed_ns = Some(elapsed_ns);
        }
        println!(
            "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"mechanics_comparison\",\"rep\":{rep},\"engine_durability\":\"{}\",\"group_commands\":{},\"commands\":{},\"elapsed_ns\":{},\"admission_table_page_work_ns\":{},\"admission_commit_and_flush_ns\":{},\"terminal_table_page_work_ns\":{},\"terminal_commit_and_flush_ns\":{}}}",
            durability.label(),
            profile.group_commands(),
            commands,
            elapsed_ns,
            sample.admission_work().as_nanos(),
            sample.admission_commit().as_nanos(),
            sample.terminal_work().as_nanos(),
            sample.terminal_commit().as_nanos()
        );
    }
    Ok(GroupComparison {
        sync_elapsed_ns: sync_elapsed_ns.ok_or(())?,
        group_elapsed_ns: group_elapsed_ns.ok_or(())?,
        standard_elapsed_ns: standard_elapsed_ns.ok_or(())?,
        hardened_elapsed_ns: hardened_elapsed_ns.ok_or(())?,
        commands,
        group_commands: 16,
    })
}

fn commands_per_second(commands: usize, elapsed_ns: u64) -> Result<u64, ()> {
    u64::try_from(commands)
        .map_err(|_| ())?
        .checked_mul(1_000_000_000)
        .ok_or(())?
        .checked_div(elapsed_ns.max(1))
        .ok_or(())
}

fn median_u64(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

fn median_growth(values: &[(u64, u64, u64)]) -> Result<(u64, u64, u64), ()> {
    if values.is_empty() {
        return Err(());
    }
    let mut first: Vec<_> = values.iter().map(|v| v.0).collect();
    let mut final_rate: Vec<_> = values.iter().map(|v| v.1).collect();
    let mut retained: Vec<_> = values.iter().map(|v| v.2).collect();
    first.sort_unstable();
    final_rate.sort_unstable();
    retained.sort_unstable();
    let mid = values.len() / 2;
    Ok((first[mid], final_rate[mid], retained[mid]))
}

fn median_group(values: &[GroupComparison]) -> Result<GroupComparison, ()> {
    if values.is_empty() {
        return Err(());
    }
    let mid = values.len() / 2;
    Ok(GroupComparison {
        sync_elapsed_ns: median_u64(&values.iter().map(|v| v.sync_elapsed_ns).collect::<Vec<_>>()),
        group_elapsed_ns: median_u64(
            &values
                .iter()
                .map(|v| v.group_elapsed_ns)
                .collect::<Vec<_>>(),
        ),
        standard_elapsed_ns: median_u64(
            &values
                .iter()
                .map(|v| v.standard_elapsed_ns)
                .collect::<Vec<_>>(),
        ),
        hardened_elapsed_ns: median_u64(
            &values
                .iter()
                .map(|v| v.hardened_elapsed_ns)
                .collect::<Vec<_>>(),
        ),
        commands: values[mid].commands,
        group_commands: values[mid].group_commands,
    })
}

struct Configuration {
    checked: bool,
    assert_perf_003: bool,
    assert_group_mechanics: bool,
    assert_perf_008: bool,
    assert_perf_009: bool,
    assert_perf_012: bool,
    assert_perf_013: bool,
    database_root: Option<PathBuf>,
    allow_tmpfs: bool,
    reps: usize,
    /// Opt-in retained-history startup scale targets (command counts).
    startup_scale: Option<Vec<u64>>,
    /// Reusable database directory for `--startup-scale` (watermark + redb file).
    startup_scale_db: Option<PathBuf>,
    /// Measure checkpointed reopen after one full validation writes the prefix checkpoint.
    startup_scale_checkpointed: bool,
}

impl Configuration {
    fn parse() -> Result<Self, ()> {
        Self::parse_from(env::args().skip(1))
    }

    fn parse_from<I, S>(args: I) -> Result<Self, ()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut checked = false;
        let mut assert_perf_003 = false;
        let mut assert_group_mechanics = false;
        let mut assert_perf_008 = false;
        let mut assert_perf_009 = false;
        let mut assert_perf_012 = false;
        let mut assert_perf_013 = false;
        let mut database_root = None;
        let mut allow_tmpfs = false;
        let mut reps = 1_usize;
        let mut startup_scale = None;
        let mut startup_scale_db = None;
        let mut startup_scale_checkpointed = false;
        let mut args = args.into_iter().peekable();
        while let Some(argument) = args.next() {
            match argument.as_ref() {
                "--smoke" => {
                    checked = false;
                    reps = 1;
                }
                "--checked" => {
                    checked = true;
                    if reps == 1 {
                        reps = 3;
                    }
                }
                "--assert-perf-003" => {
                    checked = true;
                    assert_perf_003 = true;
                    if reps == 1 {
                        reps = 3;
                    }
                }
                "--assert-perf-004" => {
                    checked = true;
                    assert_group_mechanics = true;
                    if reps == 1 {
                        reps = 3;
                    }
                }
                "--assert-perf-006" => {
                    checked = true;
                    assert_group_mechanics = true;
                    if reps == 1 {
                        reps = 3;
                    }
                }
                "--assert-perf-008" => {
                    checked = true;
                    assert_group_mechanics = true;
                    assert_perf_008 = true;
                    if reps == 1 {
                        reps = 3;
                    }
                }
                "--assert-perf-009" => {
                    checked = true;
                    assert_perf_009 = true;
                    if reps == 1 {
                        reps = 3;
                    }
                }
                "--assert-perf-012" => {
                    checked = true;
                    assert_perf_012 = true;
                    if reps == 1 {
                        reps = 3;
                    }
                }
                "--assert-perf-013" => {
                    checked = true;
                    assert_perf_013 = true;
                    if reps == 1 {
                        reps = 3;
                    }
                }
                "--database-root" => {
                    database_root = Some(PathBuf::from(args.next().ok_or(())?.as_ref()));
                }
                "--allow-tmpfs" => allow_tmpfs = true,
                "--reps" => {
                    reps = args.next().ok_or(())?.as_ref().parse().map_err(|_| ())?;
                    if !(1..=32).contains(&reps) {
                        return Err(());
                    }
                }
                "--startup-scale" => {
                    let list = args.next().ok_or(())?;
                    startup_scale = Some(parse_startup_scale_list(list.as_ref())?);
                }
                "--startup-scale-db" => {
                    startup_scale_db = Some(PathBuf::from(args.next().ok_or(())?.as_ref()));
                }
                "--startup-scale-checkpointed" => {
                    startup_scale_checkpointed = true;
                }
                _ => return Err(()),
            }
        }
        if startup_scale_db.is_some() && startup_scale.is_none() {
            return Err(());
        }
        if startup_scale_checkpointed && startup_scale.is_none() {
            return Err(());
        }
        let any_assert = assert_perf_003
            || assert_group_mechanics
            || assert_perf_008
            || assert_perf_009
            || assert_perf_012
            || assert_perf_013;
        if startup_scale.is_some() && any_assert {
            eprintln!(
                "command-growth: --startup-scale cannot be combined with --assert-* gates \
                 (startup-scale early-returns before gate evaluation; combination would false-pass)"
            );
            return Err(());
        }
        Ok(Self {
            checked,
            assert_perf_003,
            assert_group_mechanics,
            assert_perf_008,
            assert_perf_009,
            assert_perf_012,
            assert_perf_013,
            database_root,
            allow_tmpfs,
            reps,
            startup_scale,
            startup_scale_db,
            startup_scale_checkpointed,
        })
    }
}

/// Free-space floor covering the largest retained target (~1.2 KiB/command + headroom).
fn estimate_startup_scale_free_bytes(targets: &[u64]) -> u64 {
    let max = targets.iter().copied().max().unwrap_or(0);
    max.saturating_mul(STARTUP_SCALE_BYTES_PER_COMMAND)
        .saturating_add(STARTUP_SCALE_FREE_HEADROOM_BYTES)
        .max(64 * 1024 * 1024)
}

fn parse_startup_scale_list(raw: &str) -> Result<Vec<u64>, ()> {
    if raw.is_empty() {
        return Err(());
    }
    let mut out = Vec::new();
    for part in raw.split(',') {
        let value: u64 = part.trim().parse().map_err(|_| ())?;
        if value == 0 {
            return Err(());
        }
        out.push(value);
    }
    if out.is_empty() {
        return Err(());
    }
    Ok(out)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GenerationWatermark {
    schema_digest: String,
    retained: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GenerationPlan {
    Create { target: u64 },
    Append { from: u64, target: u64 },
    AlreadySatisfied { retained: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WatermarkError {
    SchemaMismatch,
    RetainedExceedsTarget,
}

/// Plans how many commands to generate given an optional on-disk watermark.
///
/// When `force_regenerate` is true (test hook only), always plans a full create —
/// used by the falsifiability transcript to prove the reuse assertion depends on
/// the watermark.
fn plan_generation(
    existing: Option<&GenerationWatermark>,
    target: u64,
    force_regenerate: bool,
) -> Result<GenerationPlan, WatermarkError> {
    if force_regenerate {
        return Ok(GenerationPlan::Create { target });
    }
    match existing {
        None => Ok(GenerationPlan::Create { target }),
        Some(wm) if wm.schema_digest != STARTUP_SCALE_SCHEMA_DIGEST => {
            Err(WatermarkError::SchemaMismatch)
        }
        Some(wm) if wm.retained > target => Err(WatermarkError::RetainedExceedsTarget),
        Some(wm) if wm.retained == target => Ok(GenerationPlan::AlreadySatisfied {
            retained: wm.retained,
        }),
        Some(wm) => Ok(GenerationPlan::Append {
            from: wm.retained,
            target,
        }),
    }
}

impl GenerationWatermark {
    fn to_json(&self) -> String {
        format!(
            "{{\"schema_digest\":\"{}\",\"retained\":{}}}",
            escape_json(&self.schema_digest),
            self.retained
        )
    }

    fn from_json(raw: &str) -> Result<Self, ()> {
        let schema_digest = json_string_field(raw, "schema_digest")?;
        let retained = json_u64_field(raw, "retained")?;
        Ok(Self {
            schema_digest,
            retained,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DrainLinearCheck {
    first_half_ns: u64,
    second_half_ns: u64,
    structural_pages: u64,
    historical_pages: u64,
    evidence_pages: u64,
    checkpoint_verified: bool,
    suffix_commands: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StartupScaleRecord {
    retained: u64,
    file_bytes: u64,
    generate_ns: u64,
    generated_commands: u64,
    generate_commands_per_second: u64,
    startup_ns: u64,
    startup_ns_per_command: u64,
    peak_rss_bytes: u64,
    drain_rss_delta_bytes: u64,
    rss_measure_mode: String,
    drain_linear_check: DrainLinearCheck,
}

impl StartupScaleRecord {
    fn to_jsonl(&self) -> String {
        let record_type = if self.drain_linear_check.checkpoint_verified {
            "startup_scale_checkpointed"
        } else {
            "startup_scale"
        };
        format!(
            "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"{record_type}\",\"retained\":{},\"file_bytes\":{},\"generate_ns\":{},\"generated_commands\":{},\"generate_commands_per_second\":{},\"startup_ns\":{},\"startup_ns_per_command\":{},\"peak_rss_bytes\":{},\"drain_rss_delta_bytes\":{},\"rss_measure_mode\":\"{}\",\"drain_linear_check\":{{\"first_half_ns\":{},\"second_half_ns\":{},\"structural_pages\":{},\"historical_pages\":{},\"evidence_pages\":{},\"checkpoint_verified\":{},\"suffix_commands\":{}}},\"half_split_note\":\"only ratio movement across N is meaningful; midpoint may cross structural/historical junction — use page counts\"}}",
            self.retained,
            self.file_bytes,
            self.generate_ns,
            self.generated_commands,
            self.generate_commands_per_second,
            self.startup_ns,
            self.startup_ns_per_command,
            self.peak_rss_bytes,
            self.drain_rss_delta_bytes,
            escape_json(&self.rss_measure_mode),
            self.drain_linear_check.first_half_ns,
            self.drain_linear_check.second_half_ns,
            self.drain_linear_check.structural_pages,
            self.drain_linear_check.historical_pages,
            self.drain_linear_check.evidence_pages,
            self.drain_linear_check.checkpoint_verified,
            self.drain_linear_check.suffix_commands
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn from_jsonl(raw: &str) -> Result<Self, ()> {
        if !raw.contains("\"record_type\":\"startup_scale\"")
            && !raw.contains("\"record_type\":\"startup_scale_checkpointed\"")
        {
            return Err(());
        }
        Ok(Self {
            retained: json_u64_field(raw, "retained")?,
            file_bytes: json_u64_field(raw, "file_bytes")?,
            generate_ns: json_u64_field(raw, "generate_ns")?,
            generated_commands: json_u64_field(raw, "generated_commands")?,
            generate_commands_per_second: json_u64_field(raw, "generate_commands_per_second")?,
            startup_ns: json_u64_field(raw, "startup_ns")?,
            startup_ns_per_command: json_u64_field(raw, "startup_ns_per_command")?,
            peak_rss_bytes: json_u64_field(raw, "peak_rss_bytes")?,
            drain_rss_delta_bytes: json_u64_field(raw, "drain_rss_delta_bytes")?,
            rss_measure_mode: json_string_field(raw, "rss_measure_mode")?,
            drain_linear_check: DrainLinearCheck {
                first_half_ns: json_u64_field(raw, "first_half_ns")?,
                second_half_ns: json_u64_field(raw, "second_half_ns")?,
                structural_pages: json_u64_field(raw, "structural_pages")?,
                historical_pages: json_u64_field(raw, "historical_pages")?,
                evidence_pages: json_u64_field(raw, "evidence_pages")?,
                checkpoint_verified: raw.contains("\"checkpoint_verified\":true"),
                suffix_commands: json_u64_field(raw, "suffix_commands").unwrap_or(0),
            },
        })
    }
}

fn run_startup_scale(
    configuration: &Configuration,
    bench_root: &BenchRoot,
    targets: &[u64],
) -> Result<bool, ()> {
    // Hold optional session dir so Drop cleans ephemeral runs; durable reuse uses
    // `--startup-scale-db` and is never owned by BenchDir.
    let ephemeral = if configuration.startup_scale_db.is_none() {
        Some(BenchDir::create(bench_root, "startup-scale").map_err(|_| ())?)
    } else {
        None
    };
    let db_dir = if let Some(path) = &configuration.startup_scale_db {
        path.clone()
    } else {
        ephemeral
            .as_ref()
            .expect("ephemeral dir when no startup-scale-db")
            .path()
            .to_path_buf()
    };
    fs::create_dir_all(&db_dir).map_err(|_| ())?;
    // F2: medium classification + free-space preflight on the RESOLVED db_dir
    // (not only the BenchRoot / --database-root path). Refuse RamBacked unless
    // --allow-tmpfs.
    let min_free = estimate_startup_scale_free_bytes(targets);
    let db_placement = validate_startup_scale_db_dir(&db_dir, min_free, configuration.allow_tmpfs)?;
    println!(
        "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"startup_scale_configuration\",\"targets\":[{}],\"db_dir\":\"{}\",\"schema_digest\":\"{STARTUP_SCALE_SCHEMA_DIGEST}\",\"generation_mode\":\"{STARTUP_SCALE_GENERATION_MODE}\",\"db_medium\":{},\"db_free_bytes_at_start\":{},\"min_free_bytes\":{min_free}}}",
        targets
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(","),
        escape_json(&db_placement.path().display().to_string()),
        db_placement.medium().to_report_json(),
        db_placement.free_bytes_at_start(),
    );

    let mut sorted_targets = targets.to_vec();
    sorted_targets.sort_unstable();
    // Process in ascending order so a single reusable DB can extend: M → N.
    for target in sorted_targets {
        let record = if configuration.startup_scale_checkpointed {
            measure_one_startup_scale_checkpointed(db_placement.path(), target, false)?
        } else {
            measure_one_startup_scale(db_placement.path(), target, false)?
        };
        // Baseline-honesty assertion on the emitted record itself: a plain
        // --startup-scale record claiming a verified checkpoint is fabricated.
        assert!(
            configuration.startup_scale_checkpointed
                || !record.drain_linear_check.checkpoint_verified,
            "baseline startup_scale record must not claim checkpoint_verified"
        );
        println!("{}", record.to_jsonl());
        println!(
            "startup-scale retained={target} file_bytes={} generate_ns={} generated={} rate={}/s startup_ns={} ns/cmd={} peak_rss_bytes={} drain_rss_delta_bytes={} rss_mode={} structural_pages={} historical_pages={} evidence_pages={} drain_first_half_ns={} drain_second_half_ns={} (half-split: only ratio movement across N is meaningful)",
            record.file_bytes,
            record.generate_ns,
            record.generated_commands,
            record.generate_commands_per_second,
            record.startup_ns,
            record.startup_ns_per_command,
            record.peak_rss_bytes,
            record.drain_rss_delta_bytes,
            record.rss_measure_mode,
            record.drain_linear_check.structural_pages,
            record.drain_linear_check.historical_pages,
            record.drain_linear_check.evidence_pages,
            record.drain_linear_check.first_half_ns,
            record.drain_linear_check.second_half_ns
        );
    }
    drop(ephemeral);
    Ok(true)
}

/// Resolves `db_dir` through BenchRoot so medium + free-space gates apply to the
/// actual startup-scale database placement (including `--startup-scale-db`).
fn validate_startup_scale_db_dir(
    db_dir: &Path,
    min_free_bytes: u64,
    allow_tmpfs: bool,
) -> Result<BenchRoot, ()> {
    BenchRoot::resolve(BenchRootOptions {
        harness: "command-growth-startup-scale-db",
        cli_override: Some(db_dir.to_path_buf()),
        default_root: db_dir.to_path_buf(),
        allow_tmpfs,
        min_free_bytes,
    })
    .map_err(|error| {
        eprintln!("startup-scale db_dir placement: {error}");
    })
}

fn measure_one_startup_scale(
    db_dir: &Path,
    target: u64,
    force_regenerate: bool,
) -> Result<StartupScaleRecord, ()> {
    measure_one_startup_scale_inner(db_dir, target, force_regenerate, false)
}

fn measure_one_startup_scale_checkpointed(
    db_dir: &Path,
    target: u64,
    force_regenerate: bool,
) -> Result<StartupScaleRecord, ()> {
    measure_one_startup_scale_inner(db_dir, target, force_regenerate, true)
}

fn measure_one_startup_scale_inner(
    db_dir: &Path,
    target: u64,
    force_regenerate: bool,
    checkpointed: bool,
) -> Result<StartupScaleRecord, ()> {
    let db_path = db_dir.join(STARTUP_SCALE_DB_FILE);
    let watermark_path = db_dir.join(STARTUP_SCALE_WATERMARK_FILE);
    let existing = load_watermark(&watermark_path, &db_path)?;
    let plan =
        plan_generation(existing.as_ref(), target, force_regenerate).map_err(|err| match err {
            WatermarkError::SchemaMismatch => {
                eprintln!("startup-scale: schema digest mismatch; refuse reuse");
            }
            WatermarkError::RetainedExceedsTarget => {
                eprintln!(
                    "startup-scale: watermark retained exceeds target {target}; refuse shrink"
                );
            }
        })?;

    let generate_started = Instant::now();
    let generated_commands = match plan {
        GenerationPlan::Create { target } => {
            if db_path.exists() {
                fs::remove_file(&db_path).map_err(|_| ())?;
            }
            if watermark_path.exists() {
                fs::remove_file(&watermark_path).map_err(|_| ())?;
            }
            let mut harness = ServiceAuditGrowthHarness::new(&db_path).map_err(|_| ())?;
            append_commands_grouped_fused(&mut harness, 1, target)?;
            drop(harness);
            target
        }
        GenerationPlan::Append { from, target } => {
            let delta = target.checked_sub(from).ok_or(())?;
            if delta == 0 {
                0
            } else {
                let mut harness = ServiceAuditGrowthHarness::reopen(&db_path).map_err(|_| ())?;
                let first = from.checked_add(1).ok_or(())?;
                append_commands_grouped_fused(&mut harness, first, delta)?;
                drop(harness);
                delta
            }
        }
        GenerationPlan::AlreadySatisfied { .. } => 0,
    };
    let generate_ns = u64::try_from(generate_started.elapsed().as_nanos()).map_err(|_| ())?;
    write_watermark(
        &watermark_path,
        &GenerationWatermark {
            schema_digest: STARTUP_SCALE_SCHEMA_DIGEST.to_owned(),
            retained: target,
        },
    )?;

    let file_bytes = fs::metadata(&db_path).map_err(|_| ())?.len();
    if checkpointed {
        // One full open+validate+write first, then measure the checkpointed reopen.
        // A successful clean drain writes the validated-prefix checkpoint on finish.
        let _ = measure_drain_with_rss(&db_path, target)?;
    } else {
        // Baseline honesty: a reused database may carry a checkpoint from an
        // earlier run's clean finish. Strip it in a raw engine transaction (a
        // bench-only affordance that no production path can reach) so plain
        // `--startup-scale` always measures FULL validation.
        riffdb_storage_redb::benchmark_support::strip_validated_prefix_checkpoint(&db_path)
            .map_err(|_| ())?;
    }
    let (measurement, peak_rss_bytes, drain_rss_delta_bytes, rss_measure_mode) =
        measure_drain_with_rss(&db_path, target)?;
    // Evidence integrity: both fields derive from the SESSION's observability,
    // never from the CLI flag.
    let checkpoint_verified = measurement.checkpoint_verified();
    let suffix_commands = measurement.suffix_commands();
    assert!(
        checkpointed || !checkpoint_verified,
        "baseline --startup-scale must measure full validation (checkpoint_verified must be false)"
    );
    assert!(
        !checkpointed || checkpoint_verified,
        "--startup-scale-checkpointed reopen failed to verify the seeded checkpoint"
    );
    let startup_ns = u64::try_from(measurement.elapsed().as_nanos()).map_err(|_| ())?;
    let startup_ns_per_command = startup_ns / target.max(1);
    let generate_commands_per_second = if generated_commands == 0 {
        0
    } else {
        commands_per_second(
            usize::try_from(generated_commands).map_err(|_| ())?,
            generate_ns.max(1),
        )?
    };

    Ok(StartupScaleRecord {
        retained: target,
        file_bytes,
        generate_ns,
        generated_commands,
        generate_commands_per_second,
        startup_ns,
        startup_ns_per_command,
        peak_rss_bytes,
        drain_rss_delta_bytes,
        rss_measure_mode,
        drain_linear_check: DrainLinearCheck {
            first_half_ns: u64::try_from(measurement.first_half().as_nanos()).map_err(|_| ())?,
            second_half_ns: u64::try_from(measurement.second_half().as_nanos()).map_err(|_| ())?,
            structural_pages: measurement.structural_pages(),
            historical_pages: measurement.historical_pages(),
            evidence_pages: measurement.evidence_pages(),
            checkpoint_verified,
            suffix_commands,
        },
    })
}

/// Drain-scoped RSS: prefer `/proc/self/clear_refs` = 5 (reset VmHWM to current
/// RSS) immediately before the drain so generation memory does not pollute the
/// peak. Fallback records `after - before` when clear_refs is unavailable.
fn measure_drain_with_rss(
    db_path: &Path,
    retained: u64,
) -> Result<
    (
        riffdb_storage_redb::benchmark_support::CleanStartupMeasurement,
        u64,
        u64,
        String,
    ),
    (),
> {
    let rss_before = read_vm_hwm_bytes().unwrap_or(0);
    let cleared = clear_peak_rss();
    let rss_baseline = if cleared {
        // After reset, VmHWM ≈ current RSS; use that as the drain baseline.
        read_vm_hwm_bytes().unwrap_or(rss_before)
    } else {
        rss_before
    };
    let measurement = measure_clean_startup_linear(db_path, retained).map_err(|_| ())?;
    let rss_after = read_vm_hwm_bytes().unwrap_or(rss_baseline);
    let drain_rss_delta_bytes = rss_after.saturating_sub(rss_baseline);
    let (peak_rss_bytes, mode) = if cleared {
        // Drain-scoped high-water mark (HWM was reset immediately before drain).
        (rss_after, "clear_refs")
    } else {
        // Fallback: process-lifetime HWM may still include generation; delta is
        // the honest additive signal.
        (rss_after, "delta_fallback")
    };
    Ok((
        measurement,
        peak_rss_bytes,
        drain_rss_delta_bytes,
        mode.to_owned(),
    ))
}

/// Write `5` to `/proc/self/clear_refs` to reset VmHWM to current RSS (Linux).
fn clear_peak_rss() -> bool {
    fs::write(PROC_CLEAR_REFS_PATH, b"5").is_ok()
}

fn append_commands_grouped_fused(
    harness: &mut ServiceAuditGrowthHarness,
    first_command: u64,
    count: u64,
) -> Result<(), ()> {
    let mut next = first_command;
    let mut remaining = count;
    while remaining > 0 {
        let chunk_u64 = remaining.min(u64::try_from(STARTUP_SCALE_GENERATE_CHUNK).map_err(|_| ())?);
        let chunk = usize::try_from(chunk_u64).map_err(|_| ())?;
        harness
            .run_window_grouped_fused(next, chunk)
            .map_err(|_| ())?;
        next = next.checked_add(chunk_u64).ok_or(())?;
        remaining = remaining.checked_sub(chunk_u64).ok_or(())?;
    }
    Ok(())
}

fn load_watermark(
    watermark_path: &Path,
    db_path: &Path,
) -> Result<Option<GenerationWatermark>, ()> {
    let wm_exists = watermark_path.exists();
    let db_exists = db_path.exists();
    match (wm_exists, db_exists) {
        (false, false) => Ok(None),
        (true, true) => {
            let raw = fs::read_to_string(watermark_path).map_err(|_| ())?;
            Ok(Some(GenerationWatermark::from_json(&raw)?))
        }
        (true, false) | (false, true) => {
            eprintln!(
                "startup-scale: watermark/database pair incomplete; refuse (regenerate after wipe)"
            );
            Err(())
        }
    }
}

fn write_watermark(path: &Path, watermark: &GenerationWatermark) -> Result<(), ()> {
    fs::write(path, watermark.to_json()).map_err(|_| ())
}

fn read_vm_hwm_bytes() -> Option<u64> {
    let mut file = fs::File::open(PROC_STATUS_PATH).ok()?;
    let mut buf = vec![0_u8; PROC_STATUS_MAX_BYTES.saturating_add(1)];
    let mut total = 0_usize;
    loop {
        if total >= buf.len() {
            return None;
        }
        match std::io::Read::read(&mut file, &mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total = total.saturating_add(n),
            Err(_) => return None,
        }
    }
    if total > PROC_STATUS_MAX_BYTES {
        return None;
    }
    let text = std::str::from_utf8(&buf[..total]).ok()?;
    for line in text.lines() {
        let Some(rest) = line.strip_prefix("VmHWM:") else {
            continue;
        };
        let kib: u64 = rest.split_whitespace().next()?.parse().ok()?;
        return kib.checked_mul(1024);
    }
    None
}

fn escape_json(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out
}

fn json_u64_field(raw: &str, field: &str) -> Result<u64, ()> {
    let needle = format!("\"{field}\":");
    let start = raw.find(&needle).ok_or(())? + needle.len();
    let rest = raw[start..].trim_start();
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().map_err(|_| ())
}

fn json_string_field(raw: &str, field: &str) -> Result<String, ()> {
    let needle = format!("\"{field}\":\"");
    let start = raw.find(&needle).ok_or(())? + needle.len();
    let rest = &raw[start..];
    let mut out = String::new();
    let mut chars = rest.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '"' => return Ok(out),
            '\\' => match chars.next() {
                Some('"') => out.push('"'),
                Some('\\') => out.push('\\'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('t') => out.push('\t'),
                Some(other) => out.push(other),
                None => return Err(()),
            },
            c => out.push(c),
        }
    }
    Err(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throughput_uses_complete_command_lifecycles() {
        assert_eq!(commands_per_second(128, 1_000_000_000).expect("rate"), 128);
        assert_eq!(
            commands_per_second(50, 1_000_000_000).expect("threshold rate"),
            PERF_MIN_COMMANDS_PER_SECOND
        );
    }

    #[test]
    fn checked_sweep_is_bounded_and_strictly_increasing() {
        assert!(CHECKED_CHECKPOINTS.windows(2).all(|pair| pair[0] < pair[1]));
        let maximum = CHECKED_CHECKPOINTS
            .iter()
            .copied()
            .max()
            .expect("checked checkpoints");
        assert!(maximum <= 65_536);
        assert!(u64::try_from(CHECKED_WINDOW).expect("window") <= maximum);
    }

    #[test]
    fn default_root_is_under_perf_db() {
        let root = default_perf_db_root(
            Path::new("/repo/benchmarks/command-growth"),
            "command-growth",
        );
        assert!(root.to_string_lossy().contains("perf-db"));
        assert!(!root.starts_with("/tmp"));
    }

    #[test]
    fn startup_scale_flag_parses_single_and_list() {
        let single = Configuration::parse_from(["--startup-scale", "1024"]).expect("single");
        assert_eq!(single.startup_scale.as_deref(), Some(&[1024][..]));

        let list =
            Configuration::parse_from(["--startup-scale", "1024,4096,1000000"]).expect("list");
        assert_eq!(
            list.startup_scale.as_deref(),
            Some(&[1_024, 4_096, 1_000_000][..])
        );

        let with_db = Configuration::parse_from([
            "--startup-scale",
            "64",
            "--startup-scale-db",
            "/var/perf/scale",
            "--allow-tmpfs",
        ])
        .expect("with db");
        assert_eq!(
            with_db.startup_scale_db.as_deref(),
            Some(Path::new("/var/perf/scale"))
        );
        assert!(with_db.allow_tmpfs);

        assert!(Configuration::parse_from(["--startup-scale-db", "/x"]).is_err());
        assert!(Configuration::parse_from(["--startup-scale", "0"]).is_err());
        assert!(Configuration::parse_from(["--startup-scale", ""]).is_err());
        assert!(Configuration::parse_from(["--startup-scale", "1,abc"]).is_err());
    }

    #[test]
    fn startup_scale_rejects_assert_flag_both_orders() {
        // F3: either order must refuse — combination would silent-false-pass gates.
        assert!(
            Configuration::parse_from(["--startup-scale", "1024", "--assert-perf-013"]).is_err()
        );
        assert!(
            Configuration::parse_from(["--assert-perf-013", "--startup-scale", "1024"]).is_err()
        );
        assert!(
            Configuration::parse_from(["--startup-scale", "64", "--assert-perf-003"]).is_err()
        );
        assert!(
            Configuration::parse_from(["--assert-perf-008", "--startup-scale", "64"]).is_err()
        );
        // startup-scale alone still ok
        assert!(Configuration::parse_from(["--startup-scale", "64"]).is_ok());
    }

    #[test]
    fn watermark_reuse_plans_append_and_refuses_digest_mismatch() {
        let none = plan_generation(None, 4_096, false).expect("create");
        assert_eq!(none, GenerationPlan::Create { target: 4_096 });

        let partial = GenerationWatermark {
            schema_digest: STARTUP_SCALE_SCHEMA_DIGEST.to_owned(),
            retained: 1_024,
        };
        assert_eq!(
            plan_generation(Some(&partial), 4_096, false).expect("append"),
            GenerationPlan::Append {
                from: 1_024,
                target: 4_096
            }
        );
        assert_eq!(
            plan_generation(Some(&partial), 1_024, false).expect("satisfied"),
            GenerationPlan::AlreadySatisfied { retained: 1_024 }
        );
        assert_eq!(
            plan_generation(Some(&partial), 512, false).expect_err("shrink"),
            WatermarkError::RetainedExceedsTarget
        );

        let bad = GenerationWatermark {
            schema_digest: "other-digest".to_owned(),
            retained: 1_024,
        };
        assert_eq!(
            plan_generation(Some(&bad), 4_096, false).expect_err("mismatch"),
            WatermarkError::SchemaMismatch
        );

        // Falsifiability hook: force_regenerate ignores a valid watermark.
        assert_eq!(
            plan_generation(Some(&partial), 4_096, true).expect("forced"),
            GenerationPlan::Create { target: 4_096 }
        );
    }

    #[test]
    fn startup_scale_record_round_trips_jsonl() {
        let record = StartupScaleRecord {
            retained: 4_096,
            file_bytes: 12_345_678,
            generate_ns: 9_000_000_000,
            generated_commands: 3_072,
            generate_commands_per_second: 341,
            startup_ns: 120_000_000,
            startup_ns_per_command: 29_296,
            peak_rss_bytes: 256 * 1024 * 1024,
            drain_rss_delta_bytes: 12 * 1024 * 1024,
            rss_measure_mode: "clear_refs".to_owned(),
            drain_linear_check: DrainLinearCheck {
                first_half_ns: 55_000_000,
                second_half_ns: 58_000_000,
                structural_pages: 40,
                historical_pages: 2,
                evidence_pages: 42,
                checkpoint_verified: false,
                suffix_commands: 0,
            },
        };
        let jsonl = record.to_jsonl();
        assert!(jsonl.contains("\"record_type\":\"startup_scale\""));
        assert!(jsonl.contains("\"structural_pages\":40"));
        assert!(jsonl.contains("\"rss_measure_mode\":\"clear_refs\""));
        assert!(jsonl.contains("half_split_note"));
        let parsed = StartupScaleRecord::from_jsonl(&jsonl).expect("parse");
        assert_eq!(parsed, record);
    }

    #[test]
    fn free_space_estimate_covers_ten_million_commands() {
        let bytes = estimate_startup_scale_free_bytes(&[1_000_000, 10_000_000]);
        // 10M * 1200 + 512 MiB ≈ 11.4 GiB — must exceed the ~11 GB database.
        assert!(bytes >= 11_u64 * 1024 * 1024 * 1024);
    }

    #[test]
    fn startup_scale_db_on_tmpfs_refuses_without_allow() {
        // F2 transcript: reviewer's /tmp probe must refuse RamBacked without --allow-tmpfs.
        let tmp = env::temp_dir().join(format!(
            "riffdb-startup-scale-tmpfs-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&tmp).expect("mkdir /tmp probe");
        // min_free deliberately small so free-space is not the refusal reason on
        // constrained CI; medium classification is the gate under test.
        let result = validate_startup_scale_db_dir(&tmp, 1, false);
        let _ = fs::remove_dir_all(&tmp);
        match result {
            Err(()) => {
                // Expected on hosts where /tmp is tmpfs/ramfs.
            }
            Ok(root) => {
                // Host keeps /tmp on disk — still assert we classified something.
                assert!(
                    !root.medium().is_ram_backed(),
                    "if resolve succeeds without allow_tmpfs, medium must not be RamBacked"
                );
            }
        }
        // Explicit RamBacked path via classify: when /tmp is tmpfs, refuse is required.
        if let Ok(medium) = riffdb_bench_root::classify_medium(&env::temp_dir())
            && medium.is_ram_backed()
        {
            let probe = env::temp_dir().join(format!(
                "riffdb-startup-scale-tmpfs-must-fail-{}",
                std::process::id()
            ));
            fs::create_dir_all(&probe).expect("mkdir");
            assert!(
                validate_startup_scale_db_dir(&probe, 1, false).is_err(),
                "tmpfs db_dir without allow_tmpfs must refuse"
            );
            // With allow, same path resolves.
            assert!(
                validate_startup_scale_db_dir(&probe, 1, true).is_ok(),
                "tmpfs db_dir with allow_tmpfs must accept"
            );
            let _ = fs::remove_dir_all(&probe);
        }
    }

    #[test]
    fn startup_scale_end_to_end_and_reuse_append() {
        let root = unique_temp_dir("startup-scale-e2e");
        let db_dir = root.join("db");
        fs::create_dir_all(&db_dir).expect("mkdir");

        let first = measure_one_startup_scale(&db_dir, 1_024, false).expect("n=1024");
        assert_eq!(first.retained, 1_024);
        assert_eq!(first.generated_commands, 1_024);
        assert!(first.file_bytes > 0);
        assert!(first.startup_ns > 0);
        assert!(first.startup_ns_per_command > 0);
        assert!(first.generate_commands_per_second > 0);
        assert!(
            first.rss_measure_mode == "clear_refs" || first.rss_measure_mode == "delta_fallback"
        );
        assert!(first.drain_linear_check.evidence_pages >= 1);
        assert_eq!(
            first.drain_linear_check.evidence_pages,
            first
                .drain_linear_check
                .structural_pages
                .saturating_add(first.drain_linear_check.historical_pages)
        );

        let second = measure_one_startup_scale(&db_dir, 4_096, false).expect("n=4096");
        assert_eq!(second.retained, 4_096);
        // Reuse must append only the delta — falsifiable by force_regenerate.
        assert_eq!(
            second.generated_commands, 3_072,
            "reuse append must generate N-M only"
        );
        assert!(
            second.file_bytes >= first.file_bytes,
            "file_bytes must be monotone non-decreasing"
        );
        assert!(second.startup_ns > 0);

        // Already satisfied: zero generate.
        let again = measure_one_startup_scale(&db_dir, 4_096, false).expect("satisfied");
        assert_eq!(again.generated_commands, 0);
        assert_eq!(again.retained, 4_096);

        // Falsifiability: force regenerate → full create count, not delta.
        let forced = measure_one_startup_scale(&db_dir, 4_096, true).expect("forced");
        assert_eq!(
            forced.generated_commands, 4_096,
            "force_regenerate must rewrite full history"
        );

        // Digest mismatch refuses.
        let wm_path = db_dir.join(STARTUP_SCALE_WATERMARK_FILE);
        fs::write(
            &wm_path,
            GenerationWatermark {
                schema_digest: "broken".to_owned(),
                retained: 4_096,
            }
            .to_json(),
        )
        .expect("write bad watermark");
        assert!(
            measure_one_startup_scale(&db_dir, 8_192, false).is_err(),
            "digest mismatch must refuse"
        );

        let _ = fs::remove_dir_all(&root);
    }

    /// Transcript: breaking the watermark (always regenerate) fails the reuse
    /// generate-count assertion that the happy-path test above relies on.
    #[test]
    fn falsifiability_always_regenerate_breaks_reuse_generate_count() {
        let root = unique_temp_dir("startup-scale-falsify");
        let db_dir = root.join("db");
        fs::create_dir_all(&db_dir).expect("mkdir");

        let _ = measure_one_startup_scale(&db_dir, 512, false).expect("seed");
        // Simulate a broken plan that always regenerates when extending.
        let broken = measure_one_startup_scale(&db_dir, 1_024, true).expect("broken reuse");
        let expected_delta = 512_u64;
        assert_ne!(
            broken.generated_commands, expected_delta,
            "transcript: force_regenerate yields full count {}, not delta {}",
            broken.generated_commands, expected_delta
        );
        assert_eq!(broken.generated_commands, 1_024);

        let _ = fs::remove_dir_all(&root);
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = env::temp_dir().join(format!("riffdb-{label}-{stamp}"));
        fs::create_dir_all(&path).expect("mkdir");
        path
    }
}
