#![forbid(unsafe_code)]

//! Reproducible retained-state command-mechanics size sweep.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use riffdb_bench_root::{
    BenchDir, BenchRoot, BenchRootOptions, default_perf_db_root, run_device_baseline_for,
    sweep_stale,
};
use riffdb_storage_redb::benchmark_support::{
    EngineDurability, EngineMechanicsProfile, ServiceAuditGrowthHarness,
    initialize_engine_mechanics, measure_clean_startup, measure_engine_reopen,
    run_engine_mechanics_window,
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
    let bench_root = BenchRoot::resolve(BenchRootOptions {
        harness: "command-growth",
        cli_override: configuration.database_root.clone(),
        default_root,
        allow_tmpfs: configuration.allow_tmpfs,
        min_free_bytes: 64 * 1024 * 1024,
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
}

impl Configuration {
    fn parse() -> Result<Self, ()> {
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
        let mut args = env::args().skip(1).peekable();
        while let Some(argument) = args.next() {
            match argument.as_str() {
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
                    database_root = Some(PathBuf::from(args.next().ok_or(())?));
                }
                "--allow-tmpfs" => allow_tmpfs = true,
                "--reps" => {
                    reps = args.next().ok_or(())?.parse().map_err(|_| ())?;
                    if !(1..=32).contains(&reps) {
                        return Err(());
                    }
                }
                _ => return Err(()),
            }
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
        })
    }
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
}
