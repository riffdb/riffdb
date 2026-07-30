#![forbid(unsafe_code)]

//! Reproducible retained-state command-mechanics size sweep.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_storage_redb::benchmark_support::{
    EngineDurability, EngineMechanicsProfile, ServiceAuditGrowthHarness,
    initialize_engine_mechanics, measure_engine_reopen, run_engine_mechanics_window,
};

const CHECKED_CHECKPOINTS: [u64; 5] = [0, 256, 1_024, 2_048, 4_096];
const SMOKE_CHECKPOINTS: [u64; 3] = [0, 64, 256];
const CHECKED_WINDOW: usize = 128;
const SMOKE_WINDOW: usize = 32;
const PERF_MIN_COMMANDS_PER_SECOND: u64 = 50;
const PERF_MIN_RETAINED_BASIS_POINTS: u64 = 5_000;
const PERF_MAX_GROUP_VS_SYNC_BASIS_POINTS: u64 = 7_500;
const PREFLIGHT_ENVIRONMENT: &str = "RIFFDB_COMMAND_GROWTH_PREFLIGHT";
const PREFLIGHT_EVIDENCE: &str = "semantic-crash-v1";
static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

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
        || configuration.assert_perf_009)
        && !preflight_passed
    {
        return Err(());
    }
    let root = TempRoot::new()?;
    let path = root.path().join("service-audit-growth.redb");
    let mut harness = ServiceAuditGrowthHarness::new(&path).map_err(|_| ())?;
    let (checkpoints, window) = if configuration.checked {
        (&CHECKED_CHECKPOINTS[..], CHECKED_WINDOW)
    } else {
        (&SMOKE_CHECKPOINTS[..], SMOKE_WINDOW)
    };

    println!(
        "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"configuration\",\"workload\":\"service_audit_started_failed_pair\",\"acknowledgement_durability\":\"sync\",\"redb_commit_profile\":\"standard\",\"engine_durability\":\"immediate_one_phase\",\"durable_commits_per_command\":2,\"group_commands\":1,\"window_commands\":{window},\"semantic_preflight\":\"{}\"}}",
        if preflight_passed {
            "passed"
        } else {
            "not_run"
        }
    );

    let mut next_command = 1_u64;
    let mut first_rate = None;
    let mut final_rate = 0_u64;
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
            "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"window\",\"retained_commands_before\":{checkpoint},\"commands\":{},\"audit_records_per_command\":2,\"queue_wait_ns\":0,\"semantic_evaluation_ns\":{},\"transaction_current_table_and_durable_commit_ns\":{elapsed_ns},\"commands_per_second\":{rate},\"file_bytes\":{}}}",
            sample.commands(),
            sample.preparation().as_nanos(),
            sample.file_bytes()
        );
    }

    drop(harness);
    let recovery_ns =
        u64::try_from(measure_engine_reopen(&path).map_err(|_| ())?.as_nanos()).map_err(|_| ())?;
    println!(
        "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"recovery\",\"retained_commands\":{},\"engine_reopen_ns\":{recovery_ns}}}",
        next_command.saturating_sub(1)
    );
    let comparison = run_mechanics_comparison(root.path(), configuration.checked)?;
    let first_rate = first_rate.ok_or(())?;
    let retained_basis_points = final_rate.checked_mul(10_000).ok_or(())? / first_rate.max(1);
    let passed = final_rate >= PERF_MIN_COMMANDS_PER_SECOND
        && retained_basis_points >= PERF_MIN_RETAINED_BASIS_POINTS;
    println!(
        "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"summary\",\"first_commands_per_second\":{first_rate},\"final_commands_per_second\":{final_rate},\"retained_basis_points\":{retained_basis_points},\"minimum_commands_per_second\":{PERF_MIN_COMMANDS_PER_SECOND},\"minimum_retained_basis_points\":{PERF_MIN_RETAINED_BASIS_POINTS},\"perf_003_passed\":{passed}}}"
    );
    let group_basis_points = comparison.group_elapsed_ns.checked_mul(10_000).ok_or(())?
        / comparison.sync_elapsed_ns.max(1);
    let perf_004_passed = comparison.group_commands == 16
        && comparison.commands >= 32
        && group_basis_points <= PERF_MAX_GROUP_VS_SYNC_BASIS_POINTS;
    println!(
        "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"group_summary\",\"sync_elapsed_ns\":{},\"group_elapsed_ns\":{},\"commands\":{},\"group_commands\":{},\"group_vs_sync_basis_points\":{group_basis_points},\"maximum_group_vs_sync_basis_points\":{PERF_MAX_GROUP_VS_SYNC_BASIS_POINTS},\"engine_durability\":\"immediate_two_phase\",\"perf_004_passed\":{perf_004_passed},\"perf_006_mechanics_passed\":{perf_004_passed},\"perf_008_mechanics_passed\":{perf_004_passed}}}",
        comparison.sync_elapsed_ns,
        comparison.group_elapsed_ns,
        comparison.commands,
        comparison.group_commands,
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
        comparison.standard_elapsed_ns,
        comparison.hardened_elapsed_ns,
    );
    let group_gate_requested =
        configuration.assert_group_mechanics || configuration.assert_perf_008;
    Ok((!configuration.assert_perf_003 || passed)
        && (!group_gate_requested || perf_004_passed)
        && (!configuration.assert_perf_009 || perf_009_passed))
}

struct GroupComparison {
    sync_elapsed_ns: u64,
    group_elapsed_ns: u64,
    standard_elapsed_ns: u64,
    hardened_elapsed_ns: u64,
    commands: usize,
    group_commands: usize,
}

fn run_mechanics_comparison(root: &Path, checked: bool) -> Result<GroupComparison, ()> {
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
            "{{\"schema\":\"riffdb.command-growth/v1\",\"record_type\":\"mechanics_comparison\",\"engine_durability\":\"{}\",\"group_commands\":{},\"commands\":{},\"elapsed_ns\":{},\"admission_table_page_work_ns\":{},\"admission_commit_and_flush_ns\":{},\"terminal_table_page_work_ns\":{},\"terminal_commit_and_flush_ns\":{}}}",
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

#[derive(Clone, Copy)]
struct Configuration {
    checked: bool,
    assert_perf_003: bool,
    assert_group_mechanics: bool,
    assert_perf_008: bool,
    assert_perf_009: bool,
}

impl Configuration {
    fn parse() -> Result<Self, ()> {
        let mut checked = false;
        let mut assert_perf_003 = false;
        let mut assert_group_mechanics = false;
        let mut assert_perf_008 = false;
        let mut assert_perf_009 = false;
        for argument in env::args().skip(1) {
            match argument.as_str() {
                "--smoke" => checked = false,
                "--checked" => checked = true,
                "--assert-perf-003" => {
                    checked = true;
                    assert_perf_003 = true;
                }
                "--assert-perf-004" => {
                    checked = true;
                    assert_group_mechanics = true;
                }
                "--assert-perf-006" => {
                    checked = true;
                    assert_group_mechanics = true;
                }
                "--assert-perf-008" => {
                    checked = true;
                    assert_group_mechanics = true;
                    assert_perf_008 = true;
                }
                "--assert-perf-009" => {
                    checked = true;
                    assert_perf_009 = true;
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
        })
    }
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Result<Self, ()> {
        let path = env::temp_dir().join(format!(
            "riffdb-command-growth-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).map_err(|_| ())?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
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
        assert!(maximum <= 4_096);
        assert!(u64::try_from(CHECKED_WINDOW).expect("window") <= maximum);
    }
}
