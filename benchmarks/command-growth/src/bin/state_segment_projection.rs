#![forbid(unsafe_code)]

//! WP-485 benchmark-only state-bearing command-segment mechanics proof.

use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use riffdb_bench_root::{BenchDir, BenchRoot, BenchRootOptions, default_perf_db_root, sweep_stale};
use riffdb_storage_redb::benchmark_support::{
    StateSegmentProjection, StateSegmentProjectionSample, StateSegmentWorkload,
    run_state_segment_projection_window,
};

const GROUP_SIZES: [usize; 4] = [1, 32, 128, 256];
const WORKLOADS: [StateSegmentWorkload; 3] = [
    StateSegmentWorkload::DistinctCreates,
    StateSegmentWorkload::RetainedUpdates,
    StateSegmentWorkload::MixedCreatesAndUpdates,
];
const PROJECTIONS: [StateSegmentProjection; 2] = [
    StateSegmentProjection::CurrentSegmentedAuthority,
    StateSegmentProjection::BenchmarkStateBearingSegment,
];
const COMMANDS: usize = 1_024;
const RETAINED_ENTITIES: usize = 4_096;
const DEFAULT_REPS: usize = 7;
const REQUIRED_MAX_PROPOSED_BASIS_POINTS: u64 = 5_000;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(()) => ExitCode::FAILURE,
    }
}

fn run() -> Result<(), ()> {
    let configuration = Configuration::parse()?;
    let default_root = default_perf_db_root(
        Path::new(env!("CARGO_MANIFEST_DIR")),
        "state-segment-projection",
    );
    let root = BenchRoot::resolve(BenchRootOptions {
        harness: "state-segment-projection",
        cli_override: configuration.database_root,
        default_root,
        allow_tmpfs: false,
        min_free_bytes: 256 * 1024 * 1024,
    })
    .map_err(|error| {
        eprintln!("state-segment-projection root: {error}");
    })?;
    let _ = sweep_stale(&root);
    let session = BenchDir::create(&root, "wp-485-state-segment").map_err(|_| ())?;
    println!(
        "{{\"schema\":\"riffdb.state-segment-projection/v1\",\"record_type\":\"environment\",\"body\":{}}}",
        root.environment_report_json()
    );
    println!(
        "{{\"schema\":\"riffdb.state-segment-projection/v1\",\"record_type\":\"configuration\",\"commands\":{COMMANDS},\"retained_entities\":{RETAINED_ENTITIES},\"group_sizes\":[1,32,128,256],\"reps\":{},\"durability\":\"none_plus_common_final_barrier\",\"minimum_table_work_speedup\":2.0,\"production_semantics_changed\":false}}",
        configuration.reps
    );

    let mut summaries = Vec::new();
    for workload in WORKLOADS {
        for group_commands in GROUP_SIZES {
            let retained_entities = if workload == StateSegmentWorkload::DistinctCreates {
                0
            } else {
                RETAINED_ENTITIES
            };
            let mut samples = Vec::new();
            for projection in PROJECTIONS {
                for rep in 0..configuration.reps {
                    let path = session.path().join(format!(
                        "{}-{}-g{group_commands}-r{rep}.redb",
                        workload.label(),
                        projection.label()
                    ));
                    let sample = run_state_segment_projection_window(
                        &path,
                        projection,
                        workload,
                        COMMANDS,
                        group_commands,
                        retained_entities,
                    )
                    .map_err(|error| {
                        eprintln!("state-segment-projection: {error}");
                    })?;
                    print_sample(rep, &sample);
                    if rep == 0 {
                        print_inventory(&sample);
                    }
                    samples.push(sample);
                }
            }
            let summary = summarize(workload, group_commands, &samples)?;
            println!("{}", summary.to_json(configuration.reps));
            summaries.push(summary);
        }
    }

    let required: Vec<_> = summaries
        .iter()
        .filter(|summary| matches!(summary.group_commands, 32 | 128))
        .collect();
    let passed = required.len() == WORKLOADS.len() * 2
        && required.iter().all(|summary| {
            summary.proposed_vs_current_basis_points <= REQUIRED_MAX_PROPOSED_BASIS_POINTS
        });
    println!(
        "{{\"schema\":\"riffdb.state-segment-projection/v1\",\"record_type\":\"decision_gate\",\"required_group_sizes\":[32,128],\"required_workloads\":[\"distinct_creates\",\"retained_updates\",\"mixed_75_create_25_update\"],\"maximum_proposed_vs_current_basis_points\":{REQUIRED_MAX_PROPOSED_BASIS_POINTS},\"minimum_speedup\":2.0,\"passed\":{passed},\"authority_note\":\"benchmark_only_no_production_semantics\"}}"
    );
    Ok(())
}

fn print_sample(rep: usize, sample: &StateSegmentProjectionSample) {
    let totals = inventory_totals(sample);
    println!(
        "{{\"schema\":\"riffdb.state-segment-projection/v1\",\"record_type\":\"sample\",\"rep\":{rep},\"projection\":\"{}\",\"workload\":\"{}\",\"commands\":{},\"group_commands\":{},\"retained_entities\":{},\"table_work_ns\":{},\"final_durable_barrier_ns\":{},\"elapsed_ns\":{},\"file_bytes_before\":{},\"file_bytes_after\":{},\"file_growth_bytes\":{},\"rows\":{},\"leaf_pages\":{},\"branch_pages\":{},\"stored_bytes\":{},\"metadata_bytes\":{},\"fragmented_bytes\":{},\"production_semantics_changed\":false}}",
        sample.projection().label(),
        sample.workload().label(),
        sample.commands(),
        sample.group_commands(),
        sample.retained_entities(),
        sample.table_work().as_nanos(),
        sample.final_durable_barrier().as_nanos(),
        sample.elapsed().as_nanos(),
        sample.file_bytes_before(),
        sample.file_bytes_after(),
        sample
            .file_bytes_after()
            .saturating_sub(sample.file_bytes_before()),
        totals.rows,
        totals.leaf_pages,
        totals.branch_pages,
        totals.stored_bytes,
        totals.metadata_bytes,
        totals.fragmented_bytes,
    );
}

fn print_inventory(sample: &StateSegmentProjectionSample) {
    for table in sample.inventory() {
        println!(
            "{{\"schema\":\"riffdb.state-segment-projection/v1\",\"record_type\":\"table_inventory\",\"projection\":\"{}\",\"workload\":\"{}\",\"group_commands\":{},\"table\":\"{}\",\"rows\":{},\"tree_height\":{},\"leaf_pages\":{},\"branch_pages\":{},\"stored_bytes\":{},\"metadata_bytes\":{},\"fragmented_bytes\":{}}}",
            sample.projection().label(),
            sample.workload().label(),
            sample.group_commands(),
            table.name(),
            table.rows(),
            table.tree_height(),
            table.leaf_pages(),
            table.branch_pages(),
            table.stored_bytes(),
            table.metadata_bytes(),
            table.fragmented_bytes(),
        );
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct InventoryTotals {
    rows: u64,
    leaf_pages: u64,
    branch_pages: u64,
    stored_bytes: u64,
    metadata_bytes: u64,
    fragmented_bytes: u64,
}

fn inventory_totals(sample: &StateSegmentProjectionSample) -> InventoryTotals {
    sample
        .inventory()
        .iter()
        .fold(InventoryTotals::default(), |mut total, table| {
            total.rows = total.rows.saturating_add(table.rows());
            total.leaf_pages = total.leaf_pages.saturating_add(table.leaf_pages());
            total.branch_pages = total.branch_pages.saturating_add(table.branch_pages());
            total.stored_bytes = total.stored_bytes.saturating_add(table.stored_bytes());
            total.metadata_bytes = total.metadata_bytes.saturating_add(table.metadata_bytes());
            total.fragmented_bytes = total
                .fragmented_bytes
                .saturating_add(table.fragmented_bytes());
            total
        })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProjectionSummary {
    workload: StateSegmentWorkload,
    group_commands: usize,
    current_table_work_ns: u64,
    proposed_table_work_ns: u64,
    proposed_vs_current_basis_points: u64,
    current_elapsed_ns: u64,
    proposed_elapsed_ns: u64,
    current_leaf_pages: u64,
    proposed_leaf_pages: u64,
    current_stored_bytes: u64,
    proposed_stored_bytes: u64,
    current_file_bytes: u64,
    proposed_file_bytes: u64,
}

impl ProjectionSummary {
    fn to_json(self, reps: usize) -> String {
        format!(
            "{{\"schema\":\"riffdb.state-segment-projection/v1\",\"record_type\":\"projection_summary\",\"workload\":\"{}\",\"group_commands\":{},\"commands\":{COMMANDS},\"reps\":{reps},\"current_table_work_ns\":{},\"proposed_table_work_ns\":{},\"proposed_vs_current_basis_points\":{},\"table_work_speedup\":{:.3},\"current_elapsed_ns\":{},\"proposed_elapsed_ns\":{},\"current_leaf_pages\":{},\"proposed_leaf_pages\":{},\"current_stored_bytes\":{},\"proposed_stored_bytes\":{},\"current_file_bytes\":{},\"proposed_file_bytes\":{},\"gate_applies\":{},\"gate_passed\":{},\"authority_note\":\"benchmark_only_proposed_layout\"}}",
            self.workload.label(),
            self.group_commands,
            self.current_table_work_ns,
            self.proposed_table_work_ns,
            self.proposed_vs_current_basis_points,
            self.current_table_work_ns as f64 / self.proposed_table_work_ns.max(1) as f64,
            self.current_elapsed_ns,
            self.proposed_elapsed_ns,
            self.current_leaf_pages,
            self.proposed_leaf_pages,
            self.current_stored_bytes,
            self.proposed_stored_bytes,
            self.current_file_bytes,
            self.proposed_file_bytes,
            matches!(self.group_commands, 32 | 128),
            !matches!(self.group_commands, 32 | 128)
                || self.proposed_vs_current_basis_points <= REQUIRED_MAX_PROPOSED_BASIS_POINTS,
        )
    }
}

fn summarize(
    workload: StateSegmentWorkload,
    group_commands: usize,
    samples: &[StateSegmentProjectionSample],
) -> Result<ProjectionSummary, ()> {
    let current = samples_for(samples, StateSegmentProjection::CurrentSegmentedAuthority);
    let proposed = samples_for(
        samples,
        StateSegmentProjection::BenchmarkStateBearingSegment,
    );
    if current.is_empty() || current.len() != proposed.len() {
        return Err(());
    }
    let current_table_work_ns = median(
        current
            .iter()
            .map(|sample| nanos(sample.table_work()))
            .collect(),
    )?;
    let proposed_table_work_ns = median(
        proposed
            .iter()
            .map(|sample| nanos(sample.table_work()))
            .collect(),
    )?;
    Ok(ProjectionSummary {
        workload,
        group_commands,
        current_table_work_ns,
        proposed_table_work_ns,
        proposed_vs_current_basis_points: proposed_table_work_ns.saturating_mul(10_000)
            / current_table_work_ns.max(1),
        current_elapsed_ns: median(
            current
                .iter()
                .map(|sample| nanos(sample.elapsed()))
                .collect(),
        )?,
        proposed_elapsed_ns: median(
            proposed
                .iter()
                .map(|sample| nanos(sample.elapsed()))
                .collect(),
        )?,
        current_leaf_pages: median(
            current
                .iter()
                .map(|sample| inventory_totals(sample).leaf_pages)
                .collect(),
        )?,
        proposed_leaf_pages: median(
            proposed
                .iter()
                .map(|sample| inventory_totals(sample).leaf_pages)
                .collect(),
        )?,
        current_stored_bytes: median(
            current
                .iter()
                .map(|sample| inventory_totals(sample).stored_bytes)
                .collect(),
        )?,
        proposed_stored_bytes: median(
            proposed
                .iter()
                .map(|sample| inventory_totals(sample).stored_bytes)
                .collect(),
        )?,
        current_file_bytes: median(current.iter().map(|v| v.file_bytes_after()).collect())?,
        proposed_file_bytes: median(proposed.iter().map(|v| v.file_bytes_after()).collect())?,
    })
}

fn samples_for(
    samples: &[StateSegmentProjectionSample],
    projection: StateSegmentProjection,
) -> Vec<&StateSegmentProjectionSample> {
    samples
        .iter()
        .filter(|sample| sample.projection() == projection)
        .collect()
}

fn nanos(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn median(mut values: Vec<u64>) -> Result<u64, ()> {
    if values.is_empty() {
        return Err(());
    }
    values.sort_unstable();
    Ok(values[values.len() / 2])
}

struct Configuration {
    database_root: Option<PathBuf>,
    reps: usize,
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
        let mut database_root = None;
        let mut reps = DEFAULT_REPS;
        let mut args = args.into_iter();
        while let Some(argument) = args.next() {
            match argument.as_ref() {
                "--database-root" => {
                    database_root = Some(PathBuf::from(args.next().ok_or(())?.as_ref()));
                }
                "--reps" => {
                    reps = args.next().ok_or(())?.as_ref().parse().map_err(|_| ())?;
                    if !(1..=32).contains(&reps) {
                        return Err(());
                    }
                }
                _ => return Err(()),
            }
        }
        Ok(Self {
            database_root,
            reps,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Configuration, median};

    #[test]
    fn median_uses_upper_middle_for_odd_and_even_samples() {
        assert_eq!(median(vec![9, 1, 5]).expect("median"), 5);
        assert_eq!(median(vec![9, 1, 5, 7]).expect("median"), 7);
    }

    #[test]
    fn configuration_rejects_zero_repetitions() {
        assert!(Configuration::parse_from(["--reps", "0"]).is_err());
    }
}
