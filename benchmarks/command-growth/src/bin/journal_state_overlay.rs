#![forbid(unsafe_code)]

//! WP-486 benchmark-only journal-authoritative state-overlay mechanics proof.

use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use riffdb_bench_root::{BenchDir, BenchRoot, BenchRootOptions, default_perf_db_root, sweep_stale};
use riffdb_storage_redb::benchmark_support::{
    JournalOverlayMechanicsSample, StateSegmentWorkload, run_journal_overlay_mechanics_window,
};

const DEFAULT_REPS: usize = 5;
const RETAINED_ENTITIES: usize = 4_096;
const REQUIRED_MAX_OVERLAY_APPLY_BASIS_POINTS: u64 = 5_000;
const REQUIRED_MAX_READ_BASIS_POINTS: u64 = 12_500;
const WORKLOADS: [StateSegmentWorkload; 3] = [
    StateSegmentWorkload::DistinctCreates,
    StateSegmentWorkload::RetainedUpdates,
    StateSegmentWorkload::MixedCreatesAndUpdates,
];

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
        "journal-state-overlay",
    );
    let root = BenchRoot::resolve(BenchRootOptions {
        harness: "journal-state-overlay",
        cli_override: configuration.database_root,
        default_root,
        allow_tmpfs: false,
        min_free_bytes: 512 * 1024 * 1024,
    })
    .map_err(|error| {
        eprintln!("journal-state-overlay root: {error}");
    })?;
    let _ = sweep_stale(&root);
    let session = BenchDir::create(&root, "wp-486-journal-overlay").map_err(|_| ())?;
    println!(
        "{{\"schema\":\"riffdb.journal-state-overlay/v2\",\"record_type\":\"environment\",\"body\":{}}}",
        root.environment_report_json()
    );
    println!(
        "{{\"schema\":\"riffdb.journal-state-overlay/v2\",\"record_type\":\"configuration\",\"reps\":{},\"retained_entities\":{RETAINED_ENTITIES},\"apply_gate_basis_points\":{REQUIRED_MAX_OVERLAY_APPLY_BASIS_POINTS},\"read_gate_basis_points\":{REQUIRED_MAX_READ_BASIS_POINTS},\"production_semantics_changed\":false}}",
        configuration.reps
    );

    let mut summaries = Vec::new();
    for workload in WORKLOADS {
        for group_commands in [32, 128] {
            let summary = run_case(
                &session,
                workload,
                1_024,
                group_commands,
                configuration.reps,
            )?;
            println!("{}", summary.to_json(configuration.reps));
            summaries.push(summary);
        }
    }
    let medium = run_case(
        &session,
        StateSegmentWorkload::MixedCreatesAndUpdates,
        256,
        256,
        configuration.reps,
    )?;
    println!("{}", medium.to_json(configuration.reps));
    summaries.push(medium);
    let maximum = run_case(
        &session,
        StateSegmentWorkload::MixedCreatesAndUpdates,
        4_096,
        256,
        configuration.reps,
    )?;
    println!("{}", maximum.to_json(configuration.reps));
    summaries.push(maximum);

    let apply_summaries = summaries
        .iter()
        .filter(|summary| summary.commands == 1_024)
        .collect::<Vec<_>>();
    let apply_passed = apply_summaries.len() == 6
        && apply_summaries.iter().all(|summary| {
            summary.overlay_vs_current_pre_ack_basis_points
                <= REQUIRED_MAX_OVERLAY_APPLY_BASIS_POINTS
        });
    let maximum = summaries
        .iter()
        .find(|summary| summary.commands == 4_096)
        .ok_or(())?;
    let read_passed = maximum.overlay_vs_control_point_basis_points
        <= REQUIRED_MAX_READ_BASIS_POINTS
        && maximum.overlay_vs_control_page_basis_points <= REQUIRED_MAX_READ_BASIS_POINTS;
    let checkpoint_passed = maximum.checkpoint_ns > 0;
    let passed = apply_passed && read_passed && checkpoint_passed;
    println!(
        "{{\"schema\":\"riffdb.journal-state-overlay/v2\",\"record_type\":\"decision_gate\",\"minimum_apply_speedup\":2.0,\"maximum_read_ratio\":1.25,\"apply_passed\":{apply_passed},\"read_passed\":{read_passed},\"checkpoint_passed\":{checkpoint_passed},\"passed\":{passed},\"authority_note\":\"benchmark_only_proposed_overlay\"}}"
    );
    Ok(())
}

fn run_case(
    session: &BenchDir,
    workload: StateSegmentWorkload,
    commands: usize,
    group_commands: usize,
    reps: usize,
) -> Result<Summary, ()> {
    let mut samples = Vec::with_capacity(reps);
    for rep in 0..reps {
        let path = session.path().join(format!(
            "{}-n{commands}-g{group_commands}-r{rep}.redb",
            workload.label()
        ));
        let sample = run_journal_overlay_mechanics_window(
            &path,
            workload,
            commands,
            group_commands,
            RETAINED_ENTITIES,
        )
        .map_err(|error| {
            eprintln!("journal-state-overlay: {error}");
        })?;
        print_sample(rep, &sample);
        samples.push(sample);
    }
    Summary::from_samples(workload, commands, group_commands, &samples)
}

fn print_sample(rep: usize, sample: &JournalOverlayMechanicsSample) {
    let mutation_census = sample
        .mutation_census()
        .iter()
        .map(|entry| {
            format!(
                "{{\"table\":\"{}\",\"mutations\":{},\"key_bytes\":{},\"value_bytes\":{},\"expected_hash_bytes\":{},\"v1_fixed_header_bytes\":{}}}",
                entry.table(),
                entry.mutations(),
                entry.key_bytes(),
                entry.value_bytes(),
                entry.expected_hash_bytes(),
                entry.v1_fixed_header_bytes(),
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "{{\"schema\":\"riffdb.journal-state-overlay/v2\",\"record_type\":\"sample\",\"rep\":{rep},\"workload\":\"{}\",\"commands\":{},\"group_commands\":{},\"current_frame_build_ns\":{},\"current_redb_apply_ns\":{},\"overlay_frame_build_ns\":{},\"overlay_apply_ns\":{},\"control_point_read_ns\":{},\"overlay_point_read_ns\":{},\"control_page_read_ns\":{},\"overlay_page_read_ns\":{},\"checkpoint_ns\":{},\"encoded_journal_bytes\":{},\"mutation_census\":[{mutation_census}],\"overlay_bytes\":{},\"current_process_write_bytes\":{},\"overlay_process_write_bytes\":{},\"checkpoint_process_write_bytes\":{},\"checksums_equal\":{},\"production_semantics_changed\":false}}",
        sample.workload().label(),
        sample.commands(),
        sample.group_commands(),
        nanos(sample.current_frame_build()),
        nanos(sample.current_redb_apply()),
        nanos(sample.overlay_frame_build()),
        nanos(sample.overlay_apply()),
        nanos(sample.control_point_reads()),
        nanos(sample.overlay_point_reads()),
        nanos(sample.control_page_reads()),
        nanos(sample.overlay_page_reads()),
        nanos(sample.checkpoint()),
        sample.encoded_journal_bytes(),
        sample.overlay_bytes(),
        sample.current_process_write_bytes(),
        sample.overlay_process_write_bytes(),
        sample.checkpoint_process_write_bytes(),
        sample.point_read_checksum() == sample.control_point_read_checksum()
            && sample.page_read_checksum() == sample.control_page_read_checksum(),
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Summary {
    workload: StateSegmentWorkload,
    commands: usize,
    group_commands: usize,
    current_pre_ack_ns: u64,
    overlay_pre_ack_ns: u64,
    overlay_vs_current_pre_ack_basis_points: u64,
    control_point_ns: u64,
    overlay_point_ns: u64,
    overlay_vs_control_point_basis_points: u64,
    control_page_ns: u64,
    overlay_page_ns: u64,
    overlay_vs_control_page_basis_points: u64,
    checkpoint_ns: u64,
    current_process_write_bytes: u64,
    overlay_process_write_bytes: u64,
    checkpoint_process_write_bytes: u64,
}

impl Summary {
    fn from_samples(
        workload: StateSegmentWorkload,
        commands: usize,
        group_commands: usize,
        samples: &[JournalOverlayMechanicsSample],
    ) -> Result<Self, ()> {
        let current_pre_ack_ns = median(
            samples
                .iter()
                .map(|sample| {
                    nanos(sample.current_frame_build())
                        .saturating_add(nanos(sample.current_redb_apply()))
                })
                .collect(),
        )?;
        let overlay_pre_ack_ns = median(
            samples
                .iter()
                .map(|sample| {
                    nanos(sample.overlay_frame_build())
                        .saturating_add(nanos(sample.overlay_apply()))
                })
                .collect(),
        )?;
        let control_point_ns = median(
            samples
                .iter()
                .map(|sample| nanos(sample.control_point_reads()))
                .collect(),
        )?;
        let overlay_point_ns = median(
            samples
                .iter()
                .map(|sample| nanos(sample.overlay_point_reads()))
                .collect(),
        )?;
        let control_page_ns = median(
            samples
                .iter()
                .map(|sample| nanos(sample.control_page_reads()))
                .collect(),
        )?;
        let overlay_page_ns = median(
            samples
                .iter()
                .map(|sample| nanos(sample.overlay_page_reads()))
                .collect(),
        )?;
        Ok(Self {
            workload,
            commands,
            group_commands,
            current_pre_ack_ns,
            overlay_pre_ack_ns,
            overlay_vs_current_pre_ack_basis_points: ratio_basis_points(
                overlay_pre_ack_ns,
                current_pre_ack_ns,
            ),
            control_point_ns,
            overlay_point_ns,
            overlay_vs_control_point_basis_points: ratio_basis_points(
                overlay_point_ns,
                control_point_ns,
            ),
            control_page_ns,
            overlay_page_ns,
            overlay_vs_control_page_basis_points: ratio_basis_points(
                overlay_page_ns,
                control_page_ns,
            ),
            checkpoint_ns: median(
                samples
                    .iter()
                    .map(|sample| nanos(sample.checkpoint()))
                    .collect(),
            )?,
            current_process_write_bytes: median(
                samples
                    .iter()
                    .map(JournalOverlayMechanicsSample::current_process_write_bytes)
                    .collect(),
            )?,
            overlay_process_write_bytes: median(
                samples
                    .iter()
                    .map(JournalOverlayMechanicsSample::overlay_process_write_bytes)
                    .collect(),
            )?,
            checkpoint_process_write_bytes: median(
                samples
                    .iter()
                    .map(JournalOverlayMechanicsSample::checkpoint_process_write_bytes)
                    .collect(),
            )?,
        })
    }

    fn to_json(self, reps: usize) -> String {
        format!(
            "{{\"schema\":\"riffdb.journal-state-overlay/v2\",\"record_type\":\"summary\",\"workload\":\"{}\",\"commands\":{},\"group_commands\":{},\"reps\":{reps},\"current_pre_ack_ns\":{},\"overlay_pre_ack_ns\":{},\"overlay_vs_current_pre_ack_basis_points\":{},\"apply_speedup\":{:.3},\"control_point_ns\":{},\"overlay_point_ns\":{},\"overlay_vs_control_point_basis_points\":{},\"control_page_ns\":{},\"overlay_page_ns\":{},\"overlay_vs_control_page_basis_points\":{},\"checkpoint_ns\":{},\"current_process_write_bytes\":{},\"overlay_process_write_bytes\":{},\"checkpoint_process_write_bytes\":{},\"apply_gate_applies\":{},\"apply_gate_passed\":{},\"authority_note\":\"benchmark_only_proposed_overlay\"}}",
            self.workload.label(),
            self.commands,
            self.group_commands,
            self.current_pre_ack_ns,
            self.overlay_pre_ack_ns,
            self.overlay_vs_current_pre_ack_basis_points,
            self.current_pre_ack_ns as f64 / self.overlay_pre_ack_ns.max(1) as f64,
            self.control_point_ns,
            self.overlay_point_ns,
            self.overlay_vs_control_point_basis_points,
            self.control_page_ns,
            self.overlay_page_ns,
            self.overlay_vs_control_page_basis_points,
            self.checkpoint_ns,
            self.current_process_write_bytes,
            self.overlay_process_write_bytes,
            self.checkpoint_process_write_bytes,
            self.commands == 1_024,
            self.commands != 1_024
                || self.overlay_vs_current_pre_ack_basis_points
                    <= REQUIRED_MAX_OVERLAY_APPLY_BASIS_POINTS,
        )
    }
}

fn ratio_basis_points(numerator: u64, denominator: u64) -> u64 {
    numerator.saturating_mul(10_000) / denominator.max(1)
}

fn nanos(duration: Duration) -> u64 {
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
                    if !(1..=16).contains(&reps) {
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
    use super::{Configuration, median, ratio_basis_points};

    #[test]
    fn ratio_and_median_are_deterministic() {
        assert_eq!(ratio_basis_points(1, 2), 5_000);
        assert_eq!(median(vec![9, 1, 5]).expect("median"), 5);
    }

    #[test]
    fn configuration_rejects_zero_repetitions() {
        assert!(Configuration::parse_from(["--reps", "0"]).is_err());
    }
}
