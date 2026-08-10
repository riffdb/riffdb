//! Digest-sensitivity pins for the trace chain (`SIM-001`).
//!
//! The determinism pin (same seed twice ⇒ identical digest) proves
//! reproducibility, not completeness: if a fault-decision fold were removed,
//! both replays would still agree. These tests pin the other direction —
//! identical operation sequences whose fault schedules resolve differently
//! MUST produce different digests, one case per fault family. Each case is
//! isolating: the acknowledged operation streams are identical, so the only
//! digest divergence comes from the fold under test, and removing that fold
//! turns the `assert_ne` red.

use std::path::Path;

use redb::StorageBackend;
use riffdb_sim::{FaultConfig, SimBackend, SimDisk, SimJournalMedia};
use riffdb_storage_redb::JournalMedia;

const FILE: &str = "engine.redb";

/// Identical unsynced workload: a synced baseline plus four one-kilobyte
/// unsynced writes (two 512-byte granules each), leaving four torn-write
/// candidates for recovery.
fn unsynced_workload(disk: &SimDisk) {
    let backend = SimBackend::new(disk, FILE);
    backend.set_len(8192).expect("grow");
    backend.sync_data().expect("baseline sync");
    for slot in 0..4_u64 {
        backend
            .write(slot * 2048, &[slot as u8 + 1; 1024])
            .expect("unsynced write");
    }
}

#[test]
fn torn_write_decisions_feed_the_digest() {
    // Seeds chosen so the four recovery decisions resolve to distinct mixes:
    // seed 11 keeps all four, seed 2 drops two and truncates one, seed 1
    // truncates two (splitmix64 draw order, trace format version 2).
    let disks = [11_u64, 2, 1].map(|seed| SimDisk::new(FaultConfig::quiet(seed)));
    for disk in &disks {
        unsynced_workload(disk);
    }
    // Control: identical operation streams fold identically until the first
    // fault decision, whatever the seed.
    assert_eq!(disks[0].trace_digest(), disks[1].trace_digest());
    assert_eq!(disks[0].trace_digest(), disks[2].trace_digest());

    for disk in &disks {
        disk.crash();
        disk.recover_after_crash();
    }
    let mixes = disks.map(|disk| {
        let counters = disk.counters();
        (
            counters.torn_kept,
            counters.torn_dropped,
            counters.torn_truncated,
            disk.trace_digest(),
        )
    });
    // The decision mixes are pairwise distinct (keep vs drop vs truncate all
    // represented), so every pairwise digest equality below would mean the
    // decisions stopped feeding the chain.
    assert_ne!(
        (mixes[0].0, mixes[0].1, mixes[0].2),
        (mixes[1].0, mixes[1].1, mixes[1].2),
        "seeds 11 and 2 must resolve to different decision mixes"
    );
    assert_ne!(
        (mixes[1].0, mixes[1].1, mixes[1].2),
        (mixes[2].0, mixes[2].1, mixes[2].2),
        "seeds 2 and 1 must resolve to different decision mixes"
    );
    assert_ne!(
        mixes[0].3, mixes[1].3,
        "torn keep/drop decisions must feed the digest"
    );
    assert_ne!(
        mixes[1].3, mixes[2].3,
        "torn truncate decisions must feed the digest"
    );
    assert_ne!(
        mixes[0].3, mixes[2].3,
        "torn prefix lengths must feed the digest"
    );
}

#[test]
fn transient_error_decisions_feed_the_digest() {
    // Seed 6 with denominator 2: the first eligible draw fires, the second
    // does not — so the faulty disk acknowledges exactly the same operations
    // as the quiet disk, plus one traced transient refusal.
    let quiet = SimDisk::new(FaultConfig::quiet(6));
    let faulty = SimDisk::new(FaultConfig {
        transient_error_denominator: 2,
        ..FaultConfig::quiet(6)
    });
    let quiet_backend = SimBackend::new(&quiet, FILE);
    let faulty_backend = SimBackend::new(&faulty, FILE);
    quiet_backend.set_len(512).expect("grow");
    faulty_backend
        .set_len(512)
        .expect("set_len is not transient-eligible");

    quiet_backend.write(0, &[9; 64]).expect("quiet write");
    let error = faulty_backend
        .write(0, &[9; 64])
        .expect_err("seed 6 fires the first transient draw");
    assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
    faulty_backend
        .write(0, &[9; 64])
        .expect("seed 6 spares the second draw");

    assert_eq!(quiet.counters().transient_errors, 0);
    assert_eq!(faulty.counters().transient_errors, 1);
    assert_eq!(
        quiet.volatile_bytes(FILE),
        faulty.volatile_bytes(FILE),
        "both disks acknowledge identical state"
    );
    assert_ne!(
        quiet.trace_digest(),
        faulty.trace_digest(),
        "the injected transient decision must feed the digest"
    );
}

#[test]
fn crash_decisions_and_placement_feed_the_digest() {
    // Decision: identical operations; exactly one disk crashes afterwards.
    let calm = SimDisk::new(FaultConfig::quiet(7));
    let crashed = SimDisk::new(FaultConfig::quiet(7));
    for disk in [&calm, &crashed] {
        let backend = SimBackend::new(disk, FILE);
        backend.set_len(512).expect("grow");
        backend.write(0, &[1; 64]).expect("write");
    }
    crashed.crash();
    assert_ne!(
        calm.trace_digest(),
        crashed.trace_digest(),
        "the crash decision itself must feed the digest"
    );

    // Placement: same write attempts and one crash each, placed one
    // operation apart.
    let late = SimDisk::new(FaultConfig::quiet(8));
    let early = SimDisk::new(FaultConfig::quiet(8));
    let late_backend = SimBackend::new(&late, FILE);
    let early_backend = SimBackend::new(&early, FILE);
    late_backend.set_len(512).expect("grow");
    early_backend.set_len(512).expect("grow");
    late_backend.write(0, &[1; 64]).expect("first write");
    early_backend.write(0, &[1; 64]).expect("first write");
    late_backend.write(64, &[2; 64]).expect("second write");
    late.crash();
    early.crash();
    assert!(
        early_backend.write(64, &[2; 64]).is_err(),
        "post-crash attempts fail closed"
    );
    assert_ne!(
        late.trace_digest(),
        early.trace_digest(),
        "crash placement must feed the digest"
    );
}

#[test]
fn schedule_toggles_and_reconfiguration_feed_the_digest() {
    // Quiesce toggle (the campaign's termination mechanism).
    let plain = SimDisk::new(FaultConfig::quiet(9));
    let toggled = SimDisk::new(FaultConfig::quiet(9));
    SimBackend::new(&plain, FILE).set_len(512).expect("grow");
    SimBackend::new(&toggled, FILE).set_len(512).expect("grow");
    toggled.set_faults_enabled(false);
    assert_ne!(
        plain.trace_digest(),
        toggled.trace_digest(),
        "the quiesce toggle must feed the digest"
    );

    // Schedule re-aim.
    let steady = SimDisk::new(FaultConfig::quiet(10));
    let reaimed = SimDisk::new(FaultConfig::quiet(10));
    reaimed.set_crash_after_operations(Some((1000, 2000)));
    assert_ne!(
        steady.trace_digest(),
        reaimed.trace_digest(),
        "schedule changes must feed the digest"
    );

    // Capacity change (the simulated operator's seam).
    let fixed = SimDisk::new(FaultConfig::quiet(12));
    let resized = SimDisk::new(FaultConfig::quiet(12));
    resized.set_capacity_bytes(Some(1 << 20));
    assert_ne!(
        fixed.trace_digest(),
        resized.trace_digest(),
        "capacity changes must feed the digest"
    );
}

#[test]
fn refusal_events_are_self_sufficient_trace_entries() {
    // Trace format version 2: refusals fold their own discriminator and
    // parameters, so two histories differing only in a refused operation
    // diverge in the digest.

    // Closed-handle refusal.
    let closed_only = SimDisk::new(FaultConfig::quiet(13));
    let closed_probed = SimDisk::new(FaultConfig::quiet(13));
    let closed_only_backend = SimBackend::new(&closed_only, FILE);
    let closed_probed_backend = SimBackend::new(&closed_probed, FILE);
    closed_only_backend.set_len(128).expect("grow");
    closed_probed_backend.set_len(128).expect("grow");
    closed_only_backend.close().expect("close");
    closed_probed_backend.close().expect("close");
    assert!(
        closed_probed_backend.len().is_err(),
        "closed handles refuse"
    );
    assert_ne!(
        closed_only.trace_digest(),
        closed_probed.trace_digest(),
        "closed-handle refusals must feed the digest"
    );

    // Out-of-range refusal, including its offset and length parameters.
    let in_range = SimDisk::new(FaultConfig::quiet(14));
    let probed = SimDisk::new(FaultConfig::quiet(14));
    let in_range_backend = SimBackend::new(&in_range, FILE);
    let probed_backend = SimBackend::new(&probed, FILE);
    in_range_backend.set_len(128).expect("grow");
    probed_backend.set_len(128).expect("grow");
    assert!(probed_backend.read(200, &mut [0; 8]).is_err());
    assert_ne!(
        in_range.trace_digest(),
        probed.trace_digest(),
        "out-of-range refusals must feed the digest"
    );
    // Two different refused offsets must also diverge.
    let probed_far = SimDisk::new(FaultConfig::quiet(14));
    let probed_far_backend = SimBackend::new(&probed_far, FILE);
    probed_far_backend.set_len(128).expect("grow");
    assert!(probed_far_backend.read(300, &mut [0; 8]).is_err());
    assert_ne!(
        probed.trace_digest(),
        probed_far.trace_digest(),
        "refused offsets must feed the digest"
    );
}

#[test]
fn media_transient_error_decisions_feed_the_digest() {
    // The journal-media fault arm (trace format version 3): identical
    // acknowledged media operation streams; only the injected transient
    // refusal on the media write path differs. Seed 6 with denominator 2
    // fires the first transient-eligible draw and spares the second, exactly
    // as in the backend arm above — media creates and opens are not
    // transient-eligible, so the first draw lands on the first write.
    let side_file = Path::new("/journal/side-file");
    let quiet = SimDisk::new(FaultConfig::quiet(6));
    let faulty = SimDisk::new(FaultConfig {
        transient_error_denominator: 2,
        ..FaultConfig::quiet(6)
    });
    let quiet_media = SimJournalMedia::new(&quiet);
    let faulty_media = SimJournalMedia::new(&faulty);
    let mut quiet_file = quiet_media
        .create_new_read_write(side_file)
        .expect("create is not transient-eligible");
    let mut faulty_file = faulty_media
        .create_new_read_write(side_file)
        .expect("create is not transient-eligible");

    quiet_file.write_all(b"frame").expect("quiet media write");
    let error = faulty_file
        .write_all(b"frame")
        .expect_err("seed 6 fires the first transient draw");
    assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
    faulty_file
        .write_all(b"frame")
        .expect("seed 6 spares the second draw");

    assert_eq!(quiet.counters().transient_errors, 0);
    assert_eq!(faulty.counters().transient_errors, 1);
    assert_eq!(
        quiet.volatile_bytes("/journal/side-file"),
        faulty.volatile_bytes("/journal/side-file"),
        "both disks acknowledge identical media state"
    );
    assert_ne!(
        quiet.trace_digest(),
        faulty.trace_digest(),
        "the injected media transient decision must feed the digest"
    );
}

#[test]
fn media_namespace_refusals_feed_the_digest() {
    // create_new exclusivity is a namespace fault surface of its own: two
    // histories whose acknowledged operations are identical, one of which
    // additionally had an exclusive create refused, must diverge.
    let side_file = Path::new("/journal/side-file");
    let plain = SimDisk::new(FaultConfig::quiet(15));
    let refused = SimDisk::new(FaultConfig::quiet(15));
    let plain_media = SimJournalMedia::new(&plain);
    let refused_media = SimJournalMedia::new(&refused);
    drop(
        plain_media
            .create_new_read_write(side_file)
            .expect("create"),
    );
    drop(
        refused_media
            .create_new_read_write(side_file)
            .expect("create"),
    );
    let error = refused_media
        .create_new_read_write(side_file)
        .expect_err("exclusive create over an existing file refuses");
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_ne!(
        plain.trace_digest(),
        refused.trace_digest(),
        "namespace refusals must feed the digest"
    );

    // Missing-file refusals fold self-sufficiently too.
    let probed = SimDisk::new(FaultConfig::quiet(16));
    let opened = SimDisk::new(FaultConfig::quiet(16));
    assert!(
        !SimJournalMedia::new(&probed)
            .try_exists(side_file)
            .expect("absence probe")
    );
    let error = SimJournalMedia::new(&opened)
        .open_read(side_file)
        .expect_err("open of a missing media file refuses");
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    assert_ne!(
        probed.trace_digest(),
        opened.trace_digest(),
        "probe and not-found refusal are distinct traced events"
    );
}
