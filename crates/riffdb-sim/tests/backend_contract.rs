//! `StorageBackend` conformance for [`SimBackend`] and direct fault-arm
//! semantics for [`SimDisk`] (`SIM-002`: every arm is reachable and behaves
//! as specified — transient errors leave state unchanged, crashes tear only
//! unsynced regions, capacity exhaustion is a typed refusal).

use redb::StorageBackend;
use riffdb_sim::{FaultConfig, SimBackend, SimDisk};

const FILE: &str = "engine.redb";

fn quiet_disk(seed: u64) -> SimDisk {
    SimDisk::new(FaultConfig::quiet(seed))
}

#[test]
fn set_len_growth_zero_initializes_and_len_tracks_it() {
    let disk = quiet_disk(1);
    let backend = SimBackend::new(&disk, FILE);
    assert_eq!(backend.len().expect("empty length"), 0);
    backend.set_len(1024).expect("grow");
    assert_eq!(backend.len().expect("grown length"), 1024);
    let mut out = vec![0xFF; 1024];
    backend.read(0, &mut out).expect("read grown region");
    assert!(out.iter().all(|byte| *byte == 0), "growth zero-initializes");
}

#[test]
fn writes_read_back_and_shrink_truncates() {
    let disk = quiet_disk(2);
    let backend = SimBackend::new(&disk, FILE);
    backend.set_len(256).expect("grow");
    backend.write(64, &[7; 32]).expect("write");
    let mut out = [0; 32];
    backend.read(64, &mut out).expect("read back");
    assert_eq!(out, [7; 32]);
    backend.set_len(64).expect("shrink");
    assert_eq!(backend.len().expect("shrunk length"), 64);
    backend.set_len(256).expect("regrow");
    let mut out = [0xFF; 32];
    backend.read(64, &mut out).expect("read regrown");
    assert_eq!(out, [0; 32], "regrown region is zeroed, not resurrected");
}

#[test]
fn reads_and_writes_beyond_length_error_without_side_effects() {
    let disk = quiet_disk(3);
    let backend = SimBackend::new(&disk, FILE);
    backend.set_len(128).expect("grow");
    let mut out = [0; 16];
    assert!(backend.read(120, &mut out).is_err(), "read past end errors");
    assert!(
        backend.write(120, &[1; 16]).is_err(),
        "write past end errors"
    );
    assert_eq!(backend.len().expect("length unchanged"), 128);
    let mut out = [9; 8];
    backend
        .read(120, &mut out)
        .expect("in-range read still works");
    assert_eq!(out, [0; 8], "failed write left no bytes behind");
}

#[test]
fn operations_after_close_error() {
    let disk = quiet_disk(4);
    let backend = SimBackend::new(&disk, FILE);
    backend.set_len(64).expect("grow");
    backend.close().expect("first close succeeds");
    assert!(backend.len().is_err());
    assert!(backend.read(0, &mut [0; 8]).is_err());
    assert!(backend.write(0, &[1; 8]).is_err());
    assert!(backend.set_len(128).is_err());
    assert!(backend.sync_data().is_err());
    assert!(backend.close().is_err(), "double close errors");
}

#[test]
fn sync_folds_volatile_into_durable_and_crash_discards_unsynced_state() {
    let disk = quiet_disk(5);
    let backend = SimBackend::new(&disk, FILE);
    backend.set_len(1024).expect("grow");
    backend.write(0, &[1; 512]).expect("synced write");
    backend.sync_data().expect("sync");
    assert_eq!(disk.durable_bytes(FILE)[..512], [1; 512]);

    backend.write(0, &[2; 512]).expect("unsynced write");
    assert_eq!(
        disk.volatile_bytes(FILE)[..512],
        [2; 512],
        "reads see volatile over durable"
    );
    assert_eq!(
        disk.durable_bytes(FILE)[..512],
        [1; 512],
        "durable unchanged before sync"
    );

    disk.crash();
    assert!(backend.len().is_err(), "outstanding handles fail closed");
    disk.recover_after_crash();
    assert!(
        backend.len().is_err(),
        "pre-crash handles stay closed after recovery"
    );
    let reopened = SimBackend::new(&disk, FILE);
    let mut out = [0; 512];
    reopened.read(0, &mut out).expect("read recovered image");
    // The unsynced [2; 512] write covers exactly one 512-byte granule, so the
    // only survivable images are the durable bytes or the whole kept write.
    assert!(
        out == [1; 512] || out == [2; 512],
        "recovered bytes must be the durable image or a kept unsynced write"
    );
    let counters = disk.counters();
    assert_eq!(counters.crashes, 1);
    assert_eq!(counters.recoveries, 1);
}

#[test]
fn every_torn_write_arm_is_reachable_across_seeds() {
    let mut kept = 0;
    let mut dropped = 0;
    let mut truncated = 0;
    for seed in 0..48 {
        let disk = quiet_disk(seed);
        let backend = SimBackend::new(&disk, FILE);
        backend.set_len(8192).expect("grow");
        backend.sync_data().expect("baseline sync");
        for slot in 0..4_u64 {
            backend
                .write(slot * 2048, &[slot as u8 + 1; 2048])
                .expect("unsynced write");
        }
        disk.crash();
        disk.recover_after_crash();
        let counters = disk.counters();
        kept += counters.torn_kept;
        dropped += counters.torn_dropped;
        truncated += counters.torn_truncated;
        // A truncated write keeps a whole number of 512-byte granules.
        let recovered = disk.durable_bytes(FILE);
        for slot in 0..4_usize {
            let region = &recovered[slot * 2048..(slot + 1) * 2048];
            let pattern = slot as u8 + 1;
            let survived = region.iter().take_while(|byte| **byte == pattern).count();
            assert_eq!(survived % 512, 0, "torn prefix respects granularity");
            assert!(
                region[survived..].iter().all(|byte| *byte == 0),
                "beyond the kept prefix the durable image (zeros) survives"
            );
        }
    }
    assert!(kept > 0, "keep arm never fired: dead fault arm");
    assert!(dropped > 0, "drop arm never fired: dead fault arm");
    assert!(truncated > 0, "truncate arm never fired: dead fault arm");
}

#[test]
fn transient_errors_fail_the_operation_leave_state_unchanged_and_allow_retry() {
    let config = FaultConfig {
        seed: 11,
        torn_write_granularity: 512,
        crash_after_operations: None,
        transient_error_denominator: 3,
        capacity_bytes: None,
    };
    let disk = SimDisk::new(config);
    let backend = SimBackend::new(&disk, FILE);
    // Retry set_len/write/sync until each succeeds; the schedule injects
    // failures along the way and every failure must leave state unchanged.
    let mut failures = 0;
    while backend.set_len(512).is_err() {
        failures += 1;
        assert!(failures < 1000, "transient arm must not be permanent");
    }
    loop {
        match backend.write(0, &[9; 512]) {
            Ok(()) => break,
            Err(error) => {
                assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
                failures += 1;
                assert!(failures < 1000);
                assert_eq!(
                    disk.volatile_bytes(FILE),
                    vec![0; 512],
                    "failed write left no bytes behind"
                );
            }
        }
    }
    // No sync has been acknowledged yet, so the durable image is still empty;
    // failed syncs must leave it that way.
    let durable_before_sync = disk.durable_bytes(FILE);
    assert_eq!(durable_before_sync, Vec::<u8>::new());
    while backend.sync_data().is_err() {
        failures += 1;
        assert!(failures < 1000);
        assert_eq!(
            disk.durable_bytes(FILE),
            durable_before_sync,
            "failed sync did not fold volatile into durable"
        );
    }
    assert_eq!(disk.durable_bytes(FILE), vec![9; 512]);
    assert!(failures > 0, "seed 11 must exercise the transient arm");
    assert_eq!(disk.counters().transient_errors, failures);
}

#[test]
fn growth_beyond_capacity_fails_with_storage_full_and_no_state_change() {
    let config = FaultConfig {
        capacity_bytes: Some(4096),
        ..FaultConfig::quiet(12)
    };
    let disk = SimDisk::new(config);
    let backend = SimBackend::new(&disk, FILE);
    backend.set_len(4096).expect("fits capacity");
    let error = backend.set_len(4097).expect_err("beyond capacity");
    assert_eq!(error.kind(), std::io::ErrorKind::StorageFull);
    assert_eq!(backend.len().expect("length unchanged"), 4096);
    assert_eq!(disk.counters().capacity_rejections, 1);
    backend.set_len(2048).expect("shrink always fits");
}

#[test]
fn scheduled_crashes_fire_at_operation_boundaries_and_recovery_reopens() {
    let config = FaultConfig {
        crash_after_operations: Some((5, 6)),
        ..FaultConfig::quiet(13)
    };
    let disk = SimDisk::new(config);
    let backend = SimBackend::new(&disk, FILE);
    backend.set_len(4096).expect("op 1");
    backend.sync_data().expect("op 2");
    let mut crashed = false;
    for index in 0..16_u64 {
        if backend.write(0, &[index as u8; 64]).is_err() {
            crashed = true;
            break;
        }
    }
    assert!(crashed, "the drawn countdown must fire within the loop");
    assert!(disk.is_crashed());
    disk.recover_after_crash();
    let reopened = SimBackend::new(&disk, FILE);
    reopened.len().expect("fresh handle over recovered image");
    assert_eq!(disk.counters().crashes, 1);
}
