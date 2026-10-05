//! Deterministic damage and lost-write schedules on the existing traced medium.
//! External corruption uses positional backend writes and sync outside engine
//! transactions; this does not change the fault RNG or trace encoding.

use redb::{ReadableDatabase, StorageBackend, TableDefinition};
use riffdb_sim::{FaultConfig, SimBackend, SimDisk};

const FILE: &str = "physical-integrity.redb";
const ROWS: TableDefinition<u64, &[u8]> = TableDefinition::new("integrity-records");
const PAYLOAD: &[u8] = b"stable-record-payload-independent-of-the-engine-leaf-reserved-byte";

fn frozen_engine(disk: &SimDisk) {
    let database = redb::Builder::new()
        .create_with_backend(SimBackend::new(disk, FILE))
        .unwrap();
    let write = database.begin_write().unwrap();
    write.open_table(ROWS).unwrap().insert(7, PAYLOAD).unwrap();
    write.commit().unwrap();
    drop(database);
    assert_eq!(disk.unsynced_mutation_count(), 0);
}

// req: SIM-001, SIM-002, REC-006, REC-008
#[test]
fn deterministic_durable_leaf_damage_preserves_record_bytes_but_fails_engine_integrity() {
    let execute = || {
        let disk = SimDisk::new(FaultConfig::quiet(0x269));
        frozen_engine(&disk);
        let original = disk.durable_bytes(FILE);
        assert!(original.len() <= 8 * 1024 * 1024);
        let offsets: Vec<_> = original
            .windows(PAYLOAD.len())
            .enumerate()
            .filter_map(|(offset, bytes)| (bytes == PAYLOAD).then_some(offset))
            .collect();
        assert_eq!(offsets.len(), 1);
        let leaf = offsets[0] / 4096 * 4096;
        assert_eq!(original[leaf], 1);
        assert!(u16::from_le_bytes(original[leaf + 2..leaf + 4].try_into().unwrap()) > 0);
        let media = SimBackend::new(&disk, FILE);
        media
            .write((leaf + 1) as u64, &[original[leaf + 1] ^ 1])
            .unwrap();
        media.sync_data().unwrap();
        media.close().unwrap();
        let damaged = disk.durable_bytes(FILE);
        assert_eq!(&damaged[offsets[0]..offsets[0] + PAYLOAD.len()], PAYLOAD);
        let mut database = redb::Builder::new()
            .create_with_backend(SimBackend::new(&disk, FILE))
            .unwrap();
        {
            let read = database.begin_read().unwrap();
            assert_eq!(
                read.open_table(ROWS)
                    .unwrap()
                    .get(7)
                    .unwrap()
                    .unwrap()
                    .value(),
                PAYLOAD
            );
        }
        assert!(
            matches!(
                database.check_integrity(),
                Ok(false)
                    | Err(redb::DatabaseError::Storage(redb::StorageError::Corrupted(
                        _
                    )))
            ),
            "physical damage cannot count as successful verification"
        );
        drop(database);
        disk.trace_digest()
    };
    assert_eq!(
        execute(),
        execute(),
        "the complete damage schedule replays exactly"
    );
}

// req: SIM-001, SIM-002, REC-001, REC-005, REC-008
#[test]
fn a_lost_unsynced_write_preserves_the_last_acknowledged_durable_image_and_stales_handles() {
    let execute = || {
        // First recovery draw drops this single unsynced mutation. Pin the
        // actual counter and bytes rather than accepting any crash outcome.
        let disk = SimDisk::new(FaultConfig::quiet(2));
        frozen_engine(&disk);
        let original = disk.durable_bytes(FILE);
        let media = SimBackend::new(&disk, FILE);
        media.write(9, &[original[9] ^ 0x80]).unwrap();
        assert_ne!(disk.volatile_bytes(FILE), original);
        assert_eq!(disk.durable_bytes(FILE), original);
        assert_eq!(disk.unsynced_mutation_count(), 1);
        disk.crash();
        disk.recover_after_crash();
        assert_eq!(disk.counters().torn_dropped, 1);
        assert_eq!(
            disk.counters().torn_kept + disk.counters().torn_truncated,
            0
        );
        assert_eq!(disk.durable_bytes(FILE), original);
        assert!(media.read(0, &mut [0; 9]).is_err());
        let mut database = redb::Builder::new()
            .create_with_backend(SimBackend::new(&disk, FILE))
            .unwrap();
        assert!(database.check_integrity().unwrap());
        let read = database.begin_read().unwrap();
        assert_eq!(
            read.open_table(ROWS)
                .unwrap()
                .get(7)
                .unwrap()
                .unwrap()
                .value(),
            PAYLOAD
        );
        drop(read);
        drop(database);
        disk.trace_digest()
    };
    assert_eq!(execute(), execute());
}
