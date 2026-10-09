//! Bounded independent acknowledgement evidence; never authoritative storage.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

use super::Result;

const HEADER: u64 = 16;
const RECORD: u64 = 32;
const MAGIC: &[u8; 8] = b"RDBTW001";

struct Writer {
    file: File,
    written: u64,
}

pub(super) struct Witness {
    writer: Mutex<Writer>,
    slots: u64,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct Acknowledged {
    pub(super) ordinal: u64,
    pub(super) sequence: u64,
    pub(super) version: u64,
    pub(super) incarnation: u64,
}

pub(super) struct Reader {
    file: File,
    slots: u64,
    position: u64,
}

impl Reader {
    pub(super) fn open(path: &Path) -> Result<Self> {
        let metadata = std::fs::symlink_metadata(path).map_err(|_| "witness_read")?;
        if !metadata.is_file() || metadata.len() > HEADER + RECORD * super::model::MAX_COMMANDS {
            return Err("witness_bound");
        }
        let mut file = File::open(path).map_err(|_| "witness_read")?;
        let mut header = [0; HEADER as usize];
        file.read_exact(&mut header).map_err(|_| "witness_read")?;
        let slots = u64::from_be_bytes(header[8..].try_into().map_err(|_| "witness_read")?);
        if &header[..8] != MAGIC
            || slots == 0
            || slots > super::model::MAX_COMMANDS
            || metadata.len() != HEADER + slots * RECORD
        {
            return Err("witness_bound");
        }
        Ok(Self {
            file,
            slots,
            position: 0,
        })
    }

    pub(super) fn next(&mut self) -> Result<Option<Acknowledged>> {
        while self.position < self.slots {
            let mut record = [0; RECORD as usize];
            self.file
                .read_exact(&mut record)
                .map_err(|_| "witness_read")?;
            let ordinal = self.position;
            self.position += 1;
            if record == [0; RECORD as usize] {
                continue;
            }
            let mut values = [0_u64; 4];
            for (value, bytes) in values.iter_mut().zip(record.chunks_exact(8)) {
                *value = u64::from_be_bytes(bytes.try_into().map_err(|_| "witness_read")?);
            }
            if values[0] != ordinal
                || values[1] == 0
                || !(1..=2).contains(&values[2])
                || values[3] == 0
            {
                return Err("witness_identity");
            }
            return Ok(Some(Acknowledged {
                ordinal,
                sequence: values[1],
                version: values[2],
                incarnation: values[3],
            }));
        }
        Ok(None)
    }

    pub(super) fn revisions(&mut self) -> Result<Vec<u64>> {
        let mut revisions = vec![0_u64; super::model::POPULATION as usize];
        while let Some(row) = self.next()? {
            accept_revision(&mut revisions, &row)?;
        }
        Ok(revisions)
    }

    /// The count is the load owner's exact, quiesced acknowledgement count.
    /// Stop after those rows; the preallocated unoffered suffix supplies no
    /// reconciliation evidence and needs no scan during a bounded handover.
    pub(super) fn acknowledged_prefix(&mut self, count: u64) -> Result<(Acknowledged, Vec<u64>)> {
        if self.position != 0 || count == 0 || count > self.slots {
            return Err("witness_prefix");
        }
        let mut first = None;
        let mut revisions = vec![0_u64; super::model::POPULATION as usize];
        for _ in 0..count {
            let row = self.next()?.ok_or("missing_command_witness")?;
            accept_revision(&mut revisions, &row)?;
            if first.is_none() {
                first = Some(row);
            }
        }
        Ok((first.ok_or("missing_command_witness")?, revisions))
    }
}

fn accept_revision(revisions: &mut [u64], row: &Acknowledged) -> Result<()> {
    let ticket = (row.ordinal % super::model::POPULATION) as usize;
    let expected = row.ordinal / super::model::POPULATION;
    if revisions[ticket] != expected {
        return Err("witness_revision_gap");
    }
    revisions[ticket] += 1;
    Ok(())
}

impl Witness {
    pub(super) fn configured(name: &str, slots: u64) -> Result<Option<Arc<Self>>> {
        let Some(root) = std::env::var_os("RIFFDB_LIFECYCLE_WITNESS_ROOT") else {
            return Ok(None);
        };
        let root = std::path::PathBuf::from(root);
        if !root.is_absolute() || root.is_symlink() || !root.is_dir() {
            return Err("witness_root");
        }
        Self::create(&root, name, slots).map(Arc::new).map(Some)
    }

    fn create(root: &Path, name: &str, slots: u64) -> Result<Self> {
        if !matches!(name, "seed.witness" | "updates.witness")
            || slots == 0
            || slots > super::model::MAX_COMMANDS
        {
            return Err("witness_bound");
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(root.join(name))
            .map_err(|_| "witness_create")?;
        file.write_all(MAGIC)
            .and_then(|()| file.write_all(&slots.to_be_bytes()))
            .map_err(|_| "witness_write")?;
        file.set_len(HEADER + slots * RECORD)
            .map_err(|_| "witness_bound")?;
        Ok(Self {
            writer: Mutex::new(Writer { file, written: 0 }),
            slots,
        })
    }

    pub(super) fn record(
        &self,
        ordinal: u64,
        sequence: u64,
        version: u64,
        incarnation: u64,
    ) -> Result<()> {
        if ordinal >= self.slots || sequence == 0 || !(1..=2).contains(&version) || incarnation == 0
        {
            return Err("witness_identity");
        }
        let mut writer = self.writer.lock().map_err(|_| "witness_owner")?;
        let offset = HEADER + ordinal * RECORD;
        writer
            .file
            .seek(SeekFrom::Start(offset))
            .map_err(|_| "witness_read")?;
        let mut previous = [0_u8; RECORD as usize];
        writer
            .file
            .read_exact(&mut previous)
            .map_err(|_| "witness_read")?;
        if previous != [0; RECORD as usize] {
            return Err("duplicate_command_witness");
        }
        let mut record = [0; RECORD as usize];
        for (chunk, value) in
            record
                .chunks_exact_mut(8)
                .zip([ordinal, sequence, version, incarnation])
        {
            chunk.copy_from_slice(&value.to_be_bytes());
        }
        writer
            .file
            .seek(SeekFrom::Start(offset))
            .and_then(|_| writer.file.write_all(&record))
            .map_err(|_| "witness_write")?;
        writer.written = writer.written.checked_add(1).ok_or("witness_bound")?;
        Ok(())
    }

    pub(super) fn finish(&self) -> Result<()> {
        let writer = self.writer.lock().map_err(|_| "witness_owner")?;
        if writer.written != self.slots {
            return Err("missing_command_witness");
        }
        writer.file.sync_all().map_err(|_| "witness_sync")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: END-003, END-007, END-008
    #[test]
    fn out_of_order_witnesses_preserve_exact_identities_and_refuse_missing_or_duplicate_acknowledgements()
     {
        let root = std::env::temp_dir().join(format!("riffdb-witness-test-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let witness = Witness::create(&root, "updates.witness", 3).unwrap();
        witness.record(2, 71, 2, 1).unwrap();
        witness.record(0, 72, 1, 1).unwrap();
        let mut snapshot = Reader::open(&root.join("updates.witness")).unwrap();
        assert_eq!(
            snapshot.next().unwrap(),
            Some(Acknowledged {
                ordinal: 0,
                sequence: 72,
                version: 1,
                incarnation: 1
            })
        );
        assert_eq!(
            snapshot.next().unwrap(),
            Some(Acknowledged {
                ordinal: 2,
                sequence: 71,
                version: 2,
                incarnation: 1
            })
        );
        assert_eq!(snapshot.next().unwrap(), None);
        assert!(witness.finish().is_err());
        assert!(witness.record(2, 71, 2, 1).is_err());
        assert!(witness.record(3, 73, 2, 1).is_err());
        assert!(witness.record(1, 0, 2, 1).is_err());
        assert!(witness.record(1, 73, 2, 0).is_err());
        witness.record(1, 73, 2, 2).unwrap();
        witness.finish().unwrap();
        drop(witness);
        let revisions = Reader::open(&root.join("updates.witness"))
            .unwrap()
            .revisions()
            .unwrap();
        assert_eq!(&revisions[..4], &[1, 1, 1, 0]);
        let encoded = std::fs::read(root.join("updates.witness")).unwrap();
        assert_eq!(&encoded[..8], MAGIC);
        assert_eq!(encoded.len(), 112);
        for (index, expected) in [[0_u64, 72, 1, 1], [1, 73, 2, 2], [2, 71, 2, 1]]
            .into_iter()
            .enumerate()
        {
            let record = &encoded[16 + index * 32..16 + (index + 1) * 32];
            let actual = record
                .chunks_exact(8)
                .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    // req: END-003, END-007, END-008
    #[test]
    fn witness_reader_refuses_corrupt_metadata_truncated_records_symlinks_and_revision_gaps() {
        let root = std::env::temp_dir().join(format!(
            "riffdb-witness-hostile-test-{}",
            std::process::id()
        ));
        std::fs::create_dir(&root).unwrap();
        let witness = Witness::create(
            &root,
            "updates.witness",
            super::super::model::POPULATION + 1,
        )
        .unwrap();
        witness
            .record(super::super::model::POPULATION, 83, 2, 2)
            .unwrap();
        assert!(
            Reader::open(&root.join("updates.witness"))
                .unwrap()
                .revisions()
                .is_err()
        );
        drop(witness);
        let path = root.join("seed.witness");
        let writer = Witness::create(&root, "seed.witness", 1).unwrap();
        writer.record(0, 84, 1, 1).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let valid = std::fs::read(&path).unwrap();
        for offset in [0, 24, 32, 40] {
            let mut corrupt = valid.clone();
            corrupt[offset..offset + 8].fill(0);
            std::fs::write(&path, corrupt).unwrap();
            let refused = match Reader::open(&path) {
                Err(_) => true,
                Ok(mut reader) => reader.next().is_err(),
            };
            assert!(refused, "corrupt witness was admitted");
        }
        std::fs::write(&path, &valid[..valid.len() - 1]).unwrap();
        assert!(Reader::open(&path).is_err());
        let mut appended = valid;
        appended.push(0);
        std::fs::write(&path, appended).unwrap();
        assert!(Reader::open(&path).is_err());
        std::os::unix::fs::symlink(&path, root.join("linked.witness")).unwrap();
        assert!(Reader::open(&root.join("linked.witness")).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    // req: END-003, END-007, END-008, END-010
    #[test]
    fn quiesced_acknowledged_prefix_reports_actual_ticket_revisions_without_scanning_future_slots()
    {
        let root =
            std::env::temp_dir().join(format!("riffdb-witness-prefix-test-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let witness = Witness::create(&root, "updates.witness", 4).unwrap();
        witness.record(1, 73, 1, 1).unwrap();
        witness.record(0, 72, 1, 1).unwrap();
        let mut reader = Reader::open(&root.join("updates.witness")).unwrap();
        let (original, revisions) = reader.acknowledged_prefix(2).unwrap();
        assert_eq!(original.ordinal, 0);
        assert_eq!(original.sequence, 72);
        assert_eq!((revisions[0], revisions[1]), (1, 1));
        assert_eq!(revisions.iter().sum::<u64>(), 2);
        assert_eq!(
            reader.position, 2,
            "unoffered day-sized suffix is not a prefix scan"
        );
        let mut reader = Reader::open(&root.join("updates.witness")).unwrap();
        assert!(reader.acknowledged_prefix(0).is_err());
        assert!(reader.acknowledged_prefix(5).is_err());
        assert!(reader.acknowledged_prefix(3).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
