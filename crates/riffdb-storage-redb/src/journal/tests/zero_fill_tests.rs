//! Extent preparation never queues more than one bounded chunk of unsynced
//! zero-fill ahead of the live journal's next `fdatasync`.
// req: STO-032, REC-001
use super::*;
use crate::media::{JournalMediaDirectory, JournalMediaFile, MediaFile, MediaFileMetadata};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Mutex;

#[derive(Debug, Default)]
struct WriteSyncCensus {
    unsynced: usize,
    max_unsynced: usize,
    data_syncs: usize,
}

impl WriteSyncCensus {
    fn wrote(&mut self, bytes: usize) {
        self.unsynced += bytes;
        self.max_unsynced = self.max_unsynced.max(self.unsynced);
    }
}

/// A real file whose writes and syncs are counted per media instance.
#[derive(Debug)]
struct CountedFile {
    file: File,
    census: Arc<Mutex<WriteSyncCensus>>,
}

impl JournalMediaFile for CountedFile {
    fn len(&mut self) -> io::Result<u64> {
        self.file.metadata().map(|metadata| metadata.len())
    }
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        Read::read(&mut self.file, out)
    }
    fn read_exact(&mut self, out: &mut [u8]) -> io::Result<()> {
        Read::read_exact(&mut self.file, out)
    }
    fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        std::io::Write::write_all(&mut self.file, data)?;
        self.census.lock().unwrap().wrote(data.len());
        Ok(())
    }
    fn write_all_at(&mut self, data: &[u8], offset: usize) -> io::Result<()> {
        write_all_at(&mut self.file, data, offset)?;
        self.census.lock().unwrap().wrote(data.len());
        Ok(())
    }
    fn read_exact_at(&mut self, out: &mut [u8], offset: usize) -> io::Result<()> {
        read_exact_at(&mut self.file, out, offset)
    }
    fn sync_data(&mut self) -> io::Result<()> {
        self.file.sync_data()?;
        let mut census = self.census.lock().unwrap();
        census.unsynced = 0;
        census.data_syncs += 1;
        Ok(())
    }
    fn sync_all(&mut self) -> io::Result<()> {
        self.file.sync_all()?;
        self.census.lock().unwrap().unsynced = 0;
        Ok(())
    }
    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }
}

/// Real filesystem media whose writable handles are [`CountedFile`]s.
#[derive(Debug, Default)]
struct CountingMedia {
    census: Arc<Mutex<WriteSyncCensus>>,
}

impl CountingMedia {
    fn counted(&self, file: io::Result<MediaFile>) -> io::Result<MediaFile> {
        match file? {
            MediaFile::Fs(file) => Ok(MediaFile::Port(Box::new(CountedFile {
                file,
                census: Arc::clone(&self.census),
            }))),
            port @ MediaFile::Port(_) => Ok(port),
        }
    }

    fn census(&self) -> (usize, usize) {
        let census = self.census.lock().unwrap();
        (census.max_unsynced, census.data_syncs)
    }
}

impl JournalMedia for CountingMedia {
    fn open_directory(&self, path: &Path) -> io::Result<Box<dyn JournalMediaDirectory>> {
        RealJournalMedia.open_directory(path)
    }
    fn try_exists(&self, path: &Path) -> io::Result<bool> {
        RealJournalMedia.try_exists(path)
    }
    fn metadata(&self, path: &Path) -> io::Result<MediaFileMetadata> {
        RealJournalMedia.metadata(path)
    }
    fn open_read(&self, path: &Path) -> io::Result<MediaFile> {
        RealJournalMedia.open_read(path)
    }
    fn open_read_write(&self, path: &Path) -> io::Result<MediaFile> {
        self.counted(RealJournalMedia.open_read_write(path))
    }
    fn create_new_read_write(&self, path: &Path) -> io::Result<MediaFile> {
        self.counted(RealJournalMedia.create_new_read_write(path))
    }
    fn create_new_write_only(&self, path: &Path) -> io::Result<MediaFile> {
        self.counted(RealJournalMedia.create_new_write_only(path))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        RealJournalMedia.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        RealJournalMedia.remove_file(path)
    }
    fn sync_parent_data(&self, path: &Path) -> io::Result<()> {
        RealJournalMedia.sync_parent_data(path)
    }
    fn sync_parent_all(&self, path: &Path) -> io::Result<()> {
        RealJournalMedia.sync_parent_all(path)
    }
}

/// Independent ceiling on unsynced zero-fill ahead of the journal: one 2 MiB
/// chunk plus the write that completes it. Deliberately not derived from the
/// implementation constant, so raising that constant fails these tests.
const MAX_UNSYNCED_ZERO_FILL: usize = 3 * 1024 * 1024;

/// Bytes `[from, to)` of `path` are all zero, read in bounded pieces.
fn assert_zero_range(path: &Path, from: usize, to: usize) {
    let mut file = File::open(path).unwrap();
    file.seek(SeekFrom::Start(u64::try_from(from).unwrap()))
        .unwrap();
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut remaining = to - from;
    while remaining != 0 {
        let count = remaining.min(buffer.len());
        file.read_exact(&mut buffer[..count]).unwrap();
        assert!(buffer[..count].iter().all(|byte| *byte == 0));
        remaining -= count;
    }
}

#[test]
fn fresh_spare_zero_fill_syncs_each_bounded_chunk_and_stays_an_empty_extent() {
    let database_path = TestPath::new("zero-fill-fresh-spare");
    let database_id = database_id(61);
    let media = CountingMedia::default();
    let header = JournalFileHeader::new(database_id, None, [0; HASH_BYTES]);
    assert!(
        prepare_spare_extent_with_media(&media, &database_path.0, &header, &Mutex::new(()))
            .unwrap()
    );
    let (max_unsynced, data_syncs) = media.census();
    assert!(
        max_unsynced <= MAX_UNSYNCED_ZERO_FILL,
        "{max_unsynced} unsynced bytes queued ahead of the journal"
    );
    assert!(data_syncs >= extent_file_bytes(EXTENT_DATA_BYTES) / MAX_UNSYNCED_ZERO_FILL);

    let spare = spare_journal_path(&database_path.0);
    assert_eq!(
        std::fs::metadata(&spare).unwrap().len(),
        u64::try_from(extent_file_bytes(EXTENT_DATA_BYTES)).unwrap()
    );
    let state = inspect_extent(&spare).unwrap();
    assert_eq!(state.header.anchor_position, EXTENT_DATA_OFFSET);
    let (_, tail) = scan_journal(&spare, database_id, |_| panic!("fresh spare has a frame"))
        .unwrap()
        .unwrap();
    assert_eq!(tail.transition_count, 0);
    assert!(!tail.incomplete_tail);
    assert_zero_range(
        &spare,
        EXTENT_DATA_OFFSET,
        extent_file_bytes(EXTENT_DATA_BYTES),
    );
    assert!(!preparing_spare_journal_path(&database_path.0).exists());
}

#[test]
fn recycled_spare_zero_fill_syncs_each_bounded_chunk_and_keeps_allocated_frames() {
    let database_path = TestPath::new("zero-fill-recycled-spare");
    let database_id = database_id(62);
    let (first, second) = sealing_extent(&database_path.0, database_id);
    let sealed = seal_extent_with_media(&RealJournalMedia, &database_path.0, database_id)
        .unwrap()
        .unwrap();
    let old = std::fs::read(&sealed).unwrap();
    let trimmed = old.len();
    std::fs::rename(&sealed, recycled_journal_path(&database_path.0)).unwrap();

    let media = CountingMedia::default();
    let header = JournalFileHeader::new(database_id, Some(sequence(2)), second.frame_hash());
    assert!(
        prepare_spare_extent_with_media(&media, &database_path.0, &header, &Mutex::new(()))
            .unwrap()
    );
    let (max_unsynced, data_syncs) = media.census();
    assert!(
        max_unsynced <= MAX_UNSYNCED_ZERO_FILL,
        "{max_unsynced} unsynced bytes queued ahead of the journal"
    );
    assert!(
        data_syncs >= (extent_file_bytes(EXTENT_DATA_BYTES) - trimmed) / MAX_UNSYNCED_ZERO_FILL
    );

    let spare = spare_journal_path(&database_path.0);
    let reused = std::fs::read(&spare).unwrap();
    assert_eq!(reused.len(), extent_file_bytes(EXTENT_DATA_BYTES));
    let first_end = EXTENT_DATA_OFFSET + extent_frame_bytes(first.as_bytes().len()).unwrap();
    assert_eq!(
        &reused[EXTENT_DATA_OFFSET..first_end],
        &old[EXTENT_DATA_OFFSET..first_end]
    );
    assert!(reused[trimmed..].iter().all(|byte| *byte == 0));
    let (_, tail) = scan_journal(&spare, database_id, |_| panic!("stale frame replayed"))
        .unwrap()
        .unwrap();
    assert_eq!(tail.transition_count, 0);
    assert!(!tail.incomplete_tail);
}
