//! Structural scanner limits; genuine command semantics are tested at startup.
// req: STO-030, STO-031, STO-032, REC-001
use super::*;
use crate::media::JournalMediaFile;

fn database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x81; 10]).unwrap()
}

fn frame(sequence: u64, previous: [u8; 32], value_bytes: usize) -> EncodedJournalFrame {
    let sequence = CommitSequence::new(sequence).unwrap();
    JournalFrame::new(
        database_id(),
        sequence,
        sequence,
        1,
        previous,
        vec![
            JournalMutation::put(
                JournalTable::Commits,
                crate::keys::encode_application_sequence_key(sequence),
                [0x41],
            )
            .unwrap(),
            JournalMutation::put(JournalTable::Entities, [0x10], vec![0x20; value_bytes]).unwrap(),
        ],
    )
    .unwrap()
    .encode()
    .unwrap()
}

#[test]
fn source_scan_accepts_exact_byte_limit_and_refuses_one_more_without_visiting() {
    let base = frame(1, [0; 32], 1).as_bytes().len();
    for excess in [0, 1] {
        let scope = crate::test_path::ScopedDirectory::new("source-scan-bytes");
        let path = scope.join("journal.riffextent");
        let header = JournalFileHeader::new(database_id(), None, [0; 32]);
        let lane = JournalLane::open(&path, &header).unwrap();
        let encoded = frame(1, [0; 32], 1 + SOURCE_SUFFIX_BYTES - base + excess);
        assert_eq!(encoded.as_bytes().len(), SOURCE_SUFFIX_BYTES + excess);
        lane.submit(encoded).unwrap().wait().unwrap();
        let mut visits = 0;
        let result = scan_online_source_suffix_with_media(
            &RealJournalMedia,
            &path,
            database_id(),
            |_, _| {
                visits += 1;
                Ok(())
            },
        );
        if excess == 0 {
            assert_eq!(result.unwrap().unwrap().1.transition_count, 1);
            assert_eq!(visits, 1);
        } else {
            assert_eq!(result.err(), Some(JournalIoError::Corrupt));
            assert_eq!(visits, 0);
            // Ordinary recovery still accepts this valid larger frame.
            assert!(scan_journal(&path, database_id(), |_| Ok(())).is_ok());
        }
    }
}

#[test]
fn source_scan_accepts_256_transitions_and_refuses_the_257th_before_visiting_it() {
    let scope = crate::test_path::ScopedDirectory::new("source-scan-transitions");
    let path = scope.join("journal.riffextent");
    let lane =
        JournalLane::open(&path, &JournalFileHeader::new(database_id(), None, [0; 32])).unwrap();
    let mut previous = [0; 32];
    for sequence in 1..=257 {
        let encoded = frame(sequence, previous, 1);
        previous = encoded.frame_hash();
        lane.submit(encoded).unwrap().wait().unwrap();
        if sequence == 256 {
            assert_eq!(
                scan_online_source_suffix_with_media(
                    &RealJournalMedia,
                    &path,
                    database_id(),
                    |_, _| Ok(())
                )
                .unwrap()
                .unwrap()
                .1
                .transition_count,
                256
            );
        }
    }
    let mut visits = 0;
    assert_eq!(
        scan_online_source_suffix_with_media(&RealJournalMedia, &path, database_id(), |_, _| {
            visits += 1;
            Ok(())
        })
        .err(),
        Some(JournalIoError::Corrupt)
    );
    assert_eq!(visits, 256);
    assert!(scan_journal(&path, database_id(), |_| Ok(())).is_ok());
}

#[derive(Debug)]
struct HeaderOnly {
    header: [u8; EXTENT_FRAME_HEADER_BYTES],
    reads: Arc<Mutex<Vec<(usize, usize)>>>,
}

impl JournalMediaFile for HeaderOnly {
    fn len(&mut self) -> io::Result<u64> {
        Err(io::ErrorKind::Unsupported.into())
    }
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::ErrorKind::Unsupported.into())
    }
    fn read_exact(&mut self, _: &mut [u8]) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
    fn write_all(&mut self, _: &[u8]) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
    fn write_all_at(&mut self, _: &[u8], _: usize) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
    fn read_exact_at(&mut self, out: &mut [u8], offset: usize) -> io::Result<()> {
        self.reads.lock().unwrap().push((offset, out.len()));
        if offset != EXTENT_DATA_OFFSET || out.len() != self.header.len() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        out.copy_from_slice(&self.header);
        Ok(())
    }
    fn sync_data(&mut self) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
    fn sync_all(&mut self) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
    fn set_len(&mut self, _: u64) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
}

#[test]
fn raw_source_bounds_refuse_oversized_payload_and_padding_after_only_a_header_read() {
    let encoded = frame(1, [0; 32], 1);
    for oversized_payload in [true, false] {
        let mut header = [0_u8; EXTENT_FRAME_HEADER_BYTES];
        let physical = EncodedExtentFrame::encode(1, EXTENT_DATA_OFFSET, &encoded).unwrap();
        header.copy_from_slice(&physical.bytes[..EXTENT_FRAME_HEADER_BYTES]);
        let length = if oversized_payload {
            SOURCE_SUFFIX_BYTES + 1
        } else {
            encoded.as_bytes().len()
        };
        let padding = extent_frame_bytes(length).unwrap()
            + if oversized_payload {
                0
            } else {
                EXTENT_FRAME_ALIGNMENT
            };
        header[28..32].copy_from_slice(&u32::try_from(length).unwrap().to_be_bytes());
        header[32..36].copy_from_slice(&u32::try_from(padding).unwrap().to_be_bytes());
        let reads = Arc::new(Mutex::new(Vec::new()));
        let mut file = MediaFile::Port(Box::new(HeaderOnly {
            header,
            reads: Arc::clone(&reads),
        }));
        let extent = ExtentHeader::initial(JournalFileHeader::new(database_id(), None, [0; 32]));
        assert_eq!(
            walk_extent_frames_with_limits(
                &mut file,
                &extent,
                ExtentWalk::FromAnchor,
                EXTENT_FILE_BYTES,
                true,
                |_, _| panic!("no payload may be visited")
            )
            .err(),
            Some(JournalIoError::Corrupt)
        );
        assert_eq!(
            *reads.lock().unwrap(),
            vec![(EXTENT_DATA_OFFSET, EXTENT_FRAME_HEADER_BYTES)]
        );
    }
}
