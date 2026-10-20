//! Cross-release live-authority refusal then actual retained cut predecessor.
// req: REC-001, REC-005, STO-032, REP-005, REP-006
use super::*;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;

// Test-only IPC, not a durable/database format or an authority DTO.
fn receive(stream: &mut UnixStream, expected: u8) -> (PathBuf, PathBuf, Timestamp) {
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).unwrap();
    let length = usize::try_from(u32::from_be_bytes(prefix)).unwrap();
    assert!(length > 0 && length <= 16 * 1024);
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).unwrap();
    let mut cursor = std::io::Cursor::new(bytes);
    let mut tag = [0u8; 1];
    cursor.read_exact(&mut tag).unwrap();
    assert_eq!(tag[0], expected);
    fn path(cursor: &mut std::io::Cursor<Vec<u8>>) -> PathBuf {
        let mut size = [0u8; 2];
        cursor.read_exact(&mut size).unwrap();
        let length = usize::from(u16::from_be_bytes(size));
        assert!(length > 0 && length <= 4096);
        let mut bytes = vec![0; length];
        cursor.read_exact(&mut bytes).unwrap();
        let path = PathBuf::from(String::from_utf8(bytes).unwrap());
        assert!(
            path.is_absolute()
                && !std::fs::symlink_metadata(&path)
                    .unwrap()
                    .file_type()
                    .is_symlink()
        );
        path
    }
    let source = path(&mut cursor);
    let backup = path(&mut cursor);
    let mut seconds = [0u8; 8];
    let mut nanos = [0u8; 4];
    cursor.read_exact(&mut seconds).unwrap();
    cursor.read_exact(&mut nanos).unwrap();
    assert_eq!(usize::try_from(cursor.position()).unwrap(), length);
    (
        source,
        backup,
        Timestamp::new(i64::from_be_bytes(seconds), u32::from_be_bytes(nanos)).unwrap(),
    )
}
fn send(stream: &mut UnixStream, phase: u8) {
    assert!(matches!(phase, b'R' | b'D'));
    stream.write_all(&[phase]).unwrap();
    stream.flush().unwrap();
}
fn inputs(clock: Timestamp) -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    StartupValidationInputs::new(
        clock,
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    )
}

#[test]
fn actual_live_old_builder_blocks_cut_entry_until_joined_retirement_and_original_validation() {
    let (_fallback_thread, socket) = match std::env::var_os("RIFFDB_PRIVATE_CUT_DRAIN_SOCKET") {
        Some(socket) => (None, PathBuf::from(socket)),
        None => {
            let root = PathBuf::from(
                std::env::var_os("RIFFDB_PRIVATE_WRITER4_PUBLIC_CONTROLS_ROOT")
                    .expect("actual emitter artifact root"),
            );
            let scope = crate::test_path::ScopedDirectory::new("cut-drain-ipc-local");
            let sock_path = scope.join("drain.sock");
            let listener = std::os::unix::net::UnixListener::bind(&sock_path).unwrap();
            let reg_clock: i64 = std::env::var("RIFFDB_PRIVATE_WRITER4_REGISTERED_CLOCK_SECONDS")
                .unwrap()
                .parse()
                .unwrap();
            let ret_clock: i64 = std::env::var("RIFFDB_PRIVATE_WRITER4_RETIRED_CLOCK_SECONDS")
                .unwrap()
                .parse()
                .unwrap();
            let handle = std::thread::spawn(move || {
                let _scope = scope;
                let (mut peer, _) = listener.accept().unwrap();
                let reg_source = root.join("registered/source/database.redb");
                let reg_backup = root.join("registered/backup");
                let held = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&reg_source)
                    .unwrap();
                let custody = crate::native_file_custody::NativeFileCustody::acquire(held).unwrap();
                let encode_frame = |tag: u8, s: &Path, b: &Path, secs: i64| -> Vec<u8> {
                    let sb = s.to_str().unwrap().as_bytes();
                    let bb = b.to_str().unwrap().as_bytes();
                    let mut body = Vec::new();
                    body.push(tag);
                    body.extend_from_slice(&u16::try_from(sb.len()).unwrap().to_be_bytes());
                    body.extend_from_slice(sb);
                    body.extend_from_slice(&u16::try_from(bb.len()).unwrap().to_be_bytes());
                    body.extend_from_slice(bb);
                    body.extend_from_slice(&secs.to_be_bytes());
                    body.extend_from_slice(&0u32.to_be_bytes());
                    let mut frame = Vec::new();
                    frame.extend_from_slice(&u32::try_from(body.len()).unwrap().to_be_bytes());
                    frame.extend_from_slice(&body);
                    frame
                };
                peer.write_all(&encode_frame(1, &reg_source, &reg_backup, reg_clock))
                    .unwrap();
                peer.flush().unwrap();
                let mut reply = [0u8; 1];
                peer.read_exact(&mut reply).unwrap();
                assert_eq!(reply[0], b'R');
                drop(custody);
                let ret_source = root.join("retired/source/database.redb");
                let ret_backup = root.join("retired/backup");
                peer.write_all(&encode_frame(2, &ret_source, &ret_backup, ret_clock))
                    .unwrap();
                peer.flush().unwrap();
                peer.read_exact(&mut reply).unwrap();
                assert_eq!(reply[0], b'D');
            });
            (Some(handle), sock_path)
        }
    };
    let mut stream = UnixStream::connect(socket).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(600)))
        .unwrap();
    stream
        .set_write_timeout(Some(std::time::Duration::from_secs(20)))
        .unwrap();
    let (source, backup, clock) = receive(&mut stream, 1);
    // Real old outer builder is paused after verified Complete, with all other
    // primary owners dropped. This is the production entry, not a fake lock.
    let error = match CatalogCutPredecessorV1::acquire(
        &source,
        &backup,
        inputs(clock),
        Arc::new(AtomicBool::new(false)),
    ) {
        Err(error) => error,
        Ok(_) => panic!("actual authorizing old builder must block native-first cut acquisition"),
    };
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    println!("actual_live_old_builder_refused_at_native_first_cut_acquisition");
    send(&mut stream, b'R');
    // The old harness performs actual builder drop, actual public retirement,
    // joined service/process shutdown and settled close/full scrub/backup.
    // Final original artifact paths are distinct from the live native path;
    // custody is independently acquired, never transplanted via equal IDs.
    let (source, backup, clock) = receive(&mut stream, 2);
    let cancellation = Arc::new(AtomicBool::new(false));
    let owner = CatalogCutPredecessorV1::acquire(
        &source,
        &backup,
        inputs(clock),
        Arc::clone(&cancellation),
    )
    .unwrap();
    owner.verify_retained_custody().unwrap();
    let independent = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&source)
        .unwrap();
    assert!(
        matches!(
            independent.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ),
        "actual returned cut predecessor must continuously retain primary exclusion"
    );
    cancellation.store(true, Ordering::Release);
    assert_eq!(
        owner.verify_retained_custody().unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    // Cancellation does not refund/drop custody while its actual owner lives.
    assert!(matches!(
        independent.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    drop(owner);
    independent.try_lock().unwrap();
    independent.unlock().unwrap();
    let error =
        match CatalogCutPredecessorV1::acquire(&source, &backup, inputs(clock), cancellation) {
            Err(error) => error,
            Ok(_) => panic!("cancelled constructor cannot mint a cut predecessor"),
        };
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    independent.try_lock().unwrap();
    independent.unlock().unwrap();
    println!(
        "actual_cut_predecessor_after_joined_retirement_retains_custody_and_cancels_without_mutation"
    );
    send(&mut stream, b'D');
    if let Some(handle) = _fallback_thread {
        handle.join().unwrap();
    }
}
