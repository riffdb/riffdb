#![forbid(unsafe_code)]

//! SIM-005 conformance for the journal media port: one shared behavior suite
//! run against BOTH the production filesystem implementation
//! (`RealJournalMedia`, rooted under the workspace `target/`) and the
//! simulated one (`SimJournalMedia` over a quiet `SimDisk`).
//!
//! The suite pins the contract the journal and format-preflight call sites
//! rely on: existence probes distinguishing absence from failure, `NotFound`
//! on opening/removing/renaming missing files, `create_new` exclusivity
//! (`AlreadyExists`), sequential-cursor versus positional access (positional
//! operations never move the cursor; positional writes beyond end of file
//! extend with zeroes), `Ok(0)` sequential reads at end of file, sync
//! acceptance, atomic rename replacing an existing destination, post-remove
//! absence, and both parent-directory sync flavors.
//!
//! Out of contract (documented divergence): behavior of a still-open handle
//! whose name was removed or renamed. The production call sites drop handles
//! before namespace changes, and the simulated handle is name-keyed while a
//! real one follows the inode.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_sim::{FaultConfig, SimDisk, SimJournalMedia};
use riffdb_storage_redb::{JournalMedia, RealJournalMedia};

static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

/// Real-filesystem root under the workspace `target/`, removed on drop.
struct RealRoot(PathBuf);

impl RealRoot {
    fn new(label: &str) -> Self {
        let ordinal = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/journal-media-contract")
            .join(format!("{label}-{}-{ordinal}", std::process::id()));
        std::fs::create_dir_all(&root).expect("create media contract root");
        Self(root)
    }
}

impl Drop for RealRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn kind(result: Result<impl std::fmt::Debug, std::io::Error>, context: &str) -> ErrorKind {
    match result {
        Ok(value) => panic!("{context} must fail, produced {value:?}"),
        Err(error) => error.kind(),
    }
}

/// The shared suite. `root` must be an existing directory (real) or a
/// directory-shaped path prefix (simulated).
fn conformance(media: &dyn JournalMedia, root: &Path) {
    let file_a = root.join("side-file-a");
    let file_b = root.join("side-file-b");

    // Absence: probes and every operation requiring existence fail closed.
    assert!(!media.try_exists(&file_a).expect("probe of absent file"));
    assert_eq!(
        kind(media.metadata(&file_a), "metadata of absent file"),
        ErrorKind::NotFound
    );
    assert_eq!(
        kind(media.open_read(&file_a), "read-open of absent file"),
        ErrorKind::NotFound
    );
    assert_eq!(
        kind(media.open_read_write(&file_a), "write-open of absent file"),
        ErrorKind::NotFound
    );
    assert_eq!(
        kind(media.remove_file(&file_a), "remove of absent file"),
        ErrorKind::NotFound
    );
    assert_eq!(
        kind(
            media.rename(&file_a, &file_b),
            "rename of absent source file"
        ),
        ErrorKind::NotFound
    );

    // create_new exclusivity, and the created file is empty and visible.
    let mut writer = media
        .create_new_read_write(&file_a)
        .expect("create-new side file");
    assert_eq!(
        kind(
            media.create_new_read_write(&file_a),
            "second exclusive create"
        ),
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        kind(
            media.create_new_write_only(&file_a),
            "second exclusive staging create"
        ),
        ErrorKind::AlreadyExists
    );
    assert!(media.try_exists(&file_a).expect("probe of created file"));
    let metadata = media.metadata(&file_a).expect("metadata of created file");
    assert!(metadata.is_file);
    assert_eq!(metadata.len, 0);

    // Sequential writes advance the cursor and extend the file; sync accepts.
    writer.write_all(b"alpha-").expect("first sequential write");
    writer.write_all(b"bravo").expect("second sequential write");
    writer.sync_data().expect("sync_data after writes");
    assert_eq!(writer.len().expect("handle length"), 11);
    drop(writer);
    assert_eq!(media.metadata(&file_a).expect("metadata").len, 11);

    // Fresh read handle: exact sequential read-back, then Ok(0) at end of
    // file, then UnexpectedEof for an exact read past it.
    let mut reader = media.open_read(&file_a).expect("read-open");
    let mut contents = [0_u8; 11];
    reader
        .read_exact(&mut contents)
        .expect("sequential read-back");
    assert_eq!(&contents, b"alpha-bravo");
    assert_eq!(reader.read(&mut [0_u8; 4]).expect("read at EOF"), 0);
    assert_eq!(
        kind(reader.read_exact(&mut [0_u8; 1]), "exact read at EOF"),
        ErrorKind::UnexpectedEof
    );
    drop(reader);

    // Positional operations do not move the sequential cursor.
    let mut mixed = media.open_read_write(&file_a).expect("mixed-open");
    mixed
        .write_all_at(b"BRAVO", 6)
        .expect("positional overwrite");
    let mut probe = [0_u8; 5];
    mixed.read_exact_at(&mut probe, 6).expect("positional read");
    assert_eq!(&probe, b"BRAVO");
    let mut head = [0_u8; 6];
    mixed
        .read_exact(&mut head)
        .expect("sequential read from unmoved cursor");
    assert_eq!(&head, b"alpha-");
    let mut tail = [0_u8; 5];
    mixed.read_exact(&mut tail).expect("sequential tail read");
    assert_eq!(&tail, b"BRAVO");

    // Positional reads past end of file fail; positional writes past end of
    // file extend, zero-filling the gap.
    assert!(
        mixed.read_exact_at(&mut [0_u8; 4], 9).is_err(),
        "a positional read crossing end of file must fail"
    );
    mixed
        .write_all_at(b"zz", 13)
        .expect("positional write beyond end of file");
    assert_eq!(mixed.len().expect("extended length"), 15);
    let mut gap = [0xFF_u8; 4];
    mixed.read_exact_at(&mut gap, 11).expect("read the gap");
    assert_eq!(&gap, &[0, 0, b'z', b'z']);
    mixed.sync_all().expect("sync_all after extension");
    drop(mixed);

    // Rename replaces an existing destination atomically.
    let mut doomed = media
        .create_new_write_only(&file_b)
        .expect("create replacement victim");
    doomed.write_all(b"doomed").expect("victim content");
    doomed.sync_all().expect("victim sync");
    drop(doomed);
    media
        .rename(&file_a, &file_b)
        .expect("rename over existing destination");
    assert!(!media.try_exists(&file_a).expect("source gone after rename"));
    assert!(media.try_exists(&file_b).expect("destination present"));
    assert_eq!(media.metadata(&file_b).expect("renamed metadata").len, 15);
    let mut renamed = media.open_read(&file_b).expect("open renamed file");
    let mut full = [0_u8; 15];
    renamed
        .read_exact(&mut full)
        .expect("read renamed contents");
    assert_eq!(&full[..11], b"alpha-BRAVO");
    assert_eq!(&full[11..], &[0, 0, b'z', b'z']);
    drop(renamed);

    // Parent-directory syncs, both flavors.
    media
        .sync_parent_data(&file_b)
        .expect("parent fdatasync flavor");
    media.sync_parent_all(&file_b).expect("parent fsync flavor");

    // Post-remove behavior: the name is gone for every operation.
    media.remove_file(&file_b).expect("remove side file");
    assert!(!media.try_exists(&file_b).expect("probe after remove"));
    assert_eq!(
        kind(media.open_read(&file_b), "read-open after remove"),
        ErrorKind::NotFound
    );
    assert_eq!(
        kind(media.remove_file(&file_b), "second remove"),
        ErrorKind::NotFound
    );
}

#[test]
fn production_filesystem_media_satisfies_the_port_contract() {
    let root = RealRoot::new("real");
    conformance(&RealJournalMedia, &root.0);
}

#[test]
fn simulated_media_satisfies_the_port_contract() {
    let disk = SimDisk::new(FaultConfig::quiet(0x51B_CAFE));
    let media = SimJournalMedia::new(&disk);
    conformance(&media, Path::new("/sim-media"));
    // The suite is also a determinism witness: replaying it on a fresh disk
    // with the same seed reproduces the identical trace digest.
    let replay_disk = SimDisk::new(FaultConfig::quiet(0x51B_CAFE));
    let replay_media = SimJournalMedia::new(&replay_disk);
    conformance(&replay_media, Path::new("/sim-media"));
    assert_eq!(
        disk.trace_digest(),
        replay_disk.trace_digest(),
        "identical media operation sequences must replay identical digests"
    );
}
