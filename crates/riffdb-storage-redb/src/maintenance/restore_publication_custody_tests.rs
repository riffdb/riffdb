// req: REC-001, REC-006, REC-008, END-001
use super::*;
use crate::test_path::ScopedDirectory;
use std::fs;
use std::os::unix::fs::MetadataExt;

fn fixture(scope: &ScopedDirectory) -> (std::path::PathBuf, RestoreCustody, Arc<AtomicBool>) {
    let path = scope.join("original.redb");
    drop(redb::Database::create(&path).unwrap());
    let cancel = Arc::new(AtomicBool::new(false));
    let owner = RestoreCustody::capture(&path, Arc::clone(&cancel)).unwrap();
    (path, owner, cancel)
}

#[test]
fn closed_publication_handoff_keeps_both_native_locks_across_directory_moves() {
    let scope = ScopedDirectory::new("publication-custody-move");
    let (path, owner, _) = fixture(&scope);
    let original = fs::metadata(&path).unwrap();
    let checksum = crate::backup::sha256_file(&path).unwrap();
    let mut owner = owner.into_closed_publication().unwrap();
    let target = scope.join("target");
    fs::create_dir(&target).unwrap();
    let parent = Arc::new(PinnedDirectory::open(&target).unwrap());
    owner
        .move_to(Arc::clone(&parent), OsStr::new("owned.tmp"))
        .unwrap();
    parent.sync().unwrap();
    owner.verify().unwrap();
    assert!(!path.exists());
    let temporary = target.join("owned.tmp");
    let moved = fs::metadata(&temporary).unwrap();
    assert_eq!(
        (moved.dev(), moved.ino(), moved.len(), moved.nlink()),
        (original.dev(), original.ino(), original.len(), 1)
    );
    assert_eq!(crate::backup::sha256_file(&temporary).unwrap(), checksum);
    super::super::tests::probe(&temporary, true);
    owner.move_to(parent, OsStr::new("published.redb")).unwrap();
    let published = target.join("published.redb");
    super::super::tests::probe(&published, true);
    assert_eq!(crate::backup::sha256_file(&published).unwrap(), checksum);
    drop(owner);
    super::super::tests::probe(&published, false);
}

#[test]
fn closed_publication_handoff_refuses_a_live_or_closed_but_retained_engine_lease() {
    for close in [false, true] {
        let scope = ScopedDirectory::new("publication-custody-live");
        let (path, owner, _) = fixture(&scope);
        let backend = owner.lease().unwrap();
        if close {
            backend.close().unwrap();
        }
        assert!(owner.into_closed_publication().is_err());
        assert!(
            backend.len().is_err(),
            "refusal invalidates the surviving lease"
        );
        super::super::tests::probe(&path, true);
        drop(backend);
        super::super::tests::probe(&path, false);
    }
}

#[test]
fn closed_publication_handoff_refuses_preparation_cancellation_and_writable_aliases() {
    for reason in [0, 1, 2] {
        let scope = ScopedDirectory::new("publication-custody-refusal");
        let (path, owner, cancel) = fixture(&scope);
        match reason {
            0 => owner.begin_controlled_closed_mutation().unwrap(),
            1 => cancel.store(true, Ordering::Release),
            2 => fs::hard_link(&path, scope.join("alias.redb")).unwrap(),
            _ => unreachable!(),
        }
        assert!(owner.into_closed_publication().is_err());
        assert!(
            path.exists(),
            "refusal preserves the actual original artifact"
        );
    }
}

#[test]
fn closed_publication_move_refuses_foreign_targets_and_preserves_original_media() {
    let scope = ScopedDirectory::new("publication-custody-foreign");
    let (path, owner, _) = fixture(&scope);
    let before = crate::backup::sha256_file(&path).unwrap();
    let mut owner = owner.into_closed_publication().unwrap();
    let target = scope.join("target");
    fs::create_dir(&target).unwrap();
    fs::write(target.join("foreign"), b"foreign sentinel").unwrap();
    let directory = Arc::new(PinnedDirectory::open(&target).unwrap());
    assert!(owner.move_to(directory, OsStr::new("foreign")).is_err());
    assert!(
        owner.verify().is_err(),
        "failed transfer permanently invalidates authority"
    );
    assert_eq!(
        fs::read(target.join("foreign")).unwrap(),
        b"foreign sentinel"
    );
    assert_eq!(crate::backup::sha256_file(&path).unwrap(), before);
    super::super::tests::probe(&path, true);
}

#[test]
fn closed_publication_move_refuses_cancellation_or_directory_substitution() {
    for replace in [false, true] {
        let scope = ScopedDirectory::new("publication-custody-discontinuity");
        let (path, owner, cancel) = fixture(&scope);
        let mut owner = owner.into_closed_publication().unwrap();
        let target = scope.join("target");
        fs::create_dir(&target).unwrap();
        let directory = Arc::new(PinnedDirectory::open(&target).unwrap());
        if replace {
            fs::rename(&target, scope.join("moved-target")).unwrap();
            fs::create_dir(&target).unwrap();
        } else {
            cancel.store(true, Ordering::Release);
        }
        assert!(owner.move_to(directory, OsStr::new("owned.tmp")).is_err());
        assert!(path.exists());
        assert!(!target.join("owned.tmp").exists());
        assert!(!scope.join("moved-target/owned.tmp").exists());
    }
}

#[test]
fn closed_publication_handoff_retains_the_source_directory_pin_after_the_move() {
    let scope = ScopedDirectory::new("publication-custody-original-directory");
    let source = scope.join("stage");
    fs::create_dir(&source).unwrap();
    let path = source.join("original.redb");
    drop(redb::Database::create(&path).unwrap());
    let owner = RestoreCustody::capture(&path, Arc::new(AtomicBool::new(false))).unwrap();
    let mut owner = owner.into_closed_publication().unwrap();
    let target = scope.join("target");
    fs::create_dir(&target).unwrap();
    let directory = Arc::new(PinnedDirectory::open(&target).unwrap());
    owner.move_to(directory, OsStr::new("owned.tmp")).unwrap();
    fs::rename(&source, scope.join("moved-stage")).unwrap();
    fs::create_dir(&source).unwrap();
    assert!(owner.verify().is_err());
    assert!(target.join("owned.tmp").exists());
    super::super::tests::probe(&target.join("owned.tmp"), true);
}
