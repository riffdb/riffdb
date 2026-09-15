//! Disposable derived files never confer authority and never touch primary paths.
// req: REP-004, PRJ-007, PRJ-008, PRJ-009, REC-002
use super::RedbFollowerColumnarScratch;
use std::{
    fs,
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
};

fn open(root: &Path, files: usize, bytes: u64) -> RedbFollowerColumnarScratch {
    RedbFollowerColumnarScratch::open(
        root,
        NonZeroUsize::new(files).unwrap(),
        NonZeroU64::new(bytes).unwrap(),
    )
    .unwrap()
}

#[test]
fn follower_columnar_scratch_is_exclusive_disposable_and_separate_from_primary() {
    let scope = crate::test_path::ScopedDirectory::new("follower-columnar-scratch");
    let primary = scope.join("source-primary/generation-0000000000000001");
    fs::create_dir_all(&primary).unwrap();
    fs::write(primary.join("ROOT-V1"), b"primary").unwrap();
    let mut owner = open(&scope.join(""), 8, 64);
    assert!(!scope.join("follower-columnar/build").exists());
    assert!(
        RedbFollowerColumnarScratch::open(
            &scope.join(""),
            NonZeroUsize::new(8).unwrap(),
            NonZeroU64::new(64).unwrap()
        )
        .is_err()
    );
    {
        let build = owner.begin().unwrap();
        let generation = build.path().join("generation-0000000000000001");
        fs::create_dir(&generation).unwrap();
        fs::write(generation.join("ROOT-V1"), b"old-root").unwrap();
        // Simulate abandonment: no destructor may claim successful cleanup.
    }
    drop(owner);
    let mut reopened = open(&scope.join(""), 8, 64);
    let build = reopened.begin().unwrap();
    assert_eq!(fs::read_dir(build.path()).unwrap().count(), 0);
    let generation = build.path().join("generation-0000000000000001.tmp");
    fs::create_dir(&generation).unwrap();
    let scratch = generation.join(".rebuild-scratch-v1");
    fs::create_dir(&scratch).unwrap();
    fs::write(scratch.join("partition-0000.lane"), b"partial").unwrap();
    build.discard().unwrap();
    assert!(!scope.join("follower-columnar/build").exists());
    assert_eq!(fs::read(primary.join("ROOT-V1")).unwrap(), b"primary");
}

#[test]
fn follower_columnar_scratch_refuses_excess_bytes_files_and_depth_before_removal() {
    for mode in 0..3 {
        let scope = crate::test_path::ScopedDirectory::new("follower-columnar-scratch");
        let mut owner = open(&scope.join(""), 2, 8);
        let build = owner.begin().unwrap();
        let path = build.path().to_path_buf();
        let generation = path.join("generation-0000000000000001.tmp");
        fs::create_dir(&generation).unwrap();
        match mode {
            0 => fs::write(generation.join("large"), b"123456789").unwrap(),
            1 => {
                for index in 0..3 {
                    fs::write(generation.join(format!("file-{index}")), b"x").unwrap();
                }
            }
            _ => fs::create_dir_all(generation.join(".rebuild-scratch-v1/unexpected")).unwrap(),
        }
        let before = fs::read_dir(&generation).unwrap().count();
        assert!(build.discard().is_err());
        assert_eq!(fs::read_dir(&generation).unwrap().count(), before);
        assert!(generation.exists());
        assert!(owner.begin().is_err());
    }
}

#[cfg(unix)]
#[test]
fn follower_columnar_scratch_refuses_links_and_substituted_lock_without_external_deletion() {
    use std::os::unix::fs::symlink;
    let scope = crate::test_path::ScopedDirectory::new("follower-columnar-scratch");
    let outside = crate::test_path::ScopedDirectory::new("follower-columnar-scratch");
    fs::write(outside.join("keep"), b"outside").unwrap();
    let mut owner = open(&scope.join(""), 8, 64);
    let build = owner.begin().unwrap();
    symlink(
        outside.join(""),
        build.path().join("generation-0000000000000001"),
    )
    .unwrap();
    assert!(build.discard().is_err());
    assert_eq!(fs::read(outside.join("keep")).unwrap(), b"outside");
    fs::remove_file(scope.join("follower-columnar/build/generation-0000000000000001")).unwrap();
    let lock = scope.join("follower-columnar/inventory.lock");
    fs::rename(&lock, scope.join("held-lock")).unwrap();
    fs::write(&lock, b"").unwrap();
    assert!(owner.begin().is_err());
}

#[test]
fn follower_columnar_scratch_recovers_abandoned_build_after_process_exit() {
    const CHILD_ROOT: &str = "RIFFDB_FOLLOWER_COLUMNAR_SCRATCH_CHILD_ROOT";
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        let mut owner = open(Path::new(&root), 8, 64);
        let build = owner.begin().unwrap();
        let generation = build.path().join("generation-0000000000000001.tmp");
        fs::create_dir(&generation).unwrap();
        fs::write(generation.join("partial"), b"unfinished").unwrap();
        // The entire process exits while holding the actual lock and build lease.
        std::process::exit(73);
    }
    let scope = crate::test_path::ScopedDirectory::new("follower-columnar-process");
    let root = scope.join("");
    fs::write(scope.join("primary-selection"), b"unchanged").unwrap();
    for _ in 0..3 {
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("maintenance::follower_columnar_scratch_tests::follower_columnar_scratch_recovers_abandoned_build_after_process_exit")
            .arg("--nocapture")
            .env(CHILD_ROOT, &root)
            .output().unwrap();
        assert_eq!(child.status.code(), Some(73));
        let mut recovered = open(&root, 8, 64);
        // Opening the cold owner never enters or adopts the retained generation.
        assert!(
            root.join("follower-columnar/build/generation-0000000000000001.tmp/partial")
                .exists()
        );
        let build = recovered.begin().unwrap();
        assert_eq!(fs::read_dir(build.path()).unwrap().count(), 0);
        build.discard().unwrap();
        assert_eq!(
            fs::read(scope.join("primary-selection")).unwrap(),
            b"unchanged"
        );
    }
}

#[cfg(unix)]
#[test]
fn follower_columnar_scratch_retained_build_rejects_same_name_replacement() {
    use std::os::unix::fs::PermissionsExt;
    let scope = crate::test_path::ScopedDirectory::new("follower-columnar-substitution");
    let mut owner = open(&scope.join(""), 8, 64);
    let build = owner.begin().unwrap();
    let retained_path = build.path().to_path_buf();
    let moved = scope.join("moved-build");
    fs::rename(&retained_path, &moved).unwrap();
    fs::create_dir(&retained_path).unwrap();
    fs::set_permissions(&retained_path, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(retained_path.join("replacement"), b"must stay").unwrap();
    assert!(build.discard().is_err());
    assert!(moved.exists());
    assert_eq!(
        fs::read(retained_path.join("replacement")).unwrap(),
        b"must stay"
    );
}
