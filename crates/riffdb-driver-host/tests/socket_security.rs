//! Unix-socket ownership and filesystem confinement tests.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_driver_host::{DriverSocket, SocketError};

static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let unique = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("riffdb-dh-socket-{}-{unique}", std::process::id()));
        fs::create_dir(&path).expect("unique test root");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).expect("permissions");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn socket_requires_private_directory_and_rejects_live_duplicate() {
    let temporary = TestRoot::new();
    let private = temporary.path().join("private");
    fs::create_dir(&private).expect("private directory");
    fs::set_permissions(&private, fs::Permissions::from_mode(0o700)).expect("permissions");
    let path = private.join("driver.sock");
    let _owner = DriverSocket::bind(&path).expect("first owner");
    assert!(matches!(
        DriverSocket::bind(&path),
        Err(SocketError::DuplicateOwner)
    ));

    let public = temporary.path().join("public");
    fs::create_dir(&public).expect("public directory");
    fs::set_permissions(&public, fs::Permissions::from_mode(0o755)).expect("permissions");
    assert!(matches!(
        DriverSocket::bind(public.join("driver.sock")),
        Err(SocketError::UnsafeDirectory)
    ));
}

#[tokio::test]
async fn stale_private_socket_is_reclaimed_but_regular_file_is_not() {
    let temporary = TestRoot::new();
    let private = temporary.path().join("private");
    fs::create_dir(&private).expect("private directory");
    fs::set_permissions(&private, fs::Permissions::from_mode(0o700)).expect("permissions");
    let stale = private.join("stale.sock");
    drop(std::os::unix::net::UnixListener::bind(&stale).expect("stale socket"));
    fs::set_permissions(&stale, fs::Permissions::from_mode(0o600)).expect("permissions");
    let _recovered = DriverSocket::bind(&stale).expect("safe stale recovery");

    let regular = private.join("regular.sock");
    fs::write(&regular, b"do not delete").expect("regular file");
    fs::set_permissions(&regular, fs::Permissions::from_mode(0o600)).expect("permissions");
    assert!(matches!(
        DriverSocket::bind(&regular),
        Err(SocketError::DuplicateOwner)
    ));
    assert_eq!(fs::read(&regular).expect("preserved"), b"do not delete");
}
