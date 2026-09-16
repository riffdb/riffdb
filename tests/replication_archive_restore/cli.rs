//! Real CLI process with bounded output and a hard process deadline.
use riffdb_types::{ArchiveRestoreStopV1, OfflineMaintenanceOperationId};
use serde_json::Value;
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
const MAX_OUTPUT: u64 = 16 * 1024;
pub(super) struct Cli {
    binary: PathBuf,
    timeout: PathBuf,
    config: PathBuf,
}
impl Cli {
    pub(super) fn new(root: &Path, server_document: &str, token: &str) -> Self {
        let binary = PathBuf::from(
            std::env::var_os("RIFFDB_ARCHIVE_CLI_BIN").expect("run scripts/check-archive-cli"),
        );
        let timeout = PathBuf::from(
            std::env::var_os("RIFFDB_ARCHIVE_TIMEOUT_BIN").expect("run scripts/check-archive-cli"),
        );
        assert!(binary.is_absolute() && binary.is_file());
        assert!(timeout.is_absolute() && timeout.is_file());
        let credential = root.join("archive-cli.token");
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&credential)
            .unwrap()
            .write_all(token.as_bytes())
            .unwrap();
        let document: toml::Value = toml::from_str(server_document).unwrap();
        let endpoint = document["server"]["application_listener"]["public_endpoint"]
            .as_str()
            .unwrap();
        let config = root.join("archive-cli.toml");
        std::fs::write(&config, format!("[client]\nendpoint = {endpoint:?}\noutput = 'json'\nmax_attempts = 1\ncredential_file = {credential:?}\ntls_trust_root = {:?}\ntls_server_name = '127.0.0.1'\n", root.join("ca.crt"))).unwrap();
        Self {
            binary,
            timeout,
            config,
        }
    }
    fn run(&self, arguments: &[&str]) -> Value {
        let mut child = Command::new(&self.timeout)
            .args(["--signal=KILL", "30s"])
            .arg(&self.binary)
            .args(["--config"])
            .arg(&self.config)
            .args(arguments)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let read = |reader: Box<dyn Read + Send>| {
            std::thread::spawn(move || {
                let mut output = Vec::new();
                reader
                    .take(MAX_OUTPUT + 1)
                    .read_to_end(&mut output)
                    .unwrap();
                output
            })
        };
        let stdout = read(Box::new(child.stdout.take().unwrap()));
        let stderr = read(Box::new(child.stderr.take().unwrap()));
        let status = child.wait().unwrap();
        let stdout = stdout.join().unwrap();
        let stderr = stderr.join().unwrap();
        assert!(stdout.len() as u64 <= MAX_OUTPUT && stderr.len() as u64 <= MAX_OUTPUT);
        assert_eq!(
            status.code(),
            Some(0),
            "CLI failed: {}",
            String::from_utf8_lossy(&stdout)
        );
        assert!(stderr.is_empty(), "unexpected CLI diagnostic");
        let value: Value = serde_json::from_slice(&stdout).unwrap();
        assert_eq!(value["schema"], "riffdb.cli.output/v1");
        assert_eq!(value["ok"], true);
        value
    }
    pub(super) fn restore(&self, stop: ArchiveRestoreStopV1) -> OfflineMaintenanceOperationId {
        let sequence = match stop {
            ArchiveRestoreStopV1::LastArchived => None,
            ArchiveRestoreStopV1::AtApplicationSequence(value) => Some(value.get().to_string()),
        };
        let mut args = vec![
            "storage",
            "restore",
            "baseline",
            "--archive",
            "daily",
            "--confirm-replace-current-database",
        ];
        if let Some(sequence) = sequence.as_deref() {
            args.extend(["--stop-at-sequence", sequence]);
        }
        let value = self.run(&args);
        assert_eq!(value["command"], "storage.restore");
        assert_eq!(value["result"]["status"], "accepted");
        assert_eq!(value["result"]["archive_restore"]["archive_name"], "daily");
        let text = value["result"]["maintenance_operation_id"]
            .as_str()
            .unwrap();
        assert_eq!(text.len(), 36);
        let compact = text.replace('-', "");
        assert_eq!(compact.len(), 32);
        let mut bytes = [0; 16];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&compact[index * 2..index * 2 + 2], 16).unwrap();
        }
        let id = OfflineMaintenanceOperationId::from_bytes(bytes).unwrap();
        assert_eq!(id.to_string(), text);
        id
    }
    pub(super) fn terminal(
        &self,
        id: OfflineMaintenanceOperationId,
        sequence: u64,
        stop: ArchiveRestoreStopV1,
    ) {
        let value = self.run(&["backup", "operation", &id.to_string()]);
        assert_eq!(value["command"], "backup.operation");
        let result = &value["result"];
        assert_eq!(result["maintenance_operation_id"], id.to_string());
        assert_eq!(result["status"], "found");
        assert_eq!(result["phase"], "succeeded");
        let detail = &result["archive_restore"];
        assert_eq!(detail["archive_name"], "daily");
        assert_eq!(detail["backup_application_frontier"]["sequence"], "1");
        assert_eq!(
            detail["restored_frontier"]["application"]["sequence"],
            sequence.to_string()
        );
        match stop {
            ArchiveRestoreStopV1::LastArchived => {
                assert_eq!(detail["stop"], "last_archived");
                assert!(detail.get("stop_at_sequence").is_none());
            }
            ArchiveRestoreStopV1::AtApplicationSequence(value) => {
                assert_eq!(detail["stop"], "at_sequence");
                assert_eq!(detail["stop_at_sequence"], value.get().to_string());
            }
        }
    }
}
