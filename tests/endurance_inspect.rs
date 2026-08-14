#![forbid(unsafe_code)]

//! Read-only stopped-database checkpoint evidence for the alpha endurance orchestrator.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let database = arguments
        .next()
        .map(PathBuf::from)
        .ok_or("database path is required")?;
    if arguments.next().is_some() || !database.is_absolute() || database.is_symlink() {
        return Err("usage: riffdb-endurance-inspect ABSOLUTE_DATABASE_PATH".into());
    }
    let checkpoint =
        riffdb_storage_redb::read_validated_prefix_checkpoint_commit_sequence_fixture(&database)?;
    println!(
        "{}",
        serde_json::json!({
            "schema": "riffdb.alpha-endurance-storage-inspection/v1",
            "checkpoint_commit_sequence": checkpoint,
        })
    );
    Ok(())
}
