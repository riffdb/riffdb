#![forbid(unsafe_code)]

//! Generates the checked redb migration compatibility fixtures.

use std::env;
use std::error::Error;
use std::fs;
use std::path::PathBuf;

use riffdb_storage_redb::migration_durable_fixture_set;

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args().skip(1);
    if arguments.next().as_deref() != Some("--output-root") {
        return Err("usage: generate_migration_durable_fixtures --output-root PATH".into());
    }
    let output_root = PathBuf::from(arguments.next().ok_or("missing output root")?);
    if arguments.next().is_some() {
        return Err("unexpected extra argument".into());
    }
    let directory = output_root.join("fixtures/migrations/durable/v1");
    fs::create_dir_all(&directory)?;
    for fixture in migration_durable_fixture_set()? {
        fs::write(directory.join(fixture.name()), fixture.bytes())?;
    }
    Ok(())
}
