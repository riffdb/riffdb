#![forbid(unsafe_code)]
//! Destination-format compatibility vector using synthetic checked values only.

use riffdb_storage_api::{
    LocalOutboxArchiveRecordV1, StoredDurableEventV1, derive_event_hash_v1,
    encode_local_outbox_archive_record_v1,
};
use riffdb_types::{CanonicalRecord, CommitSequence, DatabaseId, EventId, EventTypeId};
use std::{error::Error, fmt::Write, num::NonZeroU64, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let check = match args.next().as_deref() {
        None => false,
        Some("--check") => true,
        _ => return Err("expected --check or no arguments".into()),
    };
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let id = EventId::new(CommitSequence::first(), 0);
    let kind = EventTypeId::try_from(1)?;
    let payload = CanonicalRecord::new(vec![])?;
    let hash = derive_event_hash_v1(id, kind, &payload)?;
    let record = LocalOutboxArchiveRecordV1::new(
        DatabaseId::from_unix_milliseconds_and_random(42, [1; 10])?,
        NonZeroU64::MIN,
        StoredDurableEventV1::new(id, kind, payload, hash)?,
    );
    let envelope = encode_local_outbox_archive_record_v1(&record)?;
    let mut encoded = String::new();
    for byte in envelope.as_bytes() {
        write!(encoded, "{byte:02x}")?;
    }
    encoded.push('\n');
    for relative in [
        "../../fixtures/compatibility/local-outbox-archive-v1.hex",
        "fixtures/local-outbox-archive-v1.hex",
    ] {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
        if check {
            if std::fs::read_to_string(path)? != encoded {
                return Err("stale local outbox archive fixture".into());
            }
        } else {
            std::fs::write(path, &encoded)?;
        }
    }
    Ok(())
}
