//! Reviewed external bootstrap receipt/page V1 bytes over the V3 authority catalog.
use riffdb_storage_api::*;
use riffdb_types::{DatabaseId, DualFrontier};
use std::{collections::VecDeque, error::Error, path::Path};

struct FixtureCursor {
    history: ChangelogHistoryStateV3,
    items: VecDeque<AuthoritativeStateStepV3>,
}
impl AuthoritativeStateCursorV3 for FixtureCursor {
    fn history(&self) -> ChangelogHistoryStateV3 {
        self.history
    }
    fn next_item(&mut self) -> Result<Option<AuthoritativeStateStepV3>, ChangelogCursorErrorV3> {
        Ok(self.items.pop_front())
    }
}
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
    let lineage = ChangelogLineageV3::new(
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x74; 10])?,
        1,
        LeadershipEpochV1::initial(),
    )?;
    let point = ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(9).ok_or("sequence")?,
        [0x25; 32],
        DualFrontier::INITIAL,
    );
    let history = ChangelogHistoryStateV3::new(lineage, point, point, point)?;
    let fence = ReplicationBootstrapFenceV3::new(
        ReplicationSourceHoldIdV1::new([0x18; 16]).ok_or("hold")?,
        history,
    );
    let mut items = VecDeque::new();
    for namespace in AuthoritativeNamespaceV1::ALL
        .into_iter()
        .filter(|n| n.class() == ReplicationAuthorityClassV1::ReplicatedAuthoritative)
    {
        items.push_back(AuthoritativeStateStepV3::Row(AuthoritativeStateRowV3::new(
            namespace,
            namespace
                .metadata_key()
                .map(str::as_bytes)
                .unwrap_or(b"key"),
            b"opaque source bytes",
        )?));
        items.push_back(AuthoritativeStateStepV3::EndNamespace(namespace));
    }
    let mut cursor =
        ReplicationBootstrapPageCursorV3::new(fence, Box::new(FixtureCursor { history, items }))?;
    let mut pages = String::new();
    let mut observed = Vec::new();
    while let Some(page) = cursor.next_page()? {
        pages.push_str(&hex(&page.encode()?));
        observed.push(page);
    }
    let final_manifest = cursor.manifest()?;
    let manifest = hex(&final_manifest.encode()?);
    let mut receiver = ReplicationBootstrapTranscriptV3::new(fence);
    let mut progress = hex(&receiver.checkpoint(final_manifest)?.encode()?);
    for page in observed {
        receiver.observe(&page)?;
        progress.push_str(&hex(&receiver.checkpoint(final_manifest)?.encode()?));
    }
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/replication");
    for (name, content) in [
        ("bootstrap-pages-v1.hex", pages),
        ("bootstrap-manifest-v1.hex", manifest),
        ("bootstrap-progress-v1.hex", progress),
    ] {
        let path = directory.join(name);
        if check {
            if std::fs::read_to_string(path)? != content {
                return Err(format!("stale bootstrap fixture: {name}").into());
            }
        } else {
            std::fs::write(path, content)?;
        }
    }
    println!("Replication bootstrap fixtures are current.");
    Ok(())
}
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut result = String::with_capacity(bytes.len() * 2 + 1);
    for byte in bytes {
        let _ = write!(result, "{byte:02x}");
    }
    result.push('\n');
    result
}
