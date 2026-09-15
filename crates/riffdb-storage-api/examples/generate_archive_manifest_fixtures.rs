#![forbid(unsafe_code)]
//! Deterministic additive archive-manifest/v1 vectors; no archive daemon is started.
#[path = "../tests/support/archive_manifest_fixture.rs"]
mod fixture_support;
use riffdb_storage_api::*;
use std::{error::Error, fmt::Write, path::Path};

struct Capture {
    previous: Option<ArchiveManifestV1>,
    posture: ArchiveEncryptionPostureV1,
}
impl ArchiveFrameSinkV1 for Capture {
    fn persist(&mut self, frame: &ArchiveFrameV1) -> Result<(), ArchiveConsumerErrorV1> {
        self.previous = Some(match self.previous {
            None => ArchiveManifestV1::first(frame, [0x55; 32], self.posture)?,
            Some(previous) => previous.next(frame)?,
        });
        Ok(())
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
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/replication");
    for (name, posture) in [
        ("unencrypted", ArchiveEncryptionPostureV1::Unencrypted),
        (
            "operator-managed",
            ArchiveEncryptionPostureV1::OperatorManaged,
        ),
    ] {
        let (lineage, mut before) = fixture_support::fixture();
        let mut archive = ArchiveConsumerV1::new(
            Capture {
                previous: None,
                posture,
            },
            lineage,
            before,
        );
        for application in [3, 5] {
            let bytes = fixture_support::frame(lineage, before, application);
            before = archive.append(bytes.clone())?;
            let sink = archive.into_sink();
            let manifest = sink.previous.ok_or("missing manifest")?;
            let path = directory.join(format!("archive-manifest-v1-{name}-{application}.hex"));
            let mut encoded = String::new();
            for byte in manifest.encode() {
                write!(encoded, "{byte:02x}")?;
            }
            encoded.push('\n');
            if check {
                if std::fs::read_to_string(path)? != encoded {
                    return Err("stale archive manifest fixture".into());
                }
            } else {
                std::fs::write(path, encoded)?;
            }
            archive = ArchiveConsumerV1::new(sink, lineage, before);
        }
    }
    println!("Archive manifest fixtures are current.");
    Ok(())
}
