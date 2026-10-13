//! Closed, private sealed-artifact publication intent under ADR-0294.
//! Decoding carries durable facts only; it cannot mint validation or readiness.
use super::*;
use riffdb_storage_api::ChangelogHistoryStateV3;

const MAGIC: &[u8] = b"RIFFDB-PUBLICATION-INTENT\0";
const VERSION: u32 = 1;
const MAX_INTENT_BYTES: usize = 4096;
const MAX_HISTORY_BYTES: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationPhase {
    Prepared,
    Published,
    StartupConsumed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactKind {
    Primary,
    Journal,
    FormatMarker,
}

impl ArtifactKind {
    const ORDERED: [Self; 3] = [Self::Primary, Self::Journal, Self::FormatMarker];
    const fn tag(self) -> u8 {
        match self {
            Self::Primary => 1,
            Self::Journal => 2,
            Self::FormatMarker => 3,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
struct ArtifactSeal {
    kind: ArtifactKind,
    length: u64,
    checksum: BackupIntegrityChecksumV1,
}

#[derive(Clone, Eq, PartialEq)]
struct PublicationIntent {
    operation_id: OfflineMaintenanceOperationId,
    manifest: OfflineBackupManifestIdentityV1,
    incarnation: u64,
    history: ChangelogHistoryStateV3,
    retention_watermark: u64,
    artifacts: [ArtifactSeal; 3],
    phase: PublicationPhase,
}

impl PublicationIntent {
    fn prepared(
        operation_id: OfflineMaintenanceOperationId,
        manifest: OfflineBackupManifestIdentityV1,
        incarnation: u64,
        history: ChangelogHistoryStateV3,
        retention_watermark: u64,
        artifacts: [ArtifactSeal; 3],
    ) -> Result<Self, StorageError> {
        let intent = Self {
            operation_id,
            manifest,
            incarnation,
            history,
            retention_watermark,
            artifacts,
            phase: PublicationPhase::Prepared,
        };
        intent.validate()?;
        Ok(intent)
    }

    fn validate(&self) -> Result<(), StorageError> {
        if self.incarnation == 0
            || self.manifest.manifest_checksum().as_bytes().len() != SHA256_BYTES
            || self.history.lineage().database_id() != self.manifest.database_id()
            || self.history.lineage().history_incarnation() != self.incarnation
            || self.history.tail().frontier().application()
                != self.manifest.included_application_frontier()
            || self.retention_watermark
                > self
                    .manifest
                    .included_application_frontier()
                    .map_or(0, |sequence| sequence.get())
        {
            return Err(corrupt());
        }
        for (seal, expected) in self.artifacts.iter().zip(ArtifactKind::ORDERED) {
            if seal.kind != expected
                || seal.length == 0
                || seal.checksum.as_bytes().len() != SHA256_BYTES
            {
                return Err(corrupt());
            }
        }
        Ok(())
    }

    fn advance(&mut self, next: PublicationPhase) -> Result<(), StorageError> {
        self.validate()?;
        if !matches!(
            (self.phase, next),
            (PublicationPhase::Prepared, PublicationPhase::Published)
                | (
                    PublicationPhase::Published,
                    PublicationPhase::StartupConsumed
                )
        ) {
            return Err(corrupt());
        }
        self.phase = next;
        Ok(())
    }

    fn encode(&self) -> Result<Vec<u8>, StorageError> {
        self.validate()?;
        let mut output = Encoder::new_with_limit(MAX_INTENT_BYTES - SHA256_BYTES);
        output.bytes(MAGIC)?;
        output.u32(VERSION)?;
        output.u8(match self.phase {
            PublicationPhase::Prepared => 1,
            PublicationPhase::Published => 2,
            PublicationPhase::StartupConsumed => 3,
        })?;
        output.bytes(self.operation_id.as_bytes())?;
        output.bytes(self.manifest.manifest_checksum().as_bytes())?;
        output.bytes(self.manifest.database_id().as_bytes())?;
        match self.manifest.included_application_frontier() {
            None => output.u8(0)?,
            Some(frontier) => {
                output.u8(1)?;
                output.u64(frontier.get())?;
            }
        }
        output.u64(self.incarnation)?;
        output.u64(self.retention_watermark)?;
        // Reuse the one semantic history type and its canonical codec. The
        // intent does not serialize a process proof or invent another history.
        let history =
            riffdb_storage_api::proto_codec::encode_changelog_history_state_v3(self.history)
                .map_err(crate::error::codec_error)?;
        if history.as_bytes().len() > MAX_HISTORY_BYTES {
            return Err(limit_exceeded());
        }
        output.framed_u16(history.as_bytes())?;
        output.u8(3)?;
        for seal in &self.artifacts {
            output.u8(seal.kind.tag())?;
            output.u64(seal.length)?;
            output.bytes(seal.checksum.as_bytes())?;
        }
        let mut bytes = output.finish();
        let checksum = Sha256::digest(&bytes);
        bytes.extend_from_slice(&checksum);
        Ok(bytes)
    }

    fn decode(encoded: &[u8]) -> Result<Self, StorageError> {
        if encoded.len() > MAX_INTENT_BYTES {
            return Err(limit_exceeded());
        }
        let body_length = encoded
            .len()
            .checked_sub(SHA256_BYTES)
            .ok_or_else(corrupt)?;
        let (body, checksum) = encoded.split_at(body_length);
        if Sha256::digest(body).as_slice() != checksum {
            return Err(corrupt());
        }
        let mut input = Decoder::new(body);
        if input.bytes(MAGIC.len())? != MAGIC || input.u32()? != VERSION {
            return Err(incompatible());
        }
        let phase = match input.u8()? {
            1 => PublicationPhase::Prepared,
            2 => PublicationPhase::Published,
            3 => PublicationPhase::StartupConsumed,
            _ => return Err(corrupt()),
        };
        let operation_id =
            OfflineMaintenanceOperationId::from_bytes(input.array()?).map_err(|_| corrupt())?;
        let manifest_checksum = BackupIntegrityChecksumV1::new(input.bytes(SHA256_BYTES)?.to_vec())
            .map_err(value_error)?;
        let database = DatabaseId::from_bytes(input.array()?).map_err(|_| corrupt())?;
        let application = match input.u8()? {
            0 => None,
            1 => Some(CommitSequence::new(input.u64()?).ok_or_else(corrupt)?),
            _ => return Err(corrupt()),
        };
        let incarnation = input.u64()?;
        let retention_watermark = input.u64()?;
        let history = *riffdb_storage_api::proto_codec::decode_changelog_history_state_v3(
            input.framed_u16(MAX_HISTORY_BYTES)?,
        )
        .map_err(crate::error::codec_error)?
        .value();
        if input.u8()? != 3 {
            return Err(corrupt());
        }
        let mut read_seal = |kind: ArtifactKind| -> Result<ArtifactSeal, StorageError> {
            if input.u8()? != kind.tag() {
                return Err(corrupt());
            }
            Ok(ArtifactSeal {
                kind,
                length: input.u64()?,
                checksum: BackupIntegrityChecksumV1::new(input.bytes(SHA256_BYTES)?.to_vec())
                    .map_err(value_error)?,
            })
        };
        let artifacts = [
            read_seal(ArtifactKind::Primary)?,
            read_seal(ArtifactKind::Journal)?,
            read_seal(ArtifactKind::FormatMarker)?,
        ];
        input.finish()?;
        let mut intent = Self::prepared(
            operation_id,
            OfflineBackupManifestIdentityV1::new(manifest_checksum, database, application),
            incarnation,
            history,
            retention_watermark,
            artifacts,
        )?;
        intent.phase = phase;
        if intent.encode()? != encoded {
            return Err(corrupt());
        }
        Ok(intent)
    }
}

#[cfg(test)]
#[path = "publication_intent_tests.rs"]
mod tests;
