//! Offline operator inspection; no application client or payload output.

use riffdb_outbox::DurableLocalArchive;
use riffdb_types::DatabaseId;
use serde::Serialize;

use crate::cli::{LocalArchiveCommand, OutboxCommand};
use crate::output::{CommandIdentity, Terminal, local_error, success};

#[derive(Serialize)]
struct Inspection<'a> {
    format: &'static str,
    database_id: &'a str,
    records: u64,
    encoded_bytes: u64,
    physical_bytes: u64,
    integrity: &'static str,
    pages_examined: u16,
}

pub(crate) fn inspect(command: &OutboxCommand) -> Terminal {
    let OutboxCommand::Archive {
        command:
            LocalArchiveCommand::Inspect {
                directory,
                database_id,
                max_pages,
            },
    } = command;
    let identity = CommandIdentity::LocalArchiveInspect;
    let Some(binding) = parse_binding(database_id) else {
        return local_error(
            identity,
            "invalid_database_id",
            "expected a valid 32-character hexadecimal database identity",
        );
    };
    let result = (|| {
        let mut archive =
            DurableLocalArchive::open_existing(std::path::Path::new(directory), binding)?;
        let mut complete = false;
        let mut pages_examined = 0;
        for _ in 0..*max_pages {
            pages_examined += 1;
            if archive.validate_next_page()? {
                complete = true;
                break;
            }
        }
        let counts = archive.observation()?;
        Ok::<_, riffdb_outbox::LocalArchiveError>(Inspection {
            format: "LocalOutboxArchiveRecordV1",
            database_id,
            records: counts.records(),
            encoded_bytes: counts.encoded_bytes(),
            physical_bytes: archive.physical_bytes()?,
            integrity: if complete { "verified" } else { "incomplete" },
            pages_examined,
        })
    })();
    match result {
        Ok(report) => success(identity, report.integrity, &report),
        Err(_) => local_error(
            identity,
            "archive_inspection_refused",
            "archive inspection requires exclusive access to a compatible protected archive with the expected database binding",
        ),
    }
}

fn parse_binding(text: &str) -> Option<DatabaseId> {
    if text.len() != 32 {
        return None;
    }
    let mut bytes = [0; 16];
    for (target, pair) in bytes.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        let pair = std::str::from_utf8(pair).ok()?;
        *target = u8::from_str_radix(pair, 16).ok()?;
    }
    DatabaseId::from_bytes(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    // req: STO-020
    #[test]
    fn database_binding_is_bounded_and_semantically_checked() {
        let binding = DatabaseId::from_unix_milliseconds_and_random(42, [1; 10]).unwrap();
        let text = binding
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(parse_binding(&text), Some(binding));
        for bad in [
            "",
            "00000000000000000000000000000000",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "012345678901234567890123456789012",
        ] {
            assert_eq!(parse_binding(bad), None);
        }
    }
}
