#![forbid(unsafe_code)]

//! Offline TSV export consumed only by the operation-adapter generator.

use riffdb_operation_registry::{OPERATION_REGISTRY_VERSION, OPERATIONS};

fn main() {
    println!("version\t{OPERATION_REGISTRY_VERSION}");
    for entry in OPERATIONS {
        let mappings = entry
            .field_map
            .iter()
            .map(|mapping| format!("{}={:?}", mapping.proto_path, mapping.kind))
            .collect::<Vec<_>>()
            .join(",");
        let bounds = entry
            .bounds
            .iter()
            .map(|bound| format!("{:?}={}", bound.target, bound.maximum))
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "operation\t{}\t{:?}\t{}\t{}\t{}\t{}\t{:?}\t{:?}\t{:?}\t{}\t{}\t{}",
            entry.operation.tag(),
            entry.operation,
            entry.proto_request,
            entry.proto_response,
            entry.dto_request,
            entry.dto_result,
            entry.permission,
            entry.idempotency,
            entry.output_redaction,
            entry.audiences.len(),
            bounds,
            mappings,
        );
    }
}
