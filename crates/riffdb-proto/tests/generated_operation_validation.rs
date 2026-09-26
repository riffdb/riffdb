#![forbid(unsafe_code)]

//! WP-751 generated-validation compatibility and ownership proof.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use prost::Message;
use prost_types::FileDescriptorSet;
use riffdb_operation_registry::{
    BoundTarget, MAINTENANCE_OPERATIONS, OPERATIONS, declaration, maintenance_declaration,
};
use riffdb_types::{ServiceIngressKindV1, ServiceOperationV1};

const GENERATED: &str = include_str!("../src/generated/public_validation.rs");
const FROZEN: &str = include_str!("../../../fixtures/proto/public-validation-v1.rs.in");
const WRAPPER: &str = include_str!("../src/public_message.rs");

fn visit_hand_written_rust(path: &Path, files: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(path).expect("read source directory") {
        let entry = entry.expect("read source entry");
        let entry_path = entry.path();
        if entry_path.is_dir() {
            if entry.file_name() != "generated" {
                visit_hand_written_rust(&entry_path, files);
            }
        } else if entry_path
            .extension()
            .is_some_and(|extension| extension == "rs")
        {
            files.push(entry_path);
        }
    }
}

/// Every public RPC and its request/response pair, from the checked descriptor.
fn public_rpcs() -> Vec<(String, String, String)> {
    let descriptors = FileDescriptorSet::decode(riffdb_proto::PRODUCTION_FILE_DESCRIPTOR_SET)
        .expect("checked descriptor fixture");
    let mut rpcs = Vec::new();
    for file in &descriptors.file {
        for service in &file.service {
            for method in &service.method {
                rpcs.push((
                    format!("{}.{}/{}", file.package(), service.name(), method.name()),
                    method.input_type().trim_start_matches('.').to_owned(),
                    method.output_type().trim_start_matches('.').to_owned(),
                ));
            }
        }
    }
    rpcs
}

// req: REC-008
/// OBL-0203-2 proof (and the WP-751 ownership proof). Every public RPC in the
/// checked descriptor resolves to exactly one registry declaration: a durable
/// semantic [`ServiceOperationV1`] declaration or, for the six receipted
/// maintenance RPCs of ADR-0203 Amendment 1, one audit-exempt gRPC-only
/// maintenance declaration keyed by method identity, never both; the six RPCs
/// that predate registry coverage are pinned as an exact residual. The
/// generated inventory reproduces the maintenance declarations exactly.
#[test]
fn every_public_operation_has_exactly_one_registry_declaration() {
    let mut uncovered = Vec::new();
    for (method, request, response) in public_rpcs() {
        let semantic = OPERATIONS
            .iter()
            .filter(|entry| entry.proto_request == request && entry.proto_response == response)
            .count();
        let maintenance = MAINTENANCE_OPERATIONS
            .iter()
            .filter(|entry| entry.method == method)
            .count();
        assert!(semantic + maintenance <= 1, "{method} is declared twice");
        if let Some(entry) = maintenance_declaration(&method) {
            assert_eq!(
                (entry.proto_request, entry.proto_response),
                (request.as_str(), response.as_str())
            );
            assert_eq!(entry.ingress, &[ServiceIngressKindV1::Grpc]);
        }
        if semantic + maintenance == 0 {
            uncovered.push(method);
        }
    }
    // RPCs that predate registry coverage and belong to no ADR-0203 family.
    // Pinned exactly: a new undeclared RPC fails here, and closing one of
    // these debts must remove it from the list.
    uncovered.sort();
    assert_eq!(
        uncovered,
        [
            "riffdb.app.v1.ApplicationQueryService/GetApplicationCatalog",
            "riffdb.app.v1.ApplicationQueryService/GetQueryModule",
            "riffdb.v1.AdminService/CheckContractMigration",
            "riffdb.v1.AdminService/GetContractMigrationOperation",
            "riffdb.v1.ApplicationSessionService/Open",
            "riffdb.v1.CommandService/ExecuteBatch",
        ]
    );
    assert_eq!(
        riffdb_proto::GENERATED_MAINTENANCE_VALIDATION_MESSAGES.len(),
        MAINTENANCE_OPERATIONS.len()
    );
    for (generated, entry) in riffdb_proto::GENERATED_MAINTENANCE_VALIDATION_MESSAGES
        .iter()
        .zip(MAINTENANCE_OPERATIONS)
    {
        assert_eq!(generated.method, entry.method);
        assert_eq!(generated.request, entry.proto_request);
        assert_eq!(generated.response, entry.proto_response);
        for bound in entry.bounds {
            let generated_bound = match bound.target {
                BoundTarget::EncodedRequestBytes => generated.request_bytes,
                BoundTarget::EncodedResponseBytes => generated.response_bytes,
            };
            assert_eq!(generated_bound, bound.maximum);
        }
    }

    assert_eq!(OPERATIONS.len(), ServiceOperationV1::ALL.len());
    let declared = OPERATIONS
        .iter()
        .map(|entry| entry.operation)
        .collect::<BTreeSet<_>>();
    assert_eq!(declared, ServiceOperationV1::ALL.into_iter().collect());
    for operation in ServiceOperationV1::ALL {
        assert_eq!(declaration(operation).operation, operation);
    }

    let mut files = Vec::new();
    visit_hand_written_rust(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );
    for path in files {
        let source = fs::read_to_string(&path).expect("read hand-written Rust source");
        for (offset, _) in source.match_indices("fn validate_") {
            let suffix = &source[offset..source.len().min(offset + 160)];
            assert!(
                !suffix.contains("_exchange"),
                "hand-written exchange validation remains outside src/generated: {}",
                path.display()
            );
        }
    }
}

#[test]
fn generated_adapters_reproduce_frozen_public_fixtures_byte_for_byte() {
    assert!(GENERATED.starts_with("// @generated by scripts/generate-operation-adapters"));
    let after_header = GENERATED
        .strip_prefix("// @generated by scripts/generate-operation-adapters; DO NOT EDIT.\n\n")
        .expect("generated header");
    let (_, generated_body) = FROZEN
        .split_once("\n\n")
        .expect("frozen module context documentation");
    assert!(after_header.starts_with(generated_body));
    assert_eq!(
        GENERATED.match_indices("pub fn validate_").count(),
        FROZEN.match_indices("pub fn validate_").count(),
        "generation lost a public validation exchange"
    );
    assert_eq!(
        WRAPPER.trim(),
        "include!(\"generated/public_validation.rs\");",
        "the hand-written module regained validation ownership"
    );
}

#[test]
fn generated_inventory_covers_every_registry_operation_in_tag_order() {
    assert_eq!(riffdb_proto::GENERATED_OPERATION_REGISTRY_VERSION, 1);
    assert_eq!(riffdb_proto::GENERATED_PUBLIC_OPERATION_COUNT, 62);
    assert_eq!(riffdb_proto::GENERATED_PUBLIC_VALIDATION_MESSAGES.len(), 62);
    let stream = &riffdb_proto::GENERATED_PUBLIC_VALIDATION_MESSAGES[60];
    assert_eq!((stream.tag, stream.operation), (0x3d, "StreamChangelog"));
    assert_eq!(
        (stream.request, stream.response),
        (
            "riffdb.v1.StreamChangelogRequest",
            "riffdb.v1.StreamChangelogResponse"
        )
    );
    assert_eq!((stream.request_fields, stream.response_fields), (13, 6));
    assert_eq!(
        (stream.request_bytes, stream.response_bytes),
        (1024, 32 * 1024 * 1024 + 1024)
    );
    let promotion = &riffdb_proto::GENERATED_PUBLIC_VALIDATION_MESSAGES[61];
    assert_eq!(
        (promotion.tag, promotion.operation),
        (0x3e, "PromoteFollower")
    );
    assert_eq!(
        (promotion.request, promotion.response),
        (
            "riffdb.v1.PromoteFollowerRequest",
            "riffdb.v1.PromoteFollowerResponse"
        )
    );
    assert_eq!(
        (promotion.request_fields, promotion.response_fields),
        (5, 1)
    );
    for (index, entry) in riffdb_proto::GENERATED_PUBLIC_VALIDATION_MESSAGES
        .iter()
        .enumerate()
    {
        assert_eq!(usize::from(entry.tag), index + 1);
        assert!(!entry.operation.is_empty());
        assert!(entry.request.starts_with("riffdb."));
        assert!(entry.response.starts_with("riffdb."));
        let declaration = declaration(ServiceOperationV1::ALL[index]);
        let request_bound = declaration
            .bounds
            .iter()
            .find(|bound| bound.target == BoundTarget::EncodedRequestBytes)
            .expect("request bound");
        let response_bound = declaration
            .bounds
            .iter()
            .find(|bound| bound.target == BoundTarget::EncodedResponseBytes)
            .expect("response bound");
        assert_eq!(entry.request_bytes, request_bound.maximum);
        assert_eq!(entry.response_bytes, response_bound.maximum);
    }
}
