// req: REP-005, REC-001, STO-012
use super::*;

#[test]
fn primary_fence_control_process_exit_recovers_only_old_or_complete_fence() {
    for (profile, name) in [
        (crate::RedbCommitProfile::Standard, "standard"),
        (crate::RedbCommitProfile::Hardened, "hardened"),
    ] {
        for edge in ["audit", "roots", "committed"] {
            let (path, ports, request) = attached_source(profile);
            let head = history(&ports).tail().frontier().application();
            drop(ports);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "administration::tests::fenced_runtime::fence_control::process::primary_fence_control_process_child",
                    "--nocapture",
                ])
                .env("RIFFDB_WP748_FENCE_CONTROL_PATH", &path.0)
                .env("RIFFDB_WP748_FENCE_CONTROL_PROFILE", name)
                .env("RIFFDB_WP748_FENCE_CONTROL_GENERATION", request.generation().get().to_string())
                .env("RIFFDB_PRIMARY_FENCE_EDGE", edge)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(94));
            let reopened =
                crate::startup::open_validated_source_fixture(&path.0, profile, inputs());
            let admission = reopened.read_replication_primary_admission().unwrap();
            assert_eq!(admission.fence().is_some(), edge == "committed");
            assert_eq!(history(&reopened).tail().frontier().application(), head);
            let record = finish(&reopened, request).unwrap();
            if edge == "committed" {
                assert_eq!(Some(&record), admission.fence());
            }
            assert_eq!(record.operation_id(), request.operation_id());
            assert_eq!(record.request_id(), request.request_id());
            assert_eq!(record.target(), request.target());
            assert_eq!(record.generation(), request.generation());
            assert_eq!(record.final_application_head(), head);
        }
    }
}

#[test]
fn primary_fence_control_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_WP748_FENCE_CONTROL_PATH") else {
        return;
    };
    let profile = match std::env::var("RIFFDB_WP748_FENCE_CONTROL_PROFILE")
        .unwrap()
        .as_str()
    {
        "standard" => crate::RedbCommitProfile::Standard,
        "hardened" => crate::RedbCommitProfile::Hardened,
        _ => panic!("unknown profile"),
    };
    let ports = crate::startup::open_validated_source_fixture(
        std::path::Path::new(&path),
        profile,
        inputs(),
    );
    let lineage = history(&ports).lineage();
    let target = ReplicationFollowerAuditTargetV1::new(
        lineage.database_id(),
        lineage.history_incarnation(),
        lineage.leadership_epoch(),
        ReplicationSourceHoldIdV1::new([0x79; 16]).unwrap(),
    )
    .unwrap();
    let generation = riffdb_storage_api::ChangelogTransactionSequence::new(
        std::env::var("RIFFDB_WP748_FENCE_CONTROL_GENERATION")
            .unwrap()
            .parse()
            .unwrap(),
    )
    .unwrap();
    let request = PrimaryFenceRequestV1::new(
        request_id(31),
        ReplicationFenceOperationId::from_bytes(uuid_bytes(32)).unwrap(),
        target,
        generation,
    );
    finish(&ports, request).unwrap();
    panic!("selected process edge must exit without drops");
}
