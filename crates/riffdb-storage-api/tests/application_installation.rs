//! Durable application-installation campaign record contract.

use riffdb_storage_api::{
    MAX_APPLICATION_INSTALLATION_CAMPAIGN_STATE_BYTES, StoredApplicationInstallationCampaignV1,
    decode_application_installation_campaign_v1, encode_application_installation_campaign_v1,
};
use riffdb_types::{
    ApplicationInstallationCampaignId, ApplicationInstallationPlanHash, ContractLineage,
};

fn campaign_id(last: u8) -> ApplicationInstallationCampaignId {
    ApplicationInstallationCampaignId::from_bytes([
        0, 0, 0, 0, 0, 2, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, last,
    ])
    .expect("valid UUIDv7")
}

fn record(state: Vec<u8>) -> StoredApplicationInstallationCampaignV1 {
    StoredApplicationInstallationCampaignV1::new(
        campaign_id(1),
        ContractLineage::new("Example").expect("lineage"),
        ApplicationInstallationPlanHash::from_bytes([0x31; 32]),
        state,
    )
    .expect("campaign record")
}

#[test]
fn exact_campaign_state_round_trips_canonically_and_redacts_payload() {
    let expected = record(br#"{"schema":"campaign-state/v1"}"#.to_vec());
    let first = encode_application_installation_campaign_v1(&expected).expect("encode");
    let decoded = decode_application_installation_campaign_v1(first.as_bytes()).expect("decode");
    assert_eq!(decoded.value(), &expected);
    let second = encode_application_installation_campaign_v1(decoded.value()).expect("re-encode");
    assert_eq!(second.as_bytes(), first.as_bytes());

    let rendered = format!("{expected:?}");
    assert!(rendered.contains("[REDACTED]"));
    assert!(!rendered.contains("campaign-state"));
}

#[test]
fn campaign_state_is_nonempty_and_bounded_before_persistence() {
    assert!(
        StoredApplicationInstallationCampaignV1::new(
            campaign_id(1),
            ContractLineage::new("Example").expect("lineage"),
            ApplicationInstallationPlanHash::from_bytes([0x31; 32]),
            Vec::new(),
        )
        .is_err()
    );
    assert!(
        StoredApplicationInstallationCampaignV1::new(
            campaign_id(1),
            ContractLineage::new("Example").expect("lineage"),
            ApplicationInstallationPlanHash::from_bytes([0x31; 32]),
            vec![0; MAX_APPLICATION_INSTALLATION_CAMPAIGN_STATE_BYTES + 1],
        )
        .is_err()
    );
}
