// req: REP-005, STO-012, REC-001
use super::*;
use prost::Message;
use riffdb_proto::{
    durable::{readable_record_registry, readable_record_schema},
    envelope::{STORAGE_FORMAT_VERSION_V1, payload_crc32c},
};
#[path = "fixtures.rs"]
mod fixtures;
use fixtures::*;

fn raw(name: &str, payload: Vec<u8>) -> Vec<u8> {
    wire::StoredEnvelope {
        storage_format_version: STORAGE_FORMAT_VERSION_V1,
        record_type: name.to_owned(),
        schema_hash: readable_record_schema(name)
            .unwrap()
            .schema_hash()
            .as_bytes()
            .to_vec(),
        payload_crc32c: payload_crc32c(&payload),
        payload,
    }
    .encode_to_vec()
}
fn wire_promotion() -> wire::StoredPromotionAdministrationV2 {
    let encoded = encode_promotion_administration_v2(&promotion(false)).unwrap();
    let outer = readable_record_registry()
        .decode(encoded.as_bytes())
        .unwrap();
    wire::StoredPromotionAdministrationV2::decode(outer.payload()).unwrap()
}

#[path = "tests/bounds.rs"]
mod bounds;
#[path = "tests/refusals.rs"]
mod refusals;
#[path = "tests/roundtrip.rs"]
mod roundtrip;

#[path = "tests/wire_vectors.rs"]
mod wire_vectors;

#[path = "tests/promotion_slots.rs"]
mod promotion_slots;
#[path = "tests/structure.rs"]
mod structure;
