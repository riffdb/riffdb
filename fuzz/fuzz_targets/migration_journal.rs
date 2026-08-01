#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let _ = riffdb_storage_api::proto_codec::decode_contract_migration_journal_v1(input);
});
