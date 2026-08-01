#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let _ = riffdb_storage_redb::validate_migration_receipt_fixture(input);
});
