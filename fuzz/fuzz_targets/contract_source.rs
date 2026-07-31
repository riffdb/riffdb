#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use riffdb_contract_syntax::{
    format_migration, parse_contract_bytes, parse_migration, parse_migration_bytes,
};

fuzz_target!(|input: &[u8]| {
    let _ = parse_contract_bytes(input);

    if let Ok(document) = parse_migration_bytes(input) {
        let canonical = format_migration(&document);
        let reparsed = parse_migration(&canonical).expect("formatted migration must parse");
        assert_eq!(format_migration(&reparsed), canonical);
    }
});
