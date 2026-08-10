#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use riffdb_riffql_syntax::{format_query, parse_query, parse_query_bytes};

fuzz_target!(|input: &[u8]| {
    let _ = parse_query_bytes(input);

    let Ok(source) = std::str::from_utf8(input) else {
        return;
    };
    let Ok(document) = parse_query(source) else {
        return;
    };

    let canonical = format_query(&document);
    let reparsed = parse_query(&canonical).expect("formatter output must remain valid RiffQL");
    assert_eq!(format_query(&reparsed), canonical);
    assert_eq!(reparsed.language_version, document.language_version);
});
