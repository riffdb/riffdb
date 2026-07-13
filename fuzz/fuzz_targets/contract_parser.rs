#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use riffdb_contract_syntax::{limits::MAX_SOURCE_BYTES, parse_contract, parse_contract_bytes};

fuzz_target!(|input: &[u8]| {
    let _ = parse_contract_bytes(input);

    if let Ok(source) = std::str::from_utf8(input) {
        if source.len() <= MAX_SOURCE_BYTES.saturating_sub(128) {
            for wrapped in [
                format!("contract F version 1 {{ {source} }}"),
                format!("contract F version 1 {{ event E {{ {source} }} }}"),
                format!(
                    "contract F version 1 {{ command C {{ return Done {{ value: {source} }} }} }}"
                ),
                format!(
                    "contract F version 1 {{ command C {{ {source} return Done {{}} }} }}"
                ),
            ] {
                let _ = parse_contract(&wrapped);
            }
        }
    }
});
