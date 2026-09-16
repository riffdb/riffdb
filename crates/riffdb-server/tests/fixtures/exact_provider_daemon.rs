#![forbid(unsafe_code)]
//! Real daemon with bounded observations and explicit process-abort boundaries.

use std::{collections::BTreeMap, path::PathBuf, sync::Mutex};

use riffdb_server::test_fixtures::{ExactProviderTestPoint as Point, install_exact_provider_probe};

fn main() -> std::process::ExitCode {
    let counts = std::env::var_os("RIFFDB_EXACT_COUNTS").map(PathBuf::from);
    let abort_slot = std::env::var_os("RIFFDB_EXACT_SLOT");
    let abort_at = std::env::var("RIFFDB_EXACT_ABORT").ok();
    let abort_head = std::env::var("RIFFDB_EXACT_ABORT_HEAD")
        .ok()
        .map(|value| value.parse::<u64>().expect("fixture frontier"));
    let state = Mutex::new(BTreeMap::<PathBuf, (u64, u64)>::new());
    assert!(install_exact_provider_probe(move |point, path, head| {
        let mut state = state.lock().expect("fixture observations");
        if point == Point::Preparing {
            assert!(state.contains_key(path) || state.len() < 256);
            state.entry(path.to_owned()).or_default().0 = head.expect("captured head").get();
        }
        let Some((captured_head, full_reads)) = state.get_mut(path) else {
            return;
        };
        if point == Point::FullPartitionRead {
            *full_reads += 1;
        }
        if abort_slot.as_deref() == path.file_name()
            && abort_head == Some(*captured_head)
            && abort_at.as_deref() == Some(format!("{point:?}").as_str())
        {
            println!("riffdb-exact-abort-v1\t{point:?}\t{captured_head}");
            use std::io::Write;
            std::io::stdout().flush().expect("abort evidence");
            std::process::abort();
        }
        if point == Point::AfterSelection
            && let Some(counts) = &counts
        {
            let values = state
                .iter()
                .map(|(path, (_, reads))| {
                    (
                        path.file_name()
                            .expect("slot file")
                            .to_str()
                            .expect("fixture file name")
                            .to_owned(),
                        *reads,
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let bytes = serde_json::to_vec(&values).expect("bounded fixture counters");
            let pending = counts.with_extension("pending");
            std::fs::write(&pending, bytes).expect("write fixture counters");
            std::fs::rename(pending, counts).expect("publish fixture counters");
        }
    }));
    riffdb_server::riffdbd_main()
}
