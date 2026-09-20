#![forbid(unsafe_code)]

//! Source-level call-graph proof that derived-state apply never reaches the
//! primary mutation gate (ADR-0240 OBL-0240-1).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn production_source(path: impl AsRef<Path>) -> String {
    fs::read_to_string(path)
        .expect("architecture input is readable UTF-8")
        .split("\n#[cfg(test)]\nmod tests")
        .next()
        .expect("production source prefix")
        .to_owned()
}

fn strip_comments_and_strings(source: &str) -> String {
    let mut output = String::with_capacity(source.len());
    let bytes = source.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'/' && index + 1 < bytes.len() && bytes[index + 1] == b'/' {
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        if bytes[index] == b'/' && index + 1 < bytes.len() && bytes[index + 1] == b'*' {
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            index = index.saturating_add(2);
            continue;
        }
        if bytes[index] == b'"' {
            output.push(' ');
            index += 1;
            while index < bytes.len() && bytes[index] != b'"' {
                if bytes[index] == b'\\' {
                    index += 2;
                    continue;
                }
                index += 1;
            }
            index = index.saturating_add(1);
            continue;
        }
        output.push(bytes[index] as char);
        index += 1;
    }
    output
}

fn rust_functions(source: &str) -> BTreeMap<String, String> {
    let source = strip_comments_and_strings(source);
    let mut functions = BTreeMap::new();
    let chars: Vec<char> = source.chars().collect();
    let mut index = 0;
    while index < chars.len() {
        if remaining_starts_with(&chars, index, "fn ") {
            index += 3;
            while index < chars.len() && chars[index].is_whitespace() {
                index += 1;
            }
            let start = index;
            while index < chars.len()
                && (chars[index].is_ascii_alphanumeric() || chars[index] == '_')
            {
                index += 1;
            }
            let name: String = chars[start..index].iter().collect();
            while index < chars.len() && chars[index] != '{' && chars[index] != ';' {
                index += 1;
            }
            if index >= chars.len() || chars[index] == ';' {
                continue;
            }
            let body_start = index;
            let mut depth = 0;
            while index < chars.len() {
                match chars[index] {
                    '{' => depth += 1,
                    '}' => {
                        depth -= 1;
                        if depth == 0 {
                            index += 1;
                            break;
                        }
                    }
                    _ => {}
                }
                index += 1;
            }
            if !name.is_empty() {
                functions.insert(name, chars[body_start..index].iter().collect());
            }
            continue;
        }
        index += 1;
    }
    functions
}

fn remaining_starts_with(chars: &[char], index: usize, needle: &str) -> bool {
    let needle: Vec<char> = needle.chars().collect();
    chars[index..].starts_with(&needle)
}

fn callees(body: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let chars: Vec<char> = body.chars().collect();
    let mut index = 0;
    while index < chars.len() {
        if chars[index].is_ascii_alphabetic() || chars[index] == '_' {
            let start = index;
            while index < chars.len()
                && (chars[index].is_ascii_alphanumeric() || chars[index] == '_')
            {
                index += 1;
            }
            let name: String = chars[start..index].iter().collect();
            let mut look = index;
            while look < chars.len() && chars[look].is_whitespace() {
                look += 1;
            }
            if look < chars.len() && chars[look] == '(' {
                names.insert(name);
            }
            continue;
        }
        index += 1;
    }
    names
}

fn walk(starts: &[&str], functions: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut seen = BTreeMap::new();
    let mut queue = VecDeque::new();
    for start in starts {
        if let Some(body) = functions.get(*start) {
            queue.push_back((*start).to_owned());
            seen.insert((*start).to_owned(), body.clone());
        }
    }
    while let Some(name) = queue.pop_front() {
        let Some(body) = functions.get(&name) else {
            continue;
        };
        for callee in callees(body) {
            if matches!(
                callee.as_str(),
                "begin_write"
                    | "commit"
                    | "commit_for"
                    | "abort"
                    | "open_table"
                    | "insert"
                    | "drop"
            ) {
                continue;
            }
            if seen.contains_key(&callee) {
                continue;
            }
            if let Some(callee_body) = functions.get(&callee) {
                seen.insert(callee.clone(), callee_body.clone());
                queue.push_back(callee);
            }
        }
    }
    seen
}

#[test]
fn no_derived_state_path_acquires_the_primary_mutation_gate() {
    let root = crate_root().join("src");
    let mut functions = BTreeMap::new();
    for relative in [
        "derived.rs",
        "derived/batch.rs",
        "columnar_projection_control.rs",
        "store.rs",
        "projection_replay.rs",
    ] {
        for (name, body) in rust_functions(&production_source(root.join(relative))) {
            functions.entry(name).or_insert(body);
        }
    }
    let visited = walk(
        &[
            "apply_projection",
            "apply_projection_batch",
            "apply",
            "transition_projection_control",
            "stage_projection_replay",
        ],
        &functions,
    );
    assert!(
        !visited.is_empty(),
        "derived apply entry points must exist in the scanned sources"
    );
    for (name, body) in &visited {
        for forbidden in [
            "begin_attributed_write",
            "mutation_gate",
            "acquire_indexed_read_lease",
        ] {
            assert!(
                !body.contains(forbidden),
                "{name} reaches primary mutation machinery via `{forbidden}`"
            );
        }
    }
    let apply = functions
        .get("apply_projection")
        .expect("apply_projection owner");
    let batch = functions.get("apply").expect("batch apply owner");
    assert!(
        apply.contains("begin_derived_write") && apply.contains("begin_read"),
        "single-member apply must read the primary and write the sidecar"
    );
    assert!(
        batch.contains("begin_derived_write") && batch.contains("begin_read"),
        "batch apply must read the primary and write the sidecar"
    );
    assert!(
        !apply.contains("self.begin_write(") && !batch.contains("self.begin_write("),
        "apply must not call the primary begin_write helper"
    );
}
