#![forbid(unsafe_code)]

//! Source-level call-graph proofs for ADR-0240 OBL-0240-1 and ADR-0246
//! OBL-0246-1 / OBL-0246-2.

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

fn rust_functions(source: &str) -> BTreeMap<String, Vec<String>> {
    let source = strip_comments_and_strings(source);
    let mut functions: BTreeMap<String, Vec<String>> = BTreeMap::new();
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
                functions
                    .entry(name)
                    .or_default()
                    .push(chars[body_start..index].iter().collect());
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

fn is_external_primitive(name: &str) -> bool {
    matches!(
        name,
        "abort"
            | "as_bytes"
            | "as_ref"
            | "as_str"
            | "clone"
            | "collect"
            | "commit"
            | "commit_for"
            | "contains"
            | "decode"
            | "deref"
            | "drop"
            | "encode"
            | "err"
            | "expect"
            | "finish"
            | "first"
            | "flatten"
            | "from"
            | "get"
            | "insert"
            | "into"
            | "into_iter"
            | "is_empty"
            | "is_none"
            | "is_some"
            | "is_some_and"
            | "iter"
            | "len"
            | "map"
            | "map_err"
            | "map_or"
            | "map_or_else"
            | "matches"
            | "new"
            | "next"
            | "ok"
            | "ok_or"
            | "ok_or_else"
            | "open_table"
            | "or_else"
            | "pop"
            | "push"
            | "remove"
            | "to_owned"
            | "to_vec"
            | "transaction"
            | "unwrap"
            | "unwrap_or"
            | "unwrap_or_else"
            | "vec"
    )
}

fn must_resolve(name: &str) -> bool {
    name.starts_with("begin_")
        || name.starts_with("acquire_")
        || name.contains("mutation_gate")
        || name.contains("attributed_write")
        || name == "apply_published_journal_suffix"
        || name == "rebuild_projection_commit"
        || name == "stage_projection_replay"
}

fn production_rs_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    fn walk(dir: &Path, files: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(dir).expect("src is readable") {
            let entry = entry.expect("src entry");
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name == "tests" {
                    continue;
                }
                walk(&path, files);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name.ends_with("_tests.rs") || name.ends_with("_test.rs") {
                    continue;
                }
                files.push(path);
            }
        }
    }
    walk(root, &mut files);
    files.sort();
    files
}

fn all_functions() -> BTreeMap<String, Vec<String>> {
    let mut functions: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for path in production_rs_files(&crate_root().join("src")) {
        for (name, bodies) in rust_functions(&production_source(&path)) {
            functions.entry(name).or_default().extend(bodies);
        }
    }
    functions
}

fn home_function_names() -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let src = crate_root().join("src");
    for relative in [
        "derived.rs",
        "derived/batch.rs",
        "projection_replay.rs",
        "store_follower_projection.rs",
        "maintenance/bootstrap_projection.rs",
    ] {
        names.extend(rust_functions(&production_source(src.join(relative))).into_keys());
    }
    names
}

fn should_follow(callee: &str, home: &BTreeSet<String>) -> bool {
    home.contains(callee)
        || must_resolve(callee)
        || callee.starts_with("begin_derived")
        || callee.starts_with("begin_composite")
        || callee == "commit_at_access"
        || callee == "overlay_head"
        || callee == "control_disagrees_with_head"
        || callee == "cannot_serve_command_commit"
        || callee == "discard_projection_identity"
}

fn walk(
    starts: &[&str],
    functions: &BTreeMap<String, Vec<String>>,
) -> Result<BTreeMap<String, Vec<String>>, String> {
    let home = home_function_names();
    let mut seen: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut queue = VecDeque::new();
    for start in starts {
        let Some(bodies) = functions.get(*start) else {
            return Err(format!("missing apply entry point `{start}`"));
        };
        let ungated: Vec<String> = bodies
            .iter()
            .filter(|body| FORBIDDEN.iter().all(|forbidden| !body.contains(forbidden)))
            .cloned()
            .collect();
        if ungated.is_empty() {
            return Err(format!(
                "entry point `{start}` only has overloads that reach the primary mutation gate"
            ));
        }
        queue.push_back((*start).to_owned());
        seen.insert((*start).to_owned(), ungated);
    }
    while let Some(name) = queue.pop_front() {
        let Some(bodies) = seen.get(&name).cloned() else {
            continue;
        };
        for body in &bodies {
            for callee in callees(body) {
                if is_external_primitive(&callee) {
                    continue;
                }
                if callee == "begin_write" && !body.contains("self.begin_write(") {
                    continue;
                }
                if callee == "acquire" && !body.contains("mutation_gate") {
                    continue;
                }
                if !should_follow(&callee, &home) {
                    if must_resolve(&callee) && !functions.contains_key(&callee) {
                        return Err(format!(
                            "unresolved callee `{callee}` from `{name}` — the walk cannot drop a gate-adjacent name"
                        ));
                    }
                    continue;
                }
                if seen.contains_key(&callee) {
                    continue;
                }
                match functions.get(&callee) {
                    Some(callee_bodies) => {
                        let ungated: Vec<String> = callee_bodies
                            .iter()
                            .filter(|body| {
                                FORBIDDEN.iter().all(|forbidden| !body.contains(forbidden))
                            })
                            .cloned()
                            .collect();
                        if ungated.is_empty() {
                            return Err(format!(
                                "`{name}` calls `{callee}`, whose only overloads reach the primary mutation gate"
                            ));
                        }
                        seen.insert(callee.clone(), ungated);
                        queue.push_back(callee);
                    }
                    None if must_resolve(&callee) => {
                        return Err(format!(
                            "unresolved callee `{callee}` from `{name}` — the walk cannot drop a gate-adjacent name"
                        ));
                    }
                    None => {}
                }
            }
        }
    }
    Ok(seen)
}

const FORBIDDEN: [&str; 4] = [
    "begin_attributed_write",
    "mutation_gate",
    "acquire_indexed_read_lease",
    "self.begin_write(",
];

fn assert_no_gate(visited: &BTreeMap<String, Vec<String>>) {
    for (name, bodies) in visited {
        for body in bodies {
            for forbidden in FORBIDDEN {
                assert!(
                    !body.contains(forbidden),
                    "{name} reaches primary mutation machinery via `{forbidden}`"
                );
            }
        }
    }
}

fn source_contains(relative: &str, needle: &str) -> bool {
    production_source(crate_root().join("src").join(relative)).contains(needle)
}

#[test]
fn no_derived_state_path_acquires_the_primary_mutation_gate() {
    let functions = all_functions();
    let visited = walk(
        &[
            "apply_projection",
            "apply_projection_batch",
            "apply",
            "resolve",
            "transition_projection_control",
        ],
        &functions,
    )
    .expect("operational apply graph must resolve");
    assert!(
        !visited.is_empty(),
        "derived apply entry points must exist in the scanned sources"
    );
    assert_no_gate(&visited);
    let apply = functions
        .get("apply_projection")
        .expect("apply_projection owner")
        .iter()
        .find(|body| body.contains("begin_derived_write"))
        .expect("operational apply_projection body");
    let batch = functions
        .get("apply")
        .expect("batch apply owner")
        .iter()
        .find(|body| body.contains("begin_derived_write"))
        .expect("operational batch apply body");
    assert!(
        apply.contains("begin_derived_write") && apply.contains("begin_composite_read"),
        "single-member apply must overlay-read the primary and write the sidecar"
    );
    assert!(
        batch.contains("begin_derived_write") && batch.contains("begin_composite_read"),
        "batch apply must overlay-read the primary and write the sidecar"
    );
}

#[test]
fn only_an_unpublished_store_may_apply_derived_state_under_the_gate() {
    let functions = all_functions();
    let rebuild = functions
        .get("rebuild_projection_commit")
        .expect("rebuild_projection_commit")
        .iter()
        .find(|body| body.contains("mutation_gate"))
        .expect("follower rebuild takes the gate");
    assert!(
        rebuild.contains("cannot_serve_command_commit"),
        "gated derived apply must check the handle cannot serve a command commit"
    );
    assert!(
        source_contains("store.rs", "open_mode != OpenMode::Source"),
        "operational ports must refuse a non-source handle"
    );
    assert!(
        source_contains("store_follower.rs", "open_mode != OpenMode::Follower"),
        "follower applier must refuse a non-follower handle"
    );
    let visited = walk(
        &[
            "apply_projection",
            "apply_projection_batch",
            "apply",
            "resolve",
            "transition_projection_control",
        ],
        &functions,
    )
    .expect("operational apply graph must resolve");
    assert_no_gate(&visited);
}

#[test]
fn restore_file_set_accounts_for_derived_sidecar() {
    assert!(
        source_contains("store.rs", "derived_store_path(database)"),
        "owned_store_files must name the sidecar from what the database owns"
    );
    assert!(
        source_contains("maintenance/staged.rs", "discard_replaced_derived_sidecar"),
        "archive restore publication must drop the replaced timeline's sidecar"
    );
    assert!(
        source_contains("maintenance/store.rs", "discard_replaced_derived_sidecar"),
        "migration publish and rollback must drop the replaced timeline's sidecar"
    );
}
