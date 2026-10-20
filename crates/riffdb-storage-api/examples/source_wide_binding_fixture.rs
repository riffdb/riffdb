#![forbid(unsafe_code)]
//! Private test fixture emitter: actual compiler, no Native/Source publication.
// req: OQ-040, PRJ-004
use riffdb_contract_compiler::compile_contract_source;
use std::{fs::OpenOptions, io::Write, path::PathBuf};

fn main() {
    let mut args = std::env::args_os().skip(1);
    let output = PathBuf::from(args.next().expect("private output file"));
    assert!(args.next().is_none());
    let count = 600usize;
    let mut source = String::from(
        "contract SourceWideInitializedBinding version 1 {\nentity State {\nkey (organization_id: uuid, state_id: uuid)\n",
    );
    for field in 0..count {
        source.push_str(&format!("field f{field:03}: bool\n"));
    }
    source.push_str("}\naggregate States { root State partition_by organization_id conflict_key (organization_id, state_id) }\ncommand PutState {\ninput request_id: uuid\ninput organization_id: uuid\ninput state_id: uuid\nidempotency_key request_id\ninit_or_mutate State(organization_id, state_id) as state initialize {\n");
    for field in 0..count {
        source.push_str(&format!("f{field:03}: false,\n"));
    }
    source.push_str("}\noverwrite state.f000 = true\nreturn Written { value: state.f000 }\n}\n}\n");
    assert!(source.len() <= 64 * 1024);
    let bundle = compile_contract_source(&source).expect("real wide initializer compiles");
    assert_eq!(bundle.commands().len(), 1);
    assert_eq!(bundle.commands()[0].bindings().len(), 1);
    assert_eq!(
        bundle.commands()[0].bindings()[0].initializer().len(),
        count
    );
    let bytes = bundle.canonical_bytes();
    assert!(!bytes.is_empty() && bytes.len() <= 1024 * 1024);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
    eprintln!(
        "private_compiler_wide_binding_emitted initializer_count={count} encoded_bytes={}",
        bytes.len()
    );
}
