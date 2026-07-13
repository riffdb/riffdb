#![forbid(unsafe_code)]

//! LALRPOP build integration for contract grammar version 1.

fn main() {
    println!("cargo:rerun-if-changed=src/grammar.lalrpop");
    lalrpop::Configuration::new()
        .use_cargo_dir_conventions()
        .emit_rerun_directives(false)
        .process()
        .expect("the checked grammar must generate");
}
