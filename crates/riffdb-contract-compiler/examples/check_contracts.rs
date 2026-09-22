#![forbid(unsafe_code)]

//! Compiles contract sources offline and reports what the compiler refuses.
//!
//! `riffdb contract validate` is a service call and needs a capability, so it
//! cannot answer "does this file still compile" for a checkout that is not
//! running a server. This does, which is what a downstream adapter needs after
//! a language change.

use std::fs;
use std::process::ExitCode;

use riffdb_contract_compiler::{CompilationError, compile_contract_source};

fn main() -> ExitCode {
    let paths = std::env::args().skip(1).collect::<Vec<_>>();
    if paths.is_empty() {
        eprintln!("usage: check_contracts <contract.riff>...");
        return ExitCode::from(2);
    }

    let mut refused = 0_usize;
    for path in &paths {
        let source = match fs::read_to_string(path) {
            Ok(source) => source,
            Err(error) => {
                println!("unreadable {path}: {error}");
                refused += 1;
                continue;
            }
        };
        match compile_contract_source(&source) {
            Ok(bundle) => println!(
                "ok {path} (grammar v{}, {} command(s))",
                bundle.grammar_version(),
                bundle.commands().len()
            ),
            Err(CompilationError::Semantic(diagnostics)) => {
                refused += 1;
                for diagnostic in diagnostics.as_slice() {
                    let start = diagnostic.primary_span().start() as usize;
                    let line = source[..start.min(source.len())].lines().count();
                    println!("refused {path}:{line}: {}", diagnostic.code().as_str());
                }
            }
            Err(error) => {
                refused += 1;
                println!("refused {path}: {error:?}");
            }
        }
    }

    if refused == 0 {
        println!("{} contract(s) compile", paths.len());
        return ExitCode::SUCCESS;
    }
    eprintln!("{refused} of {} contract(s) do not compile", paths.len());
    ExitCode::FAILURE
}
