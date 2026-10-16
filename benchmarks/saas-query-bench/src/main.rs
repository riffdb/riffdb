//! CLI entry point for the RiffDB vs PostgreSQL SaaS complex-query benchmark.

#![forbid(unsafe_code)]

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

use riffdb_saas_query_bench::{
    SeedDataset, SeedScale, compile_saas_artifacts, format_markdown_summary,
    run_saas_query_benchmark,
};

fn main() -> ExitCode {
    match run_cli() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run_cli() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();
    let Some(subcommand) = args.first().map(String::as_str) else {
        return Err(usage());
    };

    match subcommand {
        "self-test" => {
            let artifacts = compile_saas_artifacts()?;
            for scale in [SeedScale::Smoke, SeedScale::Full] {
                let ds = SeedDataset::generate(scale);
                if ds.orgs.len() != scale.org_count() {
                    return Err("unexpected org count".to_owned());
                }
            }
            println!(
                "self-test passed: entities={} queries={}",
                artifacts.contract.schema().entities().len(),
                artifacts.query_module.queries().len()
            );
            Ok(())
        }
        "run" => {
            let mut riffdbd_bin: Option<PathBuf> = None;
            let mut work_dir: Option<PathBuf> = None;
            let mut postgres_url: Option<String> = None;
            let mut scale = SeedScale::Smoke;
            let mut json_out: Option<PathBuf> = None;
            let mut markdown_out: Option<PathBuf> = None;

            let mut idx = 1usize;
            while idx < args.len() {
                match args[idx].as_str() {
                    "--riffdbd-bin" => {
                        idx += 1;
                        riffdbd_bin = args.get(idx).map(PathBuf::from);
                    }
                    "--work-dir" => {
                        idx += 1;
                        work_dir = args.get(idx).map(PathBuf::from);
                    }
                    "--postgres-url" => {
                        idx += 1;
                        postgres_url = args.get(idx).cloned();
                    }
                    "--scale" => {
                        idx += 1;
                        let val = args
                            .get(idx)
                            .ok_or_else(|| "missing --scale value".to_owned())?;
                        scale = match val.as_str() {
                            "smoke" => SeedScale::Smoke,
                            "full" => SeedScale::Full,
                            other => {
                                return Err(format!(
                                    "unknown scale '{other}' (expected smoke|full)"
                                ));
                            }
                        };
                    }
                    "--json-out" => {
                        idx += 1;
                        json_out = args.get(idx).map(PathBuf::from);
                    }
                    "--markdown-out" => {
                        idx += 1;
                        markdown_out = args.get(idx).map(PathBuf::from);
                    }
                    other => return Err(format!("unknown flag {other}\n{}", usage())),
                }
                idx += 1;
            }

            let riffdbd_bin =
                riffdbd_bin.ok_or_else(|| format!("missing --riffdbd-bin\n{}", usage()))?;
            let work_dir = work_dir.ok_or_else(|| format!("missing --work-dir\n{}", usage()))?;
            let postgres_url =
                postgres_url.ok_or_else(|| format!("missing --postgres-url\n{}", usage()))?;

            let report = run_saas_query_benchmark(&riffdbd_bin, &work_dir, &postgres_url, scale)?;

            let markdown = format_markdown_summary(&report);
            println!("{markdown}");

            if let Some(path) = json_out {
                let json = serde_json::to_string_pretty(&report).map_err(|e| format!("{e}"))?;
                fs::write(&path, format!("{json}\n"))
                    .map_err(|e| format!("write {}: {e}", path.display()))?;
            }
            if let Some(path) = markdown_out {
                fs::write(&path, &markdown)
                    .map_err(|e| format!("write {}: {e}", path.display()))?;
            }
            Ok(())
        }
        other => Err(format!("unknown subcommand {other}\n{}", usage())),
    }
}

fn usage() -> String {
    "Usage:\n  riffdb-saas-query-bench self-test\n  riffdb-saas-query-bench run --riffdbd-bin <path> --work-dir <dir> --postgres-url <url> [--scale smoke|full] [--json-out <path>] [--markdown-out <path>]".to_owned()
}
