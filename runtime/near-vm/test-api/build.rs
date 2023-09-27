//! The logic that gets executed before building the binary and tests.
//! We use it to auto-generate the Wasm spectests for each of the
//! available compilers.
//!
//! Please try to keep this file as clean as possible.

use near_vm_test_generator::{
    test_directory, test_directory_module, wast_processor, with_test_module, Testsuite,
};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn main() -> anyhow::Result<()> {
    // As rerun-if-changed doesn't support globs, we use another crate
    // to check changes in directories.

    let out_dir = PathBuf::from(
        env::var_os("OUT_DIR").expect("The OUT_DIR environment variable must be set"),
    );

    // Spectests test generation
    {
        let mut spectests = Testsuite { buffer: String::new(), path: vec![] };

        with_test_module(&mut spectests, "spec", |spectests| {
            let _spec_tests = test_directory(spectests, "../tests/wast/spec", wast_processor)?;
            test_directory_module(
                spectests,
                "../tests/wast/spec/proposals/multi-value",
                wast_processor,
            )?;
            test_directory_module(spectests, "../tests/wast/spec/proposals/simd", wast_processor)?;
            // test_directory_module(spectests, "tests/wast/spec/proposals/bulk-memory-operations", wast_processor)?;
            Ok(())
        })?;
        with_test_module(&mut spectests, "wasmer", |spectests| {
            let _spec_tests = test_directory(spectests, "../tests/wast/wasmer", wast_processor)?;
            Ok(())
        })?;

        let spectests_output = out_dir.join("generated_spectests.rs");
        fs::write(&spectests_output, spectests.buffer)?;

        // Write out our auto-generated tests and opportunistically format them with
        // `rustfmt` if it's installed.
        // Note: We need drop because we don't want to run `unwrap` or `expect` as
        // the command might fail, but we don't care about it's result.
        drop(Command::new("rustfmt").arg(&spectests_output).status());
    }

    Ok(())
}
