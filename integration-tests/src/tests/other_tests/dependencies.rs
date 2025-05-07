/*
 * This test is designed to ensure that the public libraries in the project do not exceed a specified number of unique dependencies.
 * Please note that this test doesn't care about total compilation units that are displayed during cargo build
 * The output of the `cargo build` shows compilation units. Compilation unit is a either a external dependency or a file from the source
 *
 * The `LIBS_THRESHOLDS` constant defines a list of libraries along with their respective maximum allowed unique dependency counts.
 * The `THRESHOLD_IS_TOO_GENEROUS` constant is used to determine if the threshold for any library is too lenient, suggesting it might need to be restricted even further.
 *
 * The `get_and_assert_crate_dependencies` function takes a library name and a threshold, runs the `cargo tree` command to get the dependency tree for the library,
 * extracts unique dependencies using a regex, and checks if the count of unique dependencies is below the threshold.
 *
 * The purpose of this test is to maintain a lean dependency graph, promoting better performance, security, and maintainability.
 * As well, as limit the total amount of dependencies for public facing libraries
 */

use std::collections::HashSet;
use std::process::Command;
use std::str;

const LIBS_THRESHOLDS: [(&str, usize); 9] = [
    ("near-primitives", 122),
    ("near-jsonrpc-primitives", 130),
    ("near-chain-configs", 130),
    ("near-chain-primitives", 130),
    ("near-client-primitives", 152),
    ("near-parameters", 65),
    ("near-crypto", 75),
    ("near-primitives-core", 60),
    ("near-time", 30),
];

const THRESHOLD_IS_TOO_GENEROUS: usize = 30;

fn get_and_assert_crate_dependencies(name: &str, threshold: usize) -> usize {
    let output: std::process::Output =
        Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string()))
            .arg("tree")
            .arg("-p")
            .arg(name)
            .arg("--edges=normal")
            .output()
            .unwrap_or_else(|_| panic!("Failed to execute cargo tree for {name}"));

    assert!(
        output.status.success(),
        "Cargo tree failed for {name} with: {}",
        str::from_utf8(&output.stderr).unwrap()
    );

    let output_str = str::from_utf8(&output.stdout).expect("Failed to convert output to string");

    // The `cargo tree` command outputs the dependency tree of a given crate. An example line of the output might look like:
    //   │   ├── actix_derive v0.6.0 (proc-macro)
    // This line indicates that the crate `actix_derive` version `0.6.0` is a dependency.
    //
    // The regex `([\w-]+) v([\d.]+(?:-\w+)?)` is used to extract the crate name and version from each line of the `cargo tree` output.
    // - `([\w-]+)` captures the crate name, which can include alphanumeric characters and hyphens.
    // - `v` matches the literal character 'v' that precedes the version number.
    // - `([\d.]+(?:-\w+)?)` captures the version number, which can include digits, dots, and optional pre-release identifiers.
    let re = regex::Regex::new(r"([\w-]+) v([\d.]+(?:-\w+)?)").unwrap();

    let mut unique_crates = HashSet::new();

    for cap in re.captures_iter(output_str) {
        let crate_name = &cap[1];
        let crate_version = &cap[2];
        let crate_str = format!("{}-{}", crate_name, crate_version);
        unique_crates.insert(crate_str);
    }
    let crate_count = unique_crates.len();

    assert!(
        crate_count < threshold,
        "Dependencies number is too high for {name}: {} >= {}",
        crate_count,
        threshold
    );
    crate_count
}

#[derive(Debug, Clone, PartialEq)]
#[allow(unused)]
struct CrateDeps {
    pub crate_name: String,
    pub crate_deps: usize,
    pub suggested_new_threshold: usize,
}

#[test]
fn slow_test_public_libs_are_small_enough() {
    let results = LIBS_THRESHOLDS
        .into_iter()
        .map(|(name, limit)| (name, get_and_assert_crate_dependencies(name, limit), limit));
    let mut libs_to_fix = vec![];
    for (name, result, limit) in results {
        if limit - result > THRESHOLD_IS_TOO_GENEROUS {
            libs_to_fix.push(CrateDeps {
                crate_name: name.to_owned(),
                crate_deps: result,
                suggested_new_threshold: result + 10,
            });
        }
    }

    assert_eq!(
        libs_to_fix,
        vec![],
        "Good job on reducing dependency count, but it's time to review that thresholds for next components"
    );
}
