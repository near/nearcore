use std::collections::HashSet;
use std::process::{Command, Output};
use std::str;

// when you compile you see line similar to the next one:
// Building [=======        ] 50/100: near-primitives v0.1.0
// The 100 is not actually the number of dependencies, but the number of crates that are being built.
// This threshhold represents the number of dependencies
const THRESHOLD_DEFAULT: usize = 150;
const THRESHOLD_NO_DEFAULT: usize = 115;

fn process_output(output: Output, threshold: usize) {
    assert!(output.status.success(), "Cargo tree failed");

    let output_str = str::from_utf8(&output.stdout).expect("Failed to convert output to string");

    let re = regex::Regex::new(r"([\w-]+) v([\d.]+(?:-\w+)?)").unwrap();

    let mut unique_crates = HashSet::new();

    for cap in re.captures_iter(output_str) {
        let crate_name = &cap[1];
        let crate_version = &cap[2];
        let crate_str = format!("{}-{}", crate_name, crate_version);
        unique_crates.insert(crate_str);
    }
    println!("{:#?}", unique_crates);
    let crate_count = unique_crates.len();
    println!("Unique crate count: {}", crate_count);

    assert!(crate_count < threshold, "Crate count is too high: {} > {}", crate_count, threshold);
}

#[test]
fn test_crate_count() {
    // Run `cargo tree -p near-primitives --edges=normal` and capture the output
    let output = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string()))
        .arg("tree")
        .arg("-p")
        .arg("near-primitives")
        .arg("--edges=normal")
        .output()
        .expect("Failed to execute cargo tree");

    process_output(output, THRESHOLD_DEFAULT);
}

#[test]
fn test_crate_count_no_default() {
    // Run `cargo tree -p near-primitives --edges=normal` and capture the output
    let output = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string()))
        .arg("tree")
        .arg("-p")
        .arg("near-primitives")
        .arg("--no-default-features")
        .arg("--edges=normal")
        .output()
        .expect("Failed to execute cargo tree");

    process_output(output, THRESHOLD_NO_DEFAULT);
}
