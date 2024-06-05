use std::collections::HashSet;
use std::process::Command;
use std::str;

// when you compile you see line similar to the next one:
// Building [=======        ] 50/100: near-primitives v0.1.0
// The 100 is not actually the number of dependencies, but the number of crates that are being built.
// This threshhold represents the number of dependencies
const THRESHOLD: usize = 150;

#[test]
fn test_crate_count() {
    // Run `cargo tree -p near-primitives --edges=normal` and capture the output
    let output = Command::new("cargo")
        .arg("tree")
        .arg("-p")
        .arg("near-primitives")
        .arg("--edges=normal")
        .output()
        .expect("Failed to execute cargo tree");

    // Check if the command was successful
    assert!(output.status.success(), "Cargo tree failed");

    // Convert the output to a string
    let output_str = str::from_utf8(&output.stdout).expect("Failed to convert output to string");

    let re = regex::Regex::new(r"([\w-]+) v([\d.]+(?:-\w+)?)").unwrap();

    // Use a HashSet to track unique crates
    let mut unique_crates = HashSet::new();

    for cap in re.captures_iter(output_str) {
        let crate_name = &cap[1];
        let crate_version = &cap[2];
        let crate_str = format!("{}-{}", crate_name, crate_version);
        println!("{}", crate_str);
        unique_crates.insert(crate_str);
    }
    // Count the unique crates
    let crate_count = unique_crates.len();
    println!("Unique crate count: {}", crate_count);

    // Assert that the crate count is less than the threshold
    assert!(crate_count < THRESHOLD, "Crate count is too high: {} > {}", crate_count, THRESHOLD);
}
