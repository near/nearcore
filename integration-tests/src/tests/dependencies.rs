use std::collections::HashSet;
use std::process::Command;
use std::str;

// when you compile you see line similar to the next one:
// Building [=======        ] 50/100: near-primitives v0.1.0
// The 100 is not actually the number of dependencies, but the number of compilation units that are being built.
// This threshhold represents the number of dependencies, so it's smaller value
const LIBS_THRESHOLDS: [(&str, usize); 9] = [
    ("near-primitives", 115),
    ("near-jsonrpc-primitives", 130),
    ("near-chain-configs", 130),
    ("near-chain-primitives", 130),
    ("near-client-primitives", 150),
    ("near-parameters", 65),
    ("near-crypto", 75),
    ("near-primitives-core", 60),
    ("near-time", 30),
];

const THRESHOLD_IS_TOO_GENEROUS: usize = 30;

fn process_output(name: &str, threshold: usize) -> usize {
    let output = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string()))
        .arg("tree")
        .arg("-p")
        .arg(name)
        .arg("--edges=normal")
        .output()
        .unwrap_or_else(|_| panic!("Failed to execute cargo tree for {name}"));

    assert!(output.status.success(), "Cargo tree failed {name}");

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

    assert!(
        crate_count < threshold,
        "Dependencies number is too high for {name}: {} > {}",
        crate_count,
        threshold
    );
    crate_count
}

#[derive(Debug)]
#[allow(unused)]
struct Assitance {
    pub name: String,
    pub result: usize,
    pub suggested_threshold: usize,
}

#[test]
fn test_public_libs_are_small_enough() {
    let results =
        LIBS_THRESHOLDS.into_iter().map(|(name, limit)| (name, process_output(name, limit), limit));
    println!("{:#?}", results);
    let mut libs_to_fix = vec![];
    for (name, result, limit) in results {
        if limit - result > THRESHOLD_IS_TOO_GENEROUS {
            libs_to_fix.push(Assitance {
                name: name.to_owned(),
                result,
                suggested_threshold: result + 10,
            });
        }
    }

    assert!(libs_to_fix.is_empty(), "Good job on reducing dependency count, but it's time to review that thresholds for next components: {:#?}", libs_to_fix);
}
