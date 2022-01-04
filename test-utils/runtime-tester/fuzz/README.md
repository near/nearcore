# Installation with cargo

You can install `cargo-fuzz`, with the following command.

```bash
cargo install cargo-fuzz
```

# How to see if everything works

You can see if it compiles by the following command.

```
cd test-utils/runtime-tester/fuzz/
env RUSTC_BOOTSTRAP=1 'cargo' 'fuzz' 'run' 'runtime_fuzzer' '--' '-len_control=0' '-prefer_small=0' '-max_len=4000000' '-rss_limit_mb=10240'
```

# Runtime Fuzz

Currently only one target is present -- runtime_fuzzer.
This target will create random scenarios using Arbitrary trait
and execute them. This will keep happening, until one scenario fails.


To run fuzz test:
```bash
RUSTC_BOOTSTRAP=1 cargo fuzz run runtime_fuzzer
```
`max_len` here is to create bigger scenarios.

Fuzzing starts from small inputs (Unstructured) and slowly goes to bigger.
To go to bigger input faster run fuzz with additional arguments:

```bash
RUSTC_BOOTSTRAP=1 cargo fuzz run runtime_fuzzer -- -len_control=0 -prefer_small=0
```

After finding the failed test, cargo fuzz will show failed Scenario (in debug format)
and also will write path to failing input and suggest further debug commands:

```bash
Failing input:

	artifacts/runtime_fuzzer/<id>

Output of `std::fmt::Debug`:

	** full json for Scenario **

Reproduce with:

	cargo fuzz run runtime_fuzzer artifacts/runtime_fuzzer/<id>

Minimize test case with:

	cargo fuzz tmin runtime_fuzzer artifacts/runtime_fuzzer/<id>
```

So, reproducing in this case will be:
```bash
RUSTC_BOOTSTRAP=1 cargo fuzz run runtime_fuzzer artifacts/runtime_fuzzer/<id>
```

To make a smaller scenario with the same error:
```bash
RUSTC_BOOTSTRAP=1 cargo fuzz tmin runtime_fuzzer artifacts/runtime_fuzzer/<id>
```

Writing Scenario to json:
```bash
RUSTC_BOOTSTRAP=1 cargo fuzz fmt runtime_fuzzer artifacts/runtime_fuzzer/<id> 2>&1 | sed '1,3d' | tee scenario.json
```

To run specific scenario.json use test from runtime-tester
```bash
cargo test test_scenario_json --release -- --ignored
```
