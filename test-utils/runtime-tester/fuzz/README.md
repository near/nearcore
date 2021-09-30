# Runtime Fuzz

Currently only one target is present -- runtime-fuzzer.  
This target will create random scenarios using Arbitrary trait
and execute them. This will keep happening, until one scenario fails.  


To run fuzz test:
```bash
RUSTC_BOOTSTRAP=1 cargo fuzz run runtime-fuzzer
```
`max_len` here is to create bigger scenarios.

Fuzzing starts from small inputs (Unstructured) and slowly goes to bigger.
To go to bigger input faster run fuzz with additional arguments:

```bash
RUSTC_BOOTSTRAP=1 cargo fuzz run runtime-fuzzer -- -len_control=0 -prefer_small=0
```

After finding the failed test, cargo fuzz will show failed Scenario (in debug format)
and also will write path to failing input and suggest further debug commands:

```bash
Failing input:

	artifacts/runtime-fuzzer/<id>

Output of `std::fmt::Debug`:

	** full json for Scenario **

Reproduce with:

	cargo fuzz run runtime-fuzzer artifacts/runtime-fuzzer/<id>

Minimize test case with:

	cargo fuzz tmin runtime-fuzzer artifacts/runtime-fuzzer/<id>
```

So, reproducing in this case will be:
```bash
RUSTC_BOOTSTRAP=1 cargo fuzz run runtime-fuzzer artifacts/runtime-fuzzer/<id>
```

To make a smaller scenario with the same error:
```bash
RUSTC_BOOTSTRAP=1 cargo fuzz tmin runtime-fuzzer artifacts/runtime-fuzzer/<id>
```

Writing Scenario to json:
```bash
RUSTC_BOOTSTRAP=1 cargo fuzz fmt runtime-fuzzer artifacts/runtime-fuzzer/<id> 2>&1 | sed '1,3d' | tee scenario.json
```

To run specific scenario.json use test from runtime-tester
```bash
cargo test test_scenario_json --release -- --ignored
```




