---
name: migrate-pytest
description: Migrate a pytest integration test to the test-loop framework. Use when converting a pytest from pytest/tests/ to a deterministic test-loop test.
argument-hint: <path-to-pytest-file>
allowed-tools: Read, Grep, Glob, Bash, Edit, Write
---

Migrate a pytest integration test to the test-loop framework.

**Target pytest file:** $ARGUMENTS

Follow these steps in order:

## 1. Understand the pytest

- Read the target pytest file
- Identify each distinct test scenario (steps, test methods, or test functions)
- For each scenario, summarize:
  - What is being tested (the behavior under test)
  - The network topology (how many validators, archival nodes, RPC nodes, observers)
  - Key configuration (epoch_length, gc settings, archive mode, split storage, etc.)
  - The sequence of actions (start nodes, wait for blocks, kill/restart, send txs, etc.)
  - The assertions (what is verified and how)
- Present this analysis to the user before proceeding

## 2. Study the test-loop framework

- Read `test-loop-tests/README.md` for the builder API and patterns
- Read relevant examples from `test-loop-tests/src/examples/` that are relevant to the pytest being migrated (list available examples with `ls test-loop-tests/src/examples/`)
- Browse existing tests in `test-loop-tests/src/tests/` for patterns similar to what the pytest does

## 3. Design the test-loop test

For each pytest scenario, decide:
- Whether it maps to a separate `#[test]` function or can be combined
- The simplest builder API setup that covers the scenario
- Which test-loop utilities to use (runners, node accessors, transactions)
- Whether any simplification is appropriate (the README says: "it would be _extremely_ hard to migrate the logic precisely. Instead, migrate tests only if they make sense to you")

Present the plan to the user and get confirmation before writing code.

## 4. Write the test

- Create `test-loop-tests/src/tests/<test_name>.rs`
- Follow these conventions:
  - Use `init_test_logger()` at the start of each test
  - Use the `TestLoopBuilder` high-level API (avoid manual genesis unless necessary)
  - Use `use` declarations, not fully qualified paths
  - Don't capitalize string literal messages
  - End each test with `env.shutdown_and_drain_remaining_events(Duration::seconds(20))`
  - Add doc comments explaining what each test verifies and which pytest it migrates
- Register the module in `test-loop-tests/src/tests/mod.rs` (keep alphabetical order)

## 5. Verify

- Run `cargo check --package test-loop-tests --features test_features` to verify compilation
- Run each test individually:
  `cargo test --package test-loop-tests --features test_features -- tests::<module>::<test_name> --exact --show-output`
- Fix any compilation errors or test failures
- Run `cargo fmt --package test-loop-tests`

## 6. Clean up the pytest

- Delete the original pytest file
- Remove its entries from `nightly/pytest-sanity.txt` (check for both regular and `--features nightly` variants, and commented-out spice variants)
- Search for any other references to the pytest filename:
  `grep -r "<pytest_filename>" nightly/ scripts/ .github/`

## 7. Summary

Report what was done:
- Which pytest scenarios were migrated (and any that were intentionally simplified or skipped)
- The new test file location and test function names
- Approximate runtime comparison if available
