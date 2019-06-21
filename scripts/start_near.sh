#!/bin/bash
set -ex

rm -rf testdir
cargo run -p near -- --home=testdir/ init --test-seed alice.near --account-id test.near --fast
cargo run -p near -- --home=testdir/ --verbose run --produce-empty-blocks=false
