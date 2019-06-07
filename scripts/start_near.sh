#!/bin/bash
set -ex

cargo run --release -p near -- init --test-seed alice.near
cargo run --release -p near -- --verbose run --produce-empty-blocks=false
