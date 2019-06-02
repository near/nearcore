#!/bin/bash
set -ex

cargo run -p near -- init --test-seed alice.near
cargo run -p near -- --verbose run --produce-empty-blocks=false
