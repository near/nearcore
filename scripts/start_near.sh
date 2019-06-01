#!/bin/bash
set -ex

cargo run -p near -- init
cargo run -p near -- --verbose run --produce-empty-blocks=false
