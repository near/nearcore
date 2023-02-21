#!/usr/bin/env bash
# clippy adoption is in progress, see https://github.com/near/nearcore/issues/8145

LINTS=(
  -A clippy::all
  -D clippy::clone_on_copy
  -D clippy::correctness
  -D clippy::redundant_clone
  -D clippy::suspicious
)

cargo clippy -- "${LINTS[@]}"
