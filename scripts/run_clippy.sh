#!/usr/bin/env bash
# clippy adoption is in progress, see https://github.com/near/nearcore/issues/8145

LINTS=(
  -A clippy::all
  -D clippy::correctness
  -D clippy::suspicious
  -D clippy::redundant_clone
)

cargo clippy -- "${LINTS[@]}"
