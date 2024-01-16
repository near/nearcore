#!/usr/bin/env bash

## When collecting coverage we want to instrument only the crates in our workspace, and not the
## external dependencies (there is no value in doing so – we don’t run tests for our dependencies
## anyway.) Furthermore, instrumenting the crates like these run a risk of significant performance
## regressions, such as seen in #10201

RUSTC="$1"; shift
exec "$RUSTC" -Cinstrument-coverage --cfg=coverage "$@"
