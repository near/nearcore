#!/bin/bash
cargo clippy --all  -- -A clippy::unit_arg -A clippy::if_same_then_else -A clippy::collapsible_if -A clippy::map-entry -A clippy::useless-let-if-seq -D warnings
