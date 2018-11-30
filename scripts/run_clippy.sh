#!/bin/bash
cargo clippy --all  -- -A clippy::too-many-arguments -A clippy::unit_arg -A clippy::if_same_then_else -A clippy::collapsible_if -A clippy::useless-let-if-seq -A clippy::map-entry -D warnings
