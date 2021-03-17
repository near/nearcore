#!/usr/bin/env bash
cargo clippy --all  -- -A clippy::type-complexity -A clippy::needless-pass-by-value -A clippy::while-let-loop -A clippy::too-many-arguments -A clippy::unit_arg -A clippy::if_same_then_else -A clippy::collapsible_if -A clippy::useless-let-if-seq -A clippy::map-entry -D warnings -A clippy::implicit-hasher -A clippy::ptr-arg -A renamed-and-removed-lints -A clippy::needless-range-loop -A clippy::unused_io_amount -A clippy::wrong-self-convention

