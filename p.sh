
clear
tmux clear-history

RUST_BACKTRACE=all \
RUST_LOG=debug,resharding=trace \
cargo nextest run \
    --package integration-tests \
    --features nightly \
    sharding_upgrade

