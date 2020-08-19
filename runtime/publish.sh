#!/usr/bin/env bash
set -ex
for p in near-runtime-fees near-vm-errors near-vm-logic near-vm-runner near-vm-runner-standalone
do
pushd ./${p}
cargo publish
popd
# Sleep a bit to let the previous package upload to crates.io. Otherwise we fail publishing checks.
sleep 30
done
