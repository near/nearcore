#!/bin/bash -eu

cd $SRC/nearcore/core/primitives/fuzz
cargo fuzz build --release

cd $SRC/nearcore/integration-tests/fuzz
cargo fuzz build --release

cd $SRC/nearcore/runtime/runtime/fuzz
cargo fuzz build --release

cd $SRC/nearcore/runtime/near-vm-runner/fuzz
cargo fuzz build --release

cd $SRC/nearcore/chain/jsonrpc/fuzz
cargo fuzz build --release

#cd $SRC/nearcore/test-utils/runtime-tester/fuzz
#cargo fuzz build --release

find $SRC/nearcore/target/x86_64-unknown-linux-gnu/release/ -maxdepth 1 -type f -executable -exec cp {} $OUT/ \;
