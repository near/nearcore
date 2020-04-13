const shell = require('shelljs');

shell.fatal = true; // same as "set -e"

shell.exec('cargo run --package near-vm-runner-standalone --bin near-vm-runner-standalone --  --method-name=ping  ' +
    '--wasm-file=./contract/target/wasm32-unknown-unknown/release/basic_contract.wasm');