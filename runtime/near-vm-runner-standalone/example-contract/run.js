const shell = require('shelljs');

shell.fatal = true;

shell.exec('cargo run --package near-vm-runner-standalone --bin near-vm-runner-standalone --  --method-name=ping  ' +
    '--wasm-file=./example-contract/target/wasm32-unknown-unknown/release/example_contract.wasm');