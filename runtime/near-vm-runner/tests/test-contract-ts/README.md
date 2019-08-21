A smart contract written in AssemblyScript that can be used to make
sure near runtime is compatible with AssemblyScript smart contracts.

# Pre-requisites

Switch to the smart contract directory and point npm to AssemblyScript:
```bash
npm install --save-dev AssemblyScript/assemblyscript
```

Then install dependencies with
```bash
npm install
```

# Building

Build smart contract with:
```bash
npm run asbuild:untouched
```

And copy the smart contract into `res` directory:
```bash
cp build/untouched.wasm ../res/test_contract_ts.wasm
```

Then run the Rust integration test with:
```bash
cargo test --package near-vm-runner --test test_ts_contract "" -- --nocapture
```
