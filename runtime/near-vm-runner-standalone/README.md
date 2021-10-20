# near-vm-runner-standalone

A command line wrapper around `near-vm-runner.` All error messages
that can be raised during the contract execution are raised by
`near-vm-runner` and the all effects of computing the execution result
of a smart contract are encapsulated inside `near-vm-runner`.

One can use `near-vm-runner-standalone` to test the smart contracts,
e.g. with integration tests to make sure it has expected behavior once
deployed to the blockchain.

To use run like this:

```bash
cargo run -- --wasm-file=/tmp/main.wasm --method-name=hello
cargo run -- --wasm-file ./status_message.wasm --method-name set_status \
             --input '{"message": "12345"}'
сargo run -- --wasm-file ./status_message.wasm --method-name get_status \
             --input '{"account_id": "bob"}' \
             --state '{"U1RBVEU=":"AQAAAAMAAABib2IFAAAAMTIzNDU="}'
```
I.e. persistent state could be passed across runs via `--state` parameter.

Optional `--context-file=/tmp/context.json` and
`--config-file=/tmp/config.json` flags can be added to provide custom
context and VM config respectively.

There are some test contracts in this repository, for example:

```bash
cargo run -- --wasm-file=../near-test-contracts/res/test_contract_rs.wasm \
             --method-name=log_something
```
