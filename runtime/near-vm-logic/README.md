# near-vm-logic

This crate implements the specification of the interface that Near blockchain exposes to the smart contracts.
It is not dependent on the specific way the smart contract code is executed, e.g. through Wasmer or whatnot, and
therefore can be used for unit tests in smart contracts.

Note, this logic assumes the little endian byte ordering of the memory used by the smart contract.

# Run tests

`cargo test --features mocks`
