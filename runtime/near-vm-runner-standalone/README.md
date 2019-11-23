# near-vm-runner-standalone

a command line wrapper around `near-vm-runner.`
All error messages that can be raised during the contract execution are raised by `near-vm-runner`
and the all effects of computing the execution result of a smart contract are encapsulated inside `near-vm-runner`.

One can use `near-vm-runner-standalone` to test the smart contracts, e.g. with integration tests
to make sure it has expected behavior once deployed to the blockchain.
