# ETH wallet contract fixtures

These WASM files were moved byte-for-byte from `runtime/near-wallet-contract/res`.
Production runtime code only needs their hashes; tests retain the binaries to
check the legacy magic hashes and exercise wallet execution and upgrades.

The legacy mainnet, testnet, and localnet binaries were last rebuilt in
[nearcore #11968](https://github.com/near/nearcore/pull/11968). The PV70 testnet
binary preserves the earlier implementation, introduced as a separate fixture in
[nearcore #11975](https://github.com/near/nearcore/pull/11975). The vendored legacy
source declared CC0-1.0, Aurora Labs authorship, and the upstream repository
https://github.com/aurora-is-near/eth-wallet-contract. Its source and Docker rebuild
script remain available in the
[pre-move tree](https://github.com/near/nearcore/tree/bb04d86378ef66bbc0573771dad7d5fced25b2eb/runtime/near-wallet-contract).

The current mainnet global binary was updated in
[nearcore #16401](https://github.com/near/nearcore/pull/16401), from the separately
maintained [wallet contract](https://github.com/near/near-wallet-contract).
It hashes to `5LM8a65dWxesxZeWq3HjCcjLkNTwHjTUPp8R117wZJDw`.

These are protocol fixtures, not artifacts to rebuild during a nearcore build.
Changing their bytes requires reviewing the associated protocol hashes and tests.
