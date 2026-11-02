# ETH wallet contract fixtures

These two WASM files are executed by integration tests and were moved unchanged
from `runtime/near-wallet-contract/res`. Production runtime code only needs hashes.

- `wallet_contract_localnet.wasm`: original localnet wallet, last rebuilt in
  [nearcore #11968](https://github.com/near/nearcore/pull/11968), code hash
  `FAq9tQRbwJPTV3PQLn2F7AUD3FW2Fw1V8ZeZuazfeu1v`
- `global_contract_mainnet.wasm`: current global wallet, updated in
  [nearcore #16401](https://github.com/near/nearcore/pull/16401), code hash
  `5LM8a65dWxesxZeWq3HjCcjLkNTwHjTUPp8R117wZJDw`

The legacy source declared CC0-1.0, Aurora Labs authorship, and the upstream
repository https://github.com/aurora-is-near/eth-wallet-contract. The source,
Docker rebuild script, and archived mainnet/testnet binaries remain in the
[pre-move tree](https://github.com/near/nearcore/tree/bb04d86378ef66bbc0573771dad7d5fced25b2eb/runtime/near-wallet-contract).
The current global wallet is maintained in [near/near-wallet-contract](https://github.com/near/near-wallet-contract).

The runtime tests pin these original code hashes for legacy marker recognition,
so the unused archived binaries need not be embedded just to hash them:

- mainnet: `5j8XPMMKMn5cojVs4qQ65dViGtgMHgrfNtJgrC18X8Qw`
- testnet: `BL1PtbXR6CeP39LXZTVfTNap2dxruEdaWZVxptW6NufU`
- testnet PV70: `3Za8tfLX6nKa2k4u2Aq5CRrM7EmTVSL9EERxymfnSFKd`

These are protocol fixtures, not artifacts to rebuild during a nearcore build.
Changing their bytes requires reviewing the associated protocol hashes and tests.
