# ECC example wallet

> **Warning: not for production use.** This contract is an example for developers learning
> external contract calls (ECCs), and a fixture for nearcore's end-to-end tests. It has not been
> audited, and it leaves out checks a real wallet needs. Do not deploy it to hold real funds.

An account-abstraction wallet that runs requests signed by a WebAuthn passkey, through
`__external_call`.

This is a stub for now: its `__external_call` accepts every call and does nothing. The
authentication logic and its documentation come next.

## Building

The crate is not part of the nearcore workspace. Build it from this folder:

```sh
./build.sh
```

This writes `res/ecc_wallet.wasm`, which is checked in for the test-loop tests to deploy. Rebuild
and commit it whenever the source changes.
