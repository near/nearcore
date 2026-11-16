# ECC example wallet

> **Warning: not for production use.** This contract is an example for developers learning
> external contract calls (ECCs), and a fixture for nearcore's end-to-end tests. It has not been
> audited, and it leaves out checks a real wallet needs (see
> [What a production wallet needs](#what-a-production-wallet-needs)). Do not deploy it to hold
> real funds.

An account-abstraction wallet that runs requests signed by a WebAuthn passkey on P-256.

A NEAR access key can't be a passkey, so without external calls this wallet would need a relayer:
an account with an access key that wraps each request in a signed transaction and pays for it. An
external call needs no relayer. Anyone can submit the call, the wallet's own code decides whether
to accept it, and an accepted call is paid for by the wallet. A refused call costs the wallet
nothing.

## How it works

An external call (`EccTransaction`) carries a contract id, a payload and an optional state init.
The protocol runs the contract's `__external_call` method with the payload as input. Returning
accepts the call; panicking refuses it.

This wallet's payload holds a signed request and the passkey's assertion over it. The wallet
checks the assertion, checks the request is meant for this chain and this wallet and hasn't run
before, and then sends the request's actions.

### Setting up a wallet

The wallet is a universal account, so it needs no transaction to create it:

1. Deploy `res/ecc_wallet.wasm` once per chain as a global contract, by code hash. Every wallet
   shares it.
2. Build the wallet's state init, a `UniversalStateInit::V1` with:
   - `code`: the global contract's code hash;
   - `data`: the two keys below, `pk` and `rp`;
   - `access_keys`: none. The passkey is the wallet's only authority.
3. The wallet's account id is derived from the state init's bytes. Fund that id with a transfer.
   This leaves an uninitialized account holding the balance.
4. Send the wallet's first external call with the state init attached. The protocol installs the
   state init, then runs the call. This first call sets the wallet's nonce floor and runs nothing
   (see [Nonces](#nonces)). An empty request (`actions: []`) is the natural first call.

The same state init gives the same account id on every chain. The signed request names the chain,
so a request signed for one chain can't run on another.

### State

| Key     | Value                                                     | Set by     |
|---------|-----------------------------------------------------------|------------|
| `pk`    | the passkey's public key: 33-byte compressed SEC1 P-256   | state init |
| `rp`    | SHA-256 of the WebAuthn relying party id, 32 bytes        | state init |
| `nonce` | the last nonce used, u64 little-endian                    | first call |

The state init must not contain `nonce`. If it did, the first call wouldn't set the floor.

### View method

`get_nonce` takes no arguments and returns the wallet's nonce as JSON:

- `"<n>"`, the stored nonce as a decimal string, as NEAR returns u64s;
- `null` before the first call has set the floor.

Call it with a view call, for example `viewFunction` in near-api-js. A client can also read the
`nonce` key with the `view_state` RPC query and decode the 8 bytes itself, but the method keeps
clients independent of how the wallet stores the nonce.

## Payload layout

The payload is [borsh](https://borsh.io)-encoded. In short: integers are little-endian; a
`Vec<u8>` or a string is a u32 length followed by its bytes; a list is a u32 count followed by
its items; an enum is a one-byte tag followed by its fields. Nothing else is added: no padding,
no field names.

### `ExternalCallPayload`, the whole payload

| Field                | Encoding                    | Notes                                    |
|----------------------|-----------------------------|------------------------------------------|
| `signed`             | u32 length + bytes          | a `SignedRequest`, see below             |
| `authenticator_data` | u32 length + bytes          | WebAuthn authenticator data              |
| `client_data_json`   | u32 length + bytes          | WebAuthn clientDataJSON                  |
| `signature`          | 64 bytes, no length prefix  | ECDSA `r ‖ s`, each 32 bytes big-endian  |

The request is nested as bytes so that the wallet hashes exactly the bytes the passkey signed.

WebAuthn returns a DER-encoded signature; convert it to `r ‖ s`. The wallet accepts high-s
signatures, as WebAuthn authenticators produce them.

### `SignedRequest`, the bytes the passkey signs

| Field           | Encoding               | Notes                                                    |
|-----------------|------------------------|----------------------------------------------------------|
| `version`       | u8                     | `0`                                                      |
| `chain_id`      | u32 length + UTF-8     | the chain the wallet runs on, e.g. `mainnet`             |
| `contract_id`   | u32 length + UTF-8     | the wallet's account id                                  |
| `nonce`         | u64                    | see [Nonces](#nonces)                                    |
| `expiry_height` | u64                    | the last block height at which the request may run       |
| `receiver_id`   | u32 length + UTF-8     | the account the actions go to                            |
| `actions`       | u32 count + actions    | sent as one batch; none sends nothing                    |

The block height a call sees is the height of the parent of the block its chunk is in. Leave a
margin when you set `expiry_height`.

### `WalletAction`

| Tag | Action          | Fields                                                                         |
|-----|-----------------|--------------------------------------------------------------------------------|
| `0` | `Transfer`      | `deposit`: u128                                                                |
| `1` | `FunctionCall`  | `method_name`: u32 length + UTF-8, `args`: u32 length + bytes, `deposit`: u128, `gas`: u64 |
| `2` | `DeleteAccount` | `beneficiary_id`: u32 length + UTF-8                                           |

The wallet pays for the gas attached to a function call, out of its balance.

### Signing

The request's challenge is `sha256(signed)`. Pass it to `navigator.credentials.get` as the
`challenge`. The passkey signs `authenticator_data ‖ sha256(client_data_json)`, as WebAuthn does
for every assertion.

## Checks

The wallet checks a call in this order. The first check that fails refuses the call, with the
message shown.

1. The payload and the request parse, with no bytes left over: `malformed payload`,
   `malformed request`. The request's version is `0`: `unsupported request version`.
2. `chain_id` is the chain's id: `request is for another chain`. `contract_id` is the wallet's
   account id: `request is for another contract`.
3. The block height is at most `expiry_height`: `request expired`.
4. The WebAuthn assertion:
   - the authenticator data is at least 37 bytes: `malformed authenticator data`;
   - it starts with the wallet's `rp`: `authenticator data is for another relying party`;
   - its user-present flag (bit 0 of byte 32) is set: `user presence flag not set`;
   - the client data contains `"type":"webauthn.get"`:
     `client data is not a webauthn.get assertion`;
   - the client data contains `"challenge":"<base64url of sha256(signed), no padding>"`:
     `client data challenge does not match the request`;
   - `signature` verifies over `sha256(authenticator_data ‖ sha256(client_data_json))` with the
     wallet's `pk`: `invalid signature`.
5. The nonce, below: `nonce too low`, `nonce too high`.

Then the wallet stores the new nonce and sends the actions.

If the wallet's own state is broken, for example the state init left out `pk` or `rp`, every call
is refused with `wallet state is malformed`.

## Nonces

Each request carries a nonce, so that it runs at most once. Nonces work like access key nonces:
they are block heights times 1,000,000.

- **The first call** finds no `nonce` in the wallet's state. After checks 1–4 it stores the floor
  `block_height × 1,000,000` and runs nothing. It logs `nonce floor set to <floor>; request not
  run`.
- **Every later call** needs `stored nonce < nonce < (block_height + 1) × 1,000,000`, and then
  stores its nonce. Pick the value of `get_nonce` plus 1, or
  `block_height × 1,000,000 + i` for a recent block height.

To cancel requests you signed and haven't sent, send an empty request with a higher nonce.

Don't build replay protection on the call's bytes. Nodes drop an exact repeat of a call that has
run, but ECDSA signatures are malleable: `(r, n - s)` verifies as well as `(r, s)`, and the wallet
accepts both, as WebAuthn requires. The same request with the other signature is a different call
that nodes don't recognize, so only the nonce stops it.

### Why the first call runs nothing

The wallet is a universal account. If it is deleted and its id is funded again, the next call with
the state init sets up the same account, with the same initial state. A nonce kept only in the
wallet's state would start over, and every request the old wallet ran would be valid again. An
expiry stops only the requests that have expired by then, and the wallet can be set up again
within a few blocks.

The floor stops all of them:

- The upper bound means every nonce the wallet accepts is below `(block_height + 1) × 1,000,000`
  at the height it ran.
- A wallet can only be set up again after it is deleted, and it is deleted by a receipt of one of
  its own calls. So the next wallet's first call runs at a greater block height than every call
  the old wallet accepted.
- So the new floor is above every nonce the old wallet accepted, expired or not.

The first call can't run its request. Its signer couldn't know the floor when signing, and the
wallet can't tell a fresh request from an unexpired one replayed from a deleted wallet. So any
request signed for this chain and this wallet can set the floor, an old one included. That costs
the wallet one call's fee each time it is set up, and runs nothing.

## What a production wallet needs

This example leaves out, among other things:

- **A JSON parse of the client data.** The wallet looks for the `type` and `challenge` fields as
  text. A real wallet should parse the JSON and check the `origin` too.
- **Key management:** more than one key, rotating keys, and recovering a lost passkey.
- **Balance headroom.** The protocol refuses a call that would leave the wallet below the balance
  its storage needs, but the wallet keeps nothing back for fees. A request that spends down to
  that balance leaves the wallet unable to pay for any later call until someone sends it tokens.
- **Limits on what a request may do,** such as allowances or allowed receivers.
- **Clear errors for clients,** such as an error for a malformed `pk`. Today a bad key length
  fails in the host function.
- **An audit.**

## Building and testing

The crate is not part of the nearcore workspace. From this folder:

```sh
cargo test     # unit tests for the payload layout and the checks, on the host
./build.sh     # builds res/ecc_wallet.wasm
```

`build.sh` uses the toolchain that nearcore's `rust-toolchain.toml` pins, with the flags that
`runtime/near-test-contracts/build.rs` uses, so that the wasm has no features the NEAR VM
doesn't accept. `res/ecc_wallet.wasm` is checked in for the test-loop tests
(`test-loop-tests/src/tests/ecc_wallet.rs`) to deploy. Nothing in CI rebuilds it, so rebuild and
commit it whenever the source changes.

`cargo fmt` and `cargo clippy` at the nearcore root skip this crate. Run them here, and run clippy
for the wasm target too, since the host functions are only compiled for it:

```sh
cargo clippy --all-targets
cargo clippy --target wasm32-unknown-unknown --release --target-dir target
```
