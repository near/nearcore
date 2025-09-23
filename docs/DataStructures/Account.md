# Accounts

## Account ID

NEAR Protocol has an account names system. Account ID is similar to a username. Account IDs have to follow the rules.

### Account ID Rules

- minimum length is 2
- maximum length is 64
- **Account ID** consists of **Account ID parts** separated by `.`
- **Account ID part** consists of lowercase alphanumeric symbols separated by either `_` or `-`.
- **Account ID** that is 64 characters long and consists of lowercase hex characters is a specific **NEAR-implicit account ID**.
- **Account ID** that is `0x` followed by 40 lowercase hex characters is a specific **ETH-implicit account ID**.

Account names are similar to a domain names.
Top level account (TLA) like `near`, `com`, `eth` can only be created by `registrar` account (see next section for more details).
Only `near` can create `alice.near`. And only `alice.near` can create `app.alice.near` and so on.
Note, `near` can NOT create `app.alice.near` directly.

Additionally, there is an [implicit account creation path](#implicit-account-creation).

Regex for a full account ID, without checking for length:

```regex
^(([a-z\d]+[\-_])*[a-z\d]+\.)*([a-z\d]+[\-_])*[a-z\d]+$
```

### Top Level Accounts

| Name | Value |
| - | - |
| REGISTRAR_ACCOUNT_ID | `registrar` |

Top level account names (TLAs) are very valuable as they provide root of trust and discoverability for companies, applications and users.
To allow for fair access to them, the top level account names are going to be auctioned off.

Specifically, only `REGISTRAR_ACCOUNT_ID` account can create new top level accounts (other than [implicit accounts](#implicit-accounts)). `REGISTRAR_ACCOUNT_ID` implements standard Account Naming (link TODO) interface to allow create new accounts.

*Note: we are not going to deploy `registrar` auction at launch, instead allow to deploy it by Foundation after initial launch. The link to details of the auction will be added here in the next spec release post MainNet.*

### Examples

Valid accounts:

```c
ok
bowen
ek-2
ek.near
com
google.com
bowen.google.com
near
illia.cheap-accounts.near
max_99.near
100
near2019
over.9000
a.bro
// Valid, but can't be created, because "a" is too short
bro.a
```

Invalid accounts:

```c
not ok           // Whitespace characters are not allowed
a                // Too short
100-             // Suffix separator
bo__wen          // Two separators in a row
_illia           // Prefix separator
.near            // Prefix dot separator
near.            // Suffix dot separator
a..near          // Two dot separators in a row
$$$              // Non alphanumeric characters are not allowed
WAT              // Non lowercase characters are not allowed
me@google.com    // @ is not allowed (it was allowed in the past)
system           // cannot use the system account, see the section on System account below
// TOO LONG:
abcdefghijklmnopqrstuvwxyz.abcdefghijklmnopqrstuvwxyz.abcdefghijklmnopqrstuvwxyz
```

## System account

`system` is a special account that is only used to identify refund receipts. For refund receipts, we set the predecessor_id to be `system` to indicate that it is a refund receipt. Users cannot create or access the `system` account. In fact, this account does not exist as part of the state.

## Implicit accounts

Implicit accounts work similarly to Bitcoin/Ethereum accounts.
You can reserve an account ID before it's created by generating a corresponding (public, private) key pair locally.
The public key maps to the account ID. The corresponding secret key allows you to use the account once it's created on chain.

### NEAR-implicit account ID

The account ID is a lowercase hex representation of the public key.
An ED25519 public key is 32 bytes long and maps to a 64-character account ID.

Example: a public key in base58 `BGCCDDHfysuuVnaNVtEhhqeT4k9Muyem3Kpgq2U1m9HX` will map to the account ID `98793cd91a3f870fb126f66285808c7e094afcfc4eda8a970f6648cdf0dbd6de`.

### ETH-implicit account ID

The account ID is derived from a Secp256K1 public key using the following formula: `'0x' + keccak256(public_key)[12:32].hex()`.

Example: a public key in base58 `2KFsZcvNUMBfmTp5DTMmguyeQyontXZ2CirPsb21GgPG3KMhwrkRuNiFCdMyRU3R4KbopMpSMXTFQfLoMkrg4HsT` will map to the account ID `0x87b435f1fcb4519306f9b755e274107cc78ac4e3`.

### Implicit account creation

An account with implicit account ID can only be created by sending a transaction/receipt with a single `Transfer` action to the implicit account ID receiver:

- The account will be created with the account ID.
- The account balance will have a transfer balance deposited to it.
- If this is NEAR-implicit account, it will have a new full access key with the ED25519-curve public key of `decode_hex(account_id)` and nonce `(block_height - 1) * MULTIPLIER` (to address an issues discussed [here](https://gov.near.org/t/issue-with-access-key-nonce/749)).
- If this is ETH-implicit account, it will have the [Wallet Contract](#wallet-contract) deployed, which can only be used by the owner of the Secp256K1 private key where `'0x' + keccak256(public_key)[12:32].hex()` matches the account ID.

Implicit account can not be created using `CreateAccount` action to avoid being able to hijack the account without having the corresponding private key.

Once a NEAR-implicit account is created it acts as a regular account until it's deleted.

An ETH-implicit account can only be used by calling the methods of the [Wallet Contract](#wallet-contract). It cannot be deleted, nor can a full access key be added.
The primary purpose of ETH-implicit accounts is to enable seamless integration of existing Ethereum tools (such as wallets) with the NEAR blockchain.

### Wallet Contract

The Wallet Contract (see [NEP-518](https://github.com/near/NEPs/issues/518) for more details) functions as a user account and is designed to receive, validate, and execute Ethereum-compatible transactions on the NEAR blockchain.

Without going into details, an Ethereum-compatible wallet user sends a transaction to an RPC endpoint, which wraps it and passes it to the Wallet Contract (on the target account) as an `rlp_execute(target: AccountId, tx_bytes_b64: Vec<u8>)` contract call.
Then, the contract parses `tx_bytes_b64` and verifies it is signed with the private key matching the target [ETH-implicit account ID](#eth-implicit-account-id) on which the contract is hosted.

Under the hood, the transaction encodes a NEAR-native action. Currently supported actions are:

- Transfer (from ETH-implicit account).
- Function call (call another contract).
- Add `AccessKey` with `FunctionCallPermission`. This allows adding a relayer's public key to an ETH-implicit account, enabling the relayer to pay the gas fee for transactions from this account. Still, each transaction has to be signed by the owner of the account (corresponding Secp256K1 private key).
- Delete `AccessKey`.

## Account

Data for a single account is collocated in one shard. The account data consists of the following:

- Balance
- Locked balance (for staking)
- Code of the contract
- Key-value storage of the contract. Stored in a ordered trie
- [Access Keys](AccessKey.md)
- [Postponed ActionReceipts](../RuntimeSpec/Receipts.md#postponed-actionreceipt)
- [Received DataReceipts](../RuntimeSpec/Receipts.md#received-datareceipt)

#### Balances

Total account balance consists of unlocked balance and locked balance.

Unlocked balance is tokens that the account can use for transaction fees, transfers staking and other operations.

Locked balance is the tokens that are currently in use for staking to be a validator or to become a validator.
Locked balance may become unlocked at the beginning of an epoch. See [Staking](../ChainSpec/EpochAndStaking/Staking.md) for details.

#### Contracts

A contract (AKA smart contract) is a program in WebAssembly that belongs to a specific account.
When account is created, it doesn't have a contract (except ETH-implicit accounts).
A contract has to be explicitly deployed, either by the account owner, or during the account creation.
A contract can be executed by anyone who calls a method on your account. A contract has access to the storage on your account.

#### Storage

Every account has its own storage. It's a persistent key-value trie. Keys are ordered in lexicographical order.
The storage can only be modified by the contract on the account.
Current implementation on Runtime only allows your account's contract to read from the storage, but this might change in the future and other accounts's contracts will be able to read from your storage.

NOTE: Accounts must maintain a minimum amount of value at a rate of 1 NEAR per 100kb of total storage in order to remain responsive.
This includes the storage of the account itself, contract code, contract storage, and all access keys.
Any account with less than this minimum amount will not be able to maintain a responsive contract and will, instead, return an error related to this mismatch in storage vs. minimum account balance.
See [Storage Staking](https://docs.near.org/concepts/storage/storage-staking) in the docs.

#### Access Keys

An access key grants an access to a account. Each access key on the account is identified by a unique public key.
This public key is used to validate signature of transactions.
Each access key contains a unique nonce to differentiate or order transactions signed with this access key.

An access key has a permission associated with it. The permission can be one of two types:

- `FullAccess` permission. It grants full access to the account.
- `FunctionCall` permission. It grants access to only issued function call transactions.

See [Access Keys](AccessKey.md) for more details.
