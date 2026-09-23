# Plan: Accounts, keys & access control

Generation brief for `spec/accounts-keys.md`. Follow `../CONVENTIONS.md`.

## Scope
The account model and authorization: account structure and versions, contract
association (local / global / global-by-account), access keys and their permissions
(full vs function-call), gas keys, nonce rules, account-id validity and creation
(named, implicit, deterministic), and storage staking accounting.

## Out of scope
- Action execution that mutates accounts/keys → [runtime-execution](../spec/runtime-execution.md)
  (this spec defines the *model*; that one the *transitions*).
- Where accounts/keys live in the trie → [state-storage](../spec/state-storage.md)
  (TrieKey layout there; semantics here).

## Code to read
- `core/primitives-core/src/account.rs` — `Account` V1/V2, `AccountContract`,
  `AccessKey`, `AccessKeyPermission`, `FunctionCallPermission`, `GasKeyInfo`.
- `core/primitives-core/src/account_id/` (or `near-account-id`) — account-id validity,
  implicit accounts.
- `runtime/runtime/src/access_keys.rs` — key add/delete/update, nonce handling.
- `runtime/runtime/src/verifier.rs` — access-key authorization & nonce checks at tx
  validation (cross-link).
- `core/primitives/src/transaction.rs` — `TransactionNonce` (Nonce vs GasKeyNonce).
- `docs/DataStructures/{Account,AccessKey}.md`, `docs/RuntimeSpec/AccountStorage.md`
  (cross-check).

## Questions the spec must answer
- What fields does an account hold (amount, locked/staked, storage_usage, contract),
  and how do V1 and V2 differ?
- What are the contract-association modes (None / Local(code_hash) / Global(code_hash)
  / GlobalByAccount)? What are global contracts for?
- Access keys: how identified, what permissions exist, and what does a FunctionCall key
  restrict (receiver, method names, allowance)?
- Gas keys: what is a gas key, how does its balance/nonce-index model differ from a
  normal access key, and how are nonces tracked (strict vs monotonic)?
- Account-id rules: valid characters/length, top-level restrictions, implicit accounts
  (hex/ed25519), deterministic account ids (NEP-616).
- Storage staking: how `storage_usage` is computed and the balance that must back it.

## Cross-component edges
- Mutated by runtime actions; authorizes transactions in the verifier; persisted in
  state-storage; staking interacts with epoch/validators.

## Relevant ProtocolFeatures
- `GasKeys`, `StrictNonce`, global contracts, deterministic account ids (NEP-616),
  account V2, `AccountCostIncrease`. Verify against `version.rs`.
