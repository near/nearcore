# Accounts, keys & access control

> Protocol version: 87 (stable) · Release: 2.14.0-rc.1 · Derived from commit: 233252e · Generated: 2026-09-14
> Primary crates/files: `core/primitives-core/src/account.rs`, `core/primitives-core/src/universal_account_id.rs`, `core/primitives-core/src/universal_state_init.rs`, `core/primitives-core/src/deterministic_account_id.rs`, `near-account-id` 3.0.0 (external crate, read from the crates.io `.crate` whose sha256 matches `Cargo.lock`: `src/validation.rs`, `src/account_id_ref.rs`), `core/crypto/src/signature.rs`, `core/primitives/src/universal_state_init.rs`, `core/primitives/src/transaction.rs`, `core/primitives/src/views.rs`, `runtime/runtime/src/access_keys.rs`, `runtime/runtime/src/verifier.rs`, `runtime/runtime/src/actions.rs`, `runtime/runtime/src/universal_account_id.rs`, `runtime/runtime/src/action_validation.rs`, `core/store/src/utils/mod.rs`, `core/primitives-core/src/version.rs`

## Role

This component defines the **account model and authorization layer**: the on-chain `Account` record (balance, stake, storage usage, deployed contract, and — new at v87 — whether its state has been installed at all), the access keys that authorize transactions against an account, gas keys (prepaid-balance access keys), the nonce rules that prevent replay, the validity/typing rules for account IDs including the four implicit schemes, and the storage-staking accounting that ties an account's balance to its storage footprint. It is a *data + rules* component: it owns the structures and the authorization/validity predicates, but the state *transitions* that mutate accounts and keys live in [runtime-execution](runtime-execution.md) (the action handlers in `access_keys.rs`/`actions.rs`/`universal_account_id.rs`), and the trie layout where accounts/keys are persisted lives in [state-storage](state-storage.md). Stake held in `Account::locked` is read and rewritten by [epoch-validators-staking](epoch-validators-staking.md).

## Key data structures

### Account record

- **`Account`** — `core/primitives-core/src/account.rs:81` — the per-account state record, now an enum over **two independent axes**: `Uninitialized(UninitializedAccountV1)` | `Initialized(InitializedAccount)`. Version-erasing accessors (`amount`, `locked`, `contract`, `storage_usage`, `version`, `state`, `bootstrap_nonce`) live at `:293`–`:459` and answer for both arms.
- **`InitializedAccount`** — `core/primitives-core/src/account.rs:89` — the storage-layout version of an account whose state is in place: `V1(AccountV1)` | `V2(AccountV2)`. `Account::new` (`:254`) picks the *minimum* version that can represent the contract: `AccountContract::None`/`Local` stay V1; `Global`/`GlobalByAccount` force V2.
- **`AccountV1`** — `core/primitives-core/src/account.rs:105` — original layout: `amount`, `locked`, `code_hash: CryptoHash`, `storage_usage`. The contract is implicit: `code_hash == default` means no contract, otherwise a `Local` contract (`AccountContract::from_local_code_hash`, `:156`).
- **`AccountV2`** — `core/primitives-core/src/account.rs:202` — replaces the bare `code_hash` field with an explicit `contract: AccountContract`. Same `amount`/`locked`/`storage_usage`.
- **`UninitializedAccountV1`** — `core/primitives-core/src/account.rs:221` — a **universal (`0u`) account funded before its state init was installed**. Carries only `amount`, `storage_usage` (the bare account record), and `bootstrap_nonce`. It has no `locked`, no contract, no access keys and no data; `locked()` is hard-coded to zero and `contract()` reports `AccountContract::None` (`:359`,`:374`). Introduced by #16248.
- **`AccountState`** — `core/primitives-core/src/account.rs:42` — `Initialized` (default) | `Uninitialized`. Exposed through `Account::state()` (`:348`) so views can report it; a client cannot otherwise tell which transaction shape to send.
- **`InvalidAccountState`** — `core/primitives-core/src/account.rs:63` — `Uninitialized` | `AlreadyInitialized`, returned by the setters that cannot apply to an uninitialized account (`set_locked` `:435`, `set_contract` `:447`, `set_bootstrap_nonce` `:306`, `initialize` `:330`). Unreachable on a healthy chain, so callers map it to `StorageError::StorageInconsistentState` via `OrInconsistentState` (`runtime/runtime/src/actions.rs:48`).
- **`AccountContract`** — `core/primitives-core/src/account.rs:139` — the contract-association mode: `None` | `Local(CryptoHash)` (code stored under this account) | `Global(CryptoHash)` (shared code addressed by its hash) | `GlobalByAccount(AccountId)` (shared code addressed by the publisher account). `identifier_storage_usage` (`:182`) charges 0 bytes for `None`/`Local`, 32 for `Global`, and `id.len()` for `GlobalByAccount`.

### Keys

- **`AccessKey`** — `core/primitives-core/src/account.rs:801` — `{ nonce: Nonce, permission: AccessKeyPermission }`. Identified in state by `(account_id, public_key_handle)`; one account can hold many. On a fresh regular key `nonce` is seeded to `(block_height-1) * 1_000_000` to avoid tx-hash collisions on re-creation (`ACCESS_KEY_NONCE_RANGE_MULTIPLIER`, `:812`). For gas keys the on-key `nonce` is unused and forced to 0; nonces live in separate per-index trie entries. `NONCE_VALUE_LEN` (`:815`) is the borsh size of a `Nonce`.
- **`AccessKeyPermission`** — `core/primitives-core/src/account.rs:909` — `FunctionCall(FunctionCallPermission)` | `FullAccess` | `GasKeyFunctionCall(GasKeyInfo, FunctionCallPermission)` | `GasKeyFullAccess(GasKeyInfo)`. `MAX_NONCES_FOR_GAS_KEY = 1024` (`:923`). Helpers `function_call_permission` (`:925`) and `AccessKey::gas_key_info` (`:850`) project the relevant inner data regardless of variant.
- **`FunctionCallPermission`** — `core/primitives-core/src/account.rs:959` — restricts a key to function-call use: `allowance: Option<Balance>` (`None` = unlimited; spent in lockstep with account balance), `receiver_id: String` (the only allowed receiver; a `String` not `AccountId` because legacy testnet genesis holds invalid values), `method_names: Vec<String>` (allowed methods; empty = any).
- **`GasKeyInfo`** — `core/primitives-core/src/account.rs:880` — `{ balance: Balance, num_nonces: NonceIndex }`. `balance` is a prepaid pot used to pay gas; `num_nonces` is the count of independent nonce slots. `MAX_BALANCE_TO_BURN = 1 NEAR` (`:888`) caps the balance that may be burned when deleting the key/account.
- **`PublicKey` / `PublicKeyHandle`** — `core/crypto/src/signature.rs:249` / `:516` — the wire form and the on-trie form of a key. `PublicKeyHandle` replaces ML-DSA-65's 1952-byte key with its 32-byte SHA3-256 digest, making "a full ML-DSA-65 key in the trie" unrepresentable. `PublicKey::len()` (`:267`) is the wire length (`1 + 1952` for ML-DSA-65); `trie_id_len()` (`:332` / `:544`) is the on-trie identifier length (`1 + 32`).
- **`KeyTag`** — `core/crypto/src/signature.rs:353` — one shared borsh/`Hash` discriminant space with **disjoint tag sets**: `PublicKey` owns `{Ed25519=0, Secp256k1=1, MlDsa65Full=2}`, `PublicKeyHandle` owns `{0, 1, MlDsa65Hash=3}`. Both deserializers match the enum exhaustively and explicitly reject the other's reserved tag (`:423`, `:692`), so a full ML-DSA-65 key can never decode as a handle and a digest can never decode as a wire key (#16043).

### Transaction nonce

- **`TransactionNonce`** — `core/primitives/src/transaction.rs:66` — `Nonce { nonce }` (regular keys) | `GasKeyNonce { nonce, nonce_index }` (gas keys). `nonce_index()` (`:89`) returning `Some` is what routes a tx down the gas-key path. `TransactionV0` carries a bare `Nonce` (`:36`); `TransactionV1` carries a `TransactionNonce` plus a `nonce_mode` (`:122`).
- **`NonceMode`** — `core/primitives/src/transaction.rs:113` — `Monotonic` (default; any nonce strictly greater) | `Strict` (`tx_nonce == ak_nonce + 1`). `Transaction::nonce_mode` forces V0 txs to `Monotonic` (`:214`).

### State-init payloads

- **`UniversalStateInit`** — `core/primitives/src/universal_state_init.rs:51` — versioned enum (`V1(UniversalStateInitV1)`, struct at `:68`) fully describing a `0u` account: `code: Option<GlobalContractIdentifier>` (absent = key-only/EOA account), `data: BTreeMap<Vec<u8>, Vec<u8>>` (initial storage), `access_keys: BTreeSet<PublicKeyHandle>` (full-access keys, stored as on-trie handles). Flat and versioned rather than one variant per "kind": which fields are populated decides the kind.
- **`RawStateInit`** — `core/primitives-core/src/universal_state_init.rs:44` — the wire form: the exact bytes the producer borsh-serialized. **The account id is SHA3-256 over exactly these bytes**, never over a re-serialization of the decoded view. It lives in the crypto-free core crate because the VM host-function ABI names it while forwarding bytes verbatim.
- **`UniversalStateInitCounts`** — `core/primitives-core/src/universal_state_init.rs:56` — `{ num_bytes, num_entries, num_keys }`, what the action is priced on; computed by `state_init_counts` (`core/primitives/src/universal_state_init.rs:140`). `num_bytes` is the payload's own length (so a payload that collapses on decode still pays); `num_entries`/`num_keys` come from decoding, and a payload that does not decode counts as zero of each.
- **`UniversalStateInitAction`** — `core/primitives/src/action/mod.rs:244` — `{ state_init: RawStateInit, deposit: Balance }`; `Action::UniversalStateInit` is borsh discriminant 15 (`:396`).
- **`DeterministicAccountStateInit`** — `core/primitives-core/src/deterministic_account_id.rs:22` — the older NEP-616 payload for `0s…` accounts: versioned enum carrying `code: GlobalContractIdentifier` (mandatory) and `data: BTreeMap<Vec<u8>, Vec<u8>>`. The apply path lives in [runtime-execution](runtime-execution.md).

## Behavior

### 1. Account versioning & serialization

1. `Account` chooses the lowest representation that fits: an uninitialized `0u` account is `Account::Uninitialized` (`Account::new_uninitialized`, `account.rs:283`); an initialized account with `None`/`Local` contract is `InitializedAccount::V1`, anything else `V2` (`Account::new`, `:254`). `set_contract` upgrades a V1 account to V2 in place the moment a `Global`/`GlobalByAccount` contract is assigned (`:557`), and fails on an uninitialized account (`:447`).
2. `Account::initialize` (`:330`) moves an uninitialized account to `Initialized(V1)`, keeping `amount` and `storage_usage`, zeroing `locked`/`code_hash`, and **dropping `bootstrap_nonce`** — from then on each installed access key carries its own nonce.
3. **Borsh**: the first serialized field of every format is a `u128`. V1 serializes as a bare `AccountV1`. Any other form is detected by a sentinel `u128::MAX` (`SERIALIZATION_SENTINEL`, `:252`) prepended before a `BorshVersionedAccount` enum (`:731`) whose variants are `V2(AccountV2) = 0` and `Uninitialized(UninitializedAccountV1) = 1`. The sentinel is unambiguous because total supply can never reach `u128::MAX` (`BorshDeserialize for Account`, `:737`).
4. **Serde** uses `SerdeAccount` (`:588`), which carries `code_hash`, optional `global_contract_hash`/`global_contract_account_id`, a `state: AccountState` (omitted when initialized) and an optional `bootstrap_nonce`. Deserialization rejects an account that holds *both* a local and a global contract, or both global-contract forms (`:625`,`:632`); rejects a `bootstrap_nonce` on an initialized account (`:642`); and, for an uninitialized account, rejects a non-V1 version, a non-zero `locked`, any contract, and a **missing** `bootstrap_nonce` (`uninitialized_account_from_serde`, `:674`) — defaulting the nonce would reset the account's replay barrier.

### 2. Contract-association modes

A receiver account's `AccountContract` tells the runtime where to fetch code for a `FunctionCall`. `Local(hash)` reads code stored under this account; `Global(hash)` and `GlobalByAccount(id)` reference a single shared deployment so many accounts can run the same code without each paying to store it (global contracts; the deploy/use transitions and `GlobalContractIdentifier` are in [runtime-execution](runtime-execution.md)). ETH-implicit accounts are created already pointing at a `Global` wallet contract (`actions.rs:255`), and a universal account's `code` is installed through the same `use_global_contract` path (`runtime/runtime/src/global_contracts.rs:76`). Only `Local` code counts toward this account's own contract storage; `Global` costs a fixed 32-byte identifier and `GlobalByAccount` costs the publisher id's length (`account.rs:182`).

### 3. Account-id validity, typing, and the four implicit schemes

**Validity** (`near-account-id` 3.0.0 — `validation::validate`, `src/validation.rs:43`, reached via `AccountId::validate`, `src/account_id.rs:114`; used at `verifier.rs:701`,`:706`,`:757`): `MIN_LEN = 2` / `MAX_LEN = 64` (`validation.rs:4`,`:6`), allowed chars `a-z 0-9 - _ .`, and separators (`-_.`) may not lead, trail, or repeat (`:67`–`:91`). A `const fn` twin `validate_const` (`:8`) panics instead, for compile-time ids. Receipts re-validate predecessor and receiver ids (`validate_receipt`, `runtime/runtime/src/verifier.rs:681`); `validate_action_receipt` additionally re-validates the `refund_to` id (`:756`).

**Typing** — `AccountIdRef::get_account_type` (`near-account-id` 3.0.0, `src/account_id_ref.rs:246`) returns an `AccountType` (`:40`) with **five** variants as of this release. It tries the four implicit predicates in order — `is_eth_implicit` (`validation.rs:96`), `is_near_implicit` (`:126`), `is_near_deterministic` (`:105`), `is_universal` (`:117`) — and falls through to `NamedAccount` (`:259`).

| Variant | Shape | Created by |
|---|---|---|
| `EthImplicitAccount` | `0x` + 40 lowercase hex (42 chars) | transfer, alone in its receipt |
| `NearImplicitAccount` | exactly 64 lowercase hex chars | transfer, alone in its receipt |
| `NearDeterministicAccount` (NEP-616) | `0s` + 40 lowercase hex (42 chars) | transfer, alone in its receipt; or `DeterministicStateInit` |
| `UniversalAccount` (**new at v87**) | `0u` + 52 Crockford-base32 symbols (54 chars) | transfer, even batched; or `UniversalStateInit` |
| `NamedAccount` | everything else | `CreateAccount` |

`AccountType::is_implicit()` (`account_id_ref.rs:59`) is an exhaustive match returning **true for all four** non-`Named` variants and false only for `NamedAccount`; it is what `check_account_existence` uses to refuse `CreateAccount` (`actions.rs:829`).

`is_top_level` (`account_id_ref.rs:153`) = not `system` (`is_system`, `:277`) and contains no `.`; `is_sub_account_of` (`:216`) checks the id is `<label>.<parent>` with a single extra label.

**The `0u` (universal) address codec** — `core/primitives-core/src/universal_account_id.rs`:

1. `UAID_PREFIX = "0u"` (`:20`) is a scheme + hash-function marker; a different hash function would get a different letter.
2. The body is `UAID_DATA_SYMBOLS = 52` (`:22`) five-bit symbols encoding a 256-bit hash MSB-first, giving `UAID_LEN = 54` (`:24`) — comfortably inside the 64-char account-id limit.
3. The alphabet is lowercase **Crockford base32** minus `i l o u` (`CROCKFORD`, `:27`), so every emitted id is a valid account id by construction and transcription errors are reduced. `encode_universal_account_id` (`:30`) is the whole codec.
4. 256 bits do not divide into 52 five-bit symbols evenly: the final symbol carries 1 hash bit plus **4 zero padding bits** (`base32_encode`, `:44`). Consequently only two spellings of the last symbol (`0` and `g`) can ever appear, and an id whose padding bits are set is *not* classified as universal — it falls back to `NamedAccount`. The classifier is the exact mirror of this: `is_universal` (`near-account-id` 3.0.0, `src/validation.rs:117`) requires `len == 54`, the `0u` prefix, every body byte in its own copy of `CROCKFORD` (`:115`), and `bytes[53] ∈ {b'0', b'g'}` (`:123`) (asserted end-to-end by `only_canonical_encodings_classify_as_universal`, `:132`).
5. **There is no checksum.** An earlier design carried one; it was dropped (#16246), so the id is exactly prefix + hash. Nothing but the length, prefix, alphabet and padding rule separates a universal id from a named one.
6. The base32 implementation is in-tree rather than a dependency, cross-checked against `data-encoding` over 40M random cases (module doc, `:1`).

**The `0u` account-id derivation** — `derive_universal_account_id` (`core/primitives/src/utils.rs:502`): `encode_universal_account_id(SHA3-256(raw_state_init.0))`. It deliberately takes `RawStateInit`, not the typed `UniversalStateInit`, because the id commits to the exact user-supplied bytes. Compare `derive_near_deterministic_account_id` (`:484`), which is `0s` + hex of the *last 20 bytes* of its hash.

**Canonicity is not enforced** (#16140). `UniversalStateInit::from_raw` (`core/primitives/src/universal_state_init.rs:116`) accepts any well-formed borsh, rejecting only malformed or trailing bytes; borsh silently re-sorts `BTreeMap`/`BTreeSet` and deduplicates map keys on read. A non-canonical encoding of the same logical value therefore decodes fine — it simply hashes to a *different* account id. This is deliberate: canonicalization cannot be enforced end to end anyway, because contracts serialize their own nested state inside the opaque storage values. The typed API (`to_raw`, `:109`) always emits the canonical form, so ids minted through it are stable.

### 4. Account creation

**Named** (`action_create_account`, `runtime/runtime/src/actions.rs:166`): a top-level id shorter than `min_allowed_top_level_account_length` may only be created by the `registrar_account_id` (else `CreateAccountOnlyByRegistrar`, `:180`); a non-top-level id must be a direct sub-account of the predecessor (else `CreateAccountNotAllowed`, `:192`). The new account claims `actor_id` and starts with zero balance/stake, `AccountContract::None`, and `storage_usage = num_bytes_account` (`:202`). `check_account_existence` refuses `CreateAccount` for any id that types as implicit (`AccountType::is_implicit`, `actions.rs:829` → `OnlyImplicitAccountCreationAllowed`, `:836`) — claiming `actor_id` for such an id would let the rest of the receipt add a key the sender does not hold.

**Implicit, by transfer** (`action_implicit_account_creation_transfer`, `actions.rs:221`) branches on `get_account_type(account_id, config)` (`:233`) — the *config-aware* typing (`:899`), which downgrades a type whose feature flag is off to `NamedAccount` so a protocol upgrade is never implicit:

1. **NearImplicit** (`:234`) — derives a `FullAccess` key from the hex (the account id *is* the ed25519 public key), seeds its nonce, and charges account bytes + `trie_id_len` + access-key borsh + `num_extra_bytes_record`.
2. **EthImplicit** (`:255`) — created with a `Global` wallet contract addressed by `eth_wallet_global_contract_hash(chain_id)`; storage is account bytes + 32. This is now unconditional (see PV-gated section).
3. **NearDeterministic** (`:270`) — `create_deterministic_account` (`runtime/runtime/src/deterministic_account_id.rs:117`): an ordinary initialized V1 account with `None`/`num_bytes_account`. Its state init arrives separately.
4. **UniversalAccount** (`:276`) — `Account::new_uninitialized(deposit, num_bytes_account, initial_nonce_value(block_height))`. It is *not* an initialized account with no contract: it is the `Uninitialized` variant, and it stays that way until a `UniversalStateInit` runs.
5. `NamedAccount` here is unreachable and panics (`:286`).

None of these claim `actor_id` — it stays the receipt's predecessor (`:213` doc). For `0u` this is load-bearing, because…

**Whether a transfer may create the account** — `implicit_creation_allowed` (`actions.rs:929`): never for a refund; never for a named id; for NEAR-implicit / ETH-implicit / deterministic ids **only if the transfer is the receipt's only action** (`ReceiptShape::is_the_only_action`, `actions.rs:807`,`:811`); and for a universal id **always**, batched or not (`:945`, #16356). The batched case is safe precisely because the account id commits to its own access keys: a relayer sending `[Transfer, UniversalStateInit, AddKey]` cannot install a key the id does not commit to, because `actor_id` is never handed over (`:213`).

**What an uninitialized account may receive** — `check_account_existence` (`actions.rs:814`): `UniversalStateInit` is accepted whether the account is missing, uninitialized, or already initialized (`:857`); `Transfer` follows `implicit_creation_allowed`; and **every other state-touching action** (`DeployContract`, `FunctionCall`, `Stake`, `AddKey`, `DeleteKey`, `DeleteAccount`, `Delegate`/`DelegateV2`, global-contract actions, gas-key transfers) fails with `AccountNotInitialized` (`:883`). An uninitialized `0u` account is, for those purposes, as good as absent.

### 5. Installing a universal account's state

`action_universal_state_init` (`runtime/runtime/src/universal_account_id.rs:21`). Action validation has already checked that the receiver id equals the derived id, so this only installs and settles:

1. If the account does not exist, create it as `Uninitialized` with zero balance, `num_bytes_account`, and `initial_nonce_value(block_height)` (`:44`). It is created **without changing `actor_id`**, so a same-receipt follow-up cannot hijack it.
2. If the account is already initialized, skip straight to step 6 without touching existing state (`:51`). A half-installed account is never observable: a failed action rolls the whole state update back.
3. Decode `RawStateInit`. A payload that does not decode fails the *action* with `MalformedUniversalStateInit` rather than the chunk (`:58`) — receipt validation should already have rejected it, so this only guards a hypothetical gap.
4. Compute the nonce the installed keys will start at: `max(initial_nonce_value(block_height), bootstrap_nonce_consumed)` (`:66`). Installed keys must start **above** any nonce the bootstrap transaction consumed, or those same signed bytes replay through the ordinary access-key path.
5. `Account::initialize()`, then `install_universal_account` (`:102`) writes, in order:
   - **contract code**, if `code` is `Some`, via `use_global_contract` (`:115`) — which also accounts for its storage usage;
   - **storage entries**, each sized (`key.len() + value.len() + num_extra_bytes_record`) *before* the trie write so an overflow bails out early (`:126`);
   - **access keys**, each a `FullAccess` key at the computed nonce, written by handle via `set_access_key_by_handle` (`:149`) and charged `handle.trie_id_len() + access_key_borsh_len + num_extra_bytes_record` — mirroring `access_key_storage_usage`, so an ML-DSA-65 key costs its 33-byte handle, not its 1953-byte wire form.
   Storage usage is accumulated and written once at the end (`:152`).
6. `settle_state_init_deposit` (`runtime/runtime/src/deterministic_account_id.rs:73`, shared with the deterministic path): the attached `deposit` covers whatever storage staking the new state needs and the remainder is refunded; if the deposit is short, the action fails with `LackBalanceForState` (`:95`).

### 6. Self-signed state init (bootstrap transaction)

A `0u` account can send exactly one transaction before it holds any access key (#16321). Authorization is the account id itself: the id is the hash of its state init, the state init names the account's keys, so a transaction addressed to that id and signed by one of those keys is authorized by the id alone.

1. **Stateless half** — `Transaction::state_init_bootstrap` (`core/primitives/src/transaction.rs:236`): the tx must carry no `nonce_index` (a bootstrap pays from the account's own balance, and a gas key is state the account does not have), `signer_id == receiver_id`, and a **top-level** `UniversalStateInit` action whose derived id equals the signer and whose decoded `access_keys` contain the handle of the signing public key. A `UniversalStateInit` nested in a `Delegate` authorizes nothing here.
2. **Stateful half** — `is_bootstrap` (`runtime/runtime/src/verifier.rs:200`): the account exists and is still uninitialized.
3. **Routing** — `get_signer_and_authorization` (`verifier.rs:164`) returns `TxAuthorization::SelfSignedStateInit` (`:185`) when no access key is found *and* the tx is a bootstrap; otherwise a missing key is `AccessKeyNotFound` (`:188`). `TxAuthorization` (`:140`) is the new three-way shape: `AccessKey` | `GasKey { access_key, nonce_index }` | `SelfSignedStateInit`.
4. **Charging** — `verify_and_charge_bootstrap_tx_ephemeral` (`verifier.rs:424`). It **re-checks** the authorization rather than trusting the caller (three paths reach it, and the chunk producer's picks it on nothing more than a missing access key). It requires `config.wasm_config.universal_accounts` and `tx.is_state_init_bootstrap()` (`:440`), then:
   - The nonce floor is the account's `bootstrap_nonce`, and `NonceMode::Strict` is **forced regardless of what the transaction asked for** (`:474`). Monotonic would let the signer pick any value above the floor, possibly above the `initial_nonce_value` the installed keys get — the same bytes would then replay through the ordinary path. Forcing Strict also lets a V0 transaction bootstrap, since V0 cannot express a nonce mode.
   - The pending floor is `pending.max_bootstrap_nonce`, not `max_nonce`: the bootstrap nonce is the *account's*, while `max_nonce` is scoped to a signing key and the state init commits to several (`:473`).
   - Balance and `check_storage_stake` are checked as on the regular path (`:480`,`:493`); the storage check is vacuous today because an uninitialized account is always under the zero-balance limit.
   - Returns `AccessKeyUpdate::Bootstrap { nonce }`, which is applied by writing the nonce back onto the *account* (`runtime/runtime/src/lib.rs:367`). Consuming it is what makes the transaction one-shot: a failed init leaves the account uninitialized, and without the consumed nonce the same signed bytes would stay admissible for the rest of their validity window.
5. `set_tx_state_changes` (`verifier.rs:123`) writes no access key for this path — there is none yet (`:132`).

### 7. Caps on state-init payloads

| Limit | Parameter | Value at v87 | Enforced at |
|---|---|---|---|
| Access keys one `UniversalStateInit` may commit to | `max_universal_state_init_keys` | 1024 | `action_validation.rs:566` → `UniversalStateInitTooManyKeys` |
| Storage entries across **all** state-init actions in one receipt | `max_state_init_entries` | 1500 (was `u32::MAX`) | `action_validation.rs:45` → `TotalNumberOfStateInitEntriesExceeded` |
| Per-entry key / value length | `max_length_storage_key` / `max_length_storage_value` | trie limits | `action_validation.rs:575`,`:581` |

- The key cap (#16403) exists because each committed key is priced as a full `AddKey` at the **send** rate, so the whole cost lands at transaction→receipt conversion; without a cap one transaction converts for more gas than a chunk has, and conversion happens before anything is charged, so transaction selection admits it anyway (`LimitConfig::max_universal_state_init_keys`, `core/parameters/src/vm.rs:130`).
- The entry cap (#16408) is on the **total across the receipt**, not per action, because a receipt can carry many byte-identical copies of a state init. Each entry adds to the receipt's congestion gas whether or not it is ever burnt, so an uncapped count lets one receipt reserve several times `max_congestion_outgoing_gas` and pin its own shard at full outgoing congestion (`LimitConfig::max_state_init_entries`, `core/parameters/src/vm.rs:139`; the 4_294_967_295 → 1_500 change is `core/parameters/res/runtime_configs/87.yaml:61`). 1500 is just above the ceiling the contract-created path already has (~1449 entries at 200 Ggas each against 300 Tgas). It applies to `DeterministicStateInit` too (`Action::num_state_init_entries`, `core/primitives/src/action/mod.rs:515`, which also recurses into `Delegate`, `:525`), and is enforced **only in `ValidateReceiptMode::NewReceipt`** (`action_validation.rs:117`,`:122`) so receipts already in flight at the upgrade keep executing.
- `validate_universal_state_init` (`action_validation.rs:535`) additionally gates on `ProtocolFeature::UniversalAccounts` (`:541`), checks `derive_universal_account_id(state_init) == receiver_id` (`InvalidUniversalStateInitReceiver`, `:550`), requires the payload to decode (`MalformedUniversalStateInit`, `:558`), and validates the `code` identifier (`:561`).

### 8. Access keys: add / delete / update

`action_add_key` (`runtime/runtime/src/access_keys.rs:149`): rejects a duplicate public key with `AddKeyAlreadyExists` (`:158`), then branches on whether the new key's permission carries a `GasKeyInfo` (`:168`):

1. **Regular key** (`add_regular_key`, `:230`): seeds `access_key.nonce = (block_height-1)*1_000_000` (`initial_nonce_value`, `:46`), writes it, and `checked_add`s `access_key_storage_usage` (`:17`) to `storage_usage`. Storage uses `public_key.trie_id_len()` (`:26`), so an ML-DSA-65 key costs the same as ed25519.
2. **Gas key** (`add_gas_key`, `:194`): forces the on-key `nonce` to 0 (`:206`), writes the access key, then writes `num_nonces` separate nonce entries each initialized to `initial_nonce_value(block_height)` (`:212`). Storage uses `gas_key_storage_cost` (`:31`) = access-key cost plus, per nonce, `(trie_id_len + size_of::<NonceIndex>()) + size_of::<Nonce>() + num_extra_bytes_record`.

`AddKey` validation for gas keys (`action_validation.rs:364`): requires `ProtocolFeature::GasKeys`; a `GasKeyFunctionCall` permission must have `allowance: None` (`:377`); `num_nonces` must be in `1..=MAX_NONCES_FOR_GAS_KEY` (`:382`); the initial `balance` must be zero (`:391`) — balance only arrives through `TransferToGasKey`.

`action_delete_key` (`access_keys.rs:52`) looks the key up; missing → `DeleteKeyDoesNotExist` (`:84`). Otherwise it branches on `gas_key_info()` (`:62`):
- **Regular** (`delete_regular_key`, `:136`): removes the key and `saturating_sub`s its storage cost.
- **Gas key** (`delete_gas_key`, `:93`): if `balance > MAX_BALANCE_TO_BURN` (1 NEAR) it errors `GasKeyBalanceTooHigh` and leaves the key intact (`:103`); otherwise it adds the balance to `result.tokens_burnt` (the prepaid pot is **burned**, not refunded, `:112`), removes every nonce entry, charges removal compute (`:119`), removes the access key, and `saturating_sub`s the gas-key storage cost.

Gas-key balance moves through dedicated actions (never via `AddKey`): `action_transfer_to_gas_key` (`:257`) `checked_add`s a deposit to `GasKeyInfo.balance`; `action_withdraw_from_gas_key` (`:290`) subtracts from the gas-key balance (`InsufficientGasKeyBalance` on underflow, `:316`) and credits the account `amount` (`:328`). Both error `GasKeyDoesNotExist` if the key is absent or not a gas key (`:263`,`:271` / `:297`,`:305`).

**Gas-key nonce invariant at the store boundary** (#15949): `set_access_key_by_handle` (`core/store/src/utils/mod.rs:372`) `debug_assert`s that an access key carrying a `GasKeyInfo` has `nonce == 0` (`:378`) — the real nonces live in `TrieKey::gas_key_nonce` entries written by `set_gas_key_nonce` (`:386`).

### 9. Transaction authorization (verifier — used by [runtime-execution](runtime-execution.md))

These functions are ephemeral: they validate and compute a verdict (`TxVerdict`) that the runtime applies. They perform **no** trie writes and **no** in-place mutation of `access_key` — the legacy pre-`FixAccessKeyAllowanceCharging` mutation is gone (see PV-gated section).

1. **Signer & authorization lookup**: `get_signer_and_authorization` (`verifier.rs:164`) → `SignerDoesNotExist` if no account (`:173`); then one of `AccessKey` / `GasKey` / `SelfSignedStateInit`; otherwise `AccessKeyNotFound` (`:188`).
2. **Nonce** (`verify_nonce`, `verifier.rs:254`): `Monotonic` requires `tx_nonce > current_nonce` (`:262`); `Strict` requires `tx_nonce == current_nonce + 1` (`:267`); either failure is `InvalidNonce { tx_nonce, ak_nonce }`. An upper bound `block_height * 1_000_000` rejects `NonceTooLarge` (`:275`). The "current" nonce is `max(stored_nonce, pending.max_nonce)` to account for in-flight txs (`:345`,`:570`).
3. **FunctionCall permission** (`verify_function_call_permission`, `verifier.rs:209`): the tx must be exactly one action and it must be a `FunctionCall` (else `RequiresFullAccess`, `:213`,`:218`), with **zero** deposit (else `DepositWithFunctionCall`, `:223`), `tx.receiver_id == permission.receiver_id` (else `ReceiverMismatch`, `:230`), and method in `method_names` when non-empty (else `MethodNameMismatch`, `:238`).
4. **Allowance**: `check_and_compute_new_allowance` (`verifier.rs:282`) — for a FunctionCall key with a finite `allowance`, subtracts `total_cost`; underflow → `NotEnoughAllowance` (`:294`). Allowance is decremented in lockstep with the account balance, and only ever through the returned verdict.
5. **Regular path** (`verify_and_charge_tx_ephemeral`, `verifier.rs:312`): asserts the tx has no `nonce_index` (`:323`); if the key is actually a gas key it is rejected (`InvalidNonceIndex { tx_nonce_index: None }`, `:329`) — gas keys *must* use the gas-key path. Verifies nonce, checks balance (`NotEnoughBalance`, `:355`), debits `total_cost` (`:363`), computes the new allowance (`:365`), runs `check_storage_stake` (`:375`) and the FunctionCall permission check (`:389`), and returns `AccessKeyUpdate::Regular { nonce, new_allowance }` (`:402`).
6. **Bootstrap path** — see §6.
7. **Gas-key path** (`verify_and_charge_gas_key_tx_ephemeral`, `verifier.rs:524`): panics if there is no `nonce_index` (`:536`); the key must be a gas key (else `AccessKeyNotFound`, `:552`); `nonce_index` must be `< num_nonces` (else `InvalidNonceIndex`, `:562`). Then it **splits the cost**: the gas portion (`gas_cost`) is drawn from the gas-key `balance` (`NotEnoughGasKeyBalance` on shortfall, `:594`); the deposit portion (`deposit_cost`) is drawn from the account `amount` (`:645`). If the account cannot cover the deposit, the verdict is `DepositFailed` (`:646`): the gas key is still charged, but only `burnt_amount` (`new_key_balance_on_deposit_failure`, `:605`), and the deposit is dropped. A `check_storage_stake` failure on the new account amount is likewise `DepositFailed` (`:662`). Success returns `AccessKeyUpdate::GasKey { new_balance, nonce_index, nonce }` (`:627`), persisted to the indexed nonce entry (`runtime/runtime/src/lib.rs:379`).

### 10. Storage staking

`check_storage_stake` (`verifier.rs:48`) requires `amount + locked >= storage_amount_per_byte * storage_usage` (`:74`); shortfall returns `LackBalanceForStorageStaking(needed)`, surfaced to transactions as `LackBalanceForState`. **Exception**: a *zero-balance account* (NEP-448) — `storage_usage <= ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT = 770` bytes (`:25`, `is_zero_balance_account`, `:88`) — always passes regardless of balance. `storage_usage` is maintained incrementally by every action that adds/removes keys, code, or data (`access_key_storage_usage`; `install_universal_account`; `clear_account_contract_storage_usage`, `actions.rs:443`).

### 11. Views

- **`AccountView`** — `core/primitives/src/views.rs:80` — every field now documented (#16076), plus two new ones: `state: AccountState`, omitted when initialized (`:101`,`:102`), and `bootstrap_nonce: Option<Nonce>`, present only while uninitialized (`:107`,`:108`). The latter is the *only* way a client can learn the nonce a self-signed state init must carry, since there is no access key to query. Built by `From<&Account>` (`:123`).
- **`AccessKeyView`** — `core/primitives/src/views.rs:253` — `{ nonce, permission }`, documented at `:254`,`:256`.
- **`AccessKeyInfoView`** — `:316` — `public_key` is a `PublicKeyHandle`, so an ML-DSA-65 key surfaces as `ml-dsa-65-hash:<base58 of the SHA3-256 digest>`.
- **Paginated `view_access_keys`** (#16039) — `runtime/runtime/src/state_viewer/mod.rs:185`. `QueryRequest::ViewAccessKeyList` gained `after_key: Option<PublicKeyHandle>` and `limit: Option<NonZeroU32>` (`views.rs:421`), and `AccessKeyList` gained `last_key: Option<PublicKeyHandle>` as the resume cursor (`views.rs:334`). A request is *paginated* if either parameter is present (`:196`); an explicit page size above the configured maximum is clamped rather than rejected (`:202`); an **un**paginated request that exceeds the configured maximum still errors `TooManyAccessKeys` (`:254`), preserving the old behavior. Iteration is bounded by a prune condition on the account's access-key prefix (which survives `seek`, `:214`), and after each gas key it seeks past that key's whole nonce block so the scan does not walk 1024 nonce rows per key (`:269`).
- **Genesis records** (#16398) — `core/chain-configs/src/genesis_validate.rs:57` accepts an uninitialized account in genesis (it cannot occur on a real chain's genesis, but can in one produced by a state dump) and records its id; the final pass then rejects any access-key, contract, or data record for such an account (`:160`), because genesis application assumes an uninitialized account owns no state.

## Interactions

- **Consumes**: `RuntimeConfig` fees (`storage_amount_per_byte`, `num_bytes_account`, `num_extra_bytes_record`), `LimitConfig` caps (`max_universal_state_init_keys`, `max_state_init_entries`, `max_length_storage_key`/`_value`), the `eth_implicit_accounts` / `universal_accounts` / `fix_ml_dsa_cost_charging` VM-config flags (`core/parameters/src/vm.rs:243`,`:249`,`:271`), `AccountCreationConfig` (registrar id, min top-level length), block height (for nonce seeding), and the trie via `TrieUpdate` getters/setters in [state-storage](state-storage.md) (`get_account`, `set_access_key`, `set_access_key_by_handle`, `set_gas_key_nonce`, `remove_gas_key_nonce`, `compute_gas_key_balance_sum`).
- **Produces / mutated by**: action handlers in [runtime-execution](runtime-execution.md) (`AddKey`/`DeleteKey`/`Transfer`/`CreateAccount`/`DeployContract`/`Stake`/`DeleteAccount`/`TransferToGasKey`/`WithdrawFromGasKey`/`DeterministicStateInit`/`UniversalStateInit`) mutate the `Account` and keys; the verifier path there authorizes transactions using the rules defined here. Transfer fees depend on the receiver's account type (`transfer_exec_fee`/`transfer_send_fee`, `core/parameters/src/cost.rs:775`,`:812`) — a `0u` receiver adds the `create_account` fee when the feature is on, like a deterministic account.
- **Touches**: `Account::locked` is the staked balance read/rewritten by [epoch-validators-staking](epoch-validators-staking.md); global-contract code referenced by `AccountContract::Global`/`GlobalByAccount` and by a universal account's `code` is managed by [runtime-execution](runtime-execution.md); the `promise_batch_action_universal_state_init` host function and the gas it charges live in the VM ([runtime-execution](runtime-execution.md), `runtime/near-vm-runner/src/logic/logic.rs:194`); persisted layout in [state-storage](state-storage.md).

## Protocol-version-gated behavior

All versions cited from `core/primitives-core/src/version.rs` (`ProtocolFeature::protocol_version`, `:500`). `STABLE_PROTOCOL_VERSION` is 87 (`:680`) and `MIN_SUPPORTED_PROTOCOL_VERSION` is **84** (`:652`), so every feature at 83 or below is unconditional in this binary.

### Live gates touching this component

| Feature | Activates | Effect on this component |
|---|---|---|
| `UniversalAccounts` | **v87** (`version.rs:629`) | The whole `0u` scheme. Gates `validate_universal_state_init` (`action_validation.rs:541`), so the `UniversalStateInit` action is rejected pre-feature; the runtime flag `wasm_config.universal_accounts` (`vm.rs:249`, set by `87.yaml`) makes `AccountType::UniversalAccount` *implicit* — with it off, `get_account_type` (`actions.rs:918`) downgrades a `0u` id to `NamedAccount`, so a transfer to a nonexistent one fails `AccountDoesNotExist` instead of creating an uninitialized account, and the transfer fee carries no `create_account` component (`cost.rs:792`,`:830`). The flag is also re-checked on the bootstrap path (`verifier.rs:440`). |
| `RejectDelegateV2` | **v87** (`version.rs:621`) | Rejects `Action::DelegateV2` for newly created receipts (`action_validation.rs:192`), which **disables meta transactions from gas keys**: the inner nonce advances a gas key of the delegate sender that `PendingTransactionQueue` never sees, so its nonce and gas-key-balance commitments would miss that key. The variant and `VersionedDelegateActionPayload` remain for a future delegate version. In-flight receipts keep executing. |
| `RejectWithdrawFromGasKeyInDelegate` | **v87** (`version.rs:622`) | Rejects a `WithdrawFromGasKey` nested inside a delegate action (`action_validation.rs:244`); the pending-transaction queue scans only top-level actions, so a nested one drains a gas key the queue still counts as funded. New receipts only. |
| `FixMlDsaCostCharging` | **v87** (`version.rs:627`) | Gas-key fee symmetry. Post-fix the **send** fee is priced on the wire length `public_key.len()` and the **exec** fee on the on-trie `trie_id_len()`; pre-fix the send fee also used `trie_id_len()` (`gas_key_send_pk_len`, `runtime/runtime/src/config.rs:73`) and the VM host-function path priced the exec fee on the wire length (`gas_key_exec_pk_len`, `runtime/near-vm-runner/src/logic/logic.rs:194`). Also moves inner-`DelegateAction` signature-verification compute onto the receiver shard (`actions.rs:467`). Harmless for ed25519/secp256k1, where the two lengths coincide. |
| `RejectEmptyMethodName` | **v87** (`version.rs:620`) | `FunctionCall` with an empty `method_name` is rejected in action validation (`action_validation.rs:322`). Affects what a `FunctionCall`-permission key can be used for only indirectly. |
| `GasKeys` | v85 (`version.rs:604`) | Enables `TransactionV1` with `GasKeyNonce`, the `GasKeyFunctionCall`/`GasKeyFullAccess` permissions, and the `GasKeyInfo` balance/nonce model. Required for an `AddKey` carrying a `GasKeyInfo` (`action_validation.rs:373`); `Transaction::gas_keys_required()` is true for V1 (`transaction.rs:207`). |
| `StrictNonce` | v85 (`version.rs:608`) | Allows `NonceMode::Strict` on `TransactionV1` requiring `tx_nonce == ak_nonce + 1`; pre-feature/V0 txs are `Monotonic` (`verify_nonce`, `verifier.rs:266`). The bootstrap path forces Strict regardless (`verifier.rs:474`). |
| `PostQuantumSignatures` | v85 (`version.rs:609`) | ML-DSA-65 as a third key/signature scheme. Enforced centrally in `validate_actions_with_mode` (`action_validation.rs:108`) over `Action::post_quantum_signatures_required` (`core/primitives/src/action/mod.rs:457`), which covers contract-emitted receipts that never pass tx admission (#16073) and recurses into `Delegate`/`DelegateV2`; its `UniversalStateInit` arm inspects the decoded `access_keys` handles (`:471`). Storage and exec pricing use `trie_id_len()` so a PQ access key costs the same as ed25519. |
| `AccountCostIncrease` | v85 (`version.rs:616`) | Raises account-creation cost and changes gas-refund/penalty pricing for created accounts. Pricing arithmetic lives in [runtime-execution](runtime-execution.md). |
| `FixDeleteAccountGlobalContractStorageUsage` | v85 (`version.rs:602`) | `action_delete_account` subtracts the *whole* contract storage (including the global-contract identifier) via `get_contract_storage_usage`; the legacy path subtracted only local code (`actions.rs:342`,`:426`). |
| `FixDelegatedDeterministicStateInit` | v85 (`version.rs:603`) | Uses the delegate action's own `receiver_id` when validating a nested state init; the buggy path validated against the outer receiver (`action_validation.rs:250`). |
| `FixDelegateActionDepositWithFunctionCallError` | v85 (`version.rs:601`) | When a delegate action's inner `FunctionCall` carries a deposit under a FunctionCall-permission key, the error is now `DepositWithFunctionCall` and the check returns immediately; pre-fix the missing early return let the receiver/method checks overwrite it with `ReceiverMismatch`/`MethodNameMismatch` (`actions.rs:689`). Same permission rules as §9.3, applied to the inner action. |
| `DelegateV2` | v85 (`version.rs:617`) | Introduced `Action::DelegateV2` (meta transactions able to carry a `TransactionNonce`, hence gas keys). Superseded in the same release cycle by `RejectDelegateV2` at v87, so on a v87 chain it exists in the type system but is never admissible in a new receipt. |

### Gates that folded into the baseline this release

`MIN_SUPPORTED_PROTOCOL_VERSION` rose 83 → 84, so these are now **unconditional** and the code has no branch left:

| Former feature | Was | Now |
|---|---|---|
| `_DeprecatedEthImplicitGlobalContract` | v83 (`version.rs:598`) | ETH-implicit creation *always* uses a `Global` wallet contract addressed by `eth_wallet_global_contract_hash(chain_id)` (`actions.rs:255`). The legacy `near[hash]` embedded-WASM path is gone. |
| `_DeprecatedFixAccessKeyAllowanceCharging` | v83 (`version.rs:593`) | The allowance is **never** mutated in place before later checks can fail. `verify_and_charge_tx_ephemeral` computes the new allowance and returns it in the verdict (`verifier.rs:365`,`:402`); the runtime applies it only on success (`runtime/runtime/src/lib.rs:350`). |
| `_DeprecatedDeterministicAccountIds` | v82 (`version.rs:590`) | The `0s…` `NearDeterministicAccount` type and `create_deterministic_account` are part of the base protocol. (The runtime flag gating them is still `eth_implicit_accounts`, `actions.rs:911`.) |
| `_DeprecatedGlobalContracts` | v77 (`version.rs:580`) | `AccountContract::Global`/`GlobalByAccount` — hence `InitializedAccount::V2` — are base protocol. |
| `_DeprecatedZeroBalanceAccount` | v59 (`version.rs:546`) | NEP-448 zero-balance accounts are base protocol. |

Account layout versioning itself has no live flag — the historical `_DeprecatedAccountVersions` was v46 (`version.rs:518`); V1/V2 coexist purely as a serialization concern, and the new `Uninitialized` arm is gated by `UniversalAccounts` at the creation sites rather than by a serialization flag.

## Invariants & failure modes

- **One contract kind per account**: an account cannot hold both a local and a global contract, nor two global forms — enforced structurally by `AccountContract` and at serde deserialization (`account.rs:625`,`:632`).
- **An uninitialized account owns nothing**: no contract, no access keys, no data, no stake. Enforced at the action gate (`AccountNotInitialized`, `actions.rs:883`), at the setters (`InvalidAccountState`, `account.rs:435`,`:447`), at serde decode (`account.rs:674`), and in genesis validation (`core/chain-configs/src/genesis_validate.rs:160`). Because no receipt can name such an account as its actor, `InvalidAccountState` reaching a call site means corrupt state, and is reported as `StorageInconsistentState` (`actions.rs:48`).
- **An uninitialized account's bootstrap nonce is mandatory and one-shot**: it is required by serde (`account.rs:690`), consumed on the account even when the init fails (`runtime/runtime/src/lib.rs:367`), seeded from the creation height so a re-created account starts above every nonce its previous incarnation could sign for (`account.rs:221` doc), and dropped by `initialize` once real keys exist (`:330`).
- **Installed keys start above the consumed bootstrap nonce**: `max(initial_nonce_value(height), consumed_nonce)` (`runtime/runtime/src/universal_account_id.rs:67`), or the bootstrap bytes replay through the access-key path.
- **A `0u` id is exactly SHA3-256 of its state-init bytes**: checked at action validation (`InvalidUniversalStateInitReceiver`, `action_validation.rs:550`) and again, structurally, by the bootstrap shape check (`transaction.rs:249`). Non-canonical borsh is *not* rejected — it simply addresses a different account (`universal_state_init.rs:116` doc).
- **Universal account creation cannot be hijacked**: neither `action_implicit_account_creation_transfer` nor `action_universal_state_init` claims `actor_id` (`actions.rs:213`, `universal_account_id.rs:43`), which is what makes batched creation safe.
- **Gas-key on-key nonce is always 0**: forced by `add_gas_key` (`access_keys.rs:206`) and asserted at the store boundary (`core/store/src/utils/mod.rs:378`).
- **Gas-key deletion burns ≤ 1 NEAR**: `delete_gas_key` errors `GasKeyBalanceTooHigh` if `balance > MAX_BALANCE_TO_BURN`; otherwise the balance is burned, not refunded (`access_keys.rs:103`,`:112`). Account deletion sums all gas-key balances against the same threshold (`actions.rs:370`).
- **Account deletion size cap**: `MAX_ACCOUNT_DELETION_STORAGE_USAGE = 10_000` bytes (`account.rs:248`); larger accounts (after subtracting contract storage) cannot be deleted → `DeleteAccountWithLargeState` (`actions.rs:364`).
- **Storage stake backs storage usage** unless zero-balance: `check_storage_stake` (`verifier.rs:48`); violation → `LackBalanceForStorageStaking`/`LackBalanceForState`. Arithmetic overflow in `storage_amount_per_byte * storage_usage` or `amount + locked` (`:56`,`:64`) returns `StorageStakingError::StorageError`, surfaced as `StorageInconsistentState`.
- **Nonce monotonicity prevents replay**: `verify_nonce` rejects stale/equal (Monotonic) or non-sequential (Strict) nonces and enforces the `height * 1e6` ceiling (`verifier.rs:254`).
- **Key-tag spaces never cross**: a `PublicKey` cannot decode borsh tag 3 and a `PublicKeyHandle` cannot decode tag 2 (`core/crypto/src/signature.rs:423`,`:692`). Base58 decoding of an `ml-dsa-65-hash:` handle is bounded by a fixed 32-byte destination buffer, so an over-long encoding errors `BadLength` rather than allocating (`decode_bs58`, `:1281`, used at `:638`) (#16200).
- **Storage-usage overflow** while adding a key or installing a state init is fatal: `StorageInconsistentState` (`access_keys.rs:220`,`:247`) / `IntegerOverflowError` (`universal_account_id.rs:129`,`:147`).
- **Permission mismatch on a FunctionCall key** rejects the whole tx at verification (`RequiresFullAccess`/`DepositWithFunctionCall`/`ReceiverMismatch`/`MethodNameMismatch`/`NotEnoughAllowance`, `verifier.rs:209`,`:282`).

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `core/primitives-core/src/account.rs:81` | `Account` | Uninitialized/Initialized enum; version-erasing accessors. |
| `core/primitives-core/src/account.rs:89` | `InitializedAccount` | V1/V2 layout versions. |
| `core/primitives-core/src/account.rs:221` | `UninitializedAccountV1` | Funded `0u` account with only amount, storage usage, bootstrap nonce. |
| `core/primitives-core/src/account.rs:42` | `AccountState` | Initialized / Uninitialized, exposed to views. |
| `core/primitives-core/src/account.rs:63` | `InvalidAccountState` | Error for changes that do not fit the account state. |
| `core/primitives-core/src/account.rs:254` | `Account::new` | Picks min layout version per contract mode. |
| `core/primitives-core/src/account.rs:283` | `Account::new_uninitialized` | Creates the funded-but-uninstalled `0u` account. |
| `core/primitives-core/src/account.rs:330` | `Account::initialize` | Uninitialized → Initialized(V1); drops the bootstrap nonce. |
| `core/primitives-core/src/account.rs:139` | `AccountContract` | None/Local/Global/GlobalByAccount + per-mode storage cost (`:182`). |
| `core/primitives-core/src/account.rs:731` | `BorshVersionedAccount` | Sentinel-tagged V2 (=0) and Uninitialized (=1) borsh forms. |
| `core/primitives-core/src/account.rs:737` | `BorshDeserialize for Account` | Sentinel-based V1 / versioned detection. |
| `core/primitives-core/src/account.rs:674` | `uninitialized_account_from_serde` | Rejects locked balance, contract, or missing bootstrap nonce. |
| `core/primitives-core/src/account.rs:801` | `AccessKey` | nonce + permission; `ACCESS_KEY_NONCE_RANGE_MULTIPLIER` (`:812`). |
| `core/primitives-core/src/account.rs:909` | `AccessKeyPermission` | Full/FunctionCall/GasKey variants; `MAX_NONCES_FOR_GAS_KEY` (`:923`). |
| `core/primitives-core/src/account.rs:880` | `GasKeyInfo` | balance + num_nonces; `MAX_BALANCE_TO_BURN` (`:888`). |
| `core/primitives-core/src/universal_account_id.rs:30` | `encode_universal_account_id` | `0u` + 52 Crockford-base32 symbols; no checksum. |
| `core/primitives-core/src/universal_account_id.rs:44` | `base32_encode` | MSB-first 5-bit packing; final symbol carries 4 pad bits. |
| `core/primitives-core/src/universal_state_init.rs:44` | `RawStateInit` | The exact bytes the account id hashes. |
| `core/primitives/src/universal_state_init.rs:51` | `UniversalStateInit` | Typed V1: code / data / access_keys. |
| `core/primitives/src/universal_state_init.rs:116` | `UniversalStateInit::from_raw` | Accepts non-canonical borsh; rejects malformed/trailing bytes. |
| `core/primitives/src/universal_state_init.rs:140` | `state_init_counts` | Pricing counts; a payload that fails to decode counts as 0 entries/keys. |
| `core/primitives/src/utils.rs:502` | `derive_universal_account_id` | SHA3-256 over `RawStateInit`, then the UAID codec. |
| `core/primitives/src/action/mod.rs:244` | `UniversalStateInitAction` | `{ state_init, deposit }`, action discriminant 15 (`:396`). |
| `core/primitives/src/transaction.rs:66` | `TransactionNonce` | Nonce vs GasKeyNonce; `nonce_index()` routing (`:89`). |
| `core/primitives/src/transaction.rs:113` | `NonceMode` | Monotonic vs Strict. |
| `core/primitives/src/transaction.rs:236` | `Transaction::state_init_bootstrap` | Stateless half of the self-signed state-init check. |
| `core/crypto/src/signature.rs:353` | `KeyTag` | Disjoint borsh tag spaces for `PublicKey` (0,1,2) and `PublicKeyHandle` (0,1,3). |
| `core/crypto/src/signature.rs:633` | `FromStr for PublicKeyHandle` | `ml-dsa-65-hash:` form; bounded base58 decode. |
| `core/crypto/src/signature.rs:332` | `PublicKey::trie_id_len` | On-trie identifier length (33 bytes for ML-DSA-65). |
| `runtime/runtime/src/access_keys.rs:149` | `action_add_key` | Add regular/gas key; storage accounting. |
| `runtime/runtime/src/access_keys.rs:52` | `action_delete_key` | Delete regular/gas key; balance burn. |
| `runtime/runtime/src/access_keys.rs:257` | `action_transfer_to_gas_key` | Funds gas-key balance. |
| `runtime/runtime/src/access_keys.rs:290` | `action_withdraw_from_gas_key` | Moves gas-key balance to account. |
| `runtime/runtime/src/access_keys.rs:46` | `initial_nonce_value` | `(block_height-1)*1e6` nonce seed. |
| `runtime/runtime/src/verifier.rs:48` | `check_storage_stake` | Storage-staking invariant + zero-balance exemption. |
| `runtime/runtime/src/verifier.rs:140` | `TxAuthorization` | AccessKey / GasKey / SelfSignedStateInit. |
| `runtime/runtime/src/verifier.rs:164` | `get_signer_and_authorization` | Resolves signer + which of the three authorizes. |
| `runtime/runtime/src/verifier.rs:200` | `is_bootstrap` | Uninitialized account + bootstrap-shaped transaction. |
| `runtime/runtime/src/verifier.rs:209` | `verify_function_call_permission` | FunctionCall key constraints. |
| `runtime/runtime/src/verifier.rs:254` | `verify_nonce` | Monotonic/Strict nonce + upper bound. |
| `runtime/runtime/src/verifier.rs:312` | `verify_and_charge_tx_ephemeral` | Regular-tx authorization + charge; no in-place mutation. |
| `runtime/runtime/src/verifier.rs:424` | `verify_and_charge_bootstrap_tx_ephemeral` | Self-signed state init; forced Strict nonce. |
| `runtime/runtime/src/verifier.rs:524` | `verify_and_charge_gas_key_tx_ephemeral` | Gas-key authorization; gas/deposit split; DepositFailed. |
| `runtime/runtime/src/actions.rs:48` | `OrInconsistentState` | Maps `InvalidAccountState` to `StorageInconsistentState`. |
| `runtime/runtime/src/actions.rs:166` | `action_create_account` | Named/top-level/sub-account creation rules. |
| `runtime/runtime/src/actions.rs:221` | `action_implicit_account_creation_transfer` | Near/Eth/Deterministic/Universal implicit creation. |
| `runtime/runtime/src/actions.rs:330` | `action_delete_account` | Storage-cap + gas-key-burn checks on deletion. |
| `runtime/runtime/src/actions.rs:814` | `check_account_existence` | Per-action rules incl. `AccountNotInitialized`. |
| `runtime/runtime/src/actions.rs:899` | `get_account_type` | Config-aware typing; downgrades a disabled type to Named. |
| `runtime/runtime/src/actions.rs:929` | `implicit_creation_allowed` | Universal ids may be created by a batched transfer. |
| `runtime/runtime/src/universal_account_id.rs:21` | `action_universal_state_init` | Create/initialize a `0u` account, then settle the deposit. |
| `runtime/runtime/src/universal_account_id.rs:102` | `install_universal_account` | Writes code, data, and full-access keys; sums storage usage. |
| `runtime/runtime/src/action_validation.rs:45` | `validate_number_of_state_init_entries` | Per-receipt entry cap, NewReceipt mode only. |
| `runtime/runtime/src/action_validation.rs:364` | `validate_add_key_action` | Gas-key `num_nonces`, zero-balance, no-allowance rules. |
| `runtime/runtime/src/action_validation.rs:535` | `validate_universal_state_init` | Feature gate, derived-id check, decode, key/entry limits. |
| `runtime/runtime/src/deterministic_account_id.rs:73` | `settle_state_init_deposit` | Shared deposit→storage-stake settlement + refund. |
| `runtime/runtime/src/deterministic_account_id.rs:117` | `create_deterministic_account` | Bare `0s…` account record. |
| `runtime/runtime/src/config.rs:73` | `gas_key_send_pk_len` | `FixMlDsaCostCharging`: send fee on wire length. |
| `runtime/near-vm-runner/src/logic/logic.rs:194` | `gas_key_exec_pk_len` | `FixMlDsaCostCharging`: exec fee on trie id length. |
| `runtime/runtime/src/state_viewer/mod.rs:185` | `TrieViewer::view_access_keys` | Paginated access-key listing; skips gas-key nonce blocks. |
| `core/store/src/utils/mod.rs:372` | `set_access_key_by_handle` | Asserts gas-key access keys carry nonce 0. |
| `core/chain-configs/src/genesis_validate.rs:57` | `RecordValidator::process_record` | Accepts uninitialized accounts in genesis records. |
| `core/primitives/src/views.rs:80` | `AccountView` | Documented fields + `state` / `bootstrap_nonce`. |
| `core/primitives/src/views.rs:334` | `AccessKeyList` | `last_key` pagination cursor. |
| `core/parameters/src/cost.rs:775` | `transfer_exec_fee` | Per-account-type transfer fee incl. the `0u` arm. |
| `near-account-id` 3.0.0 `src/validation.rs:43` | `validation::validate` | Account-id charset / length / separator rules. |
| `near-account-id` 3.0.0 `src/validation.rs:117` | `is_universal` | `0u` classification: length 54, prefix, Crockford alphabet, zero padding bits. |
| `near-account-id` 3.0.0 `src/account_id_ref.rs:246` | `AccountIdRef::get_account_type` | Four implicit predicates in order, else `NamedAccount`. |
| `near-account-id` 3.0.0 `src/account_id_ref.rs:59` | `AccountType::is_implicit` | True for all four non-`Named` variants. |
| `core/primitives-core/src/version.rs:500` | `ProtocolFeature::protocol_version` | Activation versions for gated features. |

## Open questions

- The exact gas/cost-pricing changes introduced by `AccountCostIncrease` (v85) and the `universal_state_init_*` fee terms (`core/parameters/src/cost.rs:958`) are only referenced here; the pricing arithmetic and the `promise_batch_action_universal_state_init` host-function charging fix (#16084, `runtime/near-vm-runner/src/logic/logic.rs`) belong to [runtime-execution](runtime-execution.md) and were not re-derived.
- The full `DeterministicAccountStateInit` apply path (how `code`/`data` are written and charged for `0s…` accounts) is owned by [runtime-execution](runtime-execution.md); only the account-record creation and the shared deposit settlement are documented here.

*(Resolved at verification: the `near-account-id` 3.0.0 source was previously unavailable locally, so the account-id validity rules, the `0u` classification predicate and `AccountType::is_implicit()` were inferred. The crate was fetched from crates.io and its sha256 confirmed against `Cargo.lock` (`7d2c8642…d10`); all three are now read from source and cited above.)*
