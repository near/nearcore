# Post-Quantum Signatures (ML-DSA-65)

This document captures the load-bearing design decisions and known caveats of
the post-quantum transaction-signature integration introduced under the
`PostQuantumSignatures` protocol feature. It does not document every code
change; for that, read the protocol-feature commit and the cross-references
below.

## Summary

NEAR supports a third transaction-signature scheme - FIPS 204 **ML-DSA-65** -
alongside the existing `ed25519` and `secp256k1` schemes. Existing schemes are
NOT deprecated. ML-DSA-65 is accepted in transactions and as access keys after
the `PostQuantumSignatures` protocol feature activates (nightly version 154 at
time of writing).

Scope of the integration: transaction signing and access keys only. Validator
keys, block/chunk signatures, host-function verification primitives, and
implicit-account derivation remain unchanged and are out of scope for this
feature.

## Key design decisions

### 1. Algorithm and library

- **ML-DSA-65** is the medium of the three FIPS 204 variants (44 / 65 / 87).
  NIST Security Category 3 (~192-bit classical, ~96-bit Grover-equivalent).
  Sizes: pubkey 1952 B, signature 3309 B, raw private key 4032 B.
- Implementation: `aws-lc-rs::unstable::signature::ML_DSA_65`, backed by
  `aws-lc-sys` (AWS-LC). Library was Trail-of-Bits–recommended; per-meeting
  briefing, 2026-05-06.
- The `unstable` cargo feature is required to expose the PQDSA APIs; the gate
  reflects that AWS-LC may evolve these APIs. Pinning a version range is
  acceptable for the initial integration; rebench on every version bump.

### 2. Protocol gating

A single `ProtocolFeature::PostQuantumSignatures` gate controls:

- Whether ML-DSA-65 is permitted as a transaction signer
  (`core/primitives/src/transaction.rs::check_valid_for_config`).
- Whether `AddKey` may register an ML-DSA-65 access key
  (`runtime/runtime/src/verifier.rs::validate_add_key_action`).
- Whether a `DelegateAction` may carry an ML-DSA-65 inner signer
  (`runtime/runtime/src/verifier.rs::validate_delegate_action`).

Borsh deserialization of `PublicKey::MLDSA65` and `Signature::MLDSA65` is
**not** gated - once a value enters state it must always parse - but the
action-validation gates ensure no ML-DSA-65 value can enter state on a
pre-feature protocol. Practically that means: when `PostQuantumSignatures`
activates, no ML-DSA-65 access keys exist in state yet, so the runtime
doesn't have to handle a mixed population of "legacy keys we tolerated
before the gate existed" and "keys added under the new rules" - every
ML-DSA-65 key in state was added under the new rules.

### 3. `PublicKey` enum extension and `KeyHandle` split

`PublicKey` gains exactly one new variant - the full pubkey - and uses borsh
tag `2`:

- `PublicKey::MLDSA65(MlDsa65PublicKey)` - full 1952-byte public key. Carried
  in transactions, actions, and any wire format. Used for signature
  verification. Reports `key_type() == KeyType::MLDSA65`.

The hash form does **not** live on `PublicKey`. It lives on a separate
`KeyHandle` enum, which is the type used by the trie-key layer and view-API
responses:

- `KeyHandle::ED25519(ED25519PublicKey)` - borsh tag `0`, byte-identical to
  `PublicKey::ED25519`'s encoding.
- `KeyHandle::SECP256K1(Secp256K1PublicKey)` - borsh tag `1`, byte-identical
  to `PublicKey::SECP256K1`'s encoding.
- `KeyHandle::MlDsa65Hash(MlDsa65PublicKeyHash)` - borsh tag `3`, 48-byte
  SHA3-384 digest. Appears only as a result of parsing a trie key (e.g.
  `view_access_key_list`) or constructed via `From<&PublicKey>` from a known
  full ML-DSA-65 pubkey. Cannot sign, cannot verify, never appears in
  transactions or actions.

The tag spaces are deliberately disjoint across the two types: `PublicKey`
owns `{0, 1, 2}`, `KeyHandle` owns `{0, 1, 3}`. Tag `2` (full ML-DSA-65 key)
is reserved on `PublicKey` and is by construction never written into the
trie; tag `3` (hash) is reserved on `KeyHandle` and never appears on the
wire. This makes "a full ML-DSA-65 key in the trie" and "an ML-DSA-65 hash
in a transaction" both unrepresentable at the type level.

Boxed inner storage (`Box<[u8; 1952]>`, `Box<[u8; 3309]>`, `Box<[u8; 4032]>`)
is mandatory: an inline-sized variant would inflate every `PublicKey` /
`Signature` / `SecretKey` to ~2 KiB everywhere it is held.

### 4. Trie-storage hashing

ML-DSA-65 access keys are stored **by hash, not by pubkey**. The trie-key
encoding for an `AccessKey` entry is:

| variant    | encoding (after column + account id)                                 |
|------------|----------------------------------------------------------------------|
| ED25519    | `[tag=0] \|\| 32-byte raw pubkey`                                    |
| SECP256K1  | `[tag=1] \|\| 64-byte raw pubkey`                                    |
| ML-DSA-65  | `[tag=3] \|\| sha3_384(domain_tag \|\| raw_pubkey)` (49 bytes total) |

Domain tag: `b"near:ml-dsa-65-pubkey-hash:v1"`, hashed before the pubkey
bytes. Prevents collisions with other SHA-3 uses in the protocol.

Choice of SHA3-384 (not SHA3-256): an attacker forging an access key needs
second-preimage on the hash. Grover gives `O(2^(H/2))` quantum cost; to match
ML-DSA-65's Category-3 (~192-bit) security level, the hash needs `H ≥ 384`
bits. SHA3-256 would leave the access-key lookup at Category 2, one level
under the keypair.

Storage impact: an ML-DSA-65 access key occupies 49 bytes in the trie key
portion versus 1953 if stored raw - about a 96% reduction. Storage stake
drops from ~0.0195 NEAR to ~0.0005 NEAR per key.

UX consequence: `view_access_key_list` returns `ml-dsa-65-hash:<bs58>` for
ML-DSA-65 entries. Wallets and indexers must know their own pubkey to match
chain entries to known keys. This matches the model already in use for
ETH-style implicit accounts.

Migration is zero, because the trie cannot contain any ML-DSA-65 entry on a
pre-feature protocol - the action-validation gates prevent it.

### 5. Storage usage and fee plumbing

The storage-stake calculation
(`runtime/runtime/src/access_keys.rs::access_key_storage_usage`) and the
gas-key fee helpers (`gas_key_*_fee` in `runtime/runtime/src/config.rs`) use
`PublicKey::trie_id_len()` rather than `PublicKey::len()`:

- `len()` reports the borsh-encoded length (33 / 65 / 1953 across the
  three `PublicKey` variants).
- `trie_id_len()` reports the on-trie length (33 / 65 / **49**).

The two diverge only for `PublicKey::MLDSA65`. Every storage-stake and
trie-byte-priced fee path was updated to call `trie_id_len()`.

### 6. Variable-time verify

ML-DSA verify is variable-time (rejection sampling on signature generation
sets up patterns that show as small variance on verify). For the initial
integration:

- Concrete verify-time distribution: **to be checked with benchmarks**
  (benches live in a separate commit on top of this one).
- Adversarial-input analysis: it has been concluded that no
  maliciously-crafted signature can materially blow up verification time
  beyond the natural worst case.
- Tx-level verify gas constant: not yet set. See unresolved issues below.

### 7. Out-of-scope (and why)

- **Validator / staking keys** remain ed25519-only. `is_valid_staking_key`
  (`core/crypto/src/key_conversion.rs`) returns false for any ML-DSA-65
  variant.
- **Implicit accounts** (NEAR-style and ETH-style) unchanged. PQ implicit
  accounts are a separate research item.
- **Host functions / on-chain verification primitives**: no `ml_dsa_verify`
  host function added. Contracts wanting to verify PQ signatures will
  continue using ed25519 host fn until a separate proposal lands.
- **Rosetta / Mesh RPC** (`chain/rosetta-rpc/src/models.rs`): integration
  deferred until upstream Mesh spec is extended. The upstream
  [Mesh spec](https://github.com/coinbase/mesh-specifications/blob/master/models/CurveType.yaml)
  currently defines a closed set of six classical curves (`secp256k1`,
  `secp256k1_bip340`, `secp256r1`, `edwards25519`, `tweedle`, `pallas`) and
  has no post-quantum variant. The team has agreed to open a PR against the
  Mesh specification to add a post-quantum entry; the NEAR-side
  implementation will follow once that change is merged upstream. Until
  then, the `From<KeyType> for CurveType` arm for `MLDSA65` is a temporary
  `unreachable!()` - see the unresolved-issues section below.
- **Cross-network mirror** (`tools/mirror/src/key_mapping.rs`) panics on
  ML-DSA-65 pubkey mapping; deterministic-derivation scheme for PQ keys is a
  follow-up.
- **ETH wallet contract** (`integration-tests/src/tests/features/wallet_contract.rs`)
  marks ML-DSA-65 unreachable in the AddKey selector path - wallet contracts
  are explicitly ed25519/secp256k1.

## Caveats

1. **Borsh tag 2 is irreversibly assigned to `PublicKey::MLDSA65`** and
   tag 3 to `KeyHandle::MlDsa65Hash`. Once any block on a network has
   accepted an ML-DSA-65 transaction or AddKey, the tag-2 → full-pubkey and
   tag-3 → hash decodings are part of the chain's history.

2. **`PublicKey::trie_id_len()` is a new contract** that all
   storage-cost code must respect. Callers that still use `len()` for
   trie-storage costing will misprice ML-DSA-65 keys by ~1900 bytes.

3. **The `unstable` cargo feature on `aws-lc-rs`.** Reaching for ML-DSA APIs
   requires it. We don't know the API stability commitment; rebench on each
   version bump and consider pinning.

4. **Variable-time verify**: bounded but real. Gas pricing must cover the
   worst-case observed verify, not the average.

5. **`view_access_key_list` returns hashes, not pubkeys**, for ML-DSA-65
   entries. Public-facing wallets/explorers need to know their own pubkey to
   reconcile.

6. **`Bs58` helper now allocates** (was stack-bounded to 96 bytes via
   `debug_assert!`). ML-DSA-65 pubkeys/signatures exceed that bound by
   ~20×; the dynamic allocation is necessary. ed25519/secp256k1 display
   paths still hit the heap once each but the impact is negligible.

7. **The hash form is structurally separated from the wire form.** The
   trie returns a `KeyHandle::MlDsa65Hash`, which is a distinct type from
   `PublicKey`; `Signature::verify` cannot even be expressed against a
   `KeyHandle`. Code paths that fetch an access key from state and then
   want to verify a signature against it must obtain the full pubkey from
   the wire (the transaction or action carries it).

8. **Schema hash cascade.** Adding the new variants to `PublicKey` and
   `Signature` changes their `ProtocolSchema` hashes, which cascades to
   every containing type (`Transaction`, `Action`, `Receipt`, etc.). Schema
   check must be updated at stabilization time.

## Unresolved issues

The items below split into two groups: **must-resolve before
`PostQuantumSignatures` activates on any network**, and the remaining
items the team should resolve before stabilizing in 2.13.

### Must-resolve before activation

1. **Rosetta-RPC ML-DSA-65 representation - depends on upstream Mesh
   spec.** The current `From<KeyType> for CurveType` arm for `MLDSA65` in
   `chain/rosetta-rpc/src/models.rs` is `unreachable!()` and *will* panic
   the server. It is reachable from three production endpoints:

   - `POST /block` - panics on any block containing an ML-DSA-65
     `AddKey`, `DeleteKey`, `Stake`, `TransferToGasKey`,
     `WithdrawFromGasKey`, or `Delegate` action. This is the high-volume
     endpoint exchanges and indexers poll on every new block; one PQ-signed
     tx ⇒ every Rosetta consumer crashes on that block.
   - `POST /block/transaction` - same code path (the implementation fetches
     the whole block, then filters). Even fetching an unrelated ed25519 tx
     from the same block triggers the panic.
   - `POST /construction/parse` - panics if a client posts a tx blob with
     an ML-DSA-65 action. Reachable as a deliberate DoS vector for anyone
     with construction access.

   **Resolution path.** The team has agreed to open a PR against the
   upstream Mesh specification (Coinbase mesh-specifications repo) to add a
   post-quantum entry to the `CurveType` enum. The NEAR-side implementation
   in `chain/rosetta-rpc/` will be done once the upstream change is
   accepted and tagged. **The `PostQuantumSignatures` protocol feature
   must not activate on any network before that work has landed.**

### Before stabilization in 2.13

2. **Economic-impact audit & pricing.** Not started. The strict
   requirement from the briefing - storage and compute priced correctly -
   is partially covered: per-key storage stake now scales correctly via
   `trie_id_len()`. Outstanding work:
   - Per-byte component on `AddKey` and `DeleteKey` fees.
   - Tx-level `tx_signature_verify_ml_dsa_65` gas constant. Provisional 10
     Ggas based on a ~10× safety margin over the empirical worst case
     (1.23 ms at 1 Tgas/s); should be tightened after Phase 5.4 calibration.
   - New `parameters.yaml` diff file gated on `PostQuantumSignatures`.
   - Snapshot regeneration.

3. **Wallet/SDK story for the view-RPC change.** Add the
   `near-api-js`/`near-cli-rs` side of the change, document the hash format,
   and decide whether to expose a separate `view_access_key_hash` endpoint
   or keep everything inside `public_key` field with the new prefix.

4. **Domain-tag export.** `ML_DSA_65_HASH_DOMAIN_TAG` is currently private
   inside `core/crypto/src/signature.rs`. Wallets that want to compute the
   on-chain hash of their own pubkey need this - either publish the constant
   or provide a small public helper (`MlDsa65PublicKey::hash()` is already
   public but the underlying domain tag is not).

5. **Test-loop integration tests.** Phase 3.5 of the original implementation
    plan called for full upgrade-and-use tests; deferred. Recommended before
    stabilization to catch protocol-boundary regressions.

6. **Mirror / fork-network cross-network support for PQ keys.** Behavior is
    currently inconsistent across entry points and must be unified before any
    serious mirroring of a PQ-bearing chain:
    - `tools/mirror/src/genesis.rs` panics loudly when it encounters an
      ML-DSA-65 access-key or gas-key-nonce record.
    - `tools/mirror/src/key_util.rs`, `tools/mirror/src/{offline,online}.rs`,
      and `tools/fork-network/src/cli.rs` silently `filter_map` / `continue`
      past hash-form entries with no log or warning, so a real ML-DSA-bearing
      chain would be silently mirrored with access keys missing.
    Proper support is deferred to a follow-up - it likely needs a
    deterministic seed-derived mapping (analogous to ed25519/secp256k1
    mapping in `key_mapping.rs`). For now the design accepts that PQ keys
    cannot be mirrored, and the inconsistency above should at minimum be
    unified to "log + skip" so failures are visible.

7. **Implicit-account derivation for PQ keys.** Briefing's open question
    (1). Not part of this feature, but the access-key hashing here has set
    the precedent (SHA3-384, domain-separated) that implicit-account
    derivation should probably follow.
