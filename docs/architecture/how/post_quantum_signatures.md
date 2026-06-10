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
the `PostQuantumSignatures` protocol feature activates (stabilized at protocol
version 85).

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

### 3. `PublicKey` enum extension and `PublicKeyHandle` split

`PublicKey` gains exactly one new variant - the full pubkey - and uses borsh
tag `2`:

- `PublicKey::MLDSA65(MlDsa65PublicKey)` - full 1952-byte public key. Carried
  in transactions, actions, and any wire format. Used for signature
  verification. Reports `key_type() == KeyType::MLDSA65`.

The hash form does **not** live on `PublicKey`. It lives on a separate
`PublicKeyHandle` enum, which is the type used by the trie-key layer and view-API
responses:

- `PublicKeyHandle::ED25519(ED25519PublicKey)` - borsh tag `0`, byte-identical to
  `PublicKey::ED25519`'s encoding.
- `PublicKeyHandle::SECP256K1(Secp256K1PublicKey)` - borsh tag `1`, byte-identical
  to `PublicKey::SECP256K1`'s encoding.
- `PublicKeyHandle::MlDsa65Hash(MlDsa65PublicKeyHandle)` - borsh tag `3`, 32-byte
  SHA3-256 digest. Appears only as a result of parsing a trie key (e.g.
  `view_access_key_list`) or constructed via `From<&PublicKey>` from a known
  full ML-DSA-65 pubkey. Cannot sign, cannot verify, never appears in
  transactions or actions.

The tag spaces are deliberately disjoint across the two types: `PublicKey`
owns `{0, 1, 2}`, `PublicKeyHandle` owns `{0, 1, 3}`. Tag `2` (full ML-DSA-65 key)
is reserved on `PublicKey` and is by construction never written into the
trie; tag `3` (hash) is reserved on `PublicKeyHandle` and never appears on the
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
| ML-DSA-65  | `[tag=3] \|\| sha3_256(domain_tag \|\| raw_pubkey)` (33 bytes total) |

Domain tag: `b"near:ml-dsa-65-pubkey-hash:v1"`, hashed before the pubkey
bytes. Prevents collisions with other SHA-3 uses in the protocol.

Choice of SHA3-256: a 256-bit digest has been decided to be secure enough for
the access-key lookup. The decisive property is that the digest is short enough
to be encoded into a NEAR account id, so the hash can be used directly as an
(implicit) account id. This keeps the door open to creating ML-DSA-65 access
keys for implicit accounts in the future from the account id alone - without
having to carry or store the full 1952-byte pubkey.

Storage impact: an ML-DSA-65 access key occupies 33 bytes in the trie key
portion versus 1953 if stored raw - about a 98% reduction. Storage stake
drops from ~0.0200 NEAR (if stored raw) to ~0.00082 NEAR per full-access key -
identical to an ed25519 key (same 33-byte trie id), a ~24x reduction once the
fixed AccessKey body and per-record overhead (present in both forms) are
counted.

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
- `trie_id_len()` reports the on-trie length (33 / 65 / **33**).

The two diverge only for `PublicKey::MLDSA65`. Every storage-stake and
trie-byte-priced fee path was updated to call `trie_id_len()`.

### 6. Variable-time verify

ML-DSA verify is variable-time (rejection sampling on signature generation
sets up patterns that show as small variance on verify). For the initial
integration:

- Concrete verify-time distribution from benches at
  `core/crypto/benches/{signatures.rs, verify_distribution.rs,
  ml_dsa_worst_case.rs}`: mean ≈ 80 µs (vs ~32 µs ed25519); the tail
  (p99.9 ≈ 170 µs, p99.99+ up to ~1.3 ms) is OS scheduling noise, not
  signature content - **verified by benchmark**: verify time is flat across
  all signature hint-weights (and a single fixed signature reproduces the
  whole distribution), so no maliciously-crafted signature can blow up
  verification time. Measured single-core on an Intel Core Ultra 9 185H.
- Pricing: charged as **gas at transaction conversion** via the
  `ml_dsa_65_verification_cost` runtime parameter (100 Ggas - the extra
  verification cost of ML-DSA-65 over the classical schemes: ~2x the measured
  ~50 µs mean difference at the 1 Tgas/s calibration target, leaving tail
  headroom). The charge is added to the transaction's *burnt* gas (per
  signature, including each `Delegate` action's inner signer), so it raises
  what the signer pays to buy the transaction but never the gas available to
  the resulting receipts; in-contract cross-contract calls are unaffected. See
  the verification-pricing note under "Resolved during integration" for the
  full mechanism.

### 7. Out-of-scope (and why)

- **Validator / staking keys** remain ed25519-only. `is_valid_staking_key`
  (`core/crypto/src/key_conversion.rs`) returns false for any ML-DSA-65
  variant.
- **Implicit accounts** (NEAR-style and ETH-style) unchanged. PQ implicit
  accounts are a separate research item.
- **Host functions / on-chain verification primitives**: no `ml_dsa_verify`
  host function added. Contracts wanting to verify PQ signatures will
  continue using ed25519 host fn until a separate proposal lands.
- **Cross-network mirror** (`tools/mirror/src/key_mapping.rs`) panics on
  ML-DSA-65 pubkey mapping; deterministic-derivation scheme for PQ keys is a
  follow-up.
- **ETH wallet contract** (`integration-tests/src/tests/features/wallet_contract.rs`)
  marks ML-DSA-65 unreachable in the AddKey selector path - wallet contracts
  are explicitly ed25519/secp256k1.

## Caveats

1. **Borsh tag 2 is irreversibly assigned to `PublicKey::MLDSA65`** and
   tag 3 to `PublicKeyHandle::MlDsa65Hash`. Once any block on a network has
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
   trie returns a `PublicKeyHandle::MlDsa65Hash`, which is a distinct type from
   `PublicKey`; `Signature::verify` cannot even be expressed against a
   `PublicKeyHandle`. Code paths that fetch an access key from state and then
   want to verify a signature against it must obtain the full pubkey from
   the wire (the transaction or action carries it).

8. **Schema hash cascade.** Adding the new variants to `PublicKey` and
   `Signature` changed their `ProtocolSchema` hashes, which cascaded to
   every containing type (`Transaction`, `Action`, `Receipt`, etc.).
   `res/protocol_schema.toml` was regenerated when the variants were
   introduced; stabilization changes only protocol-version numbers, not
   type structure, so it needed no further schema update (the schema check
   reports no changes).

## Resolved during integration

These were tracked as open during development and are now done; recorded here
so the history is legible:

- **Rosetta-RPC ML-DSA-65 representation.** `chain/rosetta-rpc/src/models.rs`
  now carries `CurveType::MlDsa65` and `SignatureType::MlDsa65`, with the
  `From<KeyType> for CurveType`, `TryFrom<&PublicKey>`, and `SignatureType`
  conversions all implemented (no `unreachable!()` / panic) and unit-tested.
  The upstream Mesh `CurveType` addition landed as
  [mesh-specifications#129](https://github.com/coinbase/mesh-specifications/pull/129);
  Mesh clients that haven't picked it up yet may reject the new value, which
  is accepted.
- **Verification pricing.** Signature verification is charged as gas at
  transaction conversion via `signature_verification_costs:
  EnumMap<SignatureKind, ParameterCost>` on `RuntimeFeesConfig`, fed by the
  `ml_dsa_65_verification_cost` parameter (§6). ed25519/secp256k1 stay 0 for
  backwards compatibility; only ML-DSA-65 carries the extra 100 Ggas charge.
  See "size axis" below for the matching accounting fix.
- **Domain-tag export.** The hash domain tag is public as
  `near_crypto::hash_domain::HashDomainTag::MlDsa65PubkeyV1` (with a public
  `as_bytes()`); `MlDsa65PublicKey::to_public_key_handle()` is the public hash
  helper. Wallets can recompute the on-trie identifier from their own pubkey.
- **Test-loop integration tests.** `test-loop-tests/src/tests/ml_dsa_access_key.rs`
  (full-access and function-call key end-to-end) and
  `ml_dsa_verification_cost.rs` (outer-tx and inner-delegate gas charge) cover
  the protocol-boundary behaviour.

### Size axis (accounting done, pricing deferred)

`SignedTransaction::get_size()` counts only the inner `Transaction` body, so it
under-counted every ML-DSA-65 tx by its ~3.3 KiB signature - including inside
`combined_transactions_size_limit`, the bound on transaction bytes carried by
the `ChunkStateWitness`. `SignedTransaction::wire_size()` now returns the full
borsh length (signature included), and `size_for_limits(protocol_version)`
selects it for the `max_transaction_size` and `combined_transactions_size_limit`
gates from `PostQuantumSignatures` onward (version-gated to keep chunk
production deterministic across the boundary). The mempool counts `wire_size()`
unconditionally, since it physically buffers the signature. *Pricing* the extra
size - e.g. a per-wire-byte gas surcharge at conversion time, mirroring the
verify cost - is deferred: the accounting fix closes the witness-honesty gap on
its own, independent of any later pricing decision. A per-byte component on
`AddKey`/`DeleteKey` for the ~1952-byte pubkey they carry on the wire is in the
same deferred bucket.

## Remaining follow-ups (post-stabilization)

These do not block stabilization and are tracked as follow-up work:

1. **Mirror / fork-network cross-network support for PQ keys.** Behavior is
    currently inconsistent across entry points and should be unified before any
    serious mirroring of a PQ-bearing chain:
    - `tools/mirror/src/genesis.rs` panics loudly when it encounters an
      ML-DSA-65 access-key or gas-key-nonce record.
    - `tools/mirror/src/key_util.rs`, `tools/mirror/src/{offline,online}.rs`,
      and `tools/fork-network/src/cli.rs` silently `filter_map` / `continue`
      past hash-form entries with no log or warning, so a real ML-DSA-bearing
      chain would be silently mirrored with access keys missing.
    Proper support likely needs a deterministic seed-derived mapping
    (analogous to ed25519/secp256k1 mapping in `key_mapping.rs`). For now the
    design accepts that PQ keys cannot be mirrored; at minimum the
    inconsistency above should be unified to "log + skip" so failures are
    visible. The `TODO(post-quantum)` markers at these call sites track this.

2. **Wallet/SDK story for the view-RPC change.** Add the
   `near-api-js`/`near-cli-rs` side of the change (external repos), document
   the hash format, and decide whether to expose a separate
   `view_access_key_hash` endpoint or keep everything inside the `public_key`
   field with the new prefix.

3. **Implicit-account derivation for PQ keys.** Briefing's open question (1).
    Not part of this feature, but the access-key hashing here has set the
    precedent (SHA3-256, domain-separated) that implicit-account derivation
    should probably follow.
