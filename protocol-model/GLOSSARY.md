# Glossary

Shared protocol terms used across the specs. Seeded during the pilot; extend during
regeneration as specs surface new terms. Specs link here for definitions rather than
redefining inline.

- **Account** — A named on-chain entity holding balance, optional contract code, and
  access keys. Versioned (`Account` V1/V2). See [accounts-keys](spec/accounts-keys.md).
- **Action** — A unit of work in a transaction/receipt (Transfer, FunctionCall,
  CreateAccount, DeployContract, Stake, AddKey, DeleteKey, DeleteAccount,
  DelegateAction, …). See [runtime-execution](spec/runtime-execution.md).
- **Approval** — A validator's signed message endorsing a block (or a skip), used by
  Doomslug for finality. See [consensus-finality](spec/consensus-finality.md).
- **Catchup** — The process by which a node builds state for shards it will track in
  the next epoch but does not track now. See [sync](spec/sync.md).
- **Chunk** — The per-shard portion of a block: the shard's transactions, receipts,
  and resulting state root. Produced by chunk producers.
- **Cloud archival** — Archiving chain data to object storage as versioned blobs, with a
  reader that reconstructs shard state from them. Config lives under `cloud_archival`.
  See [state-storage](spec/state-storage.md).
- **Congestion control** — Mechanism (NEP-539) limiting how much cross-shard work a
  shard accepts when downstream shards are congested.
- **Deterministic account ID** — An account whose ID is derived from the contract state
  it is created with, rather than chosen. Superseded for new work by the universal
  account (`0u`) scheme. See [accounts-keys](spec/accounts-keys.md).
- **Doomslug** — NEAR's finality gadget: a block is final once its successor is
  produced/approved. See [consensus-finality](spec/consensus-finality.md).
- **Early kickout** — Stopping assignment of new chunks to a chunk producer on a shard
  *within* an epoch once its production record on that shard falls below threshold,
  rather than waiting for the epoch-boundary kickout set. Stable at version 87.
  See [epoch-validators-staking](spec/epoch-validators-staking.md).
- **Epoch** — A fixed span of blocks with a fixed validator set and shard layout.
- **Finality** — The point past which a block cannot be reverted under the protocol's
  assumptions. Doomslug provides near-instant finality of the previous block.
- **Flat storage** — A key→value index of trie leaves enabling O(1)-ish state reads,
  bypassing trie traversal. See [state-storage](spec/state-storage.md).
- **Gas** — Unit of computational/storage cost. Burnt (spent) and prepaid (attached).
- **Gas key** — An access key whose balance is spent as gas (NEP-611), tracked with its
  own nonce. See [accounts-keys](spec/accounts-keys.md).
- **Global contract** — Contract code deployed once and referenced by many accounts
  instead of being stored per account. See [runtime-execution](spec/runtime-execution.md).
- **Grandparent anchor** — The `(block hash, epoch id)` of a chunk's grandparent block,
  carried on V2/V3 wire messages so a receiver can resolve which producer was assigned a
  shard without re-sampling. See [stateless-validation](spec/stateless-validation.md).
- **ML-DSA-65** — The FIPS 204 post-quantum signature scheme, available both as an
  account key type and as the `ml_dsa_verify` host function.
- **Protocol feature** — A behavior change gated on a protocol version, enumerated in
  `ProtocolFeature` (`core/primitives-core/src/version.rs`).
- **Protocol version** — Monotonic `u32` identifying the active rule set; agreed via
  epoch-boundary voting. Pinned version for this model: **87**.
- **Receipt** — An asynchronous unit of execution produced from a transaction or
  another receipt. Kinds: action, data, delayed, postponed, buffered, yielded.
  See [runtime-execution](spec/runtime-execution.md) and
  [cross-shard-congestion](spec/cross-shard-congestion.md).
- **Receipt promise input** — The resolved data a receipt awaits from its promise
  dependencies. Its combined size is bounded by `max_receipt_total_input_size` from
  version 87. See [runtime-execution](spec/runtime-execution.md).
- **Resharding** — Changing the shard layout (splitting shards) at an epoch boundary,
  migrating state. Dynamic resharding is NEP-508 + later work.
- **Shard** — A partition of state and execution. Each shard has its own trie/state
  root within a block.
- **SPICE** — An in-development execution redesign. Gated at protocol version 180 and
  **not** reachable at version 87 or in nightly (157); every spec marks it as not stable.
- **State part** — One slice of a shard's state, the unit peers exchange during state
  sync. See [sync](spec/sync.md).
- **State root** — Merkle root of a shard's trie; commits the shard's state.
- **State witness** — The proof + inputs letting a validator re-execute and validate a
  chunk without holding full state (stateless validation, NEP-509).
  See [stateless-validation](spec/stateless-validation.md).
- **Storage proof limit** — A cap on the trie proof one receipt may record. From version
  87 it is checked after *every* action, not only `FunctionCall`.
  See [stateless-validation](spec/stateless-validation.md).
- **Transaction** — A signed set of actions from one account to one receiver.
- **Trie** — Merkle-Patricia trie storing all shard state; root is the state root.
- **Universal account** — An account under the `0u` scheme whose ID (a *UAID*) is derived
  by hashing its canonical state init — contract code, storage entries and access keys.
  Stable at version 87; unifies implicit and contract-created account creation.
  See [accounts-keys](spec/accounts-keys.md).
- **Validator** — A staked participant that produces and/or validates blocks/chunks.
