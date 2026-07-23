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
- **Congestion control** — Mechanism (NEP-539) limiting how much cross-shard work a
  shard accepts when downstream shards are congested.
- **Doomslug** — NEAR's finality gadget: a block is final once its successor is
  produced/approved. See [consensus-finality](spec/consensus-finality.md).
- **Epoch** — A fixed span of blocks with a fixed validator set and shard layout.
- **Finality** — The point past which a block cannot be reverted under the protocol's
  assumptions. Doomslug provides near-instant finality of the previous block.
- **Flat storage** — A key→value index of trie leaves enabling O(1)-ish state reads,
  bypassing trie traversal. See [state-storage](spec/state-storage.md).
- **Gas** — Unit of computational/storage cost. Burnt (spent) and prepaid (attached).
- **Protocol feature** — A behavior change gated on a protocol version, enumerated in
  `ProtocolFeature` (`core/primitives-core/src/version.rs`).
- **Protocol version** — Monotonic `u32` identifying the active rule set; agreed via
  epoch-boundary voting. Pinned version for this model: **86**.
- **Receipt** — An asynchronous unit of execution produced from a transaction or
  another receipt. Kinds: action, data, delayed, postponed, buffered, yielded.
  See [runtime-execution](spec/runtime-execution.md) and
  [cross-shard-congestion](spec/cross-shard-congestion.md).
- **Resharding** — Changing the shard layout (splitting shards) at an epoch boundary,
  migrating state. Dynamic resharding is NEP-508 + later work.
- **Shard** — A partition of state and execution. Each shard has its own trie/state
  root within a block.
- **State root** — Merkle root of a shard's trie; commits the shard's state.
- **State witness** — The proof + inputs letting a validator re-execute and validate a
  chunk without holding full state (stateless validation, NEP-509).
  See [stateless-validation](spec/stateless-validation.md).
- **Transaction** — A signed set of actions from one account to one receiver.
- **Trie** — Merkle-Patricia trie storing all shard state; root is the state root.
- **Validator** — A staked participant that produces and/or validates blocks/chunks.
