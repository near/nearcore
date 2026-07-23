# Plan: Sync (header / block / state / epoch + catchup)

Generation brief for `spec/sync.md`. Follow `../CONVENTIONS.md`.

## Scope
How a node that is behind (or newly joined) catches up: header sync, block sync, epoch
sync, state sync (downloading a shard's state by parts), and catchup (building state
for shards to be tracked next epoch). The state-sync request/serve protocol.

## Out of scope
- The transport → [networking-p2p](../spec/networking-p2p.md).
- Trie/state-part *construction* → [state-storage](../spec/state-storage.md)
  (cross-link; this spec covers the sync *protocol*, that one the part format).

## Code to read
- `docs/architecture/how/sync.md` (READ FIRST per repo guidance; cross-check).
- `chain/client/src/sync/{header,block,epoch}.rs`.
- `chain/client/src/sync/state/{mod,shard,downloader,network,external}.rs`,
  `chain/client/src/sync/handler.rs`.
- `chain/client/src/state_request_actor.rs` — serving state parts.
- `chain/client/src/sync_jobs_actor.rs` — job coordination.
- `nearcore/src/state_sync.rs` — orchestrator, external/cloud storage.
- `docs/misc/state_sync_dump.md`, `state_sync_from_external_storage.md`,
  `docs/architecture/next/catchup_and_state_sync.md`.

## Questions the spec must answer
- What is the ordering of the sync phases and what triggers entering each (how far
  behind)?
- Header sync: what is requested and how is the chain of headers validated?
- Block sync: how are missing blocks fetched and applied up to head?
- Epoch sync: what does it bootstrap and how does it avoid downloading all headers?
- State sync: how is a shard's state at a sync hash downloaded by parts (request,
  parallelism, validation against state root), and where do parts come from (peers vs
  external/cloud storage)?
- Catchup: how/when does a node build state for shards it will track next epoch while
  staying live on current shards?
- How does a node serve state-part requests to others?

## Cross-component edges
- Uses networking transport; produces state for block processing; relies on epoch info
  for shard tracking; writes via state-storage. Link to each.

## Relevant ProtocolFeatures
- Epoch sync data hash validation, external/cloud state sync. Verify against `version.rs`.
