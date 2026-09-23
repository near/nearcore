# Plan: Networking / P2P

Generation brief for `spec/networking-p2p.md`. Follow `../CONVENTIONS.md`.

## Scope
The peer-to-peer layer: the wire protocol and message types, handshake, peer
management and the peer store, the message tiers (T1/T2) and routing (BFS +
route-back), and rate limiting. The transport for everything else in the protocol.

## Out of scope
- Semantics of payloads (blocks, chunks, witnesses, approvals) — link to their specs.
- Sync *logic* → [sync](../spec/sync.md) (this spec covers how sync messages travel).

## Code to read
- `chain/network/src/network_protocol/mod.rs` and `network.proto` — `PeerMessage`,
  `RoutedMessage` (V1/V2/V3), `TieredMessageBody`, handshake.
- `chain/network/src/peer_manager/peer_manager_actor.rs` — connection lifecycle, event
  loop.
- `chain/network/src/peer_manager/{connection,peer_store,network_state}/`,
  `tcp_transport.rs`.
- `chain/network/src/routing/` — `bfs.rs`, `route_back_cache.rs`, `routing_table_view/`.
- `chain/network/src/rate_limits/`.
- `chain/network/src/state_sync.rs` — state-part availability announcements.
- `docs/architecture/network.md`, `docs/NetworkSpec/*`,
  `docs/advanced_configuration/networking.md` (cross-check; flag staleness).

## Questions the spec must answer
- What is the handshake and what does it negotiate (protocol/genesis, peer id, chain info)?
- What are the message tiers (T1 vs T2) and which traffic uses each, and why?
- How does routed messaging work — target selection, BFS over the routing graph, and
  the route-back cache for responses?
- How are peers discovered, scored, stored, and evicted (peer store)?
- How is the routing table exchanged/maintained between peers?
- What rate limits exist and how are abusive peers throttled/banned?
- How are blocks, chunk parts, transactions, approvals, and witnesses propagated
  (broadcast vs routed)?

## Cross-component edges
- Carries payloads for nearly every other component; link to each for payload meaning.
  State-part availability ↔ [sync](../spec/sync.md).

## Relevant ProtocolFeatures
- Tiered messaging / routed-message version changes, post-quantum signatures on
  network messages if applicable. Verify against `version.rs`.
