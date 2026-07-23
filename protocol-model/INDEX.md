# Protocol model — index

Entry point for the implementation-derived NEAR protocol model (release **2.13.0**,
version **86**, commit `499283a`). Each component has a reusable generation plan in
`plans/` and a spec in `spec/`. ✅ = written + verified, ⬜ = planned (not yet written).

## Components

| # | Component | Plan | Spec | Status |
|---|-----------|------|------|--------|
| 1 | Protocol versioning & upgrades | [plan](plans/01-protocol-versioning.md) | [spec](spec/protocol-versioning.md) | ✅ |
| 2 | Data structures & serialization | [plan](plans/02-data-structures-serialization.md) | [spec](spec/data-structures-serialization.md) | ✅ |
| 3 | Consensus & finality (Doomslug) | [plan](plans/03-consensus-finality.md) | [spec](spec/consensus-finality.md) | ✅ |
| 4 | Epoch management, validators & staking | [plan](plans/04-epoch-validators-staking.md) | [spec](spec/epoch-validators-staking.md) | ✅ |
| 5 | Chain & block-processing pipeline | [plan](plans/05-chain-block-processing.md) | [spec](spec/chain-block-processing.md) | ✅ |
| 6 | Sharding & chunk lifecycle | [plan](plans/06-sharding-chunks.md) | [spec](spec/sharding-chunks.md) | ✅ |
| 7 | Stateless validation | [plan](plans/07-stateless-validation.md) | [spec](spec/stateless-validation.md) | ✅ |
| 8 | Networking / P2P | [plan](plans/08-networking-p2p.md) | [spec](spec/networking-p2p.md) | ✅ |
| 9 | Sync (header/block/state/epoch + catchup) | [plan](plans/09-sync.md) | [spec](spec/sync.md) | ✅ |
| 10 | Runtime & transaction/receipt execution | [plan](plans/10-runtime-execution.md) | [spec](spec/runtime-execution.md) | ✅ |
| 11 | Cross-shard communication & congestion control | [plan](plans/11-cross-shard-congestion.md) | [spec](spec/cross-shard-congestion.md) | ✅ |
| 12 | Smart-contract VM | [plan](plans/12-contract-vm.md) | [spec](spec/contract-vm.md) | ✅ |
| 13 | State & storage | [plan](plans/13-state-storage.md) | [spec](spec/state-storage.md) | ✅ |
| 14 | Accounts, keys & access control | [plan](plans/14-accounts-keys.md) | [spec](spec/accounts-keys.md) | ✅ |
| 15 | Economics | [plan](plans/15-economics.md) | [spec](spec/economics.md) | ✅ |
| 16 | Genesis & node configuration | [plan](plans/16-genesis-configuration.md) | [spec](spec/genesis-configuration.md) | ✅ |

## Cross-link graph (high level)

How the components feed each other. Read top-to-bottom as the rough lifecycle of a
block; the execution column is where state actually changes.

```
                       Protocol versioning (1) ─── gates behavior in ALL components
                       Data structures (2) ─────── shared types used everywhere

  Networking/P2P (8) ──► Sync (9) ──► Chain & block processing (5)
        │                                   │
        │                                   ├──► Consensus/Doomslug (3) ── finality
        │                                   ├──► Epoch/validators/staking (4)
        │                                   ├──► Sharding & chunks (6) ──► Stateless validation (7)
        │                                   └──► Runtime execution (10)
        │                                            │
        │                                            ├──► Contract VM (12)
        │                                            ├──► Cross-shard & congestion (11)
        │                                            ├──► Accounts & keys (14)
        │                                            ├──► Economics (15)
        │                                            └──► State & storage (13)
                       Genesis & config (16) ─────── bootstraps the initial state & params
```

## Navigating for a task

- **A protocol question** → find the owning component above, read its spec.
- **A NEP review** → follow [`playbooks/nep-review.md`](playbooks/nep-review.md)
  (scope → per-component delta → synthesize + adversarial check).
- **A PR impact assessment** → follow [`playbooks/pr-review.md`](playbooks/pr-review.md)
  (map changed files → components, then trace effects outward via *Interactions*).
- **A release review** → follow [`playbooks/release-review.md`](playbooks/release-review.md)
  (diff the pinned baseline against a target commit; covers both protocol-gated changes
  and ungated `neard` behavioral changes — networking, sync, storage, ops).
