# bounty-localnet

A one-command local NEAR network for bug-bounty reporters. Spin it up, attack
it, attach the resulting `network/` directory and `.env` to your
[hackenproof](https://hackenproof.com/near/near-protocol) submission so the
team can reproduce your finding without guesswork.

See [`SECURITY.md`](../../SECURITY.md) at the repo root for the full reward
program and disclosure policy.

## Prerequisites

- Docker (with the `compose` plugin — `docker compose version` must work).
- ~10 GB free disk and **16 GiB of memory available to Docker** (Docker
  Desktop: Settings → Resources → Memory). The release build's final link
  step peaks around 12 GiB; with less, the compiler gets OOM-killed and the
  build fails with `rustc` exiting on SIGKILL or "cannot allocate memory".
  First build takes ~10–30 min; later starts use the BuildKit cache and
  finish in seconds.

That's it. No Rust toolchain, no `jq`, no Python on your host — every
dependency lives inside the init image.

## Run

```bash
cp .env.example .env
docker compose up --build
```

This launches an `init` container (generates configs, exits), then four
validators (`validator0`..`validator3`). `validator0` exposes RPC on
`localhost:3030`.

> **Warning:** if you change `NUM_VALIDATORS` in `.env`, you must also set
> `COMPOSE_PROFILES=min<NUM_VALIDATORS>` (empty for 1) — init fails loudly
> on a mismatch. Alternatively, from the repo root, `just bounty-localnet`
> derives the profile automatically.

Compose service names map to on-chain identities one-to-one: service
`validatorN` runs account `nodeN` with its home dir at `./network/nodeN`.

## Wait for the network to be ready

```bash
until curl -s http://localhost:3030/status | jq -e '.sync_info.latest_block_height >= 2' > /dev/null; do
    sleep 2
done
echo "Network ready."
```

If `jq` isn't on your host, drop the `| jq -e ...` clause and grep for
`latest_block_height` instead.

## Reporter account

A pre-funded account is created in genesis so you don't have to fish a
validator key out of the generated configs.

| Field | Value |
|---|---|
| Account ID | `reporter.test.near` |
| Initial balance | 1,000,000 NEAR |
| Key file | [`reporter-key.json`](./reporter-key.json) |
| RPC URL | `http://localhost:3030` |
| Chain ID | `localnet` (override via `.env`) |

The secret key in `reporter-key.json` is **intentionally public** —
committed to this repo so reporters don't need to generate their own. Using
this key on any other network is unsafe.

Sanity check via JSON-RPC — view the reporter account on-chain:

```bash
curl -s -H 'Content-Type: application/json' http://localhost:3030 -d '{
  "jsonrpc": "2.0", "id": 1, "method": "query",
  "params": {"request_type": "view_account",
             "finality": "final",
             "account_id": "reporter.test.near"}
}' | jq
```

For richer flows (transfers, contract deploys), point your favorite NEAR
client at `http://localhost:3030` and use `reporter-key.json` as the signer.

## Demoing byzantine validators

Some bug repros need ONE validator to run a modified `neard` (e.g. to demo
double-signing, equivocation, or malformed-message attacks). Compose
auto-merges `docker-compose.override.yml`, so you can swap one validator's
binary without touching the committed compose file:

```bash
cp docker-compose.override.yml.example docker-compose.override.yml
# edit docker-compose.override.yml — set the absolute path to your custom neard
docker compose up --build
```

The example targets `validator1` so `validator0`'s RPC stays available;
don't override `validator0` itself unless you're prepared to lose the RPC
endpoint. The example also includes a commented-out alternate-build-context
pattern with caveats — read it before using.

If you use this, attach `docker-compose.override.yml` (and ideally the
modified `neard` binary or a clear recipe to rebuild it) to your hackenproof
report alongside the items in "Submitting your repro" below.

## Configurable knobs

| `.env` var | Default | Notes |
|---|---|---|
| `NUM_VALIDATORS` | `4` | 1..8. Update `COMPOSE_PROFILES` to match. |
| `NUM_SHARDS` | `1` | Number of shards in the layout. |
| `CHAIN_ID` | `localnet` | Embedded in genesis. |
| `COMPOSE_PROFILES` | `min4` | `min<NUM_VALIDATORS>`, blank for N=1. Must match `NUM_VALIDATORS` (init fails loudly otherwise); `just bounty-localnet` derives it. |

After changing any of these, run `./reset.sh` — `init.sh` hashes the values
and refuses to start a network whose topology no longer matches.

## Reset

```bash
./reset.sh
```

Tears down containers, removes anonymous volumes, and wipes `./network/`.
Subsequent `docker compose up --build` regenerates everything from scratch.

## Submitting your repro

Open your report on
[hackenproof](https://hackenproof.com/near/near-protocol) and attach:

1. Your `.env` (so the team knows the topology you targeted).
2. The generated `./network/` directory (genesis + per-node configs and
   keys — `neard localnet` regenerates random validator keys per init, so
   sharing this dir is the only way to make the genesis hash deterministic
   between you and the triage team).
3. A small script that runs your attack against `http://localhost:3030`,
   using `reporter-key.json` as the signer.

## Known limitations (v1)

- Genesis hash is **not** deterministic across `init.sh` runs — validator
  keys are random. Share `./network/`, not just `.env`.
- No MainNet-like state. The genesis is intentionally minimal: validators
  + reporter, nothing else.
- Localnet uses test-shaped epoch config (e.g. inflated seat counts). Bugs
  whose repro depends on production-shaped validator election may not
  surface here.
- `validator0` is both validator and RPC node. If your attack stops
  `validator0`, RPC goes with it. A dedicated RPC node is a v2 concern.
- A validator that is fully offline from t=0 (e.g. a crash-stub override
  binary) stalls startup for `NUM_VALIDATORS < 5`: each node's startup
  peer gate waits for `min(N-1, 3)` peers before syncing. Run ≥5
  validators for that scenario.
- Custom stake distributions aren't a knob: edit `genesis.json` after
  init (keep `total_supply` consistent with the sum of account balances).
- No pre-built Docker images. Reporters pay a one-time build cost.
