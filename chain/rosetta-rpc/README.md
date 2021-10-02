# Rosetta API Extension for nearcore

Rosetta is a public API spec defined to be a common denominator for blockchain projects.

Rosetta RPC is built into nearcore and it happily co-exist with JSON RPC.

- [Rosetta Homepage](https://www.rosetta-api.org/docs/welcome.html)
- [Rosetta API Specification](https://github.com/coinbase/rosetta-specifications)
- [Rosetta Tooling](https://github.com/coinbase/rosetta-cli)

You can view Rosetta API specification in [OpenAPI (Swagger)
UI](https://petstore.swagger.io/) passing the link to Rosetta OpenAPI
specification:
<https://raw.githubusercontent.com/coinbase/rosetta-specifications/master/api.json>.

Also, Rosetta implementation in nearcore exposes auto-generated OpenAPI
specification that has some extra comments regarding to the particular
implementation, and you can always access it from the running node at
<http://localhost:3040/api/spec>.

## Supported Features

Our current goal is to have a minimal yet feature-complete implementation of
Rosetta RPC serving
[the main use-case Rosetta was designed for](https://community.rosetta-api.org/t/what-is-rosetta-main-use-case/92/2),
that is exposing balance-changing operations in a consistent way enabling
reconciliation through tracking individual blocks and transactions.

The Rosetta APIs are organized into two distinct categories, the Data API and
the Construction API. Simply put, the Data API is for retrieving data from a
blockchain network and the Construction API is for constructing and submitting
transactions to a blockchain network.

| Feature                      | Status                                                                                                                              |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Data API                     | Feature-complete with some quirks                                                                                                   |
| - `/network/list`            | Done                                                                                                                                |
| - `/network/status`          | Done                                                                                                                                |
| - `/network/options`         | Done                                                                                                                                |
| - `/block`                   | Feature-complete (exposes only balance-changing operations)                                                                         |
| - `/block/transaction`       | Feature-complete (exposes only balance-changing operations and the implementation is suboptimal from the performance point of view) |
| - `/account/balance`         | Done (properly exposes liquid, liquid for storage, and locked (staked) balances through sub-accounts)                               |
| - `/mempool`                 | Not implemented as mempool does not hold transactions for any meaningful time                                                       |
| - `/mempool/transaction`     | Not implemented (see above)                                                                                                         |
| Construction API             | Done                                                                                                                                |
| - `/construction/derive`     | Done (used for implicit accounts)                                                                                                   |
| - `/construction/preprocess` | Done                                                                                                                                |
| - `/construction/metadata`   | Done                                                                                                                                |
| - `/construction/payloads`   | Done                                                                                                                                |
| - `/construction/combine`    | Done                                                                                                                                |
| - `/construction/parse`      | Done                                                                                                                                |
| - `/construction/hash`       | Done                                                                                                                                |
| - `/construction/submit`     | Done                                                                                                                                |

To verify the API compliance use:

```bash
rosetta-cli check:data --configuration-file=./rosetta.cfg
rosetta-cli check:construction --configuration-file=./rosetta.cfg
```

## How to Compile

Follow [the standard nearcore procedures to run a node compiled from the source code](https://docs.near.org/docs/community/contribute/contribute-nearcore)
enabling `rosetta_rpc` feature:

```bash
cargo build --release --package neard --bin neard --features rosetta_rpc
```

## How to Configure

You need `neard` binary to proceed; if you compiled it from the source code (see
above), you can find it in `./target/release/neard`.

### Initial Configuration

#### mainnet

```bash
neard --home ~/.near/mainnet init --chain-id mainnet --download-genesis
```

#### testnet

```bash
neard --home ~/.near/testnet init --chain-id testnet --download-genesis
```

NOTE: The genesis of testnet is around 1GB, so it will take a while to download it.

#### localnet (for local development)

```bash
neard --home ~/.near/localnet init
```

### Tuning

You are free to configure your node the way you feel necessary through the
config file: `~/.near/<chain-id>/config.json`. Here are some useful
configuration options for Rosetta API.

#### Enable Rosetta Server

By default, Rosetta API is disabled even if `neard` is compiled with the
feature enabled. Thus, we need to add the following section to the top-level
of the `config.json` (next to the `"rpc"` section):

```json
  ...
  "rosetta_rpc": {
    "addr": "0.0.0.0:3040",
    "cors_allowed_origins": [
      "*"
    ]
  },
  ...
```

#### Keep Track of Everything

By default, nearcore is configured to do as little work as possible while still
operating on an up-to-date state. Indexers may have different requirements, so
there is no solution that would work for everyone, and thus we are going to
provide you with the set of knobs you can tune for your requirements.

As already has been mentioned in this README, the most common tweak you need to
apply is listing all the shards you want to index data from; to do that, you
should ensure that `"tracked_shards"` lists all the shard IDs, e.g. for the
current betanet and testnet, which have a single shard:

```json
  ...
  "tracked_shards": [0],
  ...
```

By default, nearcore is configured to automatically clean old data (performs
garbage collection), so querying the data that was observed a few epochs
before may return an error saying that the data is missing. If you only need
recent blocks, you don't need this tweak, but if you need access to the
historical data, consider updating `"archive"` setting in `config.json` to
`true`:

```json
  ...
  "archive": true,
  ...
```

## How to Run

Once you have configured the node, just execute `neard` with the relevant home dir:

```bash
neard --home ~/.near/mainnet run
```

To confirm that everything is fine, you should be able to query the Rosetta API:

```bash
curl http://127.0.0.1:3040/network/list --header 'Content-Type: application/json' --data '{"metadata": {}}'
```

Expect to see the following response:

```json
{ "network_identifiers": [{ "blockchain": "nearprotocol", "network": "mainnet" }] }
```

The `network` value should reflect the chain id you specified during
configuration (`mainnet`, `testnet`, `betanet`, or a random string like
`test-chain-ztmbv` for localnet development).
