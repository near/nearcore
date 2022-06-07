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

## API Compliance
You can verify the API compliance in each network differently. You can run the commands below to check `Data` and `Construction` compliances mentioned in [Rosetta Testing](https://www.rosetta-api.org/docs/rosetta_test.html#run-the-tool). Each network has it's own `.ros` and `.cfg` files that you can configure and run. 

```bash
rosetta-cli check:data --configuration-file=./rosetta-<mainnet|testnet|localnet>.cfg
rosetta-cli check:construction --configuration-file=./rosetta-<mainnet|testnet|localnet>.cfg
```

##### Localnet
For `localnet` you can use the account `test.near` to run the tests. You should replace the `<privateKey>` value in `rosetta-localnet.cfg` with the `privateKey` of `test.near` which you can find in `~/.near-credentials/local` in the `test.near.json` file. 


```json
  ...
  "prefunded_accounts": [{
        "privkey": "<privateKey>",
        "account_identifier": {
            "address": "test.near"
        },
  ...
```

After replacing the `privateKey` you will need to replace the `test-chain-I4wNe` with the name of your localnet in `rosetta-localnet.cfg`. 

```json
"network": {
  "blockchain": "nearprotocol",
  "network": "test-chain-I4wNe"
 },
 ```

##### Testnet
To run it against testnet or mainnet would require to also have the `pre-funded accounts` as well as network set to a proper value in the `.ros` and `rosetta-<mainnet|testnet>.cfg` files.

Start by [creating an account](https://docs.near.org/docs/tools/near-cli#near-create-account). Created account will be placed in `~/.near-credentials/testnet/<accountname>.testnet.json`. Change `<privateKey>` with the private key of newly created account and `<accountName>`  with the account name of the newly created account in `rosetta-testnet.cfg`.

```json
  ...
  "prefunded_accounts": [{
        "privkey": "<privateKey>",
        "account_identifier": {
            "address": "<accountName>"
        },
  ...
```

Next you will need to change the `faucet` with `{"address":"<accountName>"}` in `nearprotocol-testnet.ros`. Now you are ready to run the test in testnet.
##### Mainnet
For mainnet you can follow the same steps that you have followed in Testnet documentation. The difference is that the configuration files are named `rosetta-mainnet.cfg` and `nearprotocol-mainnet.ros`. The credentials can be found in `~/.near-credentials/mainnet/<accountname>.near.json`.
## How to Compile

To compile the `neard` executable you’ll need Rust and make installed.
With those dependencies fulfilled, simply invoke `make neard` to build
fully optimised executable.  Such executable is adequate for running
in production and will be located at `./target/release/neard`.

Alternatively, during development and testing it may be better to
follow the method recommended when [contributing to
nearcore](https://docs.near.org/docs/community/contribute/contribute-nearcore)
which creates a slightly less optimised executable but does it faster:

```bash
cargo build --release --package neard --bin neard
```

## How to Configure

You need `neard` binary to proceed; if you compiled it from the source code (see
above), you can find it in `./target/release/neard`.

### Initial Configuration

#### mainnet

```bash
neard --home ~/.near/mainnet init --chain-id mainnet --download-genesis --download-config
```

#### testnet

```bash
neard --home ~/.near/testnet init --chain-id testnet --download-genesis --download-config
```

NOTE: The genesis of testnet is around 5GB, so it will take a while to download it.

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
