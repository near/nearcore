Rosetta API Extension for nearcore
==================================

Rosetta is a public API spec defined to be a common denominator for blockchain projects.

Rosetta RPC is built into nearcore and it happily co-exist with JSON RPC.

* [Rosetta Homepage](https://www.rosetta-api.org/docs/welcome.html)
* [Rosetta API Specification](https://github.com/coinbase/rosetta-specifications)
* [Rosetta Tooling](https://github.com/coinbase/rosetta-cli)

You can view Rosetta API specification in [OpenAPI (Swagger) UI](https://petstore.swagger.io/)
passing the link to Rosetta OpenAPI specification:

```
https://raw.githubusercontent.com/coinbase/rosetta-specifications/master/api.json
```

Also, Rosetta implementation in nearcore exposes auto-generated OpenAPI
specification that has some extra comments regarding to the particular
implementation, and you can always access it from the running node:

```
http://localhost:3040/api/spec
```

Supported Features
------------------

Our current goal is to have a minimal yet feature-complete implementation of
Rosetta RPC serving
[the main use-case Rosetta was designed for](https://community.rosetta-api.org/t/what-is-rosetta-main-use-case/92/2),
that is exposing balance-changing operations in a consistent way enabling
reconciliation through tracking individual blocks and transactions.

The Rosetta APIs are organized into two distinct categories, the Data API and
the Construction API. Simply put, the Data API is for retrieving data from a
blockchain network and the Construction API is for constructing and submitting
transactions to a blockchain network.

| Feature                       | Status                                                        |
| ----------------------------- | ------------------------------------------------------------- |
| Data API                      | Feature-complete with some quirks                             |
| - `/network/list`             | Done                                                          |
| - `/network/status`           | Done                                                          |
| - `/network/options`          | Done                                                          |
| - `/block`                    | Feature-complete (exposes only balance-changing operations)   |
| - `/block/transaction`        | Feature-complete (exposes only balance-changing operations and the implementation is suboptimal from the performance point of view) |
| - `/account/balance`          | Done (properly exposes liquid, liquid for storage, and locked [staked] balances through sub-accounts) |
| - `/mempool`                  | Not implemented as mempool does not hold transactions for any meaningful time |
| - `/mempool/transaction`      | Not implemented (see above)                                   |
| Construction API              | Partially implemented                                         |
| - `/construction/derive`      | Done (not applicable to NEAR)                                 |
| - `/construction/preprocess`  | Not implemented                                               |
| - `/construction/metadata`    | Not implemented                                               |
| - `/construction/payloads`    | Not implemented                                               |
| - `/construction/combine`     | Not implemented                                               |
| - `/construction/parse`       | Not implemented                                               |
| - `/construction/hash`        | Done                                                          |
| - `/construction/submit`      | Done                                                          |

To verify the API compliance use:

```
rosetta-cli check:data --configuration-file=./rosetta.cfg
rosetta-cli check:construction --configuration-file=./rosetta.cfg
```

How to Run
----------

Follow [the standard nearcore procedures to run a node compiled from the source code](https://docs.near.org/docs/contribution/nearcore)
enabling `rosettarpc` feature:

```
cargo run --release --package neard --bin neard --features rosetta_rpc -- init
cargo run --release --package neard --bin neard --features rosetta_rpc -- run
```

By default, Rosetta RPC is available on port TCP/3040.
