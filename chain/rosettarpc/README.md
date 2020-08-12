Rosetta API Extension for nearcore
==================================

Rosetta is a public API spec defined to be a common denominator for blockchain projects.

Rosetta RPC is built into nearcore and it happily co-exist with JSON RPC.

* [Rosetta Homepage](https://www.rosetta-api.org/docs/welcome.html)
* [Rosetta API Specification](https://github.com/coinbase/rosetta-specifications)
* [Rosetta Tooling](https://github.com/coinbase/rosetta-cli)

Supported Features
==================

Our current goal is to have a minimal yet feature-complete implementation of
Rosetta RPC serving
[the main use-case Rosetta was designed for](https://community.rosetta-api.org/t/what-is-rosetta-main-use-case/92/2),
that is exposing balance-changing operations in a consistent way enabling
reconciliation through tracking individual blocks and transactions.

The Rosetta APIs are organized into two distinct categories, the Data API and
the Construction API. Simply put, the Data API is for retrieving data from a
blockchain network and the Construction API is for constructing and submitting
transactions to a blockchain network.

| Feature               | Status                                |
| --------------------- | ------------------------------------- |
| Data API              | Feature-complete with some quirks     |
| * `/network/list`     | Done                                  |
| * `/network/status`   | Feature-complete                      |
| * `/network/options`  | Done                                  |
| Construction API      | Rosetta 1.3 with some 1.4 features    |

To verify the API compliance use:

```
rosetta-cli check:data --server-url=http://localhost:3040
rosetta-cli check:construction --server-url=http://localhost:3040
```

How to Run
==========

Follow the standard nearcore procedures to run a node compiled from the source code.

By default, Rosetta RPC is available on port TCP/3040.
