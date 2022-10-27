# JSON-RPC API for nearcore

[JSON-RPC](https://www.jsonrpc.org/) API for nearcore node exposes handles to
inspect the data, inspect the network state, and the node state, and allows to
submit a transaction.

## Guarantees

All the APIs that are compiled by default (default-features) and not prefixed
with `EXPERIMENTAL_` are kept stable without breaking changes.

We also support `near-api-*` bindings to JSON-RPC in the latest state and
propagate deprecation warnings through them.

The breaking changes (e.g. removal or change of `EXPERIMENTAL_` APIs) are
communicated through the CHANGELOG next to this file.

## Policies for API Changes

1. We only add the APIs to the data that is already available in nearcore
   storage.
2. We don't violate the guaranties described in the section above.
3. We prefix new APIs with `EXPERIMENTAL_` (see the Experimental API Policies
   below).
4. We document the API change on [NEAR Docs](https://docs.near.org/api/rpc/introduction)
   BEFORE merging the change to nearcore.
5. We log changes to the methods and API input/output structures through
   CHANGELOG.md file in the jsonrpc crate.

## Experimental API Policies

When you introduce new API, we may go two ways:

1. Hide the new API behind a feature-flag that is not enabled by default
2. Prefix the method name with `EXPERIMENTAL_`

Either way, we need to document the new API in our [RPC endpoint docs](https://docs.near.org/api/rpc/introduction).

Stabilization of the Experimental APIs is multistage:

1. Once the `EXPERIMENTAL_` prefixed API handler lands to master it starts its
   lifetime. While the API is Experimental, we have the freedom to play with it
   and iterate fearlessly (if we need to change it, we change it and only
   record the change in the CHANGELOG and update the documentation, no need for
   backwards compatibility).
2. If we feel that the API is stable (being in **use** for a while), we need to
   release a new API method without the `EXPERIMENTAL_` prefix while keeping
   the old method name as an alias for the transition period.
3. Drop the `EXPERIMENTAL_` alias completely when nearcore version with the
   stable method name is deployed to the majority nodes in the network, and
   most (all) clients have transitioned to the new API.
