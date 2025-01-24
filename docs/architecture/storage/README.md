# Storage

## Overview

The storage subsystem of nearcore is complex and has many layers. This documentation
provides an overview of the common use cases and explains the most important
implementation details.

The main requirements are:

- Low but predictable latency for reads
- Proof generation for chunk validators

Additionally, operations with contract storage require precise gas cost computation.

## Examples

### Contract Storage

Contract-specific storage is exposed in e.g. `storage_read` and `storage_write`
methods which contracts can call.

### Runtime State Access

When Account, AccessKey, ... are read or updated during tx and receipt processing,
it is always a storage operation. Examples of that are token transfers and access
key management. More advanced example is management of receipt queues. If receipt
cannot be processed in the current block, it is added to the delayed queue, which
is part of the storage.

## RPC Node Operations

When RPC or validator node receives a transaction, it needs to do validity check.
It involves reading Accounts and AccessKeys, which is also a storage operation.

Another query to RPC node is a view call - state query without modification or
contract dry run.

## Focus

In the documentation, we focus the most on the **Contract Storage** use case because
it has the strictest requirements.

For the high-level flow, refer to [Flow diagram](./flow.md).
For more detailed information, refer to:

- [Primitives](./primitives.md)
- [Trie Storage](./trie_storage.md)
- [Flat Storage](./flat_storage.md)
- [Database](./database.md)
