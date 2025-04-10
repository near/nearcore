# Transaction Lifecycle

This document describes the end to end lifecycle of a transaction.  Note that this document will reference source code during the discussion.  These references may of course fall out of sync but hopefully they are still useful.

# End user

A transaction starts with an end user.  The user constructs a transaction and sends it to one of the RPC nodes.

The RPC node will then forward the transaction to one of the nodes on the network.

Ultimately, when the transaction is received by a node, it will do some initial processing on it.

# Receiving the transaction from a RPC node

A node receives a transaction as a `struct SignedTransaction`; it performs some initial verifications on it.  See `TxRequestHandler::process_tx_internal()` for more details.  These include:

- Verifying that the transaction has a valid signature
- The transaction has a valid TTL; i.e. it has not already expired and it is not set to expire too far into the future
- The node that received the transaction actually cares about this transaction.  Nodes will typically only care about transactions that are destined to shards that they are currently tracking or will track in the near future
- Based on the congestion control information, is there enough capacity available to accept the transaction
- Various validation checks on the list of actions in the transaction

An outcome of performing these validation checks is that the node builds a `struct ValidatedTransaction`.  This data structure uses the new type pattern and can only be constructed if some of the verifications above have been performed in particular the signature has been verified.

After all the verifications have been performed, if the node is indeed a validator, then it will store the transaction in its transaction pool.  Next, the node will (regardless of whether or not it is a validator) forward the transaction to some other validators who might become chunk producers in the next epoch and would be interested in receiving the transaction.

# Chunk production

From time to time, when a node is the chunk producer, it will take a set of transactions from its transaction pool; combine them into a chunk; and propose it as a chunk.  The entry point for this logic is the `ChunkProducer::produce_chunk()` function.  

It uses `NightshadeRuntime::prepare_transactions()` to get a list of transactions that should be included in the chunk.  Some of the checks from when the transaction was previously received are repeated as necessary.  For example, the transaction could have expired; or the congestion information has changed and we are over capacity; etc.

With this list of transactions, a `struct EncodedShardChunk` is constructed along with merkle paths for the transactions in the chunk.