# Restored Receipts Verifier

## Purpose

Verifies that receipts restored in chain after `RestoreReceiptsAfterFixApplyChunks` protocol upgrade actually were created on chain at some heights.
Because receipt hashes are unique, we only check for their presence.

We needed such a restore, because these receipts got stuck in chain due to a bug fixed by a `FixApplyChunks` protocol upgrade. See [#4248](https://github.com/near/nearcore/pull/4248) for more details.

## Requirements

Mainnet archival node dump.
