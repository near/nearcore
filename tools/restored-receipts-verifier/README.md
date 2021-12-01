# Restored Receipts Verifier

## Purpose

Verifies that receipts being restored after `apply_chunks` fix (`FixApplyChunks` protocol upgrade) actually were created on chain.
Because receipt hashes are unique, we only check for their presence.
See https://github.com/near/nearcore/pull/4248/ for more details.

## Requirements

Mainnet archival node dump.
