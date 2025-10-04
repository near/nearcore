# Runtime crate

Runtime crate encapsulates the logic of how transactions and receipts should be handled. If it encounters
a smart contract call within a transaction or a receipt it calls `near-vm-runner`, for all other actions, like account
creation, it processes them in-place.

## Runtime class

The main entry point of the `Runtime` is method `apply`.
It applies new singed transactions and incoming receipts for some chunk/shard on top of
given trie and the given state root.
If the validator accounts update is provided, updates validators accounts.
All new signed transactions should be valid and already verified by the chunk producer.
If any transaction is invalid, the method returns an `InvalidTxError`.
In case of success, the method returns `ApplyResult` that contains the new state root, trie changes,
new outgoing receipts, stats for validators (e.g. total rent paid by all the affected accounts),
execution outcomes.

### Apply arguments

It takes the following arguments:

- `trie: Arc<Trie>` - the trie that contains the latest state.
- `root: CryptoHash` - the hash of the state root in the trie.
- `validator_accounts_update: &Option<ValidatorAccountsUpdate>` - optional field that contains updates for validator accounts.
  It's provided at the beginning of the epoch or when someone is slashed.
- `apply_state: &ApplyState` - contains block index and timestamp, epoch length, gas price and gas limit.
- `prev_receipts: &[Receipt]` - the list of incoming receipts, from the previous block.
- `transactions: &[SignedTransaction]` - the list of new signed transactions.

### Apply logic

The execution consists of the following stages:

1. Snapshot the initial state.
1. Apply validator accounts update, if available.
1. Convert new signed transactions into the receipts.
1. Process receipts.
1. Check that incoming and outgoing balances match.
1. Finalize trie update.
1. Return `ApplyResult`.

## Validator accounts update

Validator accounts are accounts that staked some tokens to become a validator.
The validator accounts update usually happens when the current chunk is the first chunk of the epoch.
It also happens when there is a challenge in the current block with one of the participants belong to the current shard.

This update distributes validator rewards, return locked tokens and maybe slashes some accounts out of their stake.

## Signed Transaction conversion

New signed transaction transactions are provided by the chunk producer in the chunk. These transactions should be ordered and already validated.
Runtime does validation again for the following reasons:

- to charge accounts for transactions fees, transfer balances, prepaid gas and account rents;
- to create new receipts;
- to compute burnt gas;
- to validate transactions again, in case the chunk producer was malicious.

If the transaction has the same `signer_id` and `receiver_id`, then the new receipt is added to the list of new local receipts,
otherwise it's added to the list of new outgoing receipts.

## Receipt processing

Receipts are processed one by one in the following order:

1. Previously delayed receipts from the state.
1. New local receipts.
1. New incoming receipts.

After each processed receipt, we compare total gas burnt (so far) with the gas limit.
When the total gas burnt reaches or exceeds the gas limit, the processing stops.
The remaining receipts are considered delayed and stored into the state.

### Delayed receipts

Delayed receipts are stored as a persistent queue in the state.
Initially, the first unprocessed index and the next available index are initialized to 0.
When a new delayed receipt is added, it's written under the next available index in to the state and the next available index is incremented by 1.
When a delayed receipt is processed, it's read from the state using the first unprocessed index and the first unprocessed index is incremented.
At the end of the receipt processing, the all remaining local and incoming receipts are considered to be delayed and stored to the state in their respective order.
If during receipt processing, we've changed indices, then the delayed receipt indices are stored to the state as well.

### Receipt processing algorithm

The receipt processing algorithm is the following:

1. Read indices from the state or initialize with zeros.
1. While the first unprocessed index is less than the next available index do the following
   1. If the total burnt gas is at least the gas limit, break.
   1. Read the receipt from the first unprocessed index.
   1. Remove the receipt from the state.
   1. Increment the first unprocessed index.
   1. Process the receipt.
   1. Add the new burnt gas to the total burnt gas.
   1. Remember that the delayed queue indices has changed.
1. Process the new local receipts and then the new incoming receipts
   - If the total burnt gas is less then the gas limit:
     1. Process the receipt.
     1. Add the new burnt gas to the total burnt gas.
   - Else:
     1. Store the receipt under the next available index.
     1. Increment the next available index.
     1. Remember that the delayed queue indices has changed.
1. If the delayed queue indices has changed, store the new indices to the state.

## Balance checker

Balance checker computes the total incoming balance and the total outgoing balance.

The total incoming balance consists of the following:

- Incoming validator rewards from validator accounts update.
- Sum of the initial accounts balances for all affected accounts. We compute it using the snapshot of the initial state.
- Incoming receipts balances. The prepaid fees and gas multiplied their gas prices with the attached balances from transfers and function calls.
  Refunds are considered to be free of charge for fees, but still has attached deposits.
- Balances for the processed delayed receipts.
- Initial balances for the postponed receipts. Postponed receipts are receipts from the previous blocks that were processed, but were not executed.
  They are action receipts with some expected incoming data. Usually for a callback on top of awaited promise.
  When the expected data arrives later than the action receipt, then the action receipt is postponed.
  Note, the data receipts are 0 cost, because they are completely prepaid when issued.

The total outgoing balance consists of the following:

- Sum of the final accounts balance for all affected accounts.
- Outgoing receipts balances.
- New delayed receipts. Local and incoming receipts that were not processed this time.
- Final balances for the postponed receipts.
- Total rent paid by all affected accounts.
- Total new validator rewards. It's computed from total gas burnt rewards.
- Total balance burnt. In case the balance is burnt for some reason (e.g. account was deleted during the refund), it's accounted there.
- Total balance slashed. In case a validator is slashed for some reason, the balance is account here.

When you sum up incoming balances and outgoing balances, they should match.
If they don't match, we throw an error.
