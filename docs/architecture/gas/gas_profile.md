# Gas Profile

What if you want to understand the exact gas spending of a smart contract call?
It would be very complicated to predict exactly how much gas executing a piece
of WASM code will require, including all host function calls and actions. An
easier approach is to just run the code on testnet and see how much gas it
burns. Gas profiles allow one to dig deeper and understand the breakdown of the
gas costs per parameter.

**Gas profiles are not very reliable**, in that they are often incomplete and the
details of how they are computed can change without a protocol version bump.

## Example Transaction Gas Profile

You can query the gas profile of a transaction with
[NEAR CLI](https://docs.near.org/tools/near-cli).

```bash
NEAR_ENV=mainnet near tx-status 8vYxsqYp5Kkfe8j9LsTqZRsEupNkAs1WvgcGcUE4MUUw  \
  --accountId app.nearcrowd.near  \
  --nodeUrl https://archival-rpc.mainnet.near.org  # Allows to retrieve older transactions.
```

```
Transaction app.nearcrowd.near:8vYxsqYp5Kkfe8j9LsTqZRsEupNkAs1WvgcGcUE4MUUw
{
  receipts_outcome: [
    {
      block_hash: '2UVQKpxH6PhEqiKr6zMggqux4hwMrqqjpsbKrJG3vFXW',
      id: '14bwmJF21PXY9YWGYN1jpjF3BRuyCKzgVWfhXhZBKH4u',
      outcome: {
        executor_id: 'app.nearcrowd.near',
        gas_burnt: 5302170867180,
        logs: [],
        metadata: {
          gas_profile: [
            {
              cost: 'BASE',
              cost_category: 'WASM_HOST_COST',
              gas_used: '15091782327'
            },
            {
              cost: 'CONTRACT_LOADING_BASE',
              cost_category: 'WASM_HOST_COST',
              gas_used: '35445963'
            },
            {
              cost: 'CONTRACT_LOADING_BYTES',
              cost_category: 'WASM_HOST_COST',
              gas_used: '117474381750'
            },
            {
              cost: 'READ_CACHED_TRIE_NODE',
              cost_category: 'WASM_HOST_COST',
              gas_used: '615600000000'
            },
            # ...
            # skipping entries for presentation brevity
            # ...
            {
              cost: 'WRITE_REGISTER_BASE',
              cost_category: 'WASM_HOST_COST',
              gas_used: '48713882262'
            },
            {
              cost: 'WRITE_REGISTER_BYTE',
              cost_category: 'WASM_HOST_COST',
              gas_used: '4797573768'
            }
          ],
          version: 2
        },
        receipt_ids: [ '46Qsorkr6hy36ZzWmjPkjbgG28ko1iwz1NT25gvia51G' ],
        status: { SuccessValue: 'ZmFsc2U=' },
        tokens_burnt: '530217086718000000000'
      },
      proof: [ ... ]
    },
    { ... }
  ],
  status: { SuccessValue: 'ZmFsc2U=' },
  transaction: { ... },
  transaction_outcome: {
    block_hash: '7MgTTVi3aMG9LiGV8ezrNvoorUwQ7TwkJ4Wkbk3Fq5Uq',
    id: '8vYxsqYp5Kkfe8j9LsTqZRsEupNkAs1WvgcGcUE4MUUw',
    outcome: {
      executor_id: 'evgeniya.near',
      gas_burnt: 2428068571644,
      ...
      tokens_burnt: '242806857164400000000'
    },
  }
}
```

The gas profile is in `receipts_outcome.outcome.metadata.gas_profile`. It shows
gas costs per parameter and with associated categories such as `WASM_HOST_COST`
or `ACTION_COST`. In the example, all costs are of the former category, which is
gas expended on smart contract execution. The latter is for gas spent on
actions.

To be complete, the output above should also have a gas profile entry for the
function call action. But currently this is not included since gas profiles only
work properly on function call receipts. Improving this is planned, see
[nearcore#8261](https://github.com/near/nearcore/issues/8261).

The `tx-status` query returns one gas profile for each receipt. The output above
contains a single gas profile because the transaction only spawned one receipt.
If there was a chain of cross contract calls, there would be multiple profiles.

Besides receipts, also note the `transaction_outcome` in the output. It contains
the gas cost for converting the transaction into a receipt. To calculate the
full gas cost, add up the transaction cost with all receipt costs.

The transaction outcome currently does not have a gas profile, it only shows the
total gas spent converting the transaction. Arguably, it is not necessary to
provide the gas profile since the costs only depend on the list of actions. With
sufficient understanding of the protocol, one could reverse-engineer the exact
breakdown simply by looking at the action list. But adding the profile would
still make sense to make it easier to understand.

## Gas Profile Versions

Depending on the version in `receipts_outcome.outcome.metadata.version`, you
should expect a different format of the gas profile. Version 1 has no profile
data at all. Version 2 has a detailed profile but some parameters are conflated,
so you cannot extract the exact gas spending in some cases. Version 3 will have
the cost exactly per parameter.

Which version of the profile an RPC node returns depends on the version it had
when it first processed the transaction. The profiles are stored in the database
with one version and never updated. Therefore, older transactions will usually
only have old profiles. However, one could replay the chain from genesis with a
new nearcore client and generate the newest profile for all transactions in this
way.

Note: Due to bugs, some nodes will claim they send version 1 but actually
send version 2. (Did I mention that profiles are unreliable?)

## How Gas Profiles are Created

The transaction runtime charges gas in various places around the code.
`ActionResult` keeps a summary of all costs for an action. The `gas_burnt` and
`gas_used` fields track the total gas burned and reserved for spawned receipts.
These two fields are crucial for the protocol to function correctly, as they are
used to determine when execution runs out of gas.

Additionally, `ActionResult` also has a `profile` field which keeps a detailed
breakdown of the gas spending per parameter. Profiles are not stored on chain
but RPC nodes and archival nodes keep them in their databases. This is mostly a
debug tool and has no direct impact on the correct functioning of the protocol.

## Charging Gas

Generally speaking, gas is charged right before the computation that it pays for
is executed. It has to be before to avoid cheap resource exhaustion attacks.
Imagine the user has only 1 gas unit left, but if we start executing an expensive
step, we would waste a significant duration of computation on all validators
without anyone paying for it.

When charging gas for an action, the `ActionResult` can be updated directly. But
when charging WASM costs, it would be too slow to do a context switch each time,
Therefore, a fast gas counter exists that can be updated from within the VM.
(See
[gas_counter.rs](https://github.com/near/nearcore/blob/06711f8460f946b8d2042aa1df6abe03c5184767/runtime/near-vm-logic/src/gas_counter.rs))
At the end of a function call execution, the gas counter is read by the host and
merged into the `ActionResult`.

<!-- 
TODO: We can expand a bit more on how profiles are created and how it interacts 
with gas charging after merging https://github.com/near/nearcore/issues/8033
-->
