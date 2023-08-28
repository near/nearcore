# Gas

This page describes the technical details around gas during the lifecycle of a
_transaction_(*) while giving an intuition for why things are the way they are
from a technical perspective. For a more practical, user-oriented angle, please
refer to the [gas section in the official protocol
documentation](https://docs.near.org/concepts/basics/transactions/gas).

(*) _For this page, a transaction shall refer to the set of all recursively
generated receipts by a `SignedTransaction`. When referring to only the original
transaction object, we write `SignedTransaction`._

The topic is split into several sections.

1. [Buying Gas](#buying-gas-for-a-transaction): How are NEAR tokens converted to gas?
2. [Gas Price](#gas-price): 
    - [Block-Level Gas Price](#block-level-gas-price): How the block-level gas price is determined.
    - [Pessimistic Gas Price](#pessimistic-gas-price): How worst-case gas pricing is estimated.
    - [Effective Gas Purchase Cost](#effective-gas-purchase-cost): The cost paid for a receipt.
3. [Tracking Gas](#tracking-gas-in-receipts): How the system keeps track of purchased gas during the transaction execution.


## Buying Gas for a Transaction

A signer pays all the gas required for a transaction upfront. However, there is
no explicit act of buying gas. Instead, the fee is subtracted directly in NEAR
tokens from the balance of the signer's account. If we ignore all the details
explained further down, the fee is calculated as `gas amount` * `gas price`.

The `gas amount` is not a field of `SignedTransaction`, nor is it something the
signer can choose. It is only a virtual field that is computed on-chain following
the protocol's rules.

The `gas price` is a variable that may change during the execution of the
transaction. The way it is implemented today, a single transaction can be
charged a different gas price for different receipts.

Already we can see a fundamental problem: Gas is bought once at the beginning
but the gas price may change during execution. To solve this incompatibility,
the protocol calculates a pessimistic gas price for the initial purchase. Later
on, the delta between real and pessimistic gas prices is refunded at the end of
every receipt execution.

An alternative implementation would instead charge the gas at every receipt,
instead of once at the start. However, remember that the execution may happen on
a different shard than the signer account. Therefore we cannot access the
signer's balance while executing.

## Gas Price

Gas pricing is a surprisingly deep and complicated topic. Usually, we only think
about the value of the `gas_price` field in the block header. However, to
understand the internals, this is not enough.

### Block-Level Gas Price

`gas_price` is a field in the block header. It determines how much it costs to
burn gas at the given block height. Confusingly, this is not the same price at
which gas is purchased.
(See [Effective Gas Purchase Price](#effective-gas-price).)

The price is measured in NEAR tokens per unit of gas. It dynamically changes in
the range between 0.1 NEAR per Pgas and 1 NEAR per Pgas, based on demand. (1
Pgas = 1000 Tgas corresponds to a full chunk.)

The block producer has to set this field following the exact formula as defined
by the protocol. Otherwise, the produced block is invalid.

Intuitively, the formula checks how much gas was used compared to the total
capacity. If it exceeds 50%, the gas price increases exponentially within the
limits. When the demand is below 50%, it decreases exponentially. In practice,
it stays at the bottom most of the time.

Note that all shards share the same gas price. Hence, if one out of four shards
is at 100% capacity, this will not cause the price to increase. The 50% capacity
is calculated as an average across all shards.

Going slightly off-topic, it should also be mentioned that chunk capacity is not
constant. Chunk producers can change it by 0.1% per chunk. The nearcore client
does not currently make use of this option, so it really is a nitpick only
relevant in theory. However, any client implementation such as nearcore must
compute the total capacity as the sum of gas limits stored in the chunk headers
to be compliant. Using a hard-coded `1000 Tgas * num_shards` would lead to
incorrect block header validation.


### Pessimistic Gas Price

The pessimistic gas price calculation uses the fact that any transaction can
only have a limited depth in the generated receipt DAG. For most actions, the
depth is a constant 1 or 2. For function call actions, it is limited to a
hand-wavy `attached_gas` / `min gas per function call`. (Note: `attached_gas` is
a property of a single action and is only a part of the total gas costs of a
receipt.)

Once the maximum depth is known, the protocol assumes that the gas price will
not change more than 3% per receipt. This is not a guarantee since receipts can
be delayed for virtually unlimited blocks.

The final formula for the pessimistic gas price is the following.

```txt
pessimistic(current_gas_price, max_depth) = current_gas_price × 1.03^max_depth
```

This still is not the price at which gas is purchased. But we are very close.


### Effective Gas Purchase Cost 

When a transaction is converted to its root action receipt, the gas costs are
calculated in two parts.

Part one contains all the gas which is burnt immediately. Namely, the `send`
costs for a receipt and all the actions it includes. This is charged at the
current block-level gas price.

Part two is everything else, from execution costs of actions that are statically
known such as `CreateAccount` all the way to `attached_gas` for function calls.
All of this is purchased at the same pessimistic gas price, even if some actions
inside might have a lower maximum call depth than others.

The deducted tokens are the sum of these two parts. If the account has
insufficient balance to pay for this pessimistic pricing, it will fail with a
`NotEnoughBalance` error, with the required balance included in the error
message.

## Tracking Gas in Receipts

The previous section explained how gas is bought and what determines its price.
This section details the tracking that enables correct refunds.

First, when a `SignedTransaction` is converted to a receipt, the pessimistic gas
price is written to the receipt's `gas_price` field.

Later on, when the receipt has been executed, a gas refund is created at the
value of `receipt.gas_burnt` * (`block_header.gas_price` - `receipt.gas_price`).

Some gas goes attaches to outgoing receipts. We commonly refer to this as used
gas that was not burnt, yet. The refund excludes this gas. But it includes the
receipt send cost.

Finally, unspent gas is refunded at the full `receipt.gas_price`. This refund is
merged with the refund for burnt gas of the same receipt outcome to reduce the
number of spawned system receipts. But it makes it a bit harder to interpret
refunds when backtracking for how much gas a specific refund receipt covers.
