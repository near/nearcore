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

1. [Gas Flow](#gas-flow)
    - [Buying Gas](#buying-gas-for-a-transaction): How are NEAR tokens converted to gas?
    - [Burning Gas](#burning-gas): Who receives burnt tokens?
    - [Gas in Contract Calls](#gas-in-contract-calls): How is gas attached to calls?
    - [Contract Reward](#contract-reward): How smart contract earn a reward.
2. [Gas Price](#gas-price):
    - [Block-Level Gas Price](#block-level-gas-price): How the block-level gas price is determined.
    - [Pessimistic Gas Price](#pessimistic-gas-price): How worst-case gas pricing is estimated.
    - [Effective Gas Purchase Cost](#effective-gas-purchase-cost): The cost paid for a receipt.
3. [Tracking Gas](#tracking-gas-in-receipts): How the system keeps track of purchased gas during the transaction execution.

## Gas Flow

On the highest level, gas is bought by the signer, burnt during execution, and
contracts receive a part of the burnt gas as a reward. We will discuss each step
in more details.

### Buying Gas for a Transaction

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

### Burning Gas

Buying gas immediately removes a part of the signer's tokens from the total
supply. However, the equivalent value in gas still exists in the form of the
receipt and the unused gas will be converted back to tokens as a refund.

The gas spent on execution on the other hand is burnt and removed from total
supply forever. Unlike gas in other chains, none of it goes to validators. This
is roughly equivalent to the base fee burning mechanism which Ethereum added in
[EIP-1559](https://eips.ethereum.org/EIPS/eip-1559). But in Near Protocol, the
entire fee is burnt because there is no [priority
fee](https://ethereum.org/en/developers/docs/gas/#priority-fee) that Ethereum
pays out to validators.

The following diagram shows how gas flows through the execution of a
transaction. The transaction consists of a function call performing a cross
contract call, hence two function calls in sequence. (Note: This diagram is
heavily simplified, more accurate diagrams are further down.)

![Very Simplified Gas Flow Diagram](https://github.com/near/nearcore/assets/6342444/f52c6e4b-6fca-4f61-8e6e-ac786076aa65)
<!-- Editable source: https://github.com/near/nearcore/issues/7821#issuecomment-1705672850 -->

### Gas in Contract Calls

A function call has a fixed gas cost to be initiated. Then the execution itself
draws gas from the `attached_gas`, sometimes also called `prepaid_gas`, until it
reaches zero, at which point the function call aborts with a `GasExceeded`
error. No changes are persisted on chain.

(_Note on naming: If you see `prepaid_fee: Balance` in the nearcore code base,
this is NOT only the fee for `prepaid_gas`. It also includes prepaid fees for
other gas costs. However, `prepaid_gas: Gas` is used the same in the code base
as described in this document._)

Attaching gas to function calls is the primary way for end-users and contract
developers to interact with gas. All other gas fees are implicitly computed and
are hidden from the users except for the fact that the equivalent in tokens is
removed from their account balance.

To attach gas, the signer sets the gas field of the function call action.
Wallets and CLI tools expose this to the users in different ways. Usually just
as a `gas` field, which makes users believe this is the maximum gas the
transaction will consume. Which is not true, the maximum is the specified number
plus the fixed base cost.

Contract developers also have to pick the attached gas values when their
contract calls another contract. They cannot buy additional gas, they have to
work with the unspent gas attached to the current call. They can check how much
gas is left by subtracting the `used_gas()` from the `prepaid_gas()` host
function results. But they cannot use all the available gas, since that would
prevent the current function call from executing to the end.

The gas attached to a function can be at most `max_total_prepaid_gas`, which is
300 Tgas since the mainnet launch. Note that this limit is per
`SignedTransaction`, not per function call. In other words, batched function
calls share this limit.

There is also a limit to how much single call can burn, `max_gas_burnt`, which
used to be 200 Tgas but has been increased to 300 Tgas in protocol version 52.
(Note: When attaching gas to an outgoing function call, this is not counted as
gas burnt.) However, given a call can never burn more than was attached anyway,
this second limit is obsolete with the current configuration where the two limits
are equal.

Since protocol version 53, with the stabilization of
[NEP-264](https://github.com/near/NEPs/blob/master/neps/nep-0264.md), contract
developers do not have to specify the absolute amount of gas to attach to calls.
`promise_batch_action_function_call_weight` allows to specify a ratio of unspent
gas that is computed after the current call has finished. This allows attaching
100% of unspent gas to a call. If there are multiple calls, this allows
attaching an equal fraction to each, or any other split as defined by the weight
per call.

### Contract Reward

A rather unique property of Near Protocol is that a part of the gas fee goes to
the contract owner. This "smart contract gets paid" model is pretty much the
opposite design choice from the "smart contract pays" model that for example
[Cycles in the Internet
Computer](https://internetcomputer.org/docs/current/developer-docs/gas-cost#details-cost-of-compute-and-storage-transactions-on-the-internet-computer)
implement.

The idea is that it gives contract developers a source of income and hence an
incentive to create useful contracts that are commonly used. But there are also
downsides, such as when implementing a free meta-transaction relayer one has to
be careful not to be susceptible to faucet-draining attacks where an attacker
extracts funds from the relayer by making calls to a contract they own.

How much contracts receive from execution depends on two things.

1. How much gas is burnt on the function call execution itself. That is, only
   the gas taken from the `attached_gas` of a function call is considered for
   contract rewards. The base fees paid for creating the receipt, including the
   `action_function_call` fee, are burnt 100%.
2. The remainder of the burnt gas is multiplied by the runtime configuration
   parameter
   [`burnt_gas_reward`](../../../core/parameters/res/runtime_configs/parameters.snap#L5C5-L5C5)
   which currently is at 30%.

During receipt execution, nearcore code tracks the `gas_burnt_for_function_call`
separately from other gas burning to enable this contract reward calculations.

In the (still simplified) flow diagram, the contract reward looks like this.
For brevity, `gas_burnt_for_function_call` in the diagram is denoted as `wasm fee`.

![Slightly Simplified Gas Flow Diagram](https://github.com/near/nearcore/assets/6342444/32600ef0-1475-43af-b196-576317787578)
<!-- Editable source: https://github.com/near/nearcore/issues/7821#issuecomment-1705673349 -->

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
the range between 0.1 NEAR per Pgas and 2 NEAR per Pgas, based on demand. (1
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

Inserting the pessimistic gas pricing into the flow diagram, we finally have a
complete picture. Note how an additional refund receipt is required. Also, check
out the updated formula for the effective purchase price at the top left and the
resulting higher number.

![Complete Gas Flow
Diagram](https://github.com/near/nearcore/assets/6342444/8341fb45-9beb-4808-8a89-8144fa075930)
<!-- Editable source: https://github.com/near/nearcore/issues/7821#issuecomment-1705673807 -->

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
