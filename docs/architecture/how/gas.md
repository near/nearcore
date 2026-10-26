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
    - [Contract Reward](#contract-reward): How smart contracts earned a reward up to protocol version 86.
2. [Gas Price](#gas-price):
    - [Block-Level Gas Price](#block-level-gas-price): How the block-level gas price is determined.
    - [Pessimistic Gas Price](#pessimistic-gas-price): How worst-case gas pricing is estimated.
    - [Gas Refund Fee](#gas-refund-fee): The cost paid for a gas refund receipt.
3. [Tracking Gas](#tracking-gas-in-receipts): How the system keeps track of purchased gas during the transaction execution.

## Gas Flow

On the highest level, gas is bought by the signer and burnt during execution. Up
to protocol version 86, contracts also receive a part of the burnt gas as a
reward. We will discuss each step in more details.

### Buying Gas for a Transaction

A signer pays all the gas required for a transaction upfront. However, there is
no explicit act of buying gas. Instead, the fee is subtracted directly in NEAR
tokens from the balance of the signer's account. The fee is calculated from the
`gas amount` and the `gas price`. The gas amount for actions included in a `SignedTransaction`
are all fixed, except for function calls where the user needs to specify the attached
gas amount for the dynamic execution part.
(See [here](https://docs.near.org/protocol/transactions/gas#cost-for-common-actions) for more details.)

The gas burnt for converting the `SignedTransaction` into a receipt is charged
at the current `gas price`. All other gas, including the gas attached to
function calls, is bought at the receipt gas price. From protocol version 85
(nearcore 2.13.0), the receipt gas price is the higher of the `gas price` and
`min_gas_purchase_price`, which is 1 NEAR per Pgas (10^9 yoctoNEAR per gas).
From protocol version 78 to 84, it was equal to the `gas price`. Unused gas is refunded later, and
so is the price difference for gas that is burnt at a lower price.

If the account has insufficient balance to pay for the gas and the attached
deposits, it will fail with a `NotEnoughBalance` error, with the required
balance included in the error message.

From protocol version 85 (nearcore 2.13.0), a transaction can also be signed
with a gas key. Then the gas is paid from the balance stored on the gas key,
and the deposits are paid from the account balance. If the gas key balance is
too small, the transaction fails with a `NotEnoughGasKeyBalance` error. Gas
refunds for such a transaction go back to the gas key balance. If the gas key
no longer exists, they go to the account balance.

The `gas amount` is not a field of `SignedTransaction`, nor is it something the
signer can choose. It is only a virtual field that is computed on-chain following
the protocol's rules.

The `gas price` is a variable that may change during the execution of the
transaction. The way it is implemented today, a single transaction can be
charged a different gas price for different receipts.

Already we can see a fundamental problem: Gas is bought once at the beginning
but the gas price may change during execution. To solve this incompatibility,
the protocol used to calculate a pessimistic gas price for the initial purchase.
Later on, the delta between real and pessimistic gas prices would be refunded at
the end of every receipt execution.

Generating a refund receipts for every executed function call has become a
non-trivial overhead, limiting total throughput. Therefore, with the adoption of
[NEP-536](https://github.com/near/NEPs/pull/536) the model was changed.

From protocol version 78 to 84, the protocol simply ignored gas price changes
between time of purchase and time of use. A transaction bought gas at one price
and burnt it at the same price. While this avoided the need for refunds due to
price changes, it also meant that transactions with deep receipt calls could end
up with a cheaper gas price competing for the same chunk space. The community
took note of this trade-off and agreed to take it.

From protocol version 85 (nearcore 2.13.0), with the adoption of
[NEP-642](https://github.com/near/NEPs/pull/642), gas is no longer bought and
burnt at the same price. The gas attached to a receipt is bought at the higher
of the block gas price and `min_gas_purchase_price`, which is 1 NEAR per Pgas.
Each receipt burns gas at the lower of this purchase price and the gas price of
the block that executes it. The price difference for the burnt gas is refunded
together with the unused gas. So, unlike from protocol version 78 to 84, almost
every receipt again produces a gas refund receipt when the gas price is below
`min_gas_purchase_price`. If the receipt creates an account, part of this
price difference is kept and burnt as the account creation charge. At the usual
gas price, the execution fee for creating the account and this charge add up to
`account_creation_charge` (0.007 NEAR). A transaction that creates no account
costs the same as before at the usual gas price. But the signer needs a larger
balance upfront, because `min_gas_purchase_price` is 10 times the lowest block
gas price of 0.1 NEAR per Pgas.

### Burning Gas

Buying gas immediately removes a part of the signer's tokens from the total
supply. However, the equivalent value in gas still exists in the form of the
receipt and the unused gas will be converted back to tokens as a refund after
subtracting a small gas refund fee. (More on the fee further down.)

The gas spent on execution on the other hand is burnt and removed from total
supply forever. Unlike gas in other chains, none of it goes to validators. This
is roughly equivalent to the base fee burning mechanism which Ethereum added in
[EIP-1559](https://eips.ethereum.org/EIPS/eip-1559). But in Near Protocol, the
entire fee is burnt, whereas in Ethereum the [priority
fee](https://ethereum.org/en/developers/docs/gas/#priority-fee) goes to
validators.

The following diagram shows how gas flows through the execution of a
transaction. The transaction consists of a function call performing a cross
contract call, hence two function calls in sequence. (Note: This diagram is
slightly simplified, more accurate diagrams are further down.)

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

The gas attached to function calls can be at most `max_total_prepaid_gas`. It
was 300 Tgas from the mainnet launch, and it is 1 Pgas (1000 Tgas) since
protocol version 83 (nearcore 2.11.0). Note that this limit applies to the
total gas attached to all function calls in one `SignedTransaction` or one
receipt, not to each function call. In other words, batched function calls
share this limit. Function calls inside a delegate action count toward it too.

There is also a limit to how much single call can burn, `max_gas_burnt`, which
used to be 200 Tgas, was increased to 300 Tgas in protocol version 52, and to
1 Pgas in protocol version 83. (Note: When attaching gas to an outgoing
function call, this is not counted as gas burnt.) However, given a call can
never burn more than was attached anyway, this second limit is obsolete with
the current configuration where the two limits are equal.

Since protocol version 53, with the stabilization of
[NEP-264](https://github.com/near/NEPs/blob/master/neps/nep-0264.md), contract
developers do not have to specify the absolute amount of gas to attach to calls.
`promise_batch_action_function_call_weight` allows to specify a ratio of unspent
gas that is computed after the current call has finished. This allows attaching
100% of unspent gas to a call. If there are multiple calls, this allows
attaching an equal fraction to each, or any other split as defined by the weight
per call.

### Gas in External Contract Calls

External contract calls (the `ExternalContractCalls` protocol feature, not yet
stable) let a contract authorize a call from a user who has no balance. The
contract pays for the gas, so there is no `attached_gas` and the
`prepaid_gas()` host function fails. Instead, two limits apply:

- The call can burn at most `max_gas_burnt_external`, 20 Tgas. This bounds the
  work done before the contract has decided to accept the call.
- The gas burnt plus the gas attached to outgoing promises can be at most
  `max_total_prepaid_gas`.

The contract pays for all the gas the call uses from its balance, at the gas
price of the call: the gas it burns, the gas attached to function calls, and
the execution fees of all actions and receipts. The balance takes the place of
the prepaid gas, so the call fails with `GasExceeded` once it uses more gas
than the balance pays for, and spending the balance on deposits lowers that
limit. A promise gets exactly the gas it was created with: unspent gas is not
distributed by weight, because there is no prepaid gas to distribute.

### Contract Reward

A rather unique property of Near Protocol is that a part of the gas fee goes to
the contract owner. This "smart contract gets paid" model is pretty much the
opposite design choice from the "smart contract pays" model that for example
[Cycles in the Internet
Computer](https://internetcomputer.org/docs/current/developer-docs/gas-cost#details-cost-of-compute-and-storage-transactions-on-the-internet-computer)
implement.

From protocol version 87 (nearcore 2.14.0), this is no longer the case.
Contracts receive no part of the gas fee, and all burnt gas is removed from the
total supply. The rest of this section describes the contract reward up to
protocol version 86.

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
   [`burnt_gas_reward`](../../../core/parameters/res/runtime_configs/parameters.snap#L6)
   which is 30% up to protocol version 86. From protocol version 87 (nearcore
   2.14.0), it is 0%.

During receipt execution, nearcore code tracks the `gas_burnt_for_function_call`
separately from other gas burning to enable this contract reward calculations.

In the (still slightly simplified) flow diagram, the contract reward looks like this.
For brevity, `gas_burnt_for_function_call` in the diagram is denoted as `wasm fee`.

![Slightly Simplified Gas Flow Diagram](https://github.com/near/nearcore/assets/6342444/32600ef0-1475-43af-b196-576317787578)
<!-- Editable source: https://github.com/near/nearcore/issues/7821#issuecomment-1705673349 -->

## Gas Price

Gas pricing is a surprisingly deep and complicated topic. Usually, we only think
about the value of the `gas_price` field in the block header. However, to
understand the internals, this is not enough.

### Block-Level Gas Price

`gas_price` is a field in the block header. It determines how much it costs to
buy gas at the given block height.

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

The pessimistic gas price features was removed with protocol version 78 and
[NEP-536](https://github.com/near/NEPs/pull/536).

### Gas Refund Fee

After executing the transaction, there might be unspent gas left. For function
calls, this is the normal case, since attaching exactly the right amount of gas
is tricky. Additionally, in case of receipt failure, some actions that did not
start execution will have the execution gas unspent. This gas is converted back
to NEAR tokens and sent as a refund transfer to the original signer. If the
transaction was signed with a gas key, the refund goes to the balance of that
gas key.

Before protocol version 78, the full gas amount would be refunded and the refund
transfer action was executed for free. With [NEP-536](https://github.com/near/NEPs/pull/536)
which was implemented in version 78, the plan was to have the network charge a
fee of 5% of unspent gas or a minimum of 1 Tgas. This often removes the need to
send a refund, which avoids additional load on the network, and it compensates
for the load of the refund receipt when it needs to be issued. This is intended
to discourage users from attaching more gas than needed to their transactions.
In the nearcore code, you will find this fee under the name `gas_refund_penalty`.

However, due to existing projects that need to attach more gas than they
actually use, the introduction of the new fee has been postponed. The code is
still in place but the parameters were set to 0 in PR
[#13579](https://github.com/near/nearcore/pull/13579). Among other problems, in
the reference FT implementation, `ft_transfer_call` requires at least 30 TGas
even if most of the time only a fraction of that is burnt. Once all known
problems are resolved, the plan is to try and introduce the 5% / 1 Tgas parameters.

Technically, even when the parameters are increased again, the refund transfer
is still executed for free, since the gas price is set to 0. This keeps it in
line with balance refunds. However, when the gas refund is produced, at least 1
Tgas has been burnt on receipt execution, which more than covers for a transfer.

Finally, we can have a complete diagram, including the gas refund penalty.

![Complete Gas Flow
Diagram](https://github.com/user-attachments/assets/28aea744-979d-4e1b-88b5-541b38ce58ca)
<!-- Editable source: https://github.com/near/nearcore/issues/7821#issuecomment-2867060321 -->
