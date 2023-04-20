# Gas Cost Parameters

Gas in NEAR Protocol solves two problems.

1. To avoid spam, validator nodes only perform work if a user's tokens are
   burned. Tokens are automatically converted to gas using the current gas
   price.
2. To synchronize shards, they must all produce chunks following a strict
   schedule of 1 second execution time. Gas is used to measure how heavy the
   workload of a transaction is, so that the number of transactions that fit in
   a block can be deterministically computed by all nodes.

In other words, each transaction costs a fixed amount of gas. This gas cost
determines how much a user has to pay and how much time nearcore has to execute
the transaction.

What happens if nearcore executes a transaction too slowly? Chunk production for
the shard gets delayed, which delays block production for the entire blockchain,
increasing latency and reducing throughput for everybody. If the chunk is really
late, the block producer will decide to not include the chunk at all and inserts
an empty chunk. The chunk may be included in the next block.

By now, you probably wonder how we can know the time it takes to execute a
transaction, given that validators use hardware of their choice. Getting these
timings right is indeed a difficult problem. Or flipping the problem, assuming
the timings are already known, then we must implement nearcore such that it
guarantees to operate within the given time constraints. How we tackle this is
the topic of this chapter.

If you want to learn more about Gas from a user perspective, 
[Gas basic concepts](https://docs.near.org/concepts/basics/transactions/gas),
[Gas advanced concepts](https://docs.near.org/concepts/basics/transactions/gas-advanced),
and [the runtime fee specification](https://nomicon.io/RuntimeSpec/Fees/) are
good places to dig deeper.

## Hardware and Timing Assumptions

For timing to make sense at all, we must first define hardware constraints. The
official hardware requirements for a validator are published on
[near-nodes.io/validator/hardware](https://near-nodes.io/validator/hardware). They
may change over time but the main principle is that a moderately configured,
cloud-hosted virtual machine suffices.

For our gas computation, we assume the minimum required hardware. Then we define
10<sup>15</sup> gas to be executed in at most 1s. We commonly use 1 Tgas (=
10<sup>12</sup> gas) in conversation, which corresponds to 1ms execution time.

Obviously, this definition means that a validator running more powerful hardware
will execute the transactions faster. That is perfectly okay, as far as the
protocol is concerned we just need to make sure the chunk is available in time.
If it is ready in even less time, no problem.

Less obviously, this means that even a minimally configured validator is often
idle. Why is that? Well, the hardware must be prepared to execute chunks that
are always full. But that is rarely the case, as the gas price increases
exponentially when chunks are full, which would cause traffic to go back
eventually.

Futhermore, the hardware has to be ready for transactions of all types,
including transactions chosen by a malicious actor selecting only the most
complex transactions. Those transactions can also be unbalanced in what
bottlenecks they hit. For example, a chunk can be filled with transactions that
fully utilize the CPU's floating point units. Or they could be using all the
available disk IO bandwidth.

Because the minimum required hardware needs to meet the timing requirements for
any of those scenarios, the typical, more balanced case is usually computed
faster than the gas rule states.

## Transaction Gas Cost Model

A transaction is essentially just a list of actions to be executed on the same
account. For example it could be `CreateAccount` combined with
`FunctionCall("hello_world")`.

The [reference for available actions](https://nomicon.io/RuntimeSpec/Actions)
shows the conclusive list of possible actions. The protocol defines fixed fees
for each of them. More details on [actions fees](#action-costs) follow below.

Fixed fees are an important design decision. It means that a given action will
always cost the exact same amount of gas, no matter on what hardware it
executes. But the content of the action can impact the cost, for example a
`DeployContract` action's cost scales with the size of the contract code.

So, to be more precise, the protocol defines fixed gas cost *parameters* for
each action, together with a formula to compute the gas cost for the action. All
actions today either use a single fixed gas cost or they use a base cost and a
linear scaling parameter. With one important exception, `FunctionCall`, which
shall be discussed [further below](#fn-call-costs).

There is an entire section on [Parameter Definitions](./parameter_definition.md)
that explains how to find the source of truth for parameter values in the
nearcore repository, how they can be referenced in code, and what steps are
necessary to add a new parameter.

Let us dwell a bit more on the linear scaling factors. The fact that contract
deployment cost, which includes code compilation, scales linearly limits the
compiler to use only algorithms of linear complexity. Either that, or the
parameters must be set to match the 1ms = 1Tgas rule at the largest possible
contract size. Today, we limit ourselves to linear-time algorithms in the
compiler.

Likewise, an action that has no scaling parameters must only use constant time
to execute. Taking the `CreateAcccount` action as an example, with a cost of 0.1
Tgas, it has to execute within 0.1ms. Technically, the execution time depends
ever so slightly on the account name length. But there is a fairly low upper
limit on that length and it makes sense to absorb all the cost in the constant
base cost.

This concept of picking parameters according to algorithmic complexity is key.
If you understand this, you know how to think about gas as a nearcore developer.
This should be enough background to understand what the estimator does.

The [runtime parameter estimator](./estimator.md) is a separate binary within
the nearcore repository. It contains benchmarking-like code used to validate
existing parameter values against the 1ms = 1 Tgas rule. When implementing new
features, code should be added there to estimate the safe values of the new
parameters. This section is for you if you are adding new features such as a new
pre-compiled method or other host functions.

Next up are more details on the specific costs that occur when executing NEAR
transactions, which help to understand existing parameters and how they are
organized.

## Action Costs

Actions are executed in two steps. First, an action is verified and inserted to
an action receipt, which is sent to the receiver of the action. The `send` fee
is paid for this. It is charged either in `fn process_transaction(..)` if the
action is part of a fresh transaction, or inside
[logic.rs](https://github.com/near/nearcore/blob/14b8ae2c7465444c9b672a23b044c00be98f6e34/runtime/near-vm-logic/src/logic.rs)
through `fn pay_action_base(..)` if the action is generated by a function call.
The send fee is meant to cover the cost to validate an action and transmit it
over the network.

The second step is action execution. It is charged in `fn apply_action(..)`.
The execution cost has to cover everything required to apply the action to the
blockchain's state.

These two steps are done on the same shard for local receipts. Local receipts
are defined as those where the sender account is also the receiver, abbreviated
as `sir` which stands for "sender is receiver".

For remote receipts, which is any receipt where the sender and receiver accounts
are different, we charge a different fee since sending between shards is extra
work. Notably, we charge that extra work even if the accounts are on the same
shard. In terms of gas costs, each account is conceptually its own shard. This
makes dynamic resharding possible without user-observable impact.

When the send step is performed, the minimum required gas to start execution of
that action is known. Thus, if the receipt has not enough gas, it can be aborted
instead of forwarding it. Here we have to introduce the concept of used gas.

`gas_used` is different from `gas_burnt`. The former includes the gas that needs
to be reserved for the execution step whereas the latter only includes the gas
that has been burnt in the current chunk. The difference between the two is
sometimes also called prepaid gas, as this amount of gas is paid for during the
send step and it is available in the execution step for free.

If execution fails, the prepaid cost that has not been burned will be refunded.
But this is not the reason why it must burn on the receiver shard instead of the
sender shard. The reason is that we want to properly compute the gas limits on
the chunk that does the execution work.

In conclusion, each action parameter is split into three costs, `send_sir`,
`send_not_sir`, and `execution`. Local receipts charge the first and last
parameters, remote receipts charge the second and third. They should be
estimated, defined, and charged separately. But the reality is that today almost
all actions are estimated as a whole and the parameters are split 50/50 between
send and execution cost, without discrimination on local vs remote receipts
i.e. `send_sir` cost is the same as `send_not_sir`.

The [Gas Profile](./gas_profile.md) section goes into more details on how gas
costs of a transaction are tracked in nearcore.

## Dynamic Function Call Costs
<a name="fn-call-costs"></a>

Costs that occur while executing a function call on a deployed WASM app (a.k.a.
smart contract) are charged only at the receiver. Thus, they have only one value
to define them, in contrast to action costs.

The most fundamental dynamic gas cost is `wasm_regular_op_cost`. It is
multiplied with the exact number of WASM operations executed. You can read about
[Gas Instrumentation](https://nomicon.io/RuntimeSpec/Preparation#gas-instrumentation)
if you are curious how we count WASM ops.

Currently, all operations are charged the same, although it could be more
efficient to charge less for opcodes like `i32.add` compared to `f64.sqrt`.

The remaining dynamic costs are for work done during host function calls. Each
host function charges a base cost. Either the general `wasm_base` cost, or a
specific cost such as `wasm_utf8_decoding_base`, or sometimes both. New host
function calls should define a separate base cost and not charge `wasm_base`.

Additional host-side costs can be scaled per input byte, such as
`wasm_sha256_byte`, or costs related to moving data between host and guest, or
any other cost that is specific to the host function. Each host function must
clearly define what its costs are and how they depend on the input.

## Non-gas parameters

Not all runtime parameters are directly related to gas costs. Here is a brief
overview.

- **Gas economics config:** Defines the conversion rate when purchasing gas with
  NEAR tokens and how gas rewards are split.
- **Storage usage config:** Costs in tokens, not gas, for storing data on chain.
- **Account creation config:** Rules for account creation.
- **Smart contract limits:** Rules for WASM execution.

None of the above define any gas costs directly. But there can be interplay
between those parameters and gas costs. For example, the limits on smart
contracts changes the assumptions for how slow a contract compilation could be,
hence it affects the deploy action costs.
