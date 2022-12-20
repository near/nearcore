# Meta Transactions

[NEP-366](https://github.com/near/NEPs/pull/366) introduced the concept of meta
transactions to Near Protocol. The idea is to construct and sign a transaction
off-chain and allow someone else to release it on the chain. This someone else
is called the relayer, who will then pay for the gas and tokens costs of the
transaction. This ultimately allows executing transactions on NEAR without
owning gas or tokens.

## Overview

![Flow chart of meta
transactions](https://raw.githubusercontent.com/near/NEPs/003e589e6aba24fc70dd91c9cf7ef0007ca50735/neps/assets/nep-0366/NEP-DelegateAction.png)
_Credits for the diagram go to the NEP authors Alexander Fadeev and Egor
Uleyskiy._


TODO: Describe the flow chart

## Gas costs for meta transactions

Meta transactions challenge the traditional ways of charging gas for actions. To
see why, let's first list the normal flow of gas, outside of meta transactions.

1. Gas is purchased by the transaction signer when it is converted to a receipt.
   This happens as part of `verify_and_charge_transaction` which gets called in
   `process_transaction`.
2. For all actions listed inside the transaction, the `SEND` cost is burned
   immediately. Depending on the condition `sender == receiver`, one of two
   possible `SEND` costs is chosen. The `EXEC` cost is not burned, yet. But it
   is implicitly part of the transaction cost. The third and last part of the
   transaction cost is the gas attached to function calls. The attached gas is
   also called prepaid gas. (Not to be confused with `total_prepaid_exec_fees`
   which is the implicitly prepaid gas for `EXEC` action costs.)
3. On the receiver shard, `EXEC` costs are burned before the execution of an
   action starts. Should the execution fail and abort the transaction, the
   remaining gas will be refunded to the signer of the transaction.

Ok, now adapt for meta transactions. Let's assume Alice uses a relayer to
execute actions with Bob as the receiver.

1. The relayer purchases the gas for all inner actions, plus the gas for the
   delegate action wrapping them.
2. The cost of sending the inner actions and the delegate action from the
   relayer to Alice's shard will be burned immediately. The condition `relayer
   == Alice` determines which action `SEND` cost is taken (`sir` or `not_sir`).
3. On Alice's shard, the delegate action is executed, thus the `EXEC` gas cost
   for it is burned. Alice sends the inner actions to Bob's shard. Therefore, we
   burn the `SEND` fee again. This time based on `Alice == Bob` to figure out
   `sir` or `not_sir`.
4. On Bob's shard, we execute all inner actions and burn their `EXEC` cost.

Each of these steps should make sense and not be too surprising. But the
consequence is that the implicit costs paid at the relayer's shard are
`SEND(1)` + `SEND(2)` + `EXEC` for all inner actions plus `SEND(1)` + `EXEC` for
the delegate action. This might be surprising but hopefully with this
explanation it makes sense now!


## Balance refunds in meta transactions

Unlike gas refunds, the protocol sends balance refunds to the predecessor
(a.k.a. sender) of the receipt. This makes sense, as we deposit the attached
balance to the receiver, who has to explicitly reattach a new balance to new
receipts they might spawn.

In the world of meta transactions, this assumption is also challenged. If an
inner action requires an attached balance (for example a transfer action) then
this balance is taken from the relayer.

The relayer can see what the cost will be before submitting the meta transaction
and agrees to pay for it, so nothing wrong so far. But what if the transaction
fails execution on Bob's shard? At this point, the predecessor is `Alice` and
therefore she receives the token balance refunded, not the relayer. This is
something relayer implementations must be aware of since there is a financial
incentive for Alice to submit meta transactions that have high balances attached
but will fail on Bob's shard.