# Financial Transaction

Suppose Alice wants to transfer 100 tokens to Bob.
In this case we are talking about native Near Protocol tokens, oppose to user-defined tokens implemented through a smart contract.
There are several way this can be done:

- Direct transfer through a transaction containing transfer action;
- Alice calling a smart contract that in turn creates a financial transaction towards Bob.

In this section we are talking about the former simpler scenario.

## Pre-requisites

For this to work both Alice and Bob need to have _accounts_ and an access to them through
_the full access keys_.

Suppose Alice has account `alice_near` and Bob has account `bob_near`. Also, some time in the past,
each of them has created a public-secret key-pair, saved the secret key somewhere (e.g. in a wallet application)
and created a full access key with the public key for the account.

We also need to assume that both Alice and Bob have some number of tokens on their accounts. Alice needs >100 tokens on her account
so that she could transfer 100 tokens to Bob, but also Alice and Bob need to have some tokens on balance to pay for the data they store on-chain for their accounts -- which is essentially the refundable cost of the storage occupied by the account in the Near Protocol network.

## Creating a transaction

To send the transaction neither Alice nor Bob need to run a node.
However, Alice needs a way to create and sign a transaction structure.
Suppose Alice uses near-shell or any other third-party tool for that.
The tool then creates the following structure:

```
Transaction {
    signer_id: "alice_near",
    public_key: "ed25519:32zVgoqtuyRuDvSMZjWQ774kK36UTwuGRZMmPsS6xpMy",
    nonce: 57,
    receiver_id: "bob_near",
    block_hash: "CjNSmWXTWhC3EhRVtqLhRmWMTkRbU96wUACqxMtV1uGf",
    actions: vec![
        Action::Transfer(TransferAction {deposit: 100} )
    ],
}
```

Which contains one token transfer action, the id of the account that signs this transaction (`alice_near`)
the account towards which this transaction is addressed (`bob_near`). Alice also uses the public key
associated with one of the full access keys of `alice_near` account.

Additionally, Alice uses the _nonce_ which is unique value that allows Near Protocol to differentiate the transactions (in case there are several transfers coming in rapid
succession) which should be strictly increasing with each transaction. Unlike in Ethereum, nonces are associated with access keys, oppose to
the entire accounts, so several users using the same account through different access keys need not to worry about accidentally
reusing each other's nonces.

The block hash is used to calculate a transaction's "freshness".  It
is used to make sure a transaction does not get lost (let's say
somewhere in the network) and then arrive days, weeks, or years later
when it is not longer relevant and would be undesirable to execute.
A transaction does not need to arrive at a specific block, instead
it is required to arrive within a certain number of blocks from the block
identified by the `block_hash`.  Any transaction arriving outside this
threshold is considered to be invalid.  Allowed delay is defined by
`transaction_validity_period` option in the chain’s genesis file.  On
mainnet, this value is 86400 (which corresponds to roughly a day) and
on testnet it is 100.

near-shell or other tool that Alice uses then signs this transaction, by: computing the hash of the transaction and signing it
with the secret key, resulting in a `SignedTransaction` object.

## Sending the transaction

To send the transaction, near-shell connects through the RPC to any Near Protocol node and submits it.
If users wants to wait until the transaction is processed they can use `send_tx_commit` JSONRPC method which waits for the
transaction to appear in a block. Otherwise the user can use `send_tx_async`.

## Transaction to receipt

We skip the details on how the transaction arrives to be processed by the runtime, since it is a part of the blockchain layer
discussion.
We consider the moment where `SignedTransaction` is getting passed to `Runtime::apply` of the
`runtime` crate.
`Runtime::apply` immediately passes transaction to `Runtime::process_transaction`
which in turn does the following:

- Verifies that transaction is valid;
- Applies initial reversible and irreversible charges to `alice_near` account;
- Creates a receipt with the same set of actions directed towards `bob_near`.

The first two items are performed inside `Runtime::verify_and_charge_transaction` method.
Specifically it does the following checks:

- Verifies that the signature of the transaction is correct based on the transaction hash and the attached public key;
- Retrieves the latest state of the `alice_near` account, and simultaneously checks that it exists;
- Retrieves the state of the access key of that `alice_near` used to sign the transaction;
- Checks that transaction nonce is greater than the nonce of the latest transaction executed with that access key;
- Subtracts the `total_cost` of the transaction from the account balance, or throws `InvalidTxError::NotEnoughBalance`. If the transaction is part of a transaction by a FunctionCall Access Key, subtracts the `total_cost` from the `allowance` or throws `InvalidAccessKeyError::NotEnoughAllowance`;
- Checks whether the `signer` account has insufficient balance for the storage deposit and throws `InvalidTxError::LackBalanceForState` if so
- If the transaction is part of a transaction by a FunctionCall Access Key, throws `InvalidAccessKeyError::RequiresFullAccess`;
- Updates the `alice_near` account with the new balance and the used access key with the new nonce;

If any of the above operations fail all of the changes will be reverted.

## Processing receipt

The receipt created in the previous section will eventually arrive to a runtime on the shard that hosts `bob_near` account.
Again, it will be processed by `Runtime::apply` which will immediately call `Runtime::process_receipt`.
It will check that this receipt does not have data dependencies (which is only the case of function calls) and will then call `Runtime::apply_action_receipt` on `TransferAction`.

`Runtime::apply_action_receipt` will perform the following checks:

- Retrieves the state of `bob_near` account, if it still exists (it is possible that Bob has deleted his account concurrently with the transfer transaction);
- Computes the cost of processing a receipt and a transfer action;
- Computes how much reward should be burnt;
- Checks if `bob_near` still exists and if it does, deposits the transferred tokens to `bob_near`;
- Checks if `bob_near` still exists and if so, deposits the gas reward to the `bob_near` account;
- Checks if `bob_near` has enough balance for the storage deposit, the transaction's temporary state is dropped/rolled back if there is not enough balance;
