# Cross shard transactions - deep dive

In this article, we'll look deeper into how cross-shard transactions are working
on the simple example of user `shard0` transfering money to user `shard1`.

These users are on separate shards (`shard0` is on shard 0 and `shard1` is on
shard 1).

Imagine, we run the following command in the command line:

```console
$ NEAR_ENV=local near send shard0 shard1 500
```

What happens under the hood? How is this transaction changed into receipts and
processed by near?

## From Explorer perspective

If you look at a simple token transfer in explorer
([example](https://explorer.near.org/transactions/79gPsyYRG2xghr6oNLpMbdjP2jpafjVT35no9atS6zUf)),
you can see that it is broken into three separate sections:

* convert transaction into receipt ( executed in block B )
* receipt that transfers tokens ( executed in block B+1 )
* receipt that refunds gas ( executed in block B+2 )

But under the hood, the situation is a little bit more complex, as there is
actually one more receipt (that is created after converting the transaction).
Let's take a deeper look.

## Internal perspective (Transactions & Receipts)

One important thing to remember is that NEAR is sharded - so in all our
designs, we have to assume that each account is on a separate shard. So that the
fact that some of them are colocated doesn't give any advantage.

### Step 1 - Transaction

This is the part which we receive from the user (`SignedTransaction`) - it has 3
parts:

* signer (account + key) who signed the transaction
* receiver (in which account context should we execute this)
* payload - a.k.a Actions to execute.

As the first step, we want to change this transaction into a Receipt (a.k.a
'internal' message) - but before doing that, we must verify that:

* the message signature matches (that is - that this message was actually signed
  by this key)
* that this key is authorized to act on behalf of that account (so it is a full
  access key to this account - or a valid fuction key).

The last point above means, that we MUST execute this (Transaction to Receipt)
transition within the shard that the `signer` belongs to (as other shards don't
know the state that belongs to signer - so they don't know which keys it has).

So actually if we look inside the chunk 0 (where `shard0` belongs) at block
B, we'll see the transaction:

```
Chunk: Ok(
    V2(
        ShardChunkV2 {
            chunk_hash: ChunkHash(
                8mgtzxNxPeEKfvDcNdFisVq8TdeqpCcwfPMVk219zRfV,
            ),
            header: V3(
                ShardChunkHeaderV3 {
                    inner: V2(
                        ShardChunkHeaderInnerV2 {
                            prev_block_hash: CgTJ7FFwmawjffrMNsJ5XhvoxRtQPXdrtAjrQjG91gkQ,
                            prev_state_root: 99pXnYjQbKE7bEf277urcxzG3TaN79t2NgFJXU5NQVHv,
                            outcome_root: 11111111111111111111111111111111,
                            encoded_merkle_root: 67zdyWTvN7kB61EgTqecaNgU5MzJaCiRnstynerRbmct,
                            encoded_length: 187,
                            height_created: 1676,
                            shard_id: 0,
                            gas_used: 0,
                            gas_limit: 1000000000000000,
                            balance_burnt: 0,
                            outgoing_receipts_root: 8s41rye686T2ronWmFE38ji19vgeb6uPxjYMPt8y8pSV,
                            tx_root: HyS6YfQbfBRniVSbWRnxsxEZi9FtLqHwyzNivrF6aNAM,
                            validator_proposals: [],
                        },
                    ),
                    height_included: 0,
                    signature: ed25519:uUvmvDV2cRVf1XW93wxDU8zkYqeKRmjpat4UUrHesJ81mmr27X43gFvFuoiJHWXz47czgX68eyBN38ejwL1qQTD,
                    hash: ChunkHash(
                        8mgtzxNxPeEKfvDcNdFisVq8TdeqpCcwfPMVk219zRfV,
                    ),
                },
            ),
            transactions: [
                SignedTransaction {
                    transaction: Transaction {
                        signer_id: AccountId(
                            "shard0",
                        ),
                        public_key: ed25519:Ht8EqXGUnY8B8x7YvARE1LRMEpragRinqA6wy5xSyfj5,
                        nonce: 11,
                        receiver_id: AccountId(
                            "shard1",
                        ),
                        block_hash: 6d5L1Vru2c4Cwzmbskm23WoUP4PKFxBHSP9AKNHbfwps,
                        actions: [
                            Transfer(
                                TransferAction {
                                    deposit: 500000000000000000000000000,
                                },
                            ),
                        ],
                    },
                    signature: ed25519:63ssFeMyS2N1khzNFyDqiwSELFaUqMFtAkRwwwUgrPbd1DU5tYKxz9YL2sg1NiSjaA71aG8xSB7aLy5VdwgpvfjR,
                    hash: 6NSJFsTTEQB4EKNKoCmvB1nLuQy4wgSKD51rfXhmgjLm,
                    size: 114,
                },
            ],
            receipts: [],
        },
    ),
)
```

**Side note:** When we're converting the transaction into a receipt, we also use
this moment to deduct prepaid gas fees and transfered tokens from the 'signer'
account. The details on how much gas is charged can be found at https://nomicon.io/RuntimeSpec/Fees/.

## Step 2 - cross shard receipt

After transaction was changed into a receipt, this receipt must now be sent to
the shard where the `receiver` is (in our example `shard1` is on shard 1).

We can actually see this in the chunk of the next block:

```
Chunk: Ok(
    V2(
        ShardChunkV2 {
            chunk_hash: ChunkHash(
                DoF7yoCzyBSNzB8R7anWwx6vrimYqz9ZbEmok4eqHZ3m,
            ),
            header: V3(
                ShardChunkHeaderV3 {
                    inner: V2(
                        ShardChunkHeaderInnerV2 {
                            prev_block_hash: 82dKeRnE262qeVf31DXaxHvbYEugPUDvjGGiPkjm9Rbp,
                            prev_state_root: DpsigPFeVJDenQWVueGKyTLVYkQuQjeQ6e7bzNSC7JVN,
                            outcome_root: H34BZknAfWrPCcppcHSqbXwFvAiD9gknG8Vnrzhcc4w,
                            encoded_merkle_root: 3NDvQBrcRSAsWVPWkUTTrBomwdwEpHhJ9ofEGGaWsBv9,
                            encoded_length: 149,
                            height_created: 1677,
                            shard_id: 0,
                            gas_used: 223182562500,
                            gas_limit: 1000000000000000,
                            balance_burnt: 22318256250000000000,
                            outgoing_receipts_root: Co1UNMcKnuhXaHZz8ozMnSfgBKPqyTKLoC2oBtoSeKAy,
                            tx_root: 11111111111111111111111111111111,
                            validator_proposals: [],
                        },
                    ),
                    height_included: 0,
                    signature: ed25519:32hozA7GMqNqJzscEWzYBXsTrJ9RDhW5Ly4sp7FXP1bmxoCsma8Usxry3cjvSuywzMYSD8HvGntVtJh34G2dKJpE,
                    hash: ChunkHash(
                        DoF7yoCzyBSNzB8R7anWwx6vrimYqz9ZbEmok4eqHZ3m,
                    ),
                },
            ),
            transactions: [],
            receipts: [
                Receipt {
                    predecessor_id: AccountId(
                        "shard0",
                    ),
                    receiver_id: AccountId(
                        "shard1",
                    ),
                    receipt_id: 3EtEcg7QSc2CYzuv67i9xyZTyxBD3Dvx6X5yf2QgH83g,
                    receipt: Action(
                        ActionReceipt {
                            signer_id: AccountId(
                                "shard0",
                            ),
                            signer_public_key: ed25519:Ht8EqXGUnY8B8x7YvARE1LRMEpragRinqA6wy5xSyfj5,
                            gas_price: 103000000,
                            output_data_receivers: [],
                            input_data_ids: [],
                            actions: [
                                Transfer(
                                    TransferAction {
                                        deposit: 500000000000000000000000000,
                                    },
                                ),
                            ],
                        },
                    ),
                },
            ],
        },
    ),
)
```

**Side comment:** notice that the receipt itself no longer has a `signer` field, but
a `predecessor_id` one.

Such a receipt is sent to the destination shard (we'll explain this process in a
separate article) where it can be executed.
<!-- TODO: maybe add the link to that article here? -->

## 3. Gas refund.

When shard 1 processes the receipt above, it is then ready to refund the unused
gas to the original account (`shard0`). So it also creates the receipt, and puts
it inside the chunk. This time it is in shard 1 (as that's where it was
executed).

```
Chunk: Ok(
    V2(
        ShardChunkV2 {
            chunk_hash: ChunkHash(
                8sPHYmBFp7cfnXDAKdcATFYfh9UqjpAyqJSBKAngQQxL,
            ),
            header: V3(
                ShardChunkHeaderV3 {
                    inner: V2(
                        ShardChunkHeaderInnerV2 {
                            prev_block_hash: Fj7iu26Yy9t5e9k9n1fSSjh6ZoTafWyxcL2TgHHHskjd,
                            prev_state_root: 4y6VL9BoMJg92Z9a83iqKSfVUDGyaMaVU1RNvcBmvs8V,
                            outcome_root: 7V3xRUeWgQa7D9c8s5jTq4dwdRcyTuY4BENRmbWaHiS5,
                            encoded_merkle_root: BnCE9LZgnFEjhQv1fSYpxPNw56vpcLQW8zxNmoMS8H4u,
                            encoded_length: 149,
                            height_created: 1678,
                            shard_id: 1,
                            gas_used: 223182562500,
                            gas_limit: 1000000000000000,
                            balance_burnt: 22318256250000000000,
                            outgoing_receipts_root: HYjZzyTL5JBfe1Ar4C4qPKc5E6Vbo9xnLHBKLVAqsqG2,
                            tx_root: 11111111111111111111111111111111,
                            validator_proposals: [],
                        },
                    ),
                    height_included: 0,
                    signature: ed25519:4FzcDw2ay2gAGosNpFdTyEwABJhhCwsi9g47uffi77N21EqEaamCg9p2tALbDt5fNeCXXoKxjWbHsZ1YezT2cL94,
                    hash: ChunkHash(
                        8sPHYmBFp7cfnXDAKdcATFYfh9UqjpAyqJSBKAngQQxL,
                    ),
                },
            ),
            transactions: [],
            receipts: [
                Receipt {
                    predecessor_id: AccountId(
                        "system",
                    ),
                    receiver_id: AccountId(
                        "shard0",
                    ),
                    receipt_id: 6eei79WLYHGfv5RTaee4kCmzFx79fKsX71vzeMjCe6rL,
                    receipt: Action(
                        ActionReceipt {
                            signer_id: AccountId(
                                "shard0",
                            ),
                            signer_public_key: ed25519:Ht8EqXGUnY8B8x7YvARE1LRMEpragRinqA6wy5xSyfj5,
                            gas_price: 0,
                            output_data_receivers: [],
                            input_data_ids: [],
                            actions: [
                                Transfer(
                                    TransferAction {
                                        deposit: 669547687500000000,
                                    },
                                ),
                            ],
                        },
                    ),
                },
            ],
        },
    ),
)
```

Such gas refund receipts are a little bit special - as we'll set the
`predecessor_id` to be `system` - but the receiver is what we expect (`shard0`
account).

**Note:** `system` is a special account that doesn't really belong to any shard.
As you can see in this example, the receipt was created within shard 1.

So putting it all together would look like this:

![image](https://user-images.githubusercontent.com/91919554/200617392-00b9fa0c-2f15-40ad-9802-137ca9a5a15d.png)

But wait - NEAR was saying that transfers are happening with 2 blocks - but here
I see that it took 3 blocks. What's wrong?

The image above is a simplification, and reality is a little bit tricker -
especially as receipts in a given chunks are actually receipts received as a
result from running a PREVIOUS chunk from this shard.

We'll explain it more in the next section.

# Advanced: What's actually going on?

As you could have read in [Transactions And Receipts](./tx_receipts.md) - the
'receipts' field in the chunk is actually representing 'outgoing' receipts
from the previous block.

So our image should look more like this:

![image](https://user-images.githubusercontent.com/91919554/200621066-a5d06f2d-ff43-44ce-a52b-47dc44d6f8ab.png)

In this example, the black boxes are representing the 'processing' of the chunk,
and red arrows are cross-shard communication.

So when we process Shard 0 from block 1676, we read the transation, and output
the receipt - which later becomes the input for shard 1 in block 1677.

But you might still be wondering - so why didn't we add the Receipt (transfer)
to the list of receipts of shard0 1676?

That's because the shards & blocks are set BEFORE we do any computation. So the
more correct image would look like this:

![image](https://user-images.githubusercontent.com/91919554/200621808-1ce78047-6968-4af5-9c2a-805a0f1643fc.png)

Here you can clearly see that chunk processing (black box), is happening AFTER
the chunk is set.

In this example, the blue arrows are showing the part where we persist the
result (receipt) into next block's chunk.

<!-- TODO: maybe add the link to that article here? -->
In a future article, we'll discuss how the actual cross-shard communication
works (red arrows) in the picture, and how we could guarantee that a given shard
really gets all the red arrows, before is starts processing.
