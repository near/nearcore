`cross_shard_tx` test is a test that tests many complex features of the server
and uses `KeyValueRuntime`.

We generally do not write many new tests using `KeyValueRuntime` given the
limitations of it, and generally one wouldn't interact with it except to debug
issues with `cross_shard_tx`, so this section provides basic overview of both
the particular test and the infrastructure it uses.

## KeyValueRuntime

KeyValueRuntime is written to be able to test chain without creating the actual
Runtime. It thus doesn't use the actual storage, and doesn't use the actual
epoch manager. The storage it uses is a simple key value store, and for the
epoch manager, it is parametrized with a vector of vectors of validators
`validators: Vec<Vec<ValidatorStake>>`, where for epoch `i` the validator set
consists of `validators[i % validators.len()]`.

## setup_mock_all_validators

`setup_mock_all_validators` is given a `Vec<Vec<str>>` and a parallel set of key
pairs, and creates the `KeyValueRuntime`, `ClientActor` and `ViewClientActor`
per validator. It then establishes a fake network between them. The last
argument to `setup_mock_all_validators` is a handler that is called for each
messages that is passed between the validators, and returns a pair `(resonse,
perform_default)`. If `perform_default` is `true`, the `response` is ignored,
and the default handler is executed (that will send message to the recipient(s),
compute the response from their standpoint, and send it back. If
`perform_default` is `false`, the default handler is not executed, in particular
the message is dropped and is not delivered to the recipients. The `response` is
then sent as a response to the original sender.

`setup_mock_all_validators` receives a set of booleans and other parameters that
enable various test modes:

### validator_groups

This parameter splits the validators in each shard into that many groups, and
assigns a separate set of shards to each group. E.g. if there are 8 validators
in a particular epoch, and 4 shards, and 2 validator groups, 4 validators will
be assigned to two shards, and 4 remaining validators will be assigned to 2
remaining shards. With 8 validators, 4 shards and 4 validator groups each shard
will be assigned to two validators.

The number of shards is the number of validators in the epoch that has the
smallest number of validators.

### block_prod_time

`min_block_production_time` will be set to this value,
`max_block_production_time` to this value times 3.

### drop_chunks

If set to true, some small percentage of chunk-related messages will be dropped

### tamper_with_fg

If set to false, all the approvals are delivered. Otherwise at some heights no
approvals will be delivered, at some heights only half, and at some heights all
will be delivered. It was designed to tamper with the finality gadget when we
had it, unclear if has much effect today. Must be disabled if doomslug is
enabled (see below), because doomslug will stall if approvals are not delivered.

### enable_doomslug

If false, blocks will be created when at least one approval is present, without
waiting for 2/3. This allows for more forkfulness. `cross_shard_tx` has modes
both with enabled doomslug (to test "production" setting) and with disabled
doomslug (to test higher forkfullness)


## cross_shard_tx

The basic flow of the test spins up validators, and starts sending to them
several (at most 64) cross-shard transactions. i-th transaction sends money from
validator i/8 to validator i%8. What makes the test good is that due to very low
block production time, it creates lots of forks, delaying both the transaction
acceptance, and the receipt delivery.

It submits txs one at a time, and waits for their completion before sending the
next. Whenever the transaction completed, it traces "Finished iteration 1" (with
the ordinal increasing). Given the test takes a while, checking how far below
the last some message is a good way to early tell that the test has stalled.
E.g. if the last message is not in the last 15% of the output, it is likely that
the test will not make further progress, depending on the mode (with validator
rotation some iterations are way longer than other).

The test has multiple modes:

* `test_cross_shard_tx` -- doesn't drop chunks, disabled doomslug, no validator
  rotation (each epoch has the same set of validators)

* `test_cross_shard_tx_doomslug` -- same as above, but doomslug is enabled

* `test_cross_shard_tx_drop_chunks` -- same as the first one but the chunks are
  sometimes dropped

* `test_cross_shard_tx_with_validator_rotation_{1, 2}` -- same as the first one,
  but with validator rotation. The two versions of the test have slightly
  different block production times, and different number of iterations we expect
  to finish in the allocated time. (the one with lower block production time is
  expected to finish fewer because it has higher forkfulness).
