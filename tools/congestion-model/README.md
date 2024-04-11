# Congestion Model

A model for simulating cross-shard congestion behavior.
We use it to compare different design proposals against each other.

## Running the model

Simply run it with

```bash
cargo run
```

and you should see a summary table of model execution results.

## Architecture

A model execution takes a workload and a design proposal as inputs and then it
produces some evaluation output.

```txt
WORKLOAD                  DESIGN PROPOSAL
[trait Producer]          [trait CongestionStrategy]
       \                     /
        \                   /
         \                 /
          \               /
           \             /
           CONGESTION MODEL
           [struct Model]
                  |
                  |
                  |
                  |
                  |
              EVALUATION
```

Each component of the four components above has its own directors.
- ./src/workload
- ./src/strategy
- ./src/model
- ./src/evaluation

## Add more strategies

To add a new congestion strategy, create a new module in [./src/strategy] and
create a `struct` in it. Implement the `trait CongestionStrategy` for your struct.

```rust
pub trait CongestionStrategy {
    /// Initial state and register all necessary queues.
    fn init(&mut self, id: ShardId, other_shards: &[ShardId], queue_factory: &mut dyn QueueFactory);

    /// Decide which receipts to execute, which to delay, and which to forward.
    fn compute_chunk(&mut self, ctx: &mut ChunkExecutionContext);
}
```

In the `init` function, you may create receipt queues.

In the `compute_chunk` function, consume incoming receipts and transactions.
Then either execute them, or put them in a queue.

When executing receipts or transactions, it potentially produces outgoing
receipts. These can be forwarded or kept in a queue for later forwarding. It all
depends on your strategy for applying backpressure.

To communicate between shards, store congestion information in the current
block. It is accessible by all shards one round later, so they can make
decisions to throttle their own rate if necessary.

```rust
fn compute_chunk(&mut self, ctx: &mut ChunkExecutionContext) {
    // store own data in the block
    let shared_info = MyInfo::new();
    ctx.current_block_info().insert(shared_info);

    // read data from previous block
    for shard_info in ctx.prev_block_info().values() {
        let info = shard_info.get::<MyInfo>().unwrap();
    }
}
```

Internally, this uses a `HashMap<TypeId, Box<dyn Any>>` allowing to store any
retrieve any data in a type-safe way without data serialization.

In the end, go to [src/main.rs] and add your strategy to the list of tested
strategies.

## Add more workloads

To add more workloads, create a module in [./src/workload] and create a struct. 
Then implement the `trait Producer` for it.

```rust
/// Produces workload in the form of transactions.
///
/// Describes how many messages by which producer should be created in a model
/// execution.
pub trait Producer {
    /// Set up initial state of the producer if necessary.
    fn init(&mut self, shards: &[ShardId]);

    /// Create transactions for a round.
    fn produce_transactions(
        &mut self,
        round: Round,
        shards: &[ShardId],
        tx_factory: &mut dyn FnMut(ShardId) -> TransactionBuilder,
    ) -> Vec<TransactionBuilder>;
}
```

In the `init` function, you have the option to initialize some state that
depends on shard ids.

In the `produce_transactions` function, create as many transactions as you want
by calling `let tx = tx_factory()` followed by calls on the transaction builder.
Start with `let receipt_id = tx.add_first_receipt(receipt_definition,
conversion_gas)` and add more receipts to it by calling  `let receipt_id_1 =
tx.new_outgoing_receipt(receipt_id, receipt_definition)`.