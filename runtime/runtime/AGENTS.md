runtime/runtime directory contains the NEAR runtime logic.
NEAR is a blockchain where every block contains many chunks, one chunk per shard.
Each chunk contains a list of transactions to execute.

The runtime takes a chunk and applies it (`fn apply`).
fn apply() contains the following steps:
- Distribute validator rewards
- Run the bandwidth scheduler to determine outgoing bandwidth limits
- If the applied chunk is missing (old_chunk), skip the rest
- Read buffered outgoing receipts and send out as many as possible within the outgoing limits
- Process transactions in the chunk and convert them to the receipts
- Process receipts, executing them and producing new outgoing receipts

`process_transactions` takes all transactions in a chunk, validates them, charges gas, and converts them to local receipts.

Important types:
enum Receipt
enum ReceiptEnum
struct ActionReceiptV2
enum Action
enum Account

Receipt is an internal representation of a task to execute.
Receipts have a `predecessor_id` (the account which created the receipt) and a `receiver_id` (the account where the receipt should execute).

There are a few kinds of receipts:
 * local receipts - receipts generated from transactions where the predecessor_id equals receiver_id (sender_is_receiver, "sir" for short). These receipts will usually be applied in the same chunk as the one where the transaction executed.
 * delayed receipts - if there are too many receipts to execute in a chunk, and the gas limit is hit, the remaining receipts are not executed. They're stored in the delayed receipt queue. At the next chunk they will be taken out of the delayed queue and processed.
 * incoming receipts - when a receipt is created, it's sent out as an outgoing receipt to reach the `receiver_id` account. This account might belong to a different shard. When a receipt arrives to be executed on a shard, it's called an incoming receipt.
 * postponed receipts - receipts can have data dependencies (`input_data_ids`). A receipt can't be executed until all data dependencies are present. A receipt which doesn't have its data dependencies yet is considered postponed.
 * buffered receipts - outgoing receipts which couldn't be sent out after creation. Stored in a buffer to be sent later.
 * instant receipts - special receipts which are not sent out as outgoing receipts. They're executed immediately after being created.

There are two main types of receipts: Action and Data receipts.

Action receipts contain a list of actions to execute. They arrive on the `receiver_id`'s account and execute actions from the list, one after the other.
If any action fails, the whole receipt fails, its execution outcome is Err.
There are many types of actions. The most interesting one is `FunctionCall` action, which allows to call a method on a smart contract in WASM.

Executing an action can cause changes to the account's state.
Each shard has a state, which is stored as a Merkle Tree. Within the shard, each account has its own subtree which holds the account's state.

It can also generate new receipts which will be executed later. It's possible for one account to create a receipt to another account, which enables cross-contract calls. Unlike Ethereum, cross-contract calls are asynchronous, transactions are not atomic. Receipts are sent asynchronously to make shards fully independent of each other, enabling horizontal scalability.

NEAR allows to create a promise DAG. A FunctionCall action can generate multiple receipts and order them to execute in some specified order, expressed as a DAG (directed acyclic graph).
In smart contracts, creating a `promise` spawns a `receipt`, the naming is a bit different, but under the hood it's the same thing.
The ordering is achieved by specifying data dependencies between the receipts, by manipulating the input_data_ids and output_data_receivers.
After executing a receipt, the result is sent out as a Data receipt to all of the specified output_data_receivers. The other receipts have this data in their input_data_ids and wait for the Data receipts to be sent before being executed.

Executing actions costs gas. Each receipt has some amount of gas attached to it. If the gas runs out, receipt execution fails. Unused gas is sent back to the transaction author as a gas refund.

ReceiptSink takes new outgoing receipts and tries to send them out to other shards. If the receipt can't be sent because of outgoing limits, the receipt is stored in a buffer in the shard's trie and will be sent later.

Bandwidth scheduler sees how much data (outgoing receipts) each shard wants to send to other shards and decides how much can be transferred between a pair of shards at a given height. It makes sure that each shard receives and sends a limited amount of bandwidth at each height, keeping bandwidth requirements reasonable.

Congestion control keeps track of the size of receipt queues on all shards and stops accepting new work when the queues get too large.

Yield/Resume is a feature which allows to `yield` and create a new receipt with an unsatisfied data dependency. Later another transaction can call `resume` with some payload to satisfy the data dependency, continuing execution of the yielded receipt. If the receipt is not resumed in time, it will time out and the yielded receipt will be executed with a timeout (None payload).

Each account can have multiple access keys, which are used to validate transactions submitted for this account. NEAR supports named accounts which don't have the public key in their name (e.g. `alice.near`). When a transaction is submitted, the verification code fetches the account's access key and verifies the transaction's signature against this key.

Receipts are not signed, so they're not validated using access keys. Receipts can be trusted because their hash has been signed by 2/3 of the chunk validator stake.

That trust only holds for nodes that execute the source chunk. A node that tracks a destination shard but not the source shard applies incoming receipts (including optimistically) without executing the source chunk, so a malicious chunk producer can make it apply receipts that honest execution would never produce: zero or duplicated `input_data_ids`, deposits that overflow a balance, absurd gas prices, huge nonces, etc. Treat every receipt field as untrusted on the apply path. `Runtime::apply` errors are mapped in `chain/chain/src/runtime/mod.rs`, and `panic = 'abort'` makes any panic there a remote node crash, so:
- reject malformed receipts in `validate_receipt` when a cheap, history-independent rule exists (a failure there is a `RuntimeError::ReceiptValidationError`, which rejects the chunk),
- keep `UnexpectedIntegerOverflow`, `StorageInconsistentState` and `ExecutionInvariantBroken` for violations of invariants of the node's own state or of the execution. Use `StorageInconsistentState` only when the storage itself is corrupted, and `ExecutionInvariantBroken` when the receipts break an invariant of the execution (for example, one receipt id executed twice). They abort the node when applying a normal block. In an optimistic apply they only fail that apply and the block is applied again later, so a forged receipt that breaks such an invariant can't crash a node. Panics, `unwrap`s and asserts are not covered by this.

Under SPICE, the chunk executor applies a receipt only after its proof matches the `outgoing_receipts_root` of a certified execution result (`UnverifiedReceiptTracker::try_drain`). Its `handle_apply_chunks_done` panics on any apply error, so an error that rejects a chunk elsewhere aborts the node there. That is safe only while the executor applies certified receipts. A path that applies receipts before certification must handle these errors without panicking. State witness validation returns the error and rejects the witness.

Add a case to the forged receipts tests in `test-loop-tests/src/tests/malicious_chunk_producer.rs` when handling a new receipt field.

To modify the shard's state, the runtime uses `TrieUpdate`. This struct applies changes on top of the chunk's pre-state. It allows to rollback or commit recent changes. When a receipt fails, its state changes are rolled back using `TrieUpdate`.