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
- Finalize the post-state

`process_transactions` takes all transactions in a chunk, validates them, charges gas, and converts them to local receipts.

Receipt is an internal representation of a task to execute.

```rust
pub struct ReceiptV0 {
    /// An issuer account_id of a particular receipt.
    /// `predecessor_id` could be either `Transaction` `signer_id` or intermediate contract's `account_id`.
    pub predecessor_id: AccountId,
    /// `receiver_id` is a receipt destination.
    pub receiver_id: AccountId,
    /// An unique id for the receipt
    pub receipt_id: CryptoHash,
    /// A receipt type
    pub receipt: ReceiptEnum,
}

pub enum ReceiptEnum {
    Action(ActionReceipt) = 0,
    Data(DataReceipt) = 1,
    PromiseYield(ActionReceipt) = 2,
    PromiseResume(DataReceipt) = 3,
    GlobalContractDistribution(GlobalContractDistributionReceipt) = 4,
    ActionV2(ActionReceiptV2) = 5,
    PromiseYieldV2(ActionReceiptV2) = 6,
}

pub struct ActionReceiptV2 {
    /// A signer of the original transaction
    pub signer_id: AccountId,
    /// The receiver of any balance refunds form this receipt if it is different from receiver_id.
    pub refund_to: Option<AccountId>,
    /// An access key which was used to sign the original transaction
    pub signer_public_key: PublicKey,
    /// A gas_price which has been used to buy gas in the original transaction
    pub gas_price: Balance,
    /// If present, where to route the output data
    pub output_data_receivers: Vec<DataReceiver>,
    /// A list of the input data dependencies for this Receipt to process.
    /// If all `input_data_ids` for this receipt are delivered to the account
    /// that means we have all the `ReceivedData` input which will be than converted to a
    /// `PromiseResult::Successful(value)` or `PromiseResult::Failed`
    /// depending on `ReceivedData` is `Some(_)` or `None`
    pub input_data_ids: Vec<CryptoHash>,
    /// A list of actions to process when all input_data_ids are filled
    pub actions: Vec<Action>,
}

pub enum Action {
    CreateAccount(CreateAccountAction) = 0,
    DeployContract(DeployContractAction) = 1,
    FunctionCall(Box<FunctionCallAction>) = 2,
    Transfer(TransferAction) = 3,
    Stake(Box<StakeAction>) = 4,
    AddKey(Box<AddKeyAction>) = 5,
    DeleteKey(Box<DeleteKeyAction>) = 6,
    DeleteAccount(DeleteAccountAction) = 7,
    Delegate(Box<delegate::SignedDelegateAction>) = 8,
    DeployGlobalContract(DeployGlobalContractAction) = 9,
    UseGlobalContract(Box<UseGlobalContractAction>) = 10,
    DeterministicStateInit(Box<DeterministicStateInitAction>) = 11,
    TransferToGasKey(Box<TransferToGasKeyAction>) = 12,
    WithdrawFromGasKey(Box<WithdrawFromGasKeyAction>) = 13,
}

pub struct AccountV2 {
    /// The total not locked tokens.
    amount: Balance,
    /// The amount locked due to staking.
    locked: Balance,
    /// Storage used by the given account, includes account id, this struct, access keys and other data.
    storage_usage: StorageUsage,
    /// Type of contract deployed to this account, if any.
    contract: AccountContract,
}
```

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

It can also generate new receipts which will be executed later. It's possible for one account to create a receipt to another account, which enables cross-contract calls. Unlike ethereum, cross-contract calls are asynchronous, transactions are not atomic. Receipts are sent asynchronously to make shards fully independent of each other, enabling horizontal scalability.

NEAR allows to create a promise DAG. A FunctionCall action can generate multiple receipts and order them to execute in some specified order, expressed as a DAG (directed acyclic graph).
In smart contracts, creating a `promise` spawns a `receipt`, the naming is a bit different, but under the hood it's the same thing.
The ordering is achieved by specifying data dependencies between the receipts, by manipulating the input_data_ids and output_data_receivers.
After executing a receipt, the result is sent out as a Data receipts to all of the specified output_data_receivers. The other receipts have this data in their input_data_ids and wait for the Data receipts to be sent before being executed.

Executing actions costs gas. Each receipt has some amount of gas attached to it. If the gas runs out, receipt execution fails. Unused gas is sent back to the transaction author as a gas refund.

ReceiptSink takes new outgoing receipts and tries to send them out to other shards. If the receipt can't be sent because of outgoing limits, the receipt is stored in a buffer in the shard's trie and will be sent later.

Bandwidth scheduler sees how much data (outgoing receipts) each shard wants to send to other shards and decides how much can be transferred between a pair of shards at a given height. It makes sure that each shard receives and sends a limited amount of bandwidth at each height, keeping bandwidth requirements reasonable. Outgoing receipts that are not allowed to be sent

Congestion control keeps track of the size of receipt queues on all shards and stops accepting new work when the queues get too large.

Yield/Resume is a feature which allows to `yield` and create a new receipt with an unsatisfied data dependency. Later another transaction can call `resume` with some payload to satisfy the data dependency, continuing execution of the yielded receipt. If the receipt is not resumed in time, it will time out and the yielded receipt will be executed with a timeout (None payload).

Each account can have multiple access keys, which are used to validate transactions submitted for this account. NEAR supports named accounts which don't have the public key in their name (e.g. `alice.near`). When a transaction is submitted, the verification code fetches the account's access key and verifies the transaction's signature against this key.

Receipts are not signed, so they're not validated using access keys. Receipts can be trusted because their hash has been signed by 2/3 of the chunk validator stake.

To modify the shard's state, the runtime uses `TrieUpdate`. This struct applies changes on top of the chunk's pre-state. It allows to rollback or commit recent changes. When a receipt fails, its state changes are rolled back using `TrieUpdate`.