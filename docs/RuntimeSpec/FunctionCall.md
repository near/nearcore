# Function Call

In this section we provide an explanation how the `FunctionCall` action execution works, what are
the inputs and what are the outputs. Suppose runtime received the following ActionReceipt:

```rust
ActionReceipt {
     id: "A1",
     signer_id: "alice",
     signer_public_key: "6934...e248",
     receiver_id: "dex",
     predecessor_id: "alice",
     input_data_ids: [],
     output_data_receivers: [],
     actions: [FunctionCall { gas: 100000, deposit: 100000u128, method_name: "exchange", args: "{arg1, arg2, ...}", ... }],
 }
```

### `input_data_ids` to `PromiseResult`s

`ActionReceipt.input_data_ids` must be satisfied before execution (see
[Receipt Matching](/RuntimeSpec/Receipts#receipt-matching)). Each of `ActionReceipt.input_data_ids` will be converted to
the `PromiseResult::Successful(Vec<u8>)` if `data_id.data` is `Some(Vec<u8>)` otherwise if
`data_id.data` is `None` promise will be `PromiseResult::Failed`.

## Input

The `FunctionCall` executes in the `receiver_id` account environment.

- a vector of [Promise Results](#input_data_ids-to-promiseresults) which can be accessed by a `promise_result`
  import [PromisesAPI](Components/BindingsSpec/PromisesAPI.md) `promise_result`)
- the original Transaction `signer_id`, `signer_public_key` data from the ActionReceipt (e.g.
  `method_name`, `args`, `predecessor_id`, `deposit`, `prepaid_gas` (which is `gas` in
  FunctionCall))
- a general blockchain data (e.g. `block_index`, `block_timestamp`)
- read data from the account storage

A full list of the data available for the contract can be found in [Context
API](Components/BindingsSpec/ContextAPI.md) and [Trie](Components/BindingsSpec/TrieAPI.md)


## Execution

In order to implement this action, the runtime will:

- load the contract code from the `receiver_id` [account](../DataStructures/Account.md#account)’s
  storage;
- parse, validate and instrument the contract code (see [Preparation](./Preparation.md));
- optionally, convert the contract code to a different executable format;
- instantiate the WASM module, linking runtime-provided functions defined in the
  [Bindings Spec](Components/BindingsSpec/BindingsSpec.md) & running the start function; and
- invoke the function that has been exported from the wasm module with the name matching
  that specified in the `FunctionCall.method_name` field.

Note that some of these steps may be executed during the
[`DeployContractAction`](./Actions.md#deploycontractaction) instead. This is largely an
optimization of the `FunctionCall` gas fee, and must not result in an observable behavioral
difference of the `FunctionCall` action.

During the execution of the contract, the runtime will:

- count burnt gas on execution;
- count used gas (which is `burnt gas` + gas attached to the new created receipts);
- measure the increase in account’s storage usage as a result of this call;
- collect logs produced by the contract;
- set the return data; and
- create new receipts through [PromisesAPI](Components/BindingsSpec/PromisesAPI.md).

## Output

The output of the `FunctionCall`:

- storage updates - changes to the account trie storage which will be applied on a successful call
- `burnt_gas`, `used_gas` - see [Runtime Fees](Fees/Fees.md)
- `balance` - unspent account balance (account balance could be spent on deposits of newly created
  `FunctionCall`s or [`TransferAction`s](Actions.md#transferaction) to other contracts)
- `storage_usage` - storage_usage after ActionReceipt application
- `logs` - during contract execution, utf8/16 string log records could be created. Logs are not
  persistent currently.
- `new_receipts` - new `ActionReceipts` created during the execution. These receipts are going to
  be sent to the respective `receiver_id`s (see [Receipt Matching explanation](/RuntimeSpec/Receipts#receipt-matching))
- result could be either [`ReturnData::Value(Vec<u8>)`](#value-result) or
  [`ReturnData::ReceiptIndex(u64)`](#receiptindex-result)`


### Value Result

If applied `ActionReceipt` contains [`output_data_receivers`](Receipts.md#output_data_receivers),
runtime will create `DataReceipt` for each of `data_id` and `receiver_id` and `data` equals
returned value. Eventually, these `DataReceipt` will be delivered to the corresponding receivers.

### ReceiptIndex Result

Successful result could not return any Value, but generates a bunch of new ActionReceipts instead.
One example could be a callback. In this case, we assume the new Receipt will send its Value
Result to the [`output_data_receivers`](Receipts.md#output_data_receivers) of the current
`ActionReceipt`.

### Errors

As with other actions, errors can be divided into two categories: validation error and execution
error.

#### Validation Error

- If there is zero gas attached to the function call, a

  ```rust
  /// The attached amount of gas in a FunctionCall action has to be a positive number.
  FunctionCallZeroAttachedGas,
  ```

  error will be returned

- If the length of the method name to be called exceeds `max_length_method_name`, a genesis
  parameter whose current value is `256`, a

  ```rust
  /// The length of the method name exceeded the limit in a Function Call action.
  FunctionCallMethodNameLengthExceeded { length: u64, limit: u64 }
  ```

  error is returned.

- If the length of the argument to the function call exceeds `max_arguments_length`, a genesis
  parameter whose current value is `4194304` (4MB), a

  ```rust
  /// The length of the arguments exceeded the limit in a Function Call action.
  FunctionCallArgumentsLengthExceeded { length: u64, limit: u64 }
  ```

  error is returned.

#### Execution Error

There are three types of errors which may occur when applying a function call action:
`FunctionCallError`, `ExternalError`, and `StorageError`.

- `FunctionCallError` includes everything from around the execution of the wasm binary,
from compiling wasm to native to traps occurred while executing the compiled native binary. More specifically,
it includes the following errors:

  ```rust
  pub enum FunctionCallError {
      /// Wasm compilation error
      CompilationError(CompilationError),
      /// Wasm binary env link error
      LinkError {
          msg: String,
      },
      /// Import/export resolve error
      MethodResolveError(MethodResolveError),
      /// A trap happened during execution of a binary
      WasmTrap(WasmTrap),
      WasmUnknownError,
      HostError(HostError),
  }
  ```

- `CompilationError` includes errors that can occur during the compilation of wasm binary.
- `LinkError` is returned when wasmer runtime is unable to link the wasm module with provided imports.
- `MethodResolveError` occurs when the method in the action cannot be found in the contract code.
- `WasmTrap` error happens when a trap occurs during the execution of the binary. Traps here include

  ```rust
  pub enum WasmTrap {
      /// An `unreachable` opcode was executed.
      Unreachable,
      /// Call indirect incorrect signature trap.
      IncorrectCallIndirectSignature,
      /// Memory out of bounds trap.
      MemoryOutOfBounds,
      /// Call indirect out of bounds trap.
      CallIndirectOOB,
      /// An arithmetic exception, e.g. divided by zero.
      IllegalArithmetic,
      /// Misaligned atomic access trap.
      MisalignedAtomicAccess,
      /// Breakpoint trap.
      BreakpointTrap,
      /// Stack overflow.
      StackOverflow,
      /// Generic trap.
      GenericTrap,
  }
  ```

- `WasmUnknownError` occurs when something inside wasmer goes wrong
- `HostError` includes errors that might be returned during the execution of a host function. Those errors are

  ```rust
  pub enum HostError {
      /// String encoding is bad UTF-16 sequence
      BadUTF16,
      /// String encoding is bad UTF-8 sequence
      BadUTF8,
      /// Exceeded the prepaid gas
      GasExceeded,
      /// Exceeded the maximum amount of gas allowed to burn per contract
      GasLimitExceeded,
      /// Exceeded the account balance
      BalanceExceeded,
      /// Tried to call an empty method name
      EmptyMethodName,
      /// Smart contract panicked
      GuestPanic { panic_msg: String },
      /// IntegerOverflow happened during a contract execution
      IntegerOverflow,
      /// `promise_idx` does not correspond to existing promises
      InvalidPromiseIndex { promise_idx: u64 },
      /// Actions can only be appended to non-joint promise.
      CannotAppendActionToJointPromise,
      /// Returning joint promise is currently prohibited
      CannotReturnJointPromise,
      /// Accessed invalid promise result index
      InvalidPromiseResultIndex { result_idx: u64 },
      /// Accessed invalid register id
      InvalidRegisterId { register_id: u64 },
      /// Iterator `iterator_index` was invalidated after its creation by performing a mutable operation on trie
      IteratorWasInvalidated { iterator_index: u64 },
      /// Accessed memory outside the bounds
      MemoryAccessViolation,
      /// VM Logic returned an invalid receipt index
      InvalidReceiptIndex { receipt_index: u64 },
      /// Iterator index `iterator_index` does not exist
      InvalidIteratorIndex { iterator_index: u64 },
      /// VM Logic returned an invalid account id
      InvalidAccountId,
      /// VM Logic returned an invalid method name
      InvalidMethodName,
      /// VM Logic provided an invalid public key
      InvalidPublicKey,
      /// `method_name` is not allowed in view calls
      ProhibitedInView { method_name: String },
      /// The total number of logs will exceed the limit.
      NumberOfLogsExceeded { limit: u64 },
      /// The storage key length exceeded the limit.
      KeyLengthExceeded { length: u64, limit: u64 },
      /// The storage value length exceeded the limit.
      ValueLengthExceeded { length: u64, limit: u64 },
      /// The total log length exceeded the limit.
      TotalLogLengthExceeded { length: u64, limit: u64 },
      /// The maximum number of promises within a FunctionCall exceeded the limit.
      NumberPromisesExceeded { number_of_promises: u64, limit: u64 },
      /// The maximum number of input data dependencies exceeded the limit.
      NumberInputDataDependenciesExceeded { number_of_input_data_dependencies: u64, limit: u64 },
      /// The returned value length exceeded the limit.
      ReturnedValueLengthExceeded { length: u64, limit: u64 },
      /// The contract size for DeployContract action exceeded the limit.
      ContractSizeExceeded { size: u64, limit: u64 },
      /// The host function was deprecated.
      Deprecated { method_name: String },
  }
  ```

- `ExternalError` includes errors that occur during the execution inside `External`, which is an interface between runtime
and the rest of the system. The possible errors are:

  ```rust
  pub enum ExternalError {
      /// Unexpected error which is typically related to the node storage corruption.
      /// It's possible the input state is invalid or malicious.
      StorageError(StorageError),
      /// Error when accessing validator information. Happens inside epoch manager.
      ValidatorError(EpochError),
  }
  ```

- `StorageError` occurs when state or storage is corrupted.
