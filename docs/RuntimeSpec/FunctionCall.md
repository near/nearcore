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

- From protocol version 87 (nearcore 2.14.0), if the method name is empty, a

  ```rust
  /// The method name in a FunctionCall action must not be empty.
  FunctionCallEmptyMethodName,
  ```

  error is returned.

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
from compiling wasm to native to traps occurred while executing the compiled native binary.

  Execution outcomes report a `FunctionCallError` in one of three forms:

  - `CompilationError`, a structured error, when the contract cannot be compiled.
  - `MethodResolveError`, a structured error, when the method is not found or its signature is not
    valid. Before protocol version 87, an empty method name also gave `MethodEmptyName`. From
    protocol version 87, validation rejects an empty method name with `FunctionCallEmptyMethodName`.
  - `ExecutionError` with a message string, for all other errors: wasm traps, host errors, link
    errors and errors when loading the compiled contract.

  For example, a function call whose contract panics with the message `oops` fails with this
  status in the RPC response:

  ```json
  {
    "Failure": {
      "ActionError": {
        "index": 0,
        "kind": {
          "FunctionCallError": {
            "ExecutionError": "Smart contract panicked: oops"
          }
        }
      }
    }
  }
  ```

  The full enum also has variants that only old data uses, and one unused placeholder,
  `_EVMError`. The number after each variant is its tag in the Borsh encoding:

  ```rust
  #[borsh(use_discriminant = true)]
  #[repr(u8)]
  pub enum FunctionCallError {
      /// Wasm compilation error
      CompilationError(CompilationError) = 0,
      /// Wasm binary env link error
      ///
      /// Note: this is only to deserialize old data, use execution error for new data
      LinkError {
          msg: String,
      } = 1,
      /// Import/export resolve error
      MethodResolveError(MethodResolveError) = 2,
      /// A trap happened during execution of a binary
      ///
      /// Note: this is only to deserialize old data, use execution error for new data
      WasmTrap(WasmTrap) = 3,
      WasmUnknownError = 4,
      /// Note: this is only to deserialize old data, use execution error for new data
      HostError(HostError) = 5,
      // Unused, can be reused by a future error but must be exactly one error to keep ExecutionError
      // error borsh serialized at correct index
      _EVMError = 6,
      ExecutionError(String) = 7,
  }
  ```

  The `WasmTrap` and `HostError` enums below are the errors that the contract runtime returns. The
  `ExecutionError` message is the text of one of these errors.

- `CompilationError` includes errors that can occur during the compilation of wasm binary.
- `LinkError` happens when the runtime cannot link the wasm module with the provided imports. It is
  reported as `ExecutionError` with a message that starts with `Link Error:`.
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
      /// Indirect call to null.
      IndirectCallToNull,
      /// Stack overflow.
      StackOverflow,
      /// Generic trap.
      GenericTrap,
  }
  ```

- `WasmUnknownError` is kept only to read old data. New outcomes do not use it.
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
      /// Setting `refund_to` on a joint promise is prohibited
      CannotSetRefundToOnJointPromise,
      /// Accessed invalid promise result index
      InvalidPromiseResultIndex { result_idx: u64 },
      /// Accessed invalid register id
      InvalidRegisterId { register_id: u64 },
      /// Accessed memory outside the bounds
      MemoryAccessViolation,
      /// VM Logic returned an invalid receipt index
      InvalidReceiptIndex { receipt_index: u64 },
      /// Iterator index `iterator_index` does not exist
      InvalidIteratorIndex { iterator_index: u64 },
      /// Action index does not exist within the given receipt or does not hold
      /// the expected type of action.
      InvalidActionIndex { receipt_index: u64, action_index: u64 },
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
      /// General errors for ECDSA recover.
      ECRecoverError { msg: String },
      /// Invalid input to alt_bn128 family of functions (e.g., point which isn't
      /// on the curve).
      AltBn128InvalidInput { msg: String },
      /// Invalid input to ed25519 signature verification function (e.g. signature cannot be
      /// derived from bytes).
      Ed25519VerifyInvalidInput { msg: String },
      /// Input length mismatch for p256 signature verification (signature is not 64
      /// bytes or public key is not 33 bytes). Parse failures of otherwise
      /// well-sized inputs return 0 from the host function instead of aborting.
      P256VerifyInvalidInput { msg: String },
      /// Input length mismatch for ML-DSA-65 signature verification (signature is
      /// not 3309 bytes or public key is not 1952 bytes). Parse failures of
      /// otherwise well-sized inputs return 0 from the host function instead of
      /// aborting.
      MlDsaVerifyInvalidInput { msg: String },
      // Invalid input to bls12381 family of functions
      BLS12381InvalidInput { msg: String },
      /// Yield payload length exceeds the maximum permitted.
      YieldPayloadLength { length: u64, limit: u64 },
      /// Yield resumption data id is malformed.
      DataIdMalformed,
      /// User-provided yield id (for `promise_yield_create_with_id` /
      /// `promise_yield_resume_with_yield_id`) is malformed.
      YieldIdMalformed,
      /// Size of the recorded trie storage proof has exceeded the allowed limit.
      RecordedStorageExceeded { limit: ByteSize },
      /// Contract code hash is malformed.
      ContractCodeHashMalformed,
      /// Data entry within DeterministicStateInit already exists.
      DataEntryAlreadyExists,
  }
  ```

  `MlDsaVerifyInvalidInput` can only happen from protocol version 87 (nearcore 2.14.0), because the
  `ml_dsa_verify` host function is available only from that version.

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
