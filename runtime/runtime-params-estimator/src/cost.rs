use std::fmt;
use std::str::FromStr;

/// Kinds of things we measure in parameter estimator and charge for in runtime.
///
/// TODO: Deduplicate this enum with `ExtCosts` and `ActionCosts`.
#[derive(Copy, Clone, PartialEq, Eq, Debug, PartialOrd, Ord, clap::ValueEnum)]
#[repr(u8)]
pub enum Cost {
    // Every set of actions in a transaction needs to be transformed into a
    // action receipt, regardless of whether it is executed locally within the
    // same block or delayed. The gas cost for this is paid when the receipt is
    // created. The base amount is `action_receipt_creation_config.execution` +
    // (either `action_receipt_creation.send_sir` or
    // `action_receipt_creation.send_not_sir` depending on whether
    // `sender == receiver`).
    // On top of that, each type of action has its own costs defined, which is
    // added for each action included in the receipt.
    //
    /// Estimates `ActionCosts::new_receipt`, which is the base cost for
    /// creating a new action receipt, excluding actual action costs.
    ///
    /// Estimation: Measure the creation and execution of an empty action
    /// receipt, where sender and receiver are two different accounts. This
    /// involves applying two blocks and both contribute to the measured cost.
    /// The cost is then divided by 2 to split between `send_not_sir` and
    /// `execution` cost. `send_sir` is set to the same value as `send_not_sir`
    /// for now.
    ActionReceiptCreation,
    /// Estimates `ActionCosts::new_receipt`.`send_sir`. Although, note that it
    /// is currently configured to be the same as `send_not_sir`. But we already
    /// use this value as partial estimation of other action costs.
    ///
    /// Estimation: Measure the creation and execution of an empty action
    /// receipt, where sender and receiver are the same account.
    ActionSirReceiptCreation,
    ActionReceiptCreationSendNotSir,
    ActionReceiptCreationSendSir,
    ActionReceiptCreationExec,
    /// Estimates `data_receipt_creation_config.base_cost`, which is charged for
    /// every data dependency of created receipts. This occurs either through
    /// calls to `promise_batch_then` or `value_return`. Dispatch and execution
    /// costs are both burnt upfront.
    ///
    /// Estimation: Measure two functions that each create 1000 promises but
    /// only one of them also creates a callback that depends on the promise
    /// results. The difference in execution cost is divided by 1000.
    DataReceiptCreationBase,
    DataReceiptCreationBaseSendNotSir,
    DataReceiptCreationBaseSendSir,
    DataReceiptCreationBaseExec,
    /// Estimates `data_receipt_creation_config.cost_per_byte`, which is charged
    /// for every byte in data dependency of created receipts. This occurs
    /// either through calls to `promise_batch_then` or `value_return`. Dispatch
    /// and execution costs are both burnt upfront.
    ///
    /// Estimation: Measure two functions that each create 1000 promises with a
    /// callback that depends on the promise results. One of the functions
    /// creates small data receipts, the other large ones. The difference in
    /// execution cost is divided by the total byte difference.
    DataReceiptCreationPerByte,
    DataReceiptCreationPerByteSendNotSir,
    DataReceiptCreationPerByteSendSir,
    DataReceiptCreationPerByteExec,
    /// Estimates `action_creation_config.create_account_cost` which is charged
    /// for `CreateAccount` actions, the same value on sending and executing.
    ///
    /// Estimation: Measure a transaction that creates an account and transfers
    /// an initial balance to it. Subtract the base cost of creating a receipt.
    // TODO(jakmeier): consider also subtracting transfer fee
    ActionCreateAccount,
    ActionCreateAccountSendNotSir,
    ActionCreateAccountSendSir,
    ActionCreateAccountExec,
    // Deploying a new contract for an account on the blockchain stores the WASM
    // code in the trie. Additionally, it also triggers a compilation of the
    // code to check that it is valid WASM. The compiled code is then stored in
    // the database, in a separate column family for cached contract code. The
    // costs charged for such a contract deployment action is
    // `ActionDeployContractBase` + `N` * `ActionDeployContractPerByte`, where
    // `N` is the number of bytes in the WASM code.
    /// Estimates `action_creation_config.deploy_contract_cost`, which is
    /// charged once per contract deployment
    ///
    /// Estimation: Measure deployment cost of a "smallest" contract.
    ActionDeployContractBase,
    ActionDeployContractBaseSendNotSir,
    ActionDeployContractBaseSendSir,
    ActionDeployContractBaseExec,
    /// Estimates `action_creation_config.deploy_contract_cost_per_byte`, which
    /// is charged for every byte in the WASM code when deploying the contract
    ///
    /// Estimation: Measure several cost for deploying several core contract in
    /// a transaction. Subtract base costs and apply least-squares on the
    /// results to find the per-byte costs.
    ActionDeployContractPerByte,
    ActionDeployContractPerByteSendNotSir,
    ActionDeployContractPerByteSendSir,
    ActionDeployContractPerByteExec,
    /// Estimates `action_creation_config.function_call_cost`, which is the base
    /// cost for adding a `FunctionCallAction` to a receipt. It aims to account
    /// for all costs of calling a function that are already known on the caller
    /// side.
    ///
    /// Estimation: Measure the cost to execute a NOP function on a tiny
    /// contract. To filter out block and general receipt processing overhead,
    /// the difference between calling it N +1 times and calling it once in a
    /// transaction is divided by N. Executable loading cost is also subtracted
    /// from the final result because this is charged separately.
    ActionFunctionCallBase,
    ActionFunctionCallBaseSendNotSir,
    ActionFunctionCallBaseSendSir,
    ActionFunctionCallBaseExec,
    /// Estimates `action_creation_config.function_call_cost_per_byte`, which is
    /// the incremental cost for each byte of the method name and method
    /// arguments cost for adding a `FunctionCallAction` to a receipt.
    ///
    /// Estimation: Measure the cost for a transaction with an empty function
    /// call with a large argument value. Subtract the cost of an empty function
    /// call with no argument. Divide the difference by the length of the
    /// argument.
    ActionFunctionCallPerByte,
    ActionFunctionCallPerByteSendNotSir,
    ActionFunctionCallPerByteSendSir,
    ActionFunctionCallPerByteExec,
    /// Estimates `action_creation_config.transfer_cost` which is charged for
    /// every `Action::Transfer`, the same value for sending and executing.
    ///
    /// Estimation: Measure a transaction with only a transfer and subtract the
    /// base cost of creating a receipt.
    ActionTransfer,
    ActionTransferSendNotSir,
    ActionTransferSendSir,
    ActionTransferExec,
    /// Estimates `action_creation_config.stake_cost` which is charged for every
    /// `Action::Stake`, a slightly higher value for sending than executing.
    ///
    /// Estimation: Measure a transaction with only a staking action and
    /// subtract the base cost of creating a sir-receipt.
    ///
    /// Note: The exec cost is probably a copy-paste mistake. (#8185)
    ActionStake,
    ActionStakeSendNotSir,
    ActionStakeSendSir,
    ActionStakeExec,
    /// Estimates `action_creation_config.add_key_cost.full_access_cost` which
    /// is charged for every `Action::AddKey` where the key is a full access
    /// key. The same value is charged for sending and executing.
    ///
    /// Estimation: Measure a transaction that adds a full access key and
    /// subtract the base cost of creating a sir-receipt.
    ActionAddFullAccessKey,
    ActionAddFullAccessKeySendNotSir,
    ActionAddFullAccessKeySendSir,
    ActionAddFullAccessKeyExec,
    /// Estimates `action_creation_config.add_key_cost.function_call_cost` which
    /// is charged once for every `Action::AddKey` where the key is a function
    /// call key. The same value is charged for sending and executing.
    ///
    /// Estimation: Measure a transaction that adds a function call key and
    /// subtract the base cost of creating a sir-receipt.
    ActionAddFunctionAccessKeyBase,
    ActionAddFunctionAccessKeyBaseSendNotSir,
    ActionAddFunctionAccessKeyBaseSendSir,
    ActionAddFunctionAccessKeyBaseExec,
    /// Estimates
    /// `action_creation_config.add_key_cost.function_call_cost_per_byte` which
    /// is charged once for every byte in null-terminated method names listed in
    /// an `Action::AddKey` where the key is a function call key. The same value
    /// is charged for sending and executing.
    ///
    /// Estimation: Measure a transaction that adds a function call key with
    /// many methods. Subtract the cost of adding a function call key with a
    /// single method and of creating a sir-receipt. The result is divided by
    /// total bytes in the method names.
    ActionAddFunctionAccessKeyPerByte,
    ActionAddFunctionAccessKeyPerByteSendNotSir,
    ActionAddFunctionAccessKeyPerByteSendSir,
    ActionAddFunctionAccessKeyPerByteExec,
    /// Estimates `action_creation_config.delete_key_cost` which is charged for
    /// `DeleteKey` actions, the same value on sending and executing. It does
    /// not matter whether it is a function call or full access key.
    ///
    /// Estimation: Measure a transaction that deletes a full access key and
    /// transfers an initial balance to it. Subtract the base cost of creating a
    /// receipt.
    // TODO(jakmeier): check cost for function call keys with many methods
    ActionDeleteKey,
    ActionDeleteKeySendNotSir,
    ActionDeleteKeySendSir,
    ActionDeleteKeyExec,
    /// Estimates `action_creation_config.delete_account_cost` which is charged
    /// for `DeleteAccount` actions, the same value on sending and executing.
    ///
    /// Estimation: Measure a transaction that deletes an existing account.
    /// Subtract the base cost of creating a sir-receipt.
    /// TODO(jakmeier): Consider different account states.
    ActionDeleteAccount,
    ActionDeleteAccountSendNotSir,
    ActionDeleteAccountSendSir,
    ActionDeleteAccountExec,
    /// Estimates `action_creation_config.delegate_cost` which is charged
    /// for `DelegateAction` actions.
    ActionDelegate,
    ActionDelegateSendNotSir,
    ActionDelegateSendSir,
    ActionDelegateExec,
    /// Estimates `wasm_config.ext_costs.base` which is intended to be charged
    /// once on every host function call. However, this is currently
    /// inconsistent. First, we do not charge on Math API methods (`sha256`,
    /// `keccak256`, `keccak512`, `ripemd160`, `alt_bn128_g1_multiexp`,
    /// `alt_bn128_g1_sum`, `alt_bn128_pairing_check`). Furthermore,
    /// `promise_then` and `promise_create` are convenience wrapper around two
    /// other host functions, which means they end up charing this fee twice.
    ///
    /// Estimation: Measure a transaction with a smart contract function call
    /// that, from within the WASM runtime, invokes the host function
    /// `block_index()` many times. Subtract the cost of executing a smart
    /// contract function that does nothing. Divide the difference by the number
    /// of host function calls.
    HostFunctionCall,
    /// Estimates `wasm_config.regular_op_cost` which is charged for every
    /// executed WASM operation in function calls, as counted dynamically during
    /// execution.
    ///
    /// Estimation: Run a contract that reads and writes lots of memory in an
    /// attempt to cause slow loads and stores. The total time spent in the
    /// runtime is divided by the number of executed instructions.
    WasmInstruction,

    // # Reading and writing memory
    // The hosting runtime sometimes copies data between in and out of WASM
    // buffers defined by smart contract code. The smart contract code defines
    // their side of the buffers as WASM address + length. Copies going from
    // WASM to host memory are called *reads*, whereas *writes* are going from
    // host to WASM memory.
    //
    // The following is a best-effort list of all host functions that may
    // produce memory reads or writes
    //
    // Read:
    //  - Creating promises for actions: The data describing the actions.
    //  - Writing to a register: The data to be written to the register.
    //  - Using various math API functions: Reading the operand values.
    //  - On log or abort/panic: Reading string data from memory.
    //
    // Write
    //  - Reading from a register: The data from the register is copied into
    //    WASM memory.
    //  - Host function calls such as `account_balance()` and
    //    `validator_stake()` that return a value by writing it to a pointer.
    //
    /// Estimates `ext_costs.read_memory_base` which is charged once every time
    /// data is copied from WASM memory to the hosting runtime as a result of
    /// executing a contract.
    ///
    /// Estimation: Execute a transaction with a single function call that calls
    /// `value_return()` 10'000 times with a 10 byte value. Subtract the cost of
    /// an empty function call and divide the rest by 10'000.
    ReadMemoryBase,
    /// Estimates `ext_costs.read_memory_byte` which is charged as an
    /// incremental cost per byte each time WASM memory is copied to the host.
    ///
    /// Estimation: Execute a transaction with a single function call that calls
    /// `value_return()` 10'000 times with a 1 MiB sized value. Subtract the
    /// cost of an empty function call and divide the rest by 10'000 * 1Mi.
    ReadMemoryByte,
    /// Estimates `ext_costs.write_memory_base` which is charged once every time
    /// data is copied from the host to WASM memory as a result of executing a
    /// contract.
    ///
    /// Estimation: Execute a transaction with a single function call that
    /// writes 10 bytes to a register  and calls `read_register` 10'000 times to
    /// copy it back to WASM memory. Subtract the cost of an empty function call
    /// and divide the rest by 10'000.
    WriteMemoryBase,
    /// Estimates `ext_costs.write_memory_byte` which is charged as an
    /// incremental cost per byte each time data is copied from the host to WASM
    /// memory.
    ///
    /// Estimation: Execute a transaction with a single function call that
    /// writes 1MiB to a register  and calls `read_register` 10'000 times to
    /// copy it back to WASM memory. Subtract the cost of an empty function call
    /// and divide the rest by 10'000 * 1Mi.
    WriteMemoryByte,

    // # Register API
    // Instead of relying on WASM memory, some host functions operate on
    // registers. These registers are allocated outside the WASM memory but need
    // to be copied in and out of WASM memory if a contract want to access them.
    // This copying is done through `read_register` and `write_register`.
    /// Estimates `read_register_base` which is charged once for every reading access to a register.
    ///
    /// Estimation: Execute a transaction with a single function call that
    /// writes 10 bytes to a register once and then calls `value_return` with
    /// that register 10'000 times. Subtract the cost of an empty function call
    /// and divide the rest by 10'000.
    ReadRegisterBase,
    /// Estimates `read_register_byte` which is charged per byte for every reading access to a register.
    ///
    /// Estimation: Execute a transaction with a single function call that
    /// writes 1 MiB to a register once and then calls `value_return` with
    /// that register 10'000 times. Subtract the cost of an empty function call
    /// and divide the rest by 10'000 * 1Mi.
    ReadRegisterByte,
    /// Estimates `write_register_base` which is charged once for every writing access to a register.
    ///
    /// Estimation: Execute a transaction with a single function call that
    /// writes 10B to a register 10'000 times. Subtract the cost of an empty
    /// function call and divide the rest by 10'000.
    WriteRegisterBase,
    /// Estimates `write_register_byte` which is charged per byte for every writing access to a register.
    ///
    /// Estimation: Execute a transaction with a single function call that
    /// writes 1 MiB to a register 10'000 times. Subtract the cost of an empty
    /// function call and divide the rest by 10'000 * 1Mi.
    WriteRegisterByte,

    /// Estimates `utf8_decoding_base` which is charged once for each time
    /// something is logged in UTF8 or when panicking.
    ///
    /// Estimation: Execute a transaction with a single function that logs
    /// 10'000 times a small string. Divide the cost by 10'000.
    Utf8DecodingBase,
    /// Estimates `utf8_decoding_byte` which is charged for each byte in the
    /// output of logging in UTF8 or panicking.
    ///
    /// Estimation: Execute a transaction with a single function that logs
    /// a 10kiB string many times. Divide the cost by total bytes
    /// logged. One more details, to cover both null-terminated strings and
    /// fixed-length strings, both versions are measured and the maximum is
    /// taken.
    Utf8DecodingByte,
    /// Estimates `utf16_decoding_base` which is charged once for each time
    /// something is logged in UTF16.
    ///
    /// Estimation: Execute a transaction with a single function that logs
    /// 10'000 times a small string. Divide the cost by 10'000.
    Utf16DecodingBase,
    /// Estimates `utf16_decoding_byte` which is charged for each byte in the
    /// output of logging in UTF16.
    ///
    /// Estimation: Execute a transaction with a single function that logs a
    /// 10kiB string many times. Divide the cost by total bytes logged. One more
    /// details, to cover both null-terminated strings and fixed-length strings,
    /// both versions are measured and the maximum is taken.
    Utf16DecodingByte,
    /// Estimates `log_base` which is charged once every time log output is
    /// produced, either through logging functions (UTF8 or UTF16) or when
    /// panicking.
    ///
    /// Estimation: Execute a transaction with a single function that logs
    /// 10'000 times a small string (using UTF16 to be pessimistic). Divide the
    /// cost by 10'000.
    ///
    /// Note: This currently uses the identical estimation as
    /// `Utf16DecodingBase`
    LogBase,
    /// Estimates `log_byte` which is charged for every byte of log output
    /// produced, either through logging functions (UTF8 or UTF16) or when
    /// panicking.
    ///
    /// Estimation: Execute a transaction with a single function that logs a
    /// 10kiB string N times (using UTF16 to be pessimistic). Divide the cost by
    /// total bytes of output produced, which is 3/2 * N * 10 * 1024.
    LogByte,

    // Cryptographic host functions:
    // The runtime provides host functions for sha256, keccak256, keccak512,
    // ripemd160 hashes. Plus, there is a ECDSA signature verification host
    // function.
    // All these host functions are estimated by executing a transaction with a
    // single function call in them, that just invokes the given host function.
    // To measure the cost of additional bytes or blocks, a function call with a
    // large argument is measured and the total cost divided by total input
    // bytes (or blocks in the case of the RIPEMD hash).
    /// Estimates `sha256_base`, the cost charged once per call to the
    /// sha256-hash host function.
    Sha256Base,
    /// Estimates `sha256_byte`, the cost charged per input byte in calls to the
    /// sha256-hash host function.
    Sha256Byte,
    /// Estimates `keccak256_base`, the cost charged once per call to the
    /// keccak256-hash host function.
    Keccak256Base,
    /// Estimates `keccak256_byte`, the cost charged per input byte in calls to the
    /// keccak256-hash host function.
    Keccak256Byte,
    /// Estimates `keccak512_base`, the cost charged once per call to the
    /// keccak512-hash host function.
    Keccak512Base,
    /// Estimates `keccak512_byte`, the cost charged per input byte in calls to the
    /// keccak512-hash host function.
    Keccak512Byte,
    /// Estimates `ripemd160_base`, the cost charged once per call to the
    /// ripemd160-hash host function.
    Ripemd160Base,
    /// Estimates `ripemd160_block`, the cost charged per input block in calls
    /// to the ripemd160-hash host function. Blocks are 64 bytes, except for the
    /// last which may be smaller. The exact number of blocks charged as a
    /// function of input bytes `n` is `blocks(n) = (n + 9).div_ceil(64)`.
    Ripemd160Block,
    /// Estimates `ecrecover_base`, which covers the full cost of the host
    /// function `ecrecover` to verify an ECDSA signature and extract the
    /// signer.
    EcrecoverBase,
    /// Estimates `ed25519_verify_base`, which covers the base cost of the host
    /// function `ed25519_verify` to verify an ED25519 signature.
    ///
    /// Estimation: Use a fixed signature embedded in the test contract and
    /// verify it `N` times in a loop and divide by `N`. The overhead of other
    /// costs is negligible compared to the two elliptic curve scalar
    /// multiplications performed for signature validation.
    ///
    /// Note that the multiplication algorithm used is not constant time, i.e.
    /// it's timing varies depending on the input. Testing with a range of
    /// random signatures, the difference is +/-10%. Testing with extreme
    /// inputs, it can be more than 20% faster than a  random case. But it seems
    /// on the upper end, there is a limit to how many additions and doublings
    /// need to be performed.
    /// In conclusion, testing on a single input is okay, if we account for the
    /// 10-20% variation.
    Ed25519VerifyBase,
    /// Estimates `ed25519_verify_byte`, the cost charged per input byte in calls to the
    /// ed25519_verify host function.
    ///
    /// Estimation: Verify a signature for a large message many times, subtract
    /// the cost estimated for the base and divide the remainder by the total
    /// bytes the message.
    ///
    /// The cost per byte for pure verification is just the cost for hashing.
    /// This is comparable to the cost for reading values from memory or
    /// registers, which is currently not subtracted in the estimation.
    /// Subtracting it would lead to high variance. Instead, one has to take it
    /// into account that memory overhead is included when mapping the
    /// estimation to a parameter.
    /// In the end, the cost should be low enough, compared to the base cost,
    /// that it does not matter all that much if we overestimate it a bit.
    Ed25519VerifyByte,
    // `storage_write` records a single key-value pair, initially in the
    // prospective changes in-memory hash map, and then once a full block has
    // been processed, in the on-disk trie. If there was already a value
    // stored, it is overwritten and the old value is returned to the caller.
    /// Estimates `ExtCost::storage_write_base` which is charged once per call
    /// to `storage_write`.
    ///
    /// Estimation: Contract call that writes N small values and divide the
    /// cost by N.
    StorageWriteBase,
    /// Estimates `ExtCost::storage_write_key_byte` which is charged for each
    /// byte in keys of `storage_write` calls.
    ///
    /// Estimation: Contract call that writes N small values with a big key
    /// (10kiB) and divide the cost by total number of key bytes.
    StorageWriteKeyByte,
    /// Estimates `ExtCost::storage_write_value_byte` which is charged for each
    /// byte in values of `storage_write` calls.
    ///
    /// Estimation: Contract call that writes N big values (10kiB) and divide
    /// the cost by total number of value bytes.
    StorageWriteValueByte,
    /// Estimates `ExtCosts::storage_write_evicted_byte` which is charged for
    /// each byte in a value that is overwritten in `storage_write` calls.
    ///
    /// Estimation: Contract call that writes N values to keys that already
    /// contain big values (10kiB).
    StorageWriteEvictedByte,

    // `read_storage` reads a single value from either prospective changes if
    // present or from the on-disk trie otherwise.
    /// Estimates `ExtCost::storage_read_base` which is charged once per call
    /// to `storage_read`.
    ///
    /// Estimation: Contract call that reads N small values and divide the cost
    /// by N.
    StorageReadBase,
    /// Estimates `ExtCost::storage_read_key_byte` which is charged for each
    /// byte in keys of `storage_read` calls.
    ///
    /// Estimation: Contract call that reads N small values with a big key
    /// (10kiB) and divide the cost by total number of key bytes.
    StorageReadKeyByte,
    /// Estimates `ExtCost::storage_read_value_byte` which is charged for each
    /// byte in values of `storage_read` calls.
    ///
    /// Estimation: Contract call that reads N big values (10kiB) and divide
    /// the cost by total number of value bytes.
    StorageReadValueByte,

    // `storage_remove` adds a deletion transaction to the prospective changes,
    // which is applied at the end of the block.
    /// Estimates `ExtCost::storage_remove_base` which is charged once per call
    /// to `storage_remove`.
    ///
    /// Estimation: Contract call that removes N small values and divide the
    /// cost by N.
    StorageRemoveBase,
    /// Estimates `ExtCost::storage_remove_key_byte` which is charged for each
    /// byte in keys of `storage_remove` calls.
    ///
    /// Estimation: Contract call that removes N small values with a big key
    /// (10kiB) and divide the cost by total number of key bytes.
    StorageRemoveKeyByte,
    /// Estimates `ExtCost::storage_remove_value_byte` which is charged for
    /// each byte in values of `storage_remove` calls.
    ///
    /// Estimation: Contract call that removes N big values (10kiB) and divide
    /// the cost by total number of value bytes.
    StorageRemoveRetValueByte,

    // `storage_has_key` checks if the key currently has an associated value.
    // First, checked in the prospective changes, if nothing found, also in
    // on-disk trie.
    /// Estimates `ExtCost::storage_has_key_base` which is charged once per
    /// call to `storage_has_key`.
    ///
    /// Estimation: Contract call that removes N small values with small keys
    /// and divide the cost by N.
    StorageHasKeyBase,
    /// Estimates `ExtCost::storage_has_key_byte` which is charged for each
    /// byte in calls to `storage_has_key`.
    ///
    /// Estimation: Contract call that removes N small values with big keys
    /// (10kiB) and divide the cost by total key bytes.
    StorageHasKeyByte,

    /// DEPRECATED: Was charged in `storage_iter_prefix`
    StorageIterCreatePrefixBase,
    /// DEPRECATED: Was charged in `storage_iter_prefix`
    StorageIterCreatePrefixByte,
    /// DEPRECATED: Was charged in `storage_iter_range`
    StorageIterCreateRangeBase,
    /// DEPRECATED: Was charged in `storage_iter_range`
    StorageIterCreateFromByte,
    /// DEPRECATED: Was charged in `storage_iter_range`
    StorageIterCreateToByte,
    /// DEPRECATED: Was charged in `storage_iter_next`
    StorageIterNextBase,
    /// DEPRECATED: Was charged in `storage_iter_next`
    StorageIterNextKeyByte,
    /// DEPRECATED: Was charged in `storage_iter_next`
    StorageIterNextValueByte,

    /// Estimates `touching_trie_node` which is charged when smart contracts
    /// access storage either through `storage_has_key`, `storage_read`,
    /// `storage_write` or `storage_remove`. The fee is paid once for each
    /// unique trie node accessed.
    ///
    /// Estimation: Prepare an account that has many keys stored that are
    /// prefixes from each other. Then measure write cost for the shortest and
    /// the longest key. The gas estimation difference is divided by the
    /// difference of actually touched nodes.
    TouchingTrieNode,
    /// It is similar to `TouchingTrieNode`, but it is charged instead of this
    /// cost when we can guarantee that trie node is cached in memory, which
    /// allows us to charge less costs.
    ///
    /// Estimation: Since this is a small cost, it cannot be measured accurately
    /// through the normal process of measuring several transactions and
    /// calculating the difference. Instead, the estimation directly
    /// instantiates the caching storage and reads nodes of the largest possible
    /// size from it. This is done in a pessimistic setup and the 90th
    /// percentile of measured samples is taken as the final cost. The details
    /// for this are a bit involved but roughly speaking, it just forces values
    /// out of CPU caches so that they are always read from memory.
    ReadCachedTrieNode,
    /// Estimates `promise_and_base` which is charged for every call to
    /// `promise_and`. This should cover the base cost for creating receipt
    /// dependencies.
    ///
    /// Estimation: Currently not estimated
    PromiseAndBase,
    /// Estimates `promise_and_per_promise` which is charged for every promise in
    /// calls to `promise_and`. This should cover the additional cost for each
    /// extra receipt in the dependency.
    ///
    /// Estimation: Currently not estimated
    PromiseAndPerPromise,
    /// Estimates `promise_return` which is charged when calling
    /// `promise_return`. This should cover the cost of the dependency between a
    /// promise and the current function call return value.
    ///
    /// Estimation: Currently not estimated
    PromiseReturn,
    /// Estimates `validator_stake_base` which is charged for each call to
    /// `validator_stake`, covering the cost for looking up if an account is a
    /// validator and if so, how much it has staked. This information is
    /// available from the local EpochManager.
    ///
    /// Estimation: Currently not estimated
    ValidatorStakeBase,
    /// Estimates `validator_total_stake_base` which is charged for each call to
    /// `validator_total_stake`, covering the cost for looking up the total
    /// staked tokens for the current epoch. This information is
    /// available from the local EpochManager.
    ///
    /// Estimation: Currently not estimated
    ValidatorTotalStakeBase,

    AltBn128G1MultiexpBase,
    AltBn128G1MultiexpElement,
    AltBn128G1MultiexpSublinear,
    AltBn128PairingCheckBase,
    AltBn128PairingCheckElement,
    AltBn128G1SumBase,
    AltBn128G1SumElement,

    // Costs used only in estimator
    //
    /// Costs associated with applying an empty block. This overhead is not
    /// charged to any specific account and thus does not directly affect gas
    /// fees. However, for estimation this is a crucial value to know. Many
    /// estimation methods require to know this value in order to subtract it
    /// from the measurement.
    ApplyBlock,
    // Compilation happens during deployment and the pre-compiled code is stored
    // in the DB. Thus, compilation cost is part of deployment cost and not a
    // cost we charge in isolation. But how expensive compilation is, is an
    // important value to track nevertheless.
    // We have two alternatives to estimate compilation cost.
    //
    /// `ContractCompileBase` and `ContractCompileBytes` are estimated together,
    /// by compiling several core contracts and computing least-squares on the
    /// code sizes and execution times.
    ContractCompileBase,
    ContractCompileBytes,
    /// Contract compile costs V2 is an alternative estimation for the
    /// compilation cost. Instead of least-squares method, it finds a linear
    /// function that is higher than any of the measured contracts.
    ///
    /// Estimation: Compiles a "smallest possible" contract and several core
    /// contracts. The smallest contract is taken as the first anchor the linear
    /// function. The second defining point is which ever of the core contracts
    /// produces the steepest line.
    ContractCompileBaseV2,
    ContractCompileBytesV2,
    /// The cost of contract deployment per byte, without the compilation cost.
    ///
    /// Estimation: Measure the deployment costs of two data-only contracts,
    /// where one data sections is empty and the other is max size as allowed by
    /// contract size limits. The cost difference is pure overhead of moving
    /// around bytes that are not code. Divide this cost by the difference of
    /// bytes.
    DeployBytes,
    /// Estimates `wasm_contract_loading_base` which is charged once per contract
    /// that is loaded from the database to execute a method on it.
    ///
    /// Estimation: Measure the cost to execute an empty contract method
    /// directly on a runtime instance, using different sizes of contracts.
    /// Use least-squares to calculate base and per-byte cost.
    /// The contract size is scaled by adding more methods to it. This has been
    /// identified as particular expensive in terms of per-byte loading time.
    /// This makes it a better scaling strategy than, for example, adding large
    /// constants in the data section.
    ContractLoadingBase,
    /// Estimates the executable loading part of `wasm_contract_loading_bytes`
    /// which is charged for each byte in a contract when it is loaded as an
    /// executable.
    ///
    /// This cost also has to cover the reading of the code from the database.
    /// So technically, it covers code loading from database and also executable
    /// loading. But it is still charged right before executable loading because
    /// pre-charging for loading from the database is not possible without
    /// knowing the code size.
    ///
    /// Estimation: See `ContractLoadingBase`.
    ContractLoadingPerByte,
    /// Estimates the storage loading part of `wasm_contract_loading_bytes`.
    ///
    /// See comment on `ContractLoadingPerByte` why these are combined.
    ///
    /// Estimation: Measure the cost difference of two transactions calling a
    /// trivial smart contract method, where one contract has a large data
    /// section and the other contract is very small. Divide the difference in
    /// size.
    FunctionCallPerStorageByte,
    GasMeteringBase,
    GasMeteringOp,
    /// Cost of inserting a new value directly into a RocksDB instance.
    /// In default settings, this is an alternative estimation for
    /// `StorageWriteValueByte`, measured in a more controlled setup.
    /// Using the extra flags prefixed with `rdb-`, this can be used to measure
    /// the impact of various RocksDB settings on insertions.
    RocksDbInsertValueByte,
    /// Cost of reading values directly from a RocksDB instance.
    /// In default settings, this is an alternative estimation for
    /// `StorageReadValueByte`, measured in a more controlled setup.
    /// Using the extra flags prefixed with `rdb-`, this can be used to measure
    /// the impact of various RocksDB settings on read performance.
    RocksDbReadValueByte,
    IoReadByte,
    IoWriteByte,
    CpuBenchmarkSha256,
    OneCPUInstruction,
    OneNanosecond,

    __Count,
}

impl Cost {
    pub fn all() -> impl Iterator<Item = Cost> {
        (0..(Cost::__Count as u8)).map(Cost::try_from).map(Result::unwrap)
    }
}

impl TryFrom<u8> for Cost {
    type Error = ();

    fn try_from(d: u8) -> Result<Self, ()> {
        if d < (Cost::__Count as u8) {
            // SAFETY: `#[repr(u8)]`, the above range, and the language spec
            // guarantee that `d` can be cast to the enum.
            Ok(unsafe { std::mem::transmute::<u8, Cost>(d) })
        } else {
            Err(())
        }
    }
}

impl fmt::Display for Cost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl FromStr for Cost {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Ridiculously inefficient, but shouldn't mater.
        for cost in Cost::all() {
            if cost.to_string() == s {
                return Ok(cost);
            }
        }
        anyhow::bail!("failed parsing {s} as Cost");
    }
}
