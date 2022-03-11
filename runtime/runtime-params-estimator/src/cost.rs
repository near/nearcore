use std::fmt;
use std::str::FromStr;

/// Kinds of things we measure in parameter estimator and charge for in runtime.
///
/// TODO: Deduplicate this enum with `ExtCosts` and `ActionCosts`.
#[derive(Copy, Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
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
    DataReceiptCreationBase,
    DataReceiptCreationPerByte,
    ActionCreateAccount,
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
    /// Estimates `action_creation_config.deploy_contract_cost_per_byte`, which
    /// is charged for every byte in the WASM code when deploying the contract
    ///
    /// Estimation: Measure several cost for deploying several core contract in
    /// a transaction. Subtract base costs and apply least-squares on the
    /// results to find the per-byte costs.
    ActionDeployContractPerByte,
    ActionFunctionCallBase,
    ActionFunctionCallPerByte,
    ActionFunctionCallBaseV2,
    ActionFunctionCallPerByteV2,
    ActionTransfer,
    ActionStake,
    ActionAddFullAccessKey,
    ActionAddFunctionAccessKeyBase,
    ActionAddFunctionAccessKeyPerByte,
    ActionDeleteKey,
    ActionDeleteAccount,

    HostFunctionCall,
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
    Utf8DecodingBase,
    Utf8DecodingByte,
    Utf16DecodingBase,
    Utf16DecodingByte,

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
    LogBase,
    LogByte,

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
    /// access storage either through `storage_has_key`, `storage_read`, or
    /// `storage_write`. The fee is paid once for each unique trie node
    /// accessed.
    ///
    /// Estimation: Take the maximum of estimations for `TouchingTrieNodeRead`
    /// and `TouchingTrieNodeWrite`
    TouchingTrieNode,
    /// Helper estimation for `TouchingTrieNode`
    ///
    /// Estimation: Prepare an account that has many keys stored that are
    /// prefixes from each other. Then measure access cost for the shortest and
    /// the longest key. The gas estimation difference is divided by the
    /// difference of actually touched nodes.
    TouchingTrieNodeRead,
    /// Helper estimation for `TouchingTrieNode`
    ///
    /// Estimation: Prepare an account that has many keys stored that are
    /// prefixes from each other. Then measure write cost for the shortest and
    /// the longest key. The gas estimation difference is divided by the
    /// difference of actually touched nodes.
    TouchingTrieNodeWrite,
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
    AltBn128G1MultiexpByte,
    AltBn128G1MultiexpSublinear,
    AltBn128PairingCheckBase,
    AltBn128PairingCheckByte,
    AltBn128G1SumBase,
    AltBn128G1SumByte,

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
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Ridiculously inefficient, but shouldn't mater.
        for cost in Cost::all() {
            if cost.to_string() == s {
                return Ok(cost);
            }
        }
        Err(())
    }
}
