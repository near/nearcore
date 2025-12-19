# Contract Size Limit Analysis

## Executive Summary

This document provides a comprehensive analysis of NEAR Protocol's maximum contract size limit, addressing questions about the current limit, its rationale, historical evolution, and implications of raising it.

## Current State

### Max Contract Size
- **Current Limit**: 4,194,304 bytes (4 MiB)
- **Note**: The problem statement mentioned 1.5 MB, but this is incorrect. The actual limit has been 4 MiB.

### Where is it defined?

The max contract size is defined in multiple places:

1. **Primary Configuration**: `core/parameters/res/runtime_configs/parameters.yaml`
   ```yaml
   max_contract_size: 4_194_304
   ```

2. **Data Structure**: `core/parameters/src/vm.rs`
   ```rust
   pub struct LimitConfig {
       // ...
       /// Max contract size
       pub max_contract_size: u64,
       // ...
   }
   ```

3. **Validation**: `runtime/runtime/src/verifier.rs`
   ```rust
   fn validate_deploy_contract_action(
       limit_config: &LimitConfig,
       action: &DeployContractAction,
   ) -> Result<(), ActionsValidationError> {
       if action.code.len() as u64 > limit_config.max_contract_size {
           return Err(ActionsValidationError::ContractSizeExceeded {
               size: action.code.len() as u64,
               limit: limit_config.max_contract_size,
           });
       }
       Ok(())
   }
   ```

### Documentation References

The limit is documented in:
- `docs/RuntimeSpec/Actions.md` - States "4MiB" as the maximum contract size
- Used for both `DeployContract` and `DeployGlobalContract` actions

## Historical Analysis

### Has it always been 4 MiB?

Based on the codebase analysis:
- The repository history shows `4_194_304` has been the consistent value for recent protocol versions
- The parameter file uses the same base value across all protocol versions currently in the config store
- No evidence of the limit ever being 1.5 MB in this codebase

### Historical Protocol Features

The protocol has evolved over time with features affecting contracts:

**Protocol Version 49**: `_DeprecatedLimitContractFunctionsNumber`
- Limited the number of WASM functions in a contract
- Shows awareness of the need to limit contract complexity

**Protocol Version 53**: 
- `_DeprecatedIncreaseDeploymentCost` - Increased cost per deployed code byte to cover compilation overhead
- `_DeprecatedLimitContractLocals` - Limited global function local declarations in WASM
- Both features demonstrate fine-tuning of contract resource limits

**Protocol Version 48**: `_DeprecatedTransactionSizeLimit`
- Shows transaction size has also been adjusted historically

**Protocol Version 129**: `FixContractLoadingCost`
- Most recent contract-related protocol feature
- Fixes bugs in contract loading cost calculations

### Evolution of Contract Limits

While the max contract size has remained at 4 MiB, the protocol has continuously refined:
1. **Complexity limits** (functions, locals, tables)
2. **Economic costs** (deployment, loading)
3. **Performance characteristics** (caching, compilation)

This suggests a conservative approach: rather than increasing size limits, the protocol has focused on better managing existing limits.

## Implications of Raising the Limit

### 1. Storage Cost Implications

**Current Storage Economics:**
- Storage cost: `0.0001 NEAR per byte`
- For a 4 MiB contract: `4,194,304 * 0.0001 = 419.4304 NEAR` required for storage staking
- This is **NOT burned** - it's locked/staked while the contract exists
- If contract is deleted, the storage stake is returned

**If limit was raised to 8 MiB:**
- Storage cost would be: `8,388,608 * 0.0001 = 838.8608 NEAR`
- **Impact**: Doubles the storage staking requirement
- **Concern**: Higher barrier to entry for deploying large contracts

**If limit was raised to 16 MiB:**
- Storage cost would be: `16,777,216 * 0.0001 = 1,677.7216 NEAR`
- **Impact**: 4x the current storage staking requirement

### 2. Performance Implications

**Contract Loading:**
- Every contract invocation requires loading the WASM code
- Gas costs are charged per byte: `wasm_contract_loading_bytes: 216,750 gas/byte`
- Current max gas per call: `200 Tgas`

**Current 4 MiB contract:**
- Loading cost: `~0.91 Tgas` (0.45% of max gas)
- Storage stake: `419.43 NEAR`

**Potential increases:**

| Size | Storage Stake | Loading Cost | % of Max Gas |
|------|---------------|--------------|--------------|
| 4 MiB (current) | 419.43 NEAR | 0.91 Tgas | 0.45% |
| 8 MiB | 838.86 NEAR | 1.82 Tgas | 0.91% |
| 16 MiB | 1,677.72 NEAR | 3.64 Tgas | 1.82% |
| 32 MiB | 3,355.44 NEAR | 7.27 Tgas | 3.64% |

**Breaking point:** Contract loading would only exceed 10% of max gas at ~88 MiB (requiring 9,227 NEAR stake)

**Observations:**
- Gas costs scale linearly and remain reasonable even for large contracts
- Storage staking is the primary economic constraint
- At 16 MiB, the storage stake (~1,678 NEAR) becomes a significant barrier to entry

**Network and State Impact:**
- Larger contracts mean larger state
- State is replicated across all validators
- Affects state sync, snapshots, and backups
- **Concern**: Bandwidth for state witness proofs (stateless validation)

**Memory Considerations:**
- Contracts are validated and instantiated
- WebAssembly compilation overhead
- **Concern**: Peak memory usage during contract deployment

### 3. Transaction Size Constraints

Current configuration shows:
```yaml
max_transaction_size: 4_194_304
max_receipt_size: 4_294_967_295
```

**Critical Constraint**: `max_transaction_size` is also 4 MiB!
- A contract deployment must fit within a transaction
- **The effective max contract size is actually: `min(max_contract_size, max_transaction_size - transaction_overhead)`**
- Transaction overhead includes signatures, account IDs, nonce, etc. (~300-500 bytes)
- This means the practical limit is slightly less than 4 MiB

**Evidence from tests:**
From `integration-tests/src/tests/runtime/deployment.rs`:
```rust
// Testable max contract size is limited by both `max_contract_size` and by `max_transaction_size`
let max_contract_size = config.wasm_config.limit_config.max_contract_size;
let max_transaction_size = config.wasm_config.limit_config.max_transaction_size;
let contract_size = max_contract_size.min(max_transaction_size - tx_overhead);
```

**If we raise max_contract_size:**
- Must also raise `max_transaction_size` proportionally
- This has cascading effects on:
  - Network bandwidth
  - Transaction propagation
  - Block size limits
  - Memory pools

### 4. Stateless Validation Concerns

From the configuration:
```yaml
per_receipt_storage_proof_size_limit: 4_294_967_295
combined_transactions_size_limit: 4_294_967_295
```

The NEAR protocol is moving toward stateless validation where:
- Chunk producers generate state witnesses
- Validators verify chunks using these witnesses
- Larger contracts contribute to witness size
- **Concern**: Bandwidth limitations for witness distribution

## Why is the Limit 4 MiB?

Based on the analysis, the 4 MiB limit appears to be a balance between:

1. **Practicality**: 4 MiB is sufficient for most smart contracts
2. **Performance**: Keeps contract loading costs reasonable
3. **Storage Economics**: Manageable storage staking requirement (~419 NEAR)
4. **Network Constraints**: Fits within transaction size limits
5. **State Size**: Helps control overall blockchain state growth
6. **Powers of 2**: 4 MiB = 2^22 bytes, a clean binary value

## Recommendations

### Can we raise the limit?

**Yes, technically possible, but with trade-offs:**

1. **Short-term (e.g., to 8 MiB)**:
   - Would require protocol upgrade
   - Must also increase `max_transaction_size`
   - Storage cost doubles (~839 NEAR)
   - Still within reasonable gas costs
   - **Risk**: Medium - impacts state size and witness bandwidth

2. **Long-term (e.g., to 16+ MiB)**:
   - Significant storage staking burden (1,600+ NEAR)
   - Meaningful portion of gas budget for loading
   - State size concerns amplified
   - **Risk**: High - needs careful analysis of stateless validation impact

### Alternative Approaches

Rather than raising the limit, consider:

1. **Global Contracts** (already implemented):
   - Deploy once, use across multiple accounts
   - Amortizes storage cost
   - Reduces state duplication

2. **Code Splitting**:
   - Break large contracts into modular components
   - Use cross-contract calls
   - Better design pattern

3. **Compression**:
   - Store contracts compressed
   - Decompress during loading
   - Trades CPU for storage

4. **Dynamic Limits**:
   - Higher limits for accounts with more stake
   - Graduated pricing for larger contracts

## Existing Tests

The codebase includes a test that validates max contract size deployment:

**Location**: `integration-tests/src/tests/runtime/deployment.rs`

**Test**: `test_deploy_max_size_contract()`
- Creates a contract of maximum allowed size
- Validates it can be deployed within gas limits
- Confirms the constraint: `effective_max = min(max_contract_size, max_transaction_size - overhead)`
- Verifies total gas burnt stays within `max_gas_burnt` limit

**Key findings from the test:**
1. The test uses `near_test_contracts::sized_contract()` to generate contracts of specific sizes
2. Contract preparation (validation) is tested before deployment
3. Total gas includes both transaction conversion and deployment costs
4. The test confirms that a max-size contract CAN be deployed within current gas limits

## Testing Recommendations

If we decide to raise the limit, we should test:

1. **Performance benchmarks**:
   - Contract deployment time
   - First call latency (includes compilation)
   - Memory usage peaks

2. **State witness impact**:
   - Witness size for receipts containing large contracts
   - Bandwidth requirements

3. **Economic analysis**:
   - Impact on developer costs
   - Storage pool dynamics

## Conclusion

The current 4 MiB limit represents a well-considered balance between functionality and performance. While it's technically feasible to raise the limit, doing so would:

- **Increase storage staking requirements** proportionally
- **Impact performance** through higher contract loading costs
- **Affect state size and witness bandwidth** for stateless validation
- **Require transaction size limit increases** as well

Any change should be:
1. **Justified by real use cases** that cannot be solved with global contracts or code splitting
2. **Carefully benchmarked** for performance impact
3. **Analyzed for economic impact** on developers and the network
4. **Coordinated with stateless validation** development

The limit has remained at 4 MiB because it works well for the vast majority of use cases while keeping costs and performance reasonable.
