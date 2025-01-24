# Distributing contracts separately from state witness

The current chunk state witness structure is inefficient due to a significant portion being consumed by contract code. This document describes an implementation for optimizing the state witness size. The changes described are guarded by the protocol feature `ExcludeContractCodeFromStateWitness`.

In feature `ExcludeContractCodeFromStateWitness`, we optimize the state witness size by distributing the contract code separately from the witness, under the following observations:

1. New contracts are deployed infrequently, so the function calls are often made to the same contract and we distribute the same contract code in the witness many times at different heights.
2. Once deployed, a contract is compiled and stored in the compiled-contract cache. This cache is stored in the disk and persistent across epochs, until the VM configuration changes. The chunk application uses this cache to bypass the trie traversal and storage reads to fetch the uncompiled contract code from storage. The cache is maintained with high hit rates by both chunk producers and chunk validators.

## Deploying a contract to an account

A contract is deployed and called through a Near account.
When a contract is deployed to an account with id `account_id`, (by running a `DeployContractAction`), the deployment of the contract is recorded in *State* in two places:

1. The `code_hash` field of the `Account` struct (pointed by the key `TrieKey::Account{account_id}`) is updated to store the hash of the (uncompiled) contract code ([link](https://github.com/near/nearcore/blob/ba0b23768e21eed3428024d19e763a37a2e055dd/core/primitives-core/src/account.rs#L64)). Note that, when no contract is deployed, this field contains `CryptoHash::default()`.
2. A new entry is created with key `TrieKey::ContractCode{account_id}` and value containing the (uncompiled) code of the contract.

When the contract is deployed, it is also pre-compiled, and the compiled code is persisted on disk in the compiled-contract cache. When applying a function call to a pre-compiled contract, the implementation skips reading from the trie and directly invokes the compiled code from the cache.

## Calling a function from contract

In stateless validation, all accesses to the state by actions are recorded in the state witness, so that the chunk validators can validate the chunk by applying the same actions.
A `FunctionCallAction` represents calling a method of the contract deployed to an account.
The function call is fulfilled by either retrieving the pre-compiled contract from the cache or compiling the code right before execution.

Note that, not all chunk validators may contain the contract in their compiled-contract cache.
Thus, the contract code (which is also part of the State) should be available when validating the state witness.
To address this, independent of whether the compiled contract cache was hit or not, all the accesses to the uncompiled code are recorded by [explicitly reading](https://github.com/near/nearcore/blob/82707e8edfd1af7b1d2e5bb1c82ccf768c313e7c/core/store/src/trie/mod.rs#L1628-L1637) from the key `TrieKey::ContractCode{account_id}`, which records all the internal trie nodes from the state root upto the leaf node and the value (code) itself.
Thus, each function call will contribute to the state witness size at least the contract code, which may go up to `4 MB`.

## Excluding contract code from state witness

When `ExcludeContractCodeFromStateWitness` is enabled, we distribute the following in parallel to the state witness:

1. Hashes of the contracts code called during the chunk application. We do not include the contract code in this message. Instead, the chunk validators request the contracts missing in their compiled-contract cache.
1. Code of the contracts deployed during the chunk application. We distribute this message only to the validators other than the chunk validators, since the chunk validators can access the new contract code and update their cache by applying the deploy actions (contained in the incoming receipts) in the state witness.

### Collecting contract accesses and deployments

In order to identify which contracts to distribute, we collect (1) the hashes of the contracts called by a `FunctionCallAction` and (2) contract code deployed by a `DeployContractAction`.
When `ExcludeContractCodeFromStateWitness` is enabled, the chunk producer performs the following when applying the receipts in a chunk (note that it is done by all the chunk producers tracking the same shard):

- For function calls, it skips recoding the read of the value from `TrieKey::ContractCode{account_id}`. Instead, it just records the hash of the contract code. [The `TrieUpdate::record_contract_call` function](https://github.com/near/nearcore/blob/82707e8edfd1af7b1d2e5bb1c82ccf768c313e7c/core/store/src/trie/update.rs#L267) called when executing a `FunctionCallAction` implements the different behaviors with and without the feature enabled.
- For contract deployments, it records the code deployed when executing a `DeployContractAction`, by calling [the `TrieUpdate::record_contract_deploy` function](https://github.com/near/nearcore/blob/82707e8edfd1af7b1d2e5bb1c82ccf768c313e7c/core/store/src/trie/update.rs#L255).

Both information is collected in the `ContractStorage` in the `TrieUpdate`.
While finalizing the `TrieUpdate` after applying the chunk is finished, we [take out this information](https://github.com/near/nearcore/blob/82707e8edfd1af7b1d2e5bb1c82ccf768c313e7c/runtime/runtime/src/lib.rs#L2082), packages them in a struct called [`ContractUpdates`](https://github.com/near/nearcore/blob/82707e8edfd1af7b1d2e5bb1c82ccf768c313e7c/core/primitives/src/stateless_validation/contract_distribution.rs#L374) and [pass it](https://github.com/near/nearcore/blob/82707e8edfd1af7b1d2e5bb1c82ccf768c313e7c/runtime/runtime/src/lib.rs#L2143) to upstream callers in the `ApplyChunk` and then to `ApplyChunkResult`. Finally, we write the data in `ContractUpdates` to the database [in the `StoredChunkStateTransitionData`](https://github.com/near/nearcore/blob/82707e8edfd1af7b1d2e5bb1c82ccf768c313e7c/core/primitives/src/stateless_validation/stored_chunk_state_transition_data.rs#L35) along with the partial state recorded during the chunk application.

### Sending contract updates to validators

Upon finishing producing the new chunk, the chunk producer reconstructs `ContractUpdates` from the database (by reading `StoredChunkStateTransitionData`) along with the other state-transition data, generates the state witness, and [sends (in the same `DistributeStateWitnessRequest`)](https://github.com/near/nearcore/blob/82707e8edfd1af7b1d2e5bb1c82ccf768c313e7c/chain/client/src/stateless_validation/state_witness_producer.rs#L75-L114) both the state witness and `ContractUpdates` to the `PartialWitnessActor`.

NOTE: All the operations described in the rest of this document are performed in the `PartialWitnessActor`.

`PartialWitnessActor` distributes the state witness and the contract updates in the following order ([see code here](https://github.com/near/nearcore/blob/82707e8edfd1af7b1d2e5bb1c82ccf768c313e7c/chain/client/src/stateless_validation/partial_witness/partial_witness_actor.rs#L207-L246)):

1. It first sends the hashes of the contract code accessed to the chunk validators (except for the validators that track the same shard). This allows validators to check their compiled-contract cache and request code for the missing contracts, while waiting for the witness parts. This is sent in a message called `ChunkContractAccesses`.
1. It then send the state witness parts to witness-part owners.
1. It finally sends the new contracts deployed to the validators that do not validate the witness in the current turn. This allows the other validators to update their compiled-contract cache for the later turns when they become a chunk validator for the respective shard. The parts are sent in a message called `PartialEncodedContractDeploys`. The code for deployed contracts is distributed to validators in parts after compressing and encoding in `Reed-Solomon code`, similarly to how the state witness is sent in parts.

### Handling contract accesses messages in chunk validators

When a chunk validator receives `ChunkContractAccesses`, it [checks its local cache](https://github.com/near/nearcore/blob/82707e8edfd1af7b1d2e5bb1c82ccf768c313e7c/chain/client/src/stateless_validation/partial_witness/partial_witness_actor.rs#L567) to see if it is missing any contracts needed to validate the incoming witness. If any contract is missing, it sends a message called `ContractCodeRequest` to a random chunk producer that tracks the same shard (which balances the requests across the chunk producers that can provide the missing contract code).
While waiting for the parts of the state witness, the validator also waits for the response to the contract code request (if any). If this request is not fulfilled before enough state-witness parts are collected, the validation fails.

A chunk producer receiving the `ContractCodeRequest` validates the request against the saved state transition data (note that all chunk producers tracking the same shard saves the `ContractUpdates` in the `StoredChunkStateTransitionData` in the database) and responds with the `ContractCodeResponse` containing the (compressed) contract code.

### Handling deployed code messages in other validators

When a validator receives a `PartialEncodedContractDeploys` message for a newly deployed contract, it starts collecting the parts using its [`PartialEncodedContractDeploysTracker`](https://github.com/near/nearcore/blob/82707e8edfd1af7b1d2e5bb1c82ccf768c313e7c/chain/client/src/stateless_validation/partial_witness/partial_deploys_tracker.rs#L74). The tracker waits until the sufficient number of parts are collected, then decodes and decompresses the original contracts and [compiles them in parallel](https://github.com/near/nearcore/blob/82707e8edfd1af7b1d2e5bb1c82ccf768c313e7c/chain/client/src/stateless_validation/partial_witness/partial_witness_actor.rs#L522). The compilation internally persists the compiled contract code in the local cache, so that the validator can use the compiled code later when validating the upcoming chunk witnesses.
