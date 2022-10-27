# near-epoch-manager crate

Epoch manager crate is responsible for code related to epochs and epoch switching.
An epoch is a unit of time when the set of validators of the network remain constant.

You can read more about the epoch here: https://docs.near.org/concepts/basics/epoch

You can read more about Epoch finalization and Epoch changes here: https://github.com/near/NEPs/blob/master/specs/BlockchainLayer/EpochManager/EpochManager.md 

## EpochManager
Main class that has two main functions:
* it creates new epochs (EpochIds)
* allows accessing information about past and current epochs (who is producing/approving given blocks, info about validators/fishermen etc  ).

### New Epoch Creation
When 'finalize_epoch' is called, the EpochManager will do all the necessary processing (like computing validator rewards for the current epoch (T), selecting validators for the next next epoch (T+2) etc) and create the new EpochId/EpochInfo.

### Accessing epoch information
EpochManager has also a lot of methords that allows you to fetch information from different past and present epochs (like who is the chunk/block producer for a given chunk/block, whether the block is at the end of epoch boundary and requires more signatures etc)


## RewardCalculator
RewardCalculator is responsible for computing rewards for the validators at the end of the epoch, based on their block/chunk productions.
You can see more details on the https://nomicon.io/Economics/README.html#validator-rewards-calculation

## Validator Selection / proposals / proposals_to_epoch_info
These files/functions are responsible for selecting the validators for the next epoch (and internally - also deciding which validator will produce which block and which chunk).

We've recently (Dec 2021) introduced a new algorithm for validator selection (AliasValidatorSelectionAlgorithm), which is the reason why you can see both the old 
and the new implementation present in the code - with new code existing in `validator_selection.rs`, while old code in `proposals.rs`.


## Shard assignments
This code is responsible for assigning validators (and chunk producers) to shards (chunks). This wil be used only once we enable `chunk_only_producers` feature (as before, we're simply assigning all the validators to validate each chunk).



## Epoch info aggregator
This is the class that keeps 'stats' for a given epoch (for example: info on how many blocks/chunks did the validators produce in the epoch, protocol version that validators support etc.). It is used to compute the validator rewards and new validators at the end of the epoch.

