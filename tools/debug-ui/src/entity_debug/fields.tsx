// Defines the field semantics for specific entity types.

import { NumericEntityKey, StringEntityKey } from './keys';
import { EntityKey, EntityKeyType, EntityType, FieldSemantic } from './types';

function stringField(keyType: EntityKeyType): FieldSemantic {
    return {
        parser: (_, v) => [new StringEntityKey(keyType, v)],
    };
}

function numericField(keyType: EntityKeyType): FieldSemantic {
    return {
        parser: (_, v) => {
            return [new NumericEntityKey(keyType, parseInt(v))] as EntityKey[];
        },
    };
}

const blockHash = stringField('block_hash');
const blockOrdinal = numericField('block_ordinal');
const blockHeight = numericField('block_height');
const chunkHash = stringField('chunk_hash');
const epochId = stringField('epoch_id');
const transactionHash = stringField('transaction_hash');
const receiptId = stringField('receipt_id');
const accountId = stringField('account_id');
const shardId = numericField('shard_id');
const shardUId: FieldSemantic = {
    parser: (_, v) => {
        const shardId = v.match(/^s(\d+)[.]v\d+$/);
        const shardIdKey = shardId ? [new NumericEntityKey('shard_id', parseInt(shardId[1]))] : [];
        return [new StringEntityKey('shard_uid', v), ...shardIdKey];
    },
};
const stateRoot = stringField('state_root');
const triePath: FieldSemantic = {
    parser: (_, v) => {
        return [new StringEntityKey('trie_path', v)] as EntityKey[];
    },
    display: 'trie_path',
};

const nibbles: FieldSemantic = {
    display: 'nibbles',
};

const trieKey: FieldSemantic = {
    display: 'nibbles',
    parser: (_, v) => {
        return [new StringEntityKey('trie_key', v)] as EntityKey[];
    },
};

const blockHeader = {
    struct: {
        height: blockHeight,
        prev_height: blockHeight,
        epoch_id: epochId,
        next_epoch_id: epochId,
        hash: blockHash,
        prev_hash: blockHash,
        block_ordinal: blockOrdinal,
        last_final_block: blockHash,
        last_ds_final_block: blockHash,
    },
};

const validatorStake = {
    titleKey: 'account_id',
};

const blockInfoV1V2 = {
    struct: {
        hash: blockHash,
        height: blockHeight,
        last_finalized_height: blockHeight,
        last_final_block_hash: blockHash,
        prev_hash: blockHash,
        epoch_first_block: blockHash,
        epoch_id: epochId,
        proposals: { array: validatorStake },
    },
};

const blockInfo = {
    struct: {
        V1: blockInfoV1V2,
        V2: blockInfoV1V2,
    },
};

const chunkHeader = {
    struct: {
        chunk_hash: chunkHash,
        prev_block_hash: blockHash,
        prev_state_root: stateRoot,
        height_created: blockHeight,
        height_included: blockHeight,
        shard_id: shardId,
    },
    titleKey: 'chunk_hash',
};

const chunkExtraV1V2V3 = {
    struct: {
        state_root: stateRoot,
        validator_proposals: { array: validatorStake },
    },
};

const chunkExtra = {
    struct: {
        V1: chunkExtraV1V2V3,
        V2: chunkExtraV1V2V3,
        V3: chunkExtraV1V2V3,
    },
};

const transaction = {
    struct: {
        signer_id: accountId,
        receiver_id: accountId,
        hash: transactionHash,
    },
    titleKey: 'hash',
};

const receipt = {
    struct: {
        predecessor_id: accountId,
        receiver_id: accountId,
        receipt_id: receiptId,
    },
    titleKey: 'receipt_id',
};

const chunk = {
    struct: {
        author: accountId,
        header: chunkHeader,
        transactions: { array: transaction },
        receipts: { array: receipt },
    },
};

const block = {
    struct: {
        author: accountId,
        header: blockHeader,
        chunks: {
            array: {
                ...chunkHeader,
                parser: (k: string) => [new NumericEntityKey('shard_id', parseInt(k))],
            },
        },
    },
};

const epochInfoV1V2V3V4 = {
    struct: {
        validators: {
            array: validatorStake,
        },
    },
};

const epochInfo = {
    struct: {
        V1: epochInfoV1V2V3V4,
        V2: epochInfoV1V2V3V4,
        V3: epochInfoV1V2V3V4,
        V4: epochInfoV1V2V3V4,
    },
};

const epochInfoAggregator = {
    struct: {
        epoch_id: epochId,
        last_block_hash: blockHash,
    },
};

const executionOutcome = {
    struct: {
        receipt_ids: { array: receiptId },
        executor_id: accountId,
    },
};

const trieNode = {
    struct: {
        path: nibbles,
        leaf_path: trieKey,
        extension: triePath,
        // The following is a bit of a hack. The children of a trie node are
        // returned with keys 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, a, , b, c, d, e, f;
        // the first 10 are numeric so they get interpreted as array indices,
        // the last 6 are not numeric so they get interpreted as struct fields.
        a: triePath,
        b: triePath,
        c: triePath,
        d: triePath,
        e: triePath,
        f: triePath,
    },
    array: triePath,
};

const tip = {
    struct: {
        height: blockHeight,
        last_block_hash: blockHash,
        prev_block_hash: blockHash,
        epoch_id: epochId,
        next_epoch_id: epochId,
    },
};

const stateSyncDumpProgress = {
    struct: {
        AllDumped: {
            struct: {
                epoch_id: epochId,
            },
        },
        Skipped: {
            struct: {
                epoch_id: epochId,
            },
        },
        InProgress: {
            struct: {
                epoch_id: epochId,
                sync_hash: blockHash,
            },
        },
    },
};

const blockMiscData = {
    struct: {
        head: tip,
        tail: blockHeight,
        chunk_tail: blockHeight,
        fork_tail: blockHeight,
        header_head: tip,
        final_head: tip,
        largest_target_height: blockHeight,
        genesis_state_roots: { array: stateRoot },
        cold_head: tip,
        state_sync_dump_progress: stateSyncDumpProgress,
    },
};

const flatStorageStatus = {
    struct: {
        Creation: {
            struct: {
                FetchingState: {
                    struct: {
                        block_hash: blockHash,
                    },
                },
                CatchingUp: blockHash,
            },
        },
        Ready: {
            struct: {
                flat_head: blockInfo,
            },
        },
    },
};

const flatStateChangeView = {
    struct: {
        key: trieKey,
    },
};

const flatStateChanges = {
    array: flatStateChangeView,
};

const flatStateDeltaMetadata = {
    struct: {
        block: blockInfo,
    },
};

const partialTrieNode: any = {
    struct: {
        extension: nibbles,
    },
};
partialTrieNode.struct.a = partialTrieNode;
partialTrieNode.struct.b = partialTrieNode;
partialTrieNode.struct.c = partialTrieNode;
partialTrieNode.struct.d = partialTrieNode;
partialTrieNode.struct.e = partialTrieNode;
partialTrieNode.struct.f = partialTrieNode;
partialTrieNode.array = partialTrieNode;

const partialTrie = {
    struct: {
        root: partialTrieNode,
    },
};

const chunkStateTransitionData = {
    struct: {
        base_state: partialTrie,
    },
};

const stateTransitionData = {
    array: chunkStateTransitionData,
};

const oneValidatorAssignment = {
    struct: {
        account_id: accountId,
    },
    titleKey: 'account_id',
};

const validatorAssignmentsAtHeight = {
    struct: {
        blockProducer: accountId,
        chunkProducers: { array: accountId },
        chunkValidatorAssignments: { array: { array: oneValidatorAssignment } },
    },
};

export const fieldSemantics: Record<EntityType, FieldSemantic> = {
    AllShards: { array: shardUId },
    Block: block,
    BlockHash: blockHash,
    BlockHeader: blockHeader,
    BlockInfo: blockInfo,
    BlockMerkleTree: undefined,
    BlockMiscData: blockMiscData,
    Chunk: chunk,
    ChunkExtra: chunkExtra,
    EpochInfo: epochInfo,
    EpochInfoAggregator: epochInfoAggregator,
    ExecutionOutcome: executionOutcome,
    FlatState: undefined,
    FlatStateChanges: flatStateChanges,
    FlatStateDeltaMetadata: flatStateDeltaMetadata,
    FlatStorageStatus: flatStorageStatus,
    Receipt: receipt,
    ShardId: shardId,
    ShardLayout: undefined,
    ShardUId: shardUId,
    StateTransitionData: stateTransitionData,
    Tip: tip,
    Transaction: transaction,
    TrieNode: trieNode,
    TrieRoot: triePath,
    ValidatorAssignmentsAtHeight: validatorAssignmentsAtHeight,
};
