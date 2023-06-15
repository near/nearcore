/// Each key type represents a unique kind of input to an entity debug query.
export type EntityKeyType =
    | 'block_hash'
    | 'block_ordinal'
    | 'block_height'
    | 'chunk_hash'
    | 'epoch_id'
    | 'transaction_hash'
    | 'receipt_id'
    | 'account_id'
    | 'shard_id'
    | 'shard_uid'
    | 'state_root'
    | 'trie_path'
    | 'trie_key';

/// Each entity type represents a unique kind of output from an entity debug
/// query.
export type EntityType =
    | 'Tip'
    | 'Block'
    | 'BlockHeader'
    | 'BlockHash'
    | 'Chunk'
    | 'EpochInfo'
    | 'Transaction'
    | 'Receipt'
    | 'ExecutionOutcome'
    | 'TrieRoot'
    | 'TrieNode'
    | 'ShardId'
    | 'ShardUId'
    | 'ShardLayout'
    | 'AllShards'
    | 'FlatStorageStatus'
    | 'FlatStateChanges'
    | 'FlatStateDeltaMetadata'
    | 'FlatState';

/// Interface for a concrete entity key.
export interface EntityKey {
    type(): EntityKeyType;

    /// A string representation, for display.
    toString(): string;

    /// JSON representation, for composing a query that takes this key as input.
    toJSON(): unknown;
}

/// Root node for any query.
export class EntityDataRootNode {
    constructor(
        /// The query that produces the data in this node.
        public query: EntityQuery,
        /// A promise that resolves to the result data.
        public entry: Promise<EntityDataValueNode>
    ) {}
}

/// A struct node.
export class EntityDataStructNode {
    public entries: EntityDataValueNode[] = [];
}

/// The semantics of a field that enhances its display.
export type FieldSemantic =
    | {
          /// Customizes how the field should be displayed.
          display?: CustomFieldDisplay;
          /// If present, this field represents one or more entity keys,
          /// and the parser provides the logic for parsing these keys out
          /// of the field name and value. Typically it's just one key.
          parser?: (key: string, value: string) => EntityKey[];
          /// If present, this field should be a struct, and we specify the
          /// semantic of each of its fields. Fields can be missing if it doesn't
          /// have any customizations.
          struct?: Record<string, FieldSemantic>;
          /// If present, this field should be an array, and we specify the
          /// semantic for each element of the array.
          array?: FieldSemantic;
          /// For struct or array fields, when displaying a node for the struct, if
          /// titleKey is present then display the value of that child field as the
          /// title of the struct node. This is useful for visualizing arrays where
          /// otherwise each element of the array would have to be separately expanded
          /// to know which element that is (for example an array of ValidatorStake).
          titleKey?: string;
      }
    /// Undefined means there's no special customization for this field.
    | undefined;

export type CustomFieldDisplay =
    /// Displays the value as a trie path (shard_uid/state_root/nibble_hex).
    | 'trie_path'
    /// Displays the value as a nibbles hex string.
    | 'nibbles';

/// Represents a value node in the entity data tree.
export class EntityDataValueNode {
    /// Any keys derived from this node.
    keys: EntityKey[] = [];
    /// Any children queries the user spawned from this node.
    queriedChildren: EntityDataRootNode[] = [];

    constructor(
        public semantic: FieldSemantic,
        public name: string,
        public value: string | EntityDataStructNode
    ) {
        const valueUsedForKeyDerivation = typeof value === 'string' ? value : '';
        if (semantic !== undefined && semantic.parser) {
            for (const key of semantic.parser(name, valueUsedForKeyDerivation)) {
                this.keys.push(key);
            }
        }
    }
}

/// Mirrors the EntityQuery type from Rust side.
export type EntityQuery = {
    TipAtHead?: null;
    TipAtHeaderHead?: null;
    TipAtFinalHead?: null;
    BlockByHash?: { block_hash: string };
    BlockHeaderByHash?: { block_hash: string };
    BlockHashByHeight?: { block_height: number };
    ChunkByHash?: { chunk_hash: string };
    EpochInfoByEpochId?: { epoch_id: string };
    TransactionByHash?: { transaction_hash: string };
    ReceiptById?: { receipt_id: string };
    OutcomeByTransactionHash?: { transaction_hash: string };
    OutcomeByTransactionHashAndBlockHash?: { transaction_hash: string; block_hash: string };
    OutcomeByReceiptId?: { receipt_id: string };
    OutcomeByReceiptIdAndBlockHash?: { receipt_id: string; block_hash: string };
    TrieRootByChunkHash?: { chunk_hash: string };
    TrieRootByStateRoot?: { state_root: string; shard_uid: string };
    TrieNode?: { trie_path: string };
    ShardIdByAccountId?: { account_id: string };
    ShardUIdByShardId?: { shard_id: number; epoch_id: string };
    ShardLayoutByEpochId?: { epoch_id: string };
    AllShardsByEpochId?: { epoch_id: string };
    FlatStorageStatusByShardUId?: { shard_uid: string };
    FlatStateByTrieKey?: { trie_key: string };
    FlatStateChangesByBlockHash?: { block_hash: string };
    FlatStateDeltaMetadataByBlockHash?: { block_hash: string };
};

export type EntityQueryType = keyof EntityQuery;

export function getQueryType(query: EntityQuery): EntityQueryType {
    return Object.keys(query)[0] as EntityQueryType;
}

export const entityQueryTypes: EntityQueryType[] = [
    'TipAtHead',
    'TipAtHeaderHead',
    'TipAtFinalHead',
    'BlockByHash',
    'BlockHeaderByHash',
    'BlockHashByHeight',
    'ChunkByHash',
    'EpochInfoByEpochId',
    'TransactionByHash',
    'ReceiptById',
    'OutcomeByTransactionHash',
    'OutcomeByTransactionHashAndBlockHash',
    'OutcomeByReceiptId',
    'OutcomeByReceiptIdAndBlockHash',
    'TrieRootByChunkHash',
    'TrieRootByStateRoot',
    'TrieNode',
    'ShardIdByAccountId',
    'ShardUIdByShardId',
    'ShardLayoutByEpochId',
    'AllShardsByEpochId',
    'FlatStorageStatusByShardUId',
    'FlatStateByTrieKey',
    'FlatStateChangesByBlockHash',
    'FlatStateDeltaMetadataByBlockHash',
];

/// See entityQueryKeyTypes.
export type EntityQueryKeySpec = {
    keyType: EntityKeyType;
    implicitOnly: boolean;
};

/// See entityQueryKeyTypes.
function queryKey(keyType: EntityKeyType): EntityQueryKeySpec {
    return { keyType, implicitOnly: false };
}

/// See entityQueryKeyTypes.
function implicitQueryKey(keyType: EntityKeyType): EntityQueryKeySpec {
    return { keyType, implicitOnly: true };
}

/// For each query, we specify here the key types it requires.
/// Each query's set of key types must be the same as the keys that the query
/// actually requires (as specified by the EntityQuery type).
///
/// Additionally, each key is either explicit or implicit. If a key is explicit,
/// it means that the query will show up as a prompt if the user hovers over a
/// node that provides that key. This is not true if the key is implicit.
///
/// For example, querying a ShardUIdByShardId requires a shard_id (explicit) and
/// an epoch_id (implicit). If the user hovers over a node that provides a shard_id,
/// this query would show up as a button, as that logically makes sense. But if the
/// user hovers over an epoch_id, this query would not show up.
///
/// For queries that require multiple keys, additional keys are specified by pinning.
export const entityQueryKeyTypes: Record<EntityQueryType, EntityQueryKeySpec[]> = {
    TipAtHead: [],
    TipAtHeaderHead: [],
    TipAtFinalHead: [],
    BlockByHash: [queryKey('block_hash')],
    BlockHeaderByHash: [queryKey('block_hash')],
    BlockHashByHeight: [queryKey('block_height')],
    ChunkByHash: [queryKey('chunk_hash')],
    EpochInfoByEpochId: [queryKey('epoch_id')],
    TransactionByHash: [queryKey('transaction_hash')],
    ReceiptById: [queryKey('receipt_id')],
    OutcomeByTransactionHash: [queryKey('transaction_hash')],
    OutcomeByTransactionHashAndBlockHash: [
        queryKey('transaction_hash'),
        implicitQueryKey('block_hash'),
    ],
    OutcomeByReceiptId: [queryKey('receipt_id')],
    OutcomeByReceiptIdAndBlockHash: [queryKey('receipt_id'), implicitQueryKey('block_hash')],
    TrieRootByChunkHash: [queryKey('chunk_hash')],
    TrieRootByStateRoot: [queryKey('state_root'), implicitQueryKey('shard_uid')],
    TrieNode: [queryKey('trie_path')],
    ShardIdByAccountId: [queryKey('account_id'), implicitQueryKey('epoch_id')],
    ShardUIdByShardId: [queryKey('shard_id'), implicitQueryKey('epoch_id')],
    ShardLayoutByEpochId: [queryKey('epoch_id')],
    AllShardsByEpochId: [queryKey('epoch_id')],
    FlatStorageStatusByShardUId: [queryKey('shard_uid')],
    FlatStateByTrieKey: [queryKey('trie_key'), implicitQueryKey('shard_uid')],
    FlatStateChangesByBlockHash: [queryKey('block_hash'), implicitQueryKey('shard_uid')],
    FlatStateDeltaMetadataByBlockHash: [queryKey('block_hash'), implicitQueryKey('shard_uid')],
};

/// Specifies the expected output entity type for each query.
export const entityQueryOutputType: Record<EntityQueryType, EntityType> = {
    TipAtHead: 'Tip',
    TipAtHeaderHead: 'Tip',
    TipAtFinalHead: 'Tip',
    BlockByHash: 'Block',
    BlockHeaderByHash: 'BlockHeader',
    BlockHashByHeight: 'BlockHash',
    ChunkByHash: 'Chunk',
    EpochInfoByEpochId: 'EpochInfo',
    TransactionByHash: 'Transaction',
    ReceiptById: 'Receipt',
    OutcomeByTransactionHash: 'ExecutionOutcome',
    OutcomeByTransactionHashAndBlockHash: 'ExecutionOutcome',
    OutcomeByReceiptId: 'ExecutionOutcome',
    OutcomeByReceiptIdAndBlockHash: 'ExecutionOutcome',
    TrieRootByChunkHash: 'TrieRoot',
    TrieRootByStateRoot: 'TrieRoot',
    TrieNode: 'TrieNode',
    ShardIdByAccountId: 'ShardId',
    ShardUIdByShardId: 'ShardUId',
    ShardLayoutByEpochId: 'ShardLayout',
    AllShardsByEpochId: 'AllShards',
    FlatStorageStatusByShardUId: 'FlatStorageStatus',
    FlatStateByTrieKey: 'FlatState',
    FlatStateChangesByBlockHash: 'FlatStateChanges',
    FlatStateDeltaMetadataByBlockHash: 'FlatStateDeltaMetadata',
};
