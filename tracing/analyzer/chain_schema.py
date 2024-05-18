from trace_schema import *
from enum import Enum


BlockHash = str
ChunkHash = str

@dataclass
class BlockId:
    height: int | None
    block_hash: BlockHash | None

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, BlockId):
            return False
        if self.height is not None and self.height == value.height:
            return True
        if self.block_hash is not None and self.block_hash == value.block_hash:
            return True
        return False
    
    def __hash__(self) -> int:
        return hash((self.height, self.block_hash))

@dataclass
class ChunkId:
    shard_id: ShardId | None
    chunk_hash: ChunkHash | None

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, ChunkHash):
            return False
        if self.shard_id is not None and self.shard_id == value.shard_id:
            return True
        if self.chunk_hash is not None and self.chunk_hash == value.chunk_hash:
            return True
        return False
    
    def __hash__(self) -> int:
        return hash((self.shard_id, self.chunk_hash))


################################

@dataclass
class ChainEvent:
    time: datetime
    node: NodeId

################################

@dataclass
class BlockEvent(ChainEvent):
    block_id: BlockId
    block_hash: BlockHash


@dataclass
class BlockProduced(BlockEvent):
    producer: AccountId


@dataclass
class BlockEndorsementSent(BlockEvent):
    validator: AccountId
    producer: AccountId

@dataclass
class BlockEndorsementReceived(BlockEvent):
    validator: AccountId
    producer: AccountId
    


################################

@dataclass
class ShardEvent(ChainEvent):
    shard_id: ShardId

@dataclass
class ChunkEvent(ShardEvent):
    chunk_id: ChunkId

################################


@dataclass
class BlockHistory:
    block_id: BlockId
    events: list[BlockEvent]

@dataclass
class ShardHistory:
    shard_id: ShardId
    events: list[ChunkEvent]


@dataclass
class ChainHistory:
    block_histories: dict[BlockId, list[BlockHistory]] = field(default_factory=dict)
    shard_histories: dict[ShardId, list[ShardHistory]] = field(default_factory=dict)

    def add_block_event(self, block_id: BlockId, event: BlockEvent):
        if block_id in self.block_histories:
            self.block_histories[block_id].append(BlockHistory(block_id, [event]))
        else:
            self.block_histories[block_id] = [BlockHistory(block_id, [event])]
    
    def add_shard_event(self, shard_id: ShardId, event: ChunkEvent):
        if shard_id in self.shard_histories:
            self.shard_histories[shard_id].append(ShardHistory(shard_id, [event]))
        else:
            self.shard_histories[shard_id] = [ShardHistory(shard_id, [event])]


def check_block_event(event: TraceEvent):
    return (None, None)


def check_shard_event(event: TraceEvent):
    return (None, None)

def generate(trace_input: TraceInput) -> ChainHistory:

    chain_history = ChainHistory()

    for resource_span in trace_input.resource_spans:
        for span in resource_span.spans:
            for event in span.events:
                (block_id, block_event) = check_block_event(event)
                if block_id is not None:
                    assert block_event is not None
                    chain_history.add_block_event(block_id, block_event)
                (shard_id, shard_event) = check_shard_event(event)
                if shard_id is not None:
                    assert shard_event is not None
                    chain_history.add_shard_event(shard_id, shard_event)
    
    return chain_history
