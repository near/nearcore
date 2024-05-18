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

@dataclass
class ChainSpan:
    start_time: datetime
    end_time: datetime
    node: NodeId

################################

@dataclass
class BlockEvent(ChainEvent):
    block_id: BlockId


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
class BlockSpan(ChainSpan):
    block_id: BlockId
    events: list[BlockEvent]    


@dataclass
class ShardSpan(ChainSpan):
    shard_id: ShardId
    events: list[ShardEvent]

@dataclass
class ChunkSpan(ShardSpan):
    chunk_id: ChunkId 

################################


@dataclass
class BlockHistory:
    block_id: BlockId
    spans: list[BlockSpan]

@dataclass
class ShardHistory:
    shard_id: ShardId
    spans: list[ShardSpan]


@dataclass
class ChainHistory:
    block_histories: dict[BlockId, list[BlockHistory]] = field(default_factory=dict)
    shard_histories: dict[ShardId, list[ShardHistory]] = field(default_factory=dict)

    def add_block_span(self, block_id: BlockId, span: BlockSpan):
        if block_id in self.block_histories:
            self.block_histories[block_id].append(BlockHistory(block_id, [span]))
        else:
            self.block_histories[block_id] = [BlockHistory(block_id, [span])]
    
    def add_shard_span(self, shard_id: ShardId, span: ShardSpan):
        if shard_id in self.shard_histories:
            self.shard_histories[shard_id].append(ShardHistory(shard_id, [span]))
        else:
            self.shard_histories[shard_id] = [ShardHistory(shard_id, [span])]


def check_block_event(event: TraceEvent) -> tuple[BlockId, BlockEvent] | None:
    return None


def check_shard_event(event: TraceEvent) -> tuple[ShardId, ShardEvent] | None:
    return None

def check_block_span(span: TraceSpan) -> tuple[BlockId, BlockSpan] | None:
    return None 

def check_shard_span(span: TraceSpan) -> tuple[ShardId, ShardSpan] | None:
    return None

def check_shard_span(event: TraceEvent):
    return None


def generate(trace_input: TraceInput) -> ChainHistory:

    chain_history = ChainHistory()

    for resource_span in trace_input.resource_spans:
        for span in resource_span.spans:
            block_id_span = check_block_span(span)
            if block_id_span is not None:
                (block_id, block_span) = block_id_span
                chain_history.add_block_event(block_id, block_span)
            shard_id_span = check_shard_span(span)
            if shard_id_span is not None:
                (shard_id_span, shard_span) = shard_id_span
                chain_history.add_shard_event(shard_id_span, shard_span)
    
    return chain_history
