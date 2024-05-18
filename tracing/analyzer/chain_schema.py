from trace_schema import *
from enum import Enum


BlockHash = str
ChunkHash = str


@dataclass
class AccountId:
    regex: ClassVar[re.Pattern] = re.compile(r'^AccountId\("(.+)"\)$')
    account_id: str = field(default_factory=str)

    def __post_init__(self):
        assert type(self.account_id) is str, f"Account id must be a string, found {type(self.account_id)}"
        self.account_id = self.account_id.lower()
        m = AccountId.regex.match(self.account_id)
        if m:
            self.account_id = m.group(1)

    def __eq__(self, other):
        if not isinstance(other, AccountId):
            return False
        return self.account_id == other.account_id

    def __hash__(self) -> int:
        return hash(self.account_id)

    @staticmethod
    def extract(fields: Fields):
        if fields.account_id is not None:
            return AccountId(account_id=fields.account_id)
        return None


@dataclass
class NodeId:
    node_id: str = field(default_factory=str)

    @staticmethod
    def extract(fields: Fields):
        if fields.node_id is not None:
            return NodeId(node_id=fields.node_id)
        return None


@dataclass
class ShardId:
    regex: ClassVar[re.Pattern] = re.compile(r'^([^.]+).[^.]+$')
    shard_id: int = field(default_factory=int)

    def __post_init__(self):
        if type(self.shard_id) is str:
            m = ShardId.regex.match(self.shard_id)
            if m:
                self.shard_id = int(m.group(1))
            else:
                self.shard_id = int(self.shard_id)

    def __eq__(self, other):
        if not isinstance(other, ShardId):
            return False
        return self.shard_id == other.shard_id

    def __hash__(self) -> int:
        return hash(self.shard_id)
    
    @staticmethod
    def extract(fields: Fields):
        if fields.shard_id is not None:
            return ShardId(shard_id=fields.shard_id)
        if  fields.shard_uid is not None:
            return ShardId(shard_uid=fields.shard_uid)
        return None


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

    @staticmethod
    def extract(fields: Fields):
        if fields.height is not None or fields.block_hash is not None:
            return BlockId(height=fields.height, block_hash=fields.block_hash)
        return None


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

    @staticmethod
    def extract(fields: Fields):
        assert fields.shard_id is not None or fields.chunk_hash is not None, f"Failed to extract chunk id from fields: {fields}"
        return ChunkId(shard_id=fields.shard_id, chunk_hash=fields.chunk_hash)

################################


@dataclass
class ChainEvent:
    name: str
    time: datetime


@dataclass
class ChainSpan:
    name: str
    start_time: datetime
    end_time: datetime
    node_id: NodeId
    account_id: AccountId
    events: list[ChainEvent]

################################


@dataclass
class BlockEvent(ChainEvent):
    block_id: BlockId

    @staticmethod
    def extract(event: TraceEvent):
        return BlockEvent(
            name=event.name,
            time=event.timestamp,
            block_id=BlockId.extract(event.fields),
        )

@dataclass
class ChunkEvent(ChainEvent):
    chunk_id: ChunkId

    @staticmethod
    def extract(event: TraceEvent):
        return ChunkEvent(
            name=event.name,
            time=event.timestamp,
            chunk_id=ChunkId.extract(event.fields),
        )

################################


@dataclass
class BlockSpan(ChainSpan):
    block_id: BlockId

    @staticmethod
    def extract(span: TraceSpan, events: list[BlockEvent]=[]):
        return BlockSpan(
            name=span.name,
            start_time=span.start_time,
            end_time=span.end_time,
            node_id=NodeId.extract(span.fields),
            account_id=AccountId.extract(span.fields),
            block_id=BlockId.extract(span.fields),
            events=[events if events else [BlockEvent.extract(e) for e in span.events]],
        )

@dataclass
class ChunkSpan(ChainSpan):
    chunk_id: ChunkId

    @staticmethod
    def extract(span: TraceSpan, events: list[ChunkEvent]=[]):
        return ChunkSpan(
            name=span.name,
            start_time=span.start_time,
            end_time=span.end_time,
            node_id=NodeId.extract(span.fields),
            account_id=AccountId.extract(span.fields),
            chunk_id=ChunkId.extract(span.fields),
            events=[events if events else [ChunkEvent.extract(e) for e in span.events]],
        )

################################

@dataclass
class BlockHistory:
    block_id: BlockId
    spans: list[BlockSpan]


@dataclass
class ChunkHistory:
    chunk_id: ChunkId
    spans: list[ChunkSpan]


################################


@dataclass
class ChainHistory:
    block_histories: dict[BlockId, BlockHistory] = field(default_factory=dict)
    chunk_histories: dict[ChunkId, ChunkHistory] = field(default_factory=dict)

    def add_block_span(self, block_id: BlockId, span: BlockSpan):
        if block_id in self.block_histories:
            self.block_histories[block_id].spans.append(span)
        else:
            self.block_histories[block_id] = BlockHistory(block_id, [span])

    def add_chunk_span(self, chunk_id: ChunkId, span: ChunkSpan):
        if chunk_id in self.chunk_histories:
            self.chunk_histories[chunk_id].spans.append(span)
        else:
            self.chunk_histories[chunk_id] = ChunkHistory(chunk_id, [span])


def check_block_event(event: TraceEvent) -> BlockEvent | None:
    BLOCK_EVENTS = {"Sending an approval"}
    if event.name in BLOCK_EVENTS:
        return BlockEvent.extract(event)

def check_chunk_event(event: TraceEvent) -> ChunkEvent | None:
    CHUNK_EVENTS = {}
    if event.name in CHUNK_EVENTS:
        return ChunkEvent.extract(event)

def check_block_span(span: TraceSpan) -> BlockSpan | None:
    BLOCK_SPANS = {"produce_block"}
    if span.name in BLOCK_SPANS:
        return BlockSpan.extract(span)
    block_events = []
    for event in span.events:
        block_event = check_block_event(event)
        if block_event is not None:
            assert block_event.block_id is not None, f"Failed to extract block id from event: {event}"
            block_events.append(BlockSpan.extract(span))
    if len(block_events) > 0:
        block_id = block_events[0].block_id
        for block_event in block_events:
            assert block_event.block_id == block_id, f"Events have different block id. Event1={block_events[0]}, Event2={block_event}"
        return BlockSpan.extract(span, block_events)

def check_chunk_span(span: TraceSpan) -> ChunkSpan | None:
    CHUNK_SPANS = {}
    if span.name in CHUNK_SPANS:
        return ChunkSpan.extract(span)
    chunk_events = []
    for event in span.events:
        chunk_event = check_chunk_event(event)
        if chunk_event is not None:
            assert chunk_event.chunk_id is not None, f"Failed to extract chunk id from event: {event}"
            chunk_events.append(ChunkSpan.extract(span))
    if len(chunk_events) > 0:
        return ChunkSpan.extract(span, chunk_events)


def generate(trace_input: TraceInput) -> ChainHistory:

    chain_history = ChainHistory()

    for resource_span in trace_input.resource_spans:
        for span in resource_span.spans:
            block_span = check_block_span(span)
            if block_span is not None:
                assert block_span.block_id is not None, f"Failed to extract block id from span: {span}"
                chain_history.add_block_span(block_span.block_id, block_span)
            
            chunk_span = check_chunk_span(span)
            if chunk_span is not None:
                assert chunk_span.chunk_id is not None, f"Failed to extract chunk id from span: {span}"
                chain_history.add_chunk_span(chunk_span.chunk_id, chunk_span)

    return chain_history
