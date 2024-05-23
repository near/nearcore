from dataclasses import dataclass, field
from trace_schema import *
from typing import Optional


BlockHash = str
ChunkHash = str


@dataclass
class AccountId:
    regex: ClassVar[re.Pattern] = re.compile(r'^AccountId\("(.+)"\)$')
    account_id: str = field(default_factory=str)

    def __post_init__(self):
        assert type(self.account_id) is str, "Account id must be a string, found %s" % str(
            type(self.account_id))
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
        if fields.shard_uid is not None:
            return ShardId(shard_uid=fields.shard_uid)
        return None


@dataclass
class BlockId:
    height: int

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, BlockId):
            return False
        return self.height == value.height

    def __hash__(self) -> int:
        return hash(self.height)

    def __str__(self) -> str:
        return f"Block(height={self.height})"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def extract(fields: Fields):
        if fields.height is not None:
            return BlockId(height=fields.height)
        return None


@dataclass
class ChunkId:
    shard_id: int

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, ChunkId):
            return False
        return self.shard_id == value.shard_id

    def __hash__(self) -> int:
        return hash(self.shard_id)
    
    def __str__(self) -> str:
        return f"Chunk(shard={self.shard_id})"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def extract(fields: Fields):
        if fields.shard_id is not None:
            return ChunkId(shard_id=fields.shard_id)
        return None

################################


@dataclass
class ChainEvent:
    name: str
    time: datetime
    fields: Fields


@dataclass
class ChainSpan:
    name: str
    start_time: datetime
    end_time: datetime
    fields: Fields
    events: list[ChainEvent]

################################


@dataclass
class BlockEvent(ChainEvent):
    block_id: BlockId

    @staticmethod
    def extract(event: TraceEvent, block_id: Optional[BlockId]=None):
        return BlockEvent(
            name=event.name,
            time=event.timestamp,
            block_id=block_id if block_id is not None else BlockId.extract(
                event.fields),
            fields=event.fields,
        )


@dataclass
class ChunkEvent(ChainEvent):
    chunk_id: ChunkId

    @staticmethod
    def extract(event: TraceEvent, chunk_id: Optional[ChunkId]=None):
        return ChunkEvent(
            name=event.name,
            time=event.timestamp,
            chunk_id=chunk_id if chunk_id is not None else ChunkId.extract(
                event.fields),
            fields=event.fields,
        )

################################


@dataclass
class BlockSpan(ChainSpan):
    block_id: BlockId

    @staticmethod
    def extract(span: TraceSpan, events: list[BlockEvent] = []):
        # If no events are given, use all the events in the span.
        # Otherwise, check if the ids are consistent.
        block_id = BlockId.extract(span.fields)
        if len(events) == 0:
            assert block_id is not None, "Failed to extract block id from span fields: %s" % str(
                span)
            events = [] # [BlockEvent.extract(e, block_id) for e in span.events]
        else:
            block_id = block_id if block_id is not None else events[0].block_id
            for event in events:
                assert event.block_id == block_id, "Span has events with different block ids: %s" % str(
                    span)

        return BlockSpan(
            block_id=block_id,
            name=span.name,
            start_time=span.start_time,
            end_time=span.end_time,
            fields=span.fields,
            events=events,
        )


@dataclass
class ChunkSpan(ChainSpan):
    chunk_id: ChunkId

    @staticmethod
    def extract(span: TraceSpan, events: list[ChunkEvent] = []):
        # If no events are given, use all the events in the span.
        # Otherwise, check if the ids are consistent.
        chunk_id = ChunkId.extract(span.fields)
        if len(events) == 0:
            assert chunk_id is not None, "Failed to extract chunk id from span fields: %s" % str(
                span)
            events = [] # [ChunkEvent.extract(e, chunk_id) for e in span.events]
        else:
            chunk_id = chunk_id if chunk_id is not None else events[0].chunk_id
            for event in events:
                assert event.chunk_id == chunk_id, "Span has events with different block ids: %s" % str(
                    span)

        return ChunkSpan(
            chunk_id=chunk_id,
            name=span.name,
            start_time=span.start_time,
            end_time=span.end_time,
            fields=span.fields,
            events=events,
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
    start_time: datetime
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


BLOCK_SPAN_OR_EVENT_NAMES = {"send_block_approval", "collect_block_approval",
                             "produce_block", "postprocess_block"}

CHUNK_SPAN_OR_EVENT_NAMES = {"produced_chunk", "produce_chunk", "request_missing_chunk",
                             "process_partial_encoded_chunk_request", "process_partial_encoded_chunk", 
                             "process_chunk_state_witness", "send_chunk_state_witness", "validate_chunk_state_witness",
                             "apply_new_chunk", "apply_old_chunk"}


def check_block_event(event: TraceEvent) -> Optional[BlockEvent]:
    if event.name in BLOCK_SPAN_OR_EVENT_NAMES:
        return BlockEvent.extract(event)
    return None


def check_chunk_event(event: TraceEvent) -> Optional[ChunkEvent]:
    if event.name in CHUNK_SPAN_OR_EVENT_NAMES:
        return ChunkEvent.extract(event)
    return None


def check_block_span(span: TraceSpan) -> Optional[BlockSpan]:
    block_events = []
    for event in span.events:
        block_event = check_block_event(event)
        if block_event is not None:
            assert block_event.block_id is not None, "Failed to extract block id from event: %s" % str(
                event)
            block_events.append(block_event)

    if len(block_events) > 0 or span.name in BLOCK_SPAN_OR_EVENT_NAMES:
        return BlockSpan.extract(span, block_events)
    return None


def check_chunk_span(span: TraceSpan) -> Optional[ChunkSpan]:
    chunk_events = []
    for event in span.events:
        chunk_event = check_chunk_event(event)
        if chunk_event is not None:
            assert chunk_event.chunk_id is not None, "Failed to extract chunk id from event: %s" % str(
                event)
            chunk_events.append(chunk_event)

    if len(chunk_events) > 0 or span.name in CHUNK_SPAN_OR_EVENT_NAMES:
        return ChunkSpan.extract(span, chunk_events)
    return None


def generate(trace_input: TraceInput) -> ChainHistory:

    chain_history = ChainHistory(start_time=trace_input.start_time)

    for resource_span in trace_input.resource_spans:
        for span in resource_span.spans:
            block_span = check_block_span(span)
            if block_span is not None:
                assert block_span.block_id is not None, "Failed to extract block id from span: %s" % str(
                    span)
                print("Adding new block span: %s events=%s" % (block_span.name, str([e.name for e in block_span.events])))
                chain_history.add_block_span(block_span.block_id, block_span)

            chunk_span = check_chunk_span(span)
            if chunk_span is not None:
                assert chunk_span.chunk_id is not None, "Failed to extract chunk id from span: %s" % str(
                    span)
                print("Adding new chunk span: %s events=%s" % (chunk_span.name, str([e.name for e in chunk_span.events])))
                chain_history.add_chunk_span(chunk_span.chunk_id, chunk_span)

    return chain_history
