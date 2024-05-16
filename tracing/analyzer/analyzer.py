#!/usr/bin/env python3

import datetime
import json
import re
import sys
import time
import pathlib

from typing import ClassVar
from dataclasses import dataclass, field


@dataclass
class AccountId:
    regex: ClassVar[re.Pattern] = re.compile(r'^AccountId\("(.+)"\)$')
    account_id: str = field(default_factory=str)

    def __post_init__(self):
        assert type(self.account_id) is str
        self.account_id = self.account_id.lower()
        m = AccountId.regex.match(self.account_id)
        if m:
            self.account_id = m.group(1)


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

SKIP_FIELDS = {"code.filepath", "code.namespace", "code.lineno", "level",
               'was_requested', 'target_height', 'hash', 'prev_height',
               'block_producers', 'approval_inner', 'sync_status',
               'new_chunks_count', 'blocks_missing_chunks', 'me',
               'endorsement', 'num_outgoing_receipts',
               'num_filtered_transactions', 'service.name',
               'should_produce_chunk', 'peer_id', 'orphans_missing_chunks',
               'err', 'provenance', 'busy_ns', 'num_blocks', 'validator', 'next_height', 'new_chunks', 'skip_produce_chunk', 'epoch_id', 'next_bp', 'pool_size', 'is_syncing', 'prev_block_hash', 'status', 'thread.name', 'idle_ns', 'header_head_height'}


@dataclass
class Fields:
    node_id: str = field(default_factory=str)
    account_id: str = field(default_factory=AccountId)
    chain_id: str = field(default_factory=str)
    servie_name: str = field(default_factory=str)
    target: str = field(default_factory=str)
    thread_id: int = field(default_factory=int)
    shard_id: int = field(default_factory=ShardId)
    height: int = field(default_factory=int)
    block_hash: str = field(default_factory=str)
    prev_hash: str = field(default_factory=str)
    chunk_hash: str = field(default_factory=str)

    unknown_fields: ClassVar[set] = set()

    @staticmethod
    def load(attributes: dict):
        fields = Fields()
        for attribute in attributes:
            if attribute['key'] in SKIP_FIELDS:
                continue
            if attribute['key'] == 'node_id':
                fields.node_id = attribute['value']['stringValue']
            elif attribute['key'] in {'account_id', 'validator_id'}:
                fields.account_id = AccountId(
                    account_id=attribute['value']['stringValue'])
            elif attribute['key'] == 'chain_id':
                fields.chain_id = attribute['value']['stringValue']
            elif attribute['key'] in {'service_name', 'service.name'}:
                fields.servie_name = attribute['value']['stringValue']
            elif attribute['key'] == 'target':
                fields.target = attribute['value']['stringValue']
            elif attribute['key'] in {'thread_id', 'thread.id'}:
                fields.thread_id = attribute['value']['intValue']
            elif attribute['key'] == 'shard_id':
                fields.shard_id = ShardId(attribute['value']['stringValue'])
            elif attribute['key'] in {'height', 'block_height'}:
                fields.height = int(attribute['value']['stringValue'])
            elif attribute['key'] == 'block_hash':
                fields.block_hash = attribute['value']['stringValue']
            elif attribute['key'] == 'prev_hash':
                fields.prev_hash = attribute['value']['stringValue']
            elif attribute['key'] == 'chunk_hash':
                fields.chunk_hash = attribute['value']['stringValue']
            else:
                Fields.unknown_fields.add(attribute['key'])
        return fields


@dataclass
class Event:
    fields: Fields
    name: str = field(default_factory=str)
    timestamp: datetime = field(default_factory=datetime)


@dataclass
class Span:
    trace_id: str
    span_id: str
    parent_id: str
    name: str
    start_time: datetime
    end_time: datetime
    fields: Fields
    events: list[Event]


@dataclass
class ScopeSpan:
    spans: list[Span]


@dataclass
class ResourceSpan:
    fields: Fields
    spans: list[Span]



def parse_trace_file(trace_file_path) -> list[ResourceSpan]:
    trace_file_path = sys.argv[1]
    with open(trace_file_path, 'r') as trace_file:
        traces_json = json.load(trace_file)

    num_spans = 0
    num_events = 0
    resource_spans = []

    for trace_json in traces_json:
        for resource_span_json in trace_json['resourceSpans']:
            for scope_span_json in resource_span_json['scopeSpans']:
                spans = []
                for span_json in scope_span_json['spans']:
                    events = []
                    for event_json in span_json["events"]:
                        events.append(Event(fields=Fields.load(event_json['attributes']), name=event_json['name'],
                                            timestamp=datetime.datetime.fromtimestamp(event_json['timeUnixNano'] / 10.0**9),))
                    num_events += len(events)
                    spans.append(Span(fields=Fields.load(span_json['attributes']),
                                      events=events,
                                      trace_id=span_json['traceId'],
                                      span_id=span_json['spanId'],
                                      parent_id=span_json['parentSpanId'], name=span_json['name'],
                                      start_time=datetime.datetime.fromtimestamp(
                                          span_json['startTimeUnixNano'] / 10.0**9),
                                      end_time=datetime.datetime.fromtimestamp(span_json['endTimeUnixNano'] / 10.0**9)))
                num_spans += len(spans)
            resource_spans.append(ResourceSpan(fields=Fields.load(resource_span_json['resource']['attributes']),
                                               spans=spans))
    print(f"Processed {num_spans} spans and {num_events} events")
    print(f"Unknown fields: {Fields.unknown_fields}")

    return resource_spans




def main():
    assert len(sys.argv) == 2
    resource_spans = parse_trace_file(sys.argv[1])
    print(f"Parsed {len(resource_spans)} spans")





if __name__ == '__main__':
    main()
