#!/usr/bin/env python3

import datetime
import json
import re
import sys

from typing import ClassVar
from dataclasses import dataclass, field


SKIP_FIELDS = {"code.filepath", "code.namespace", "code.lineno", "level",
               'was_requested', 'hash',
               'block_producers', 'approval_inner', 'sync_status',
               'new_chunks_count', 'blocks_missing_chunks', 'me',
               'endorsement', 'num_outgoing_receipts',
               'num_filtered_transactions',
               'should_produce_chunk', 'peer_id', 'orphans_missing_chunks',
               'err', 'provenance', 'busy_ns', 'num_blocks', 'validator',
               'new_chunks', 'skip_produce_chunk', 'next_bp',
               'pool_size', 'is_syncing', 'prev_block_hash', 'status', 'thread.name',
               'idle_ns', 'header_head_height'}


@dataclass(repr=False)
class Fields:
    node_id: str | None = None
    account_id: str | None = None
    chain_id: str | None = None
    service_name: str | None = None
    target: str | None = None
    thread_id: int | None = None
    shard_id: int | None = None
    epoch_id: str | None = None
    height: int | None = None
    block_hash: str | None = None
    prev_hash: str | None = None
    chunk_hash: str | None = None

    unknown_fields: ClassVar[set] = set()

    def payload(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}

    def __str__(self) -> str:
        return str(self.payload())
    
    def __repr__(self) -> str:
        return str(self)

    @staticmethod
    def load(attributes: dict):
        fields = Fields()
        for attribute in attributes:
            if attribute['key'] in SKIP_FIELDS:
                continue
            if attribute['key'] == 'node_id':
                fields.node_id = attribute['value']['stringValue']
            elif attribute['key'] in {'account_id', 'validator_id'}:
                fields.account_id = attribute['value']['stringValue']
            elif attribute['key'] == 'chain_id':
                fields.chain_id = attribute['value']['stringValue']
            elif attribute['key'] in {'service_name', 'service.name'}:
                fields.service_name = attribute['value']['stringValue']
            elif attribute['key'] == 'target':
                fields.target = attribute['value']['stringValue']
            elif attribute['key'] in {'thread_id', 'thread.id'}:
                fields.thread_id = attribute['value']['intValue']
            elif attribute['key'] == 'shard_id':
                fields.shard_id = int(attribute['value']['stringValue'])
            elif attribute['key'] == 'epoch_id':
                fields.epoch_id = attribute['value']['stringValue']
            elif attribute['key'] in {'height', 'block_height', 'next_height', 'prev_height', 'target_height'}:
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
class TraceEvent:
    fields: Fields
    name: str = field(default_factory=str)
    timestamp: datetime = field(default_factory=datetime)


@dataclass
class TraceSpan:
    trace_id: str
    span_id: str
    parent_id: str
    name: str
    start_time: datetime
    end_time: datetime
    fields: Fields
    events: list[TraceEvent]


@dataclass
class ScopeSpan:
    spans: list[TraceSpan]


@dataclass
class ResourceSpan:
    fields: Fields
    spans: list[TraceSpan]


@dataclass
class TraceInput:
    start_time: datetime
    end_time: datetime
    resource_spans: list[ResourceSpan]

    @staticmethod
    def parse(json_file_path) -> "TraceInput":
        json_file_path = sys.argv[1]
        with open(json_file_path, 'r') as json_file:
            traces_json = json.load(json_file)

        num_spans = 0
        num_events = 0
        resource_spans = []

        min_timestamp = None
        max_timestamp = None

        def record_timestamp(nanos):
            nonlocal min_timestamp, max_timestamp
            millis = nanos / 10.0**9
            if min_timestamp is None or millis < min_timestamp:
                min_timestamp = millis
            if max_timestamp is None or millis > max_timestamp:
                max_timestamp = millis
            return datetime.datetime.fromtimestamp(millis)

        for trace_json in traces_json:
            for resource_span_json in trace_json['resourceSpans']:
                for scope_span_json in resource_span_json['scopeSpans']:
                    spans = []
                    for span_json in scope_span_json['spans']:
                        events = []
                        for event_json in span_json["events"]:
                            events.append(TraceEvent(fields=Fields.load(event_json['attributes']), name=event_json['name'],
                                                     timestamp=record_timestamp(event_json['timeUnixNano']),))
                        num_events += len(events)
                        spans.append(TraceSpan(fields=Fields.load(span_json['attributes']),
                                               events=events,
                                               trace_id=span_json['traceId'],
                                               span_id=span_json['spanId'],
                                               parent_id=span_json['parentSpanId'], name=span_json['name'],
                                               start_time=record_timestamp(
                            span_json['startTimeUnixNano']),
                            end_time=record_timestamp(span_json['endTimeUnixNano'])))
                    num_spans += len(spans)
                resource_spans.append(ResourceSpan(fields=Fields.load(resource_span_json['resource']['attributes']),
                                                   spans=spans))
        print("Processed %d spans and %s events" % (num_spans, num_events))
        print("Unknown fields: %s" % str(Fields.unknown_fields))

        return TraceInput(start_time=datetime.datetime.fromtimestamp(min_timestamp), end_time=datetime.datetime.fromtimestamp(max_timestamp), resource_spans=resource_spans)
