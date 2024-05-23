from dataclasses import dataclass, field
from chain_schema import *
from profile_schema import *
from trace_schema import *
import random


@dataclass
class BlockInfo:
    category_id: int


@dataclass
class ChunkInfo:
    category_id: int


categories = []

html_colors = [
    "aqua", "black", "blue", "crimson", "darkblue", "darkgreen", "darkorange",
    "darkred", "darkslateblue", "darkslategray", "darkturquoise", "deeppink",
    "fuchsia", "forestgreen", "gold", "goldenrod", "gray", "green", "greenyellow",
    "indigo", "indianred", "khaki", "lavender", "lime", "limegreen", "magenta",
    "maroon", "mediumblue", "mediumseagreen", "mediumslateblue", "mediumvioletred",
    "midnightblue", "navy", "olive", "olivedrab", "orange", "orangered", "orchid",
    "palegoldenrod", "palegreen", "paleturquoise", "pink", "plum", "purple", "red",
    "royalblue", "saddlebrown", "salmon", "seagreen", "sienna", "silver", "skyblue",
    "slateblue", "slategray", "teal", "tomato", "turquoise", "violet", "wheat", "yellow",
]
random.shuffle(html_colors)


def category_generator(name: str) -> int:
    for color in html_colors:
        index = len(categories)
        categories.append(Category(
            name=name,
            color=color,
            subcategories=[],
        ))
        yield index


def span_payload(span: ChainSpan):
    payload = {
        "name": span.name,
        "type": "span",
    }
    payload.update(span.fields.payload())
    return payload

def event_payload(event: ChainEvent):
    payload = {
        "name": event.name,
        "type": "event",
    }
    payload.update(event.fields.payload())
    return payload


def generate_from_chain_schema(chain_history: ChainHistory):
    global_start_time = chain_history.start_time
    global_start_time_ms = global_start_time.timestamp() * 1000

    threads = []
    for block_id, block_history in chain_history.block_histories.items():
        thread_name = f"Block(height={block_id.height})"
        category_id = next(category_generator(name=thread_name))

        thread = Thread(
            name=str(block_id),
            process_name=thread_name,
            process_type="default",
            tid=block_id.height,
            pid=block_id.height,
            is_main_thread=True,
            process_startup_time=global_start_time_ms,
            show_markers_in_timeline=True
        )

        strings_builder = StringTableBuilder()
        for span in block_history.spans:
            thread.markers.add_interval_marker(strings_builder=strings_builder,
                                               name=span.name,
                                               start_time=span.start_time - global_start_time,
                                               end_time=span.end_time - global_start_time,
                                               category=category_id,
                                               data=span_payload(span))
            for event in span.events:
                thread.markers.add_instant_marker(strings_builder=strings_builder,
                                                  name=event.name,
                                                  time=event.time - global_start_time,
                                                  category=category_id,
                                                  data=event_payload(event))
        thread.string_array = strings_builder.strings
        threads.append(thread)

    for chunk_id, chunk_history in chain_history.chunk_histories.items():
        thread_name = f"Chunk(shard={chunk_id.shard_id})"
        category_id = next(category_generator(name=thread_name))

        thread = Thread(
            name=str(chunk_id),
            process_name=thread_name,
            process_type="default",
            tid=chunk_id.shard_id,
            pid=chunk_id.shard_id,
            is_main_thread=True,
            process_startup_time=global_start_time_ms,
            show_markers_in_timeline=True
        )

        strings_builder = StringTableBuilder()
        for span in chunk_history.spans:
            thread.markers.add_interval_marker(strings_builder=strings_builder,
                                               name=span.name,
                                               start_time=span.start_time - global_start_time,
                                               end_time=span.end_time - global_start_time,
                                               category=category_id,
                                               data=span_payload(span))
            for event in span.events:
                thread.markers.add_instant_marker(strings_builder=strings_builder,
                                                  name=event.name,
                                                  time=event.time - global_start_time,
                                                  category=category_id,
                                                  data=event_payload(event))
        thread.string_array = strings_builder.strings
        threads.append(thread)

    profile = Profile(
        meta=ProfileMeta(
            version=29,
            preprocessed_profile_version=48,
            interval=1,
            start_time=global_start_time_ms,
            process_type=0,
            product="near-tracing",
            stackwalk=0,
            categories=categories,
            marker_schema=[],
        ),
        threads=threads,
    )

    return profile


def generate_from_trace_schema(trace_input: TraceInput):
    global_start_time = trace_input.start_time
    global_start_time_ms = global_start_time.timestamp() * 1000

    category = Category(
        name="Span",
        color="blue",
        subcategories=[],
    )
    profile = Profile(
        meta=ProfileMeta(
            version=29,
            preprocessed_profile_version=48,
            interval=1,
            start_time=global_start_time_ms,
            process_type=0,
            product="near-tracing",
            stackwalk=0,
            categories=[category],
            marker_schema=[],
        ),
    )
    thread = Thread(
        name="spans",
        process_name="trace",
        process_type="default",
        is_main_thread=True,
        process_startup_time=global_start_time_ms,
        show_markers_in_timeline=True
    )

    strings_builder = StringTableBuilder()

    for resource_span in trace_input.resource_spans:
        node_id = resource_span.fields.node_id
        thread_id = resource_span.fields.thread_id
        for span in resource_span.spans:
            thread.markers.add_interval_marker(strings_builder=strings_builder,
                                               name="%s (%s)" % (
                                                   node_id, thread_id),
                                               start_time=span.start_time - global_start_time,
                                               end_time=span.end_time - global_start_time,
                                               category=0,
                                               data={
                                                   "name": span.name,
                                                   "type": "span",
                                               }.update(span.fields.payload()))
            for event in span.events:
                thread.markers.add_instant_marker(strings_builder=strings_builder,
                                                  name="%s (%s)" % (
                                                      node_id, thread_id),
                                                  time=event.timestamp - global_start_time,
                                                  category=0,
                                                  data={
                                                      "name": event.name,
                                                      "type": "event",
                                                  }.update(event.fields.payload()))
    thread.string_array = strings_builder.strings
    profile.threads.append(thread)
    return profile
