from profile_schema import *
from trace_schema import *


def generate(trace_input: TraceInput):
    start_time_ms = trace_input.start_time.timestamp() * 1000

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
            start_time=start_time_ms,
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
        process_startup_time=start_time_ms,
        show_markers_in_timeline=True
    )

    global_start_time = trace_input.start_time
    strings_builder = StringTableBuilder()

    for resource_span in trace_input.resource_spans:
        node_id = resource_span.fields.node_id
        thread_id = resource_span.fields.thread_id
        for span in resource_span.spans:
            thread.markers.add_interval_marker(strings_builder=strings_builder,
                                               name=f"{node_id} ({thread_id})",
                                               start_time=span.start_time - global_start_time,
                                               end_time=span.end_time - global_start_time,
                                               category=0,
                                               data={
                                                   "name": span.name,
                                                   "type": "span",
                                               }.update(span.fields.payload()))
            for event in span.events:
                thread.markers.add_instant_marker(strings_builder=strings_builder,
                                                  name=f"{node_id} ({thread_id})",
                                                  time=event.timestamp - global_start_time,
                                                  category=0,
                                                  data={
                                                      "name": event.name,
                                                      "type": "event",
                                                  }.update(event.fields.payload()))
    thread.string_array = strings_builder.strings
    profile.threads.append(thread)
    return profile
