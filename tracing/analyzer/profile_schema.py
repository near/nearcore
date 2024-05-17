from dataclasses import dataclass, field
from datetime import timedelta
import trace_schema

MarkerPhase = int
MarkerPayload = dict[str, str]


@dataclass
class Lib:
    def json(self):
        return {}


@dataclass
class PausedRange:
    def json(self):
        return {}


@dataclass
class SamplesTable:
    stack: list[None] = field(default_factory=list)
    time: list[None] = field(default_factory=list)
    weight: int = field(default_factory=int)
    weight_type: str = "samples"
    length: int = field(default_factory=int)

    def json(self):
        return {
            "stack": self.stack,
            "time": self.time,
            "weight": self.weight,
            "weightType": self.weight_type,
            "length": self.length
        }


@dataclass
class StackTable:
    frame: list[None] = field(default_factory=list)
    category: list[None] = field(default_factory=list)
    subcategory: list[None] = field(default_factory=list)
    prefix: list[None] = field(default_factory=list)
    length: int = field(default_factory=int)

    def json(self):
        return {
            "frame": self.frame,
            "category": self.category,
            "subcategory": self.subcategory,
            "prefix": self.prefix,
            "length": self.length
        }


@dataclass
class FrameTable:
    address: list[None] = field(default_factory=list)
    inline_depth: list[None] = field(default_factory=list)
    category: list[None] = field(default_factory=list)
    subcategory: list[None] = field(default_factory=list)
    func: list[None] = field(default_factory=list)
    native_symbol: list[None] = field(default_factory=list)
    inner_window_id: list[None] = field(default_factory=list)
    implementation: list[None] = field(default_factory=list)
    line: list[None] = field(default_factory=list)
    column: list[None] = field(default_factory=list)
    length: int = field(default_factory=int)

    def json(self):
        return {
            "address": self.address,
            "inlineDepth": self.inline_depth,
            "category": self.category,
            "subcategory": self.subcategory,
            "func": self.func,
            "nativeSymbol": self.native_symbol,
            "innerWindowID": self.inner_window_id,
            "implementation": self.implementation,
            "line": self.line,
            "column": self.column,
            "length": self.length
        }


@dataclass
class FuncTable:
    name: list[None] = field(default_factory=list)
    is_js: list[None] = field(default_factory=list)
    relevant_for_js: list[None] = field(default_factory=list)
    resource: list[None] = field(default_factory=list)
    file_name: list[None] = field(default_factory=list)
    line_number: list[None] = field(default_factory=list)
    column_number: list[None] = field(default_factory=list)
    length: int = field(default_factory=int)

    def json(self):
        return {
            "name": self.name,
            "isJS": self.is_js,
            "relevantForJS": self.relevant_for_js,
            "resource": self.resource,
            "fileName": self.file_name,
            "lineNumber": self.line_number,
            "columnNumber": self.column_number,
            "length": self.length
        }


@dataclass
class ResourceTable:
    length: int = field(default_factory=int)
    lib: list[None] = field(default_factory=list)
    name: list[None] = field(default_factory=list)
    host: list[None] = field(default_factory=list)
    type: list[None] = field(default_factory=list)

    def json(self):
        return {
            "length": self.length,
            "lib": self.lib,
            "name": self.name,
            "host": self.host,
            "type": self.type,
        }


@dataclass
class NativeSymbolTable:
    lib_index: list[None] = field(default_factory=list)
    address: list[None] = field(default_factory=list)
    name: list[None] = field(default_factory=list)
    function_size: list[None] = field(default_factory=list)
    length: int = field(default_factory=int)

    def json(self):
        return {
            "libIndex": self.lib_index,
            "address": self.address,
            "name": self.name,
            "functionSize": self.function_size,
            "length": self.length
        }


@dataclass
class StringTableBuilder:
    strings: list[str] = field(default_factory=list)
    existing: dict[str, int] = field(default_factory=dict)

    def json(self):
        return {
            "string": self.strings,
            "existing": self.existing
        }

    def insert(self, string):
        if string in self.existing:
            return self.existing[string]
        index = len(self.strings)
        self.strings.append(string)
        self.existing[string] = index
        return index


@dataclass
class MarkerSchema:
    def json(self):
        return {}


@dataclass
class Category:
    name: str = field(default_factory=str)
    color: str = field(default_factory=str)
    subcategories: list[str] = field(default_factory=list)

    def json(self):
        return {
            "name": self.name,
            "color": self.color,
            "subcategories": self.subcategories
        }


@dataclass
class ProfileMeta:
    interval: int = field(default_factory=int)
    start_time: int = field(default_factory=int)
    process_type: int = field(default_factory=int)
    product: str = field(default_factory=str)
    stackwalk: int = field(default_factory=int)
    version: int = field(default_factory=int)
    preprocessed_profile_version: int = field(default_factory=int)
    marker_schema: list[MarkerSchema] = field(default_factory=list)
    categories: list[Category] = field(default_factory=list)

    def json(self):
        return {
            "interval": self.interval,
            "startTime": self.start_time,
            "processType": self.process_type,
            "product": self.product,
            "stackwalk": self.stackwalk,
            "version": self.version,
            "preprocessedProfileVersion": self.preprocessed_profile_version,
            "markerSchema": [s.json() for s in self.marker_schema],
            "categories": [c.json() for c in self.categories]
        }


@dataclass
class RawMarkerTable:
    data: list[MarkerPayload | None] = field(default_factory=list)
    name: list[int] = field(default_factory=list)
    start_time: list[float | None] = field(default_factory=list)
    end_time: list[float | None] = field(default_factory=list)
    phase: list[MarkerPhase] = field(default_factory=list)
    category: list[int] = field(default_factory=list)
    length: int = field(default_factory=int)

    def json(self):
        return {
            "data": self.data,
            "name": self.name,
            "startTime": self.start_time,
            "endTime": self.end_time,
            "phase": self.phase,
            "category": self.category,
            "length": self.length
        }

    def add_interval_marker(self, strings_builder: StringTableBuilder, name: str, start_time: timedelta, end_time: timedelta, category: int, data: MarkerPayload):
        self.name.append(strings_builder.insert(name))
        self.start_time.append(start_time.total_seconds() * 1000.0)
        self.end_time.append(end_time.total_seconds() * 1000.0)
        self.phase.append(1)
        self.category.append(category)
        self.data.append(data)
        self.length += 1

    def add_instant_marker(self, strings_builder: StringTableBuilder, name: str, time: timedelta, category: int, data: MarkerPayload):
        self.name.append(strings_builder.insert(name))
        self.start_time.append(time.total_seconds() * 1000.0)
        self.end_time.append(None)
        self.phase.append(0)
        self.category.append(category)
        self.data.append(data)
        self.length += 1


@dataclass
class Thread:
    process_type: str = field(default_factory=str)
    process_startup_time: int = field(default_factory=int)
    process_shutdown_time: int | None = None
    register_time: int = field(default_factory=int)
    unregister_time: int | None = field(default_factory=int)
    paused_ranges: list[PausedRange] = field(default_factory=list)
    show_markers_in_timeline: bool = field(default_factory=bool)
    name: str = field(default_factory=str)
    is_main_thread: bool = field(default_factory=bool)
    process_name: str = field(default_factory=str)
    pid: int = field(default_factory=int)
    tid: int = field(default_factory=int)
    samples: SamplesTable = field(default_factory=SamplesTable)
    markers: RawMarkerTable = field(default_factory=RawMarkerTable)
    stack_table: StackTable = field(default_factory=StackTable)
    frame_table: FrameTable = field(default_factory=FrameTable)
    string_array: list[str] = field(default_factory=list)
    func_table: FuncTable = field(default_factory=FuncTable)
    resource_table: ResourceTable = field(default_factory=ResourceTable)
    native_symbols: NativeSymbolTable = field(
        default_factory=NativeSymbolTable)

    def json(self):
        return {
            "processType": self.process_type,
            "processStartupTime": self.process_startup_time,
            "processShutdownTime": self.process_shutdown_time,
            "registerTime": self.register_time,
            "unregisterTime": self.unregister_time,
            "pausedRanges": [p.json() for p in self.paused_ranges],
            "showMarkersInTimeline": self.show_markers_in_timeline,
            "name": self.name,
            "isMainThread": self.is_main_thread,
            "processName": self.process_name,
            "pid": self.pid,
            "tid": self.tid,
            "samples": self.samples.json(),
            "markers": self.markers.json(),
            "stackTable": self.stack_table.json(),
            "frameTable": self.frame_table.json(),
            "stringArray": self.string_array,
            "funcTable": self.func_table.json(),
            "resourceTable": self.resource_table.json(),
            "nativeSymbols": self.native_symbols.json(),
        }


@dataclass
class Profile:
    meta: ProfileMeta = field(default_factory=ProfileMeta)
    libs: list[Lib] = field(default_factory=list)
    threads: list[Thread] = field(default_factory=list)

    def json(self):
        return {
            "meta": self.meta.json(),
            "libs": [l.json() for l in self.libs],
            "threads": [t.json() for t in self.threads]
        }
