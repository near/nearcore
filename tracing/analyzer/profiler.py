from dataclasses import dataclass, field


@dataclass
class Lib:
    pass


@dataclass
class PausedRange:
    pass


@dataclass
class SamplesTable:
    stack: list[None]
    time: list[None]
    weight: int = field(default_factory=int)
    weight_type: str = "samples"
    length: int


@dataclass
class StackTable:
    frame: list[None]
    category: list[None]
    subcategory: list[None]
    prefix: list[None]
    length: int


@dataclass
class FrameTable:
    address: list[None]
    inline_depth: list[None]
    category: list[None]
    subcategory: list[None]
    func: list[None]
    native_symbol: list[None]
    inner_window_id: list[None]
    implementation: list[None]
    line: list[None]
    column: list[None]
    length: int


@dataclass
class FuncTable:
    name: list[None]
    is_js: list[None]
    relevant_for_js: list[None]
    resource: list[None]
    file_name: list[None]
    line_number: list[None]
    column_number: list[None]
    length: int


@dataclass
class ResourceTable:
    length: int
    lib: list[None]
    name: list[None]
    host: list[None]
    type: list[None]


@dataclass
class NativeSymbolTable:
    lib_index: list[None]
    address: list[None]
    name: list[None]
    function_size: list[None]
    length: int


@dataclass
class StringTableBuilder:
    string: list[str]
    existing: dict[str, int]

@dataclass
class MarkerSchema:
    pass

@dataclass
class MarkerPhase:
    pass

@dataclass
class ProfileMeta:
    interval: int
    start_time: int
    process_type: int
    product: str = field(default_factory=str)
    stackwalk: int
    version: int
    preprocessed_profile_version: int
    marker_schema: list[MarkerSchema]
    categories: list["Category"]


@dataclass
class Category:
    name: str = field(default_factory=str)
    color: str = field(default_factory=str)
    subcategories: list[str]


@dataclass
class RawMarkerTable:
    data: list[dict[str, str]]  # Use dict for MarkerPayload
    name: list[int]
    start_time: list[float]
    end_time: list[float]
    phase: list[MarkerPhase]
    category: list[int]
    length: int


@dataclass
class Thread:
    process_type: str = field(default_factory=str)
    process_startup_time: int
    process_shutdown_time: int | None
    register_time: int
    unregister_time: int | None
    paused_ranges: list[PausedRange]
    show_markers_in_timeline: bool
    name: str = field(default_factory=str)
    is_main_thread: bool
    process_name: str = field(default_factory=str)
    pid: int
    tid: int
    samples: SamplesTable
    markers: RawMarkerTable
    stack_table: StackTable
    frame_table: FrameTable
    string_array: list[str]
    func_table: FuncTable
    resource_table: ResourceTable
    native_symbols: NativeSymbolTable
