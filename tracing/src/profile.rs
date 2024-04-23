use std::collections::HashMap;

use serde::Serialize;

#[derive(Serialize)]
pub struct Lib {}

#[derive(Serialize)]
pub struct Profile {
    pub meta: ProfileMeta,
    pub libs: Vec<Lib>,
    pub threads: Vec<Thread>,
}

#[derive(Serialize, Default)]
pub struct Thread {
    #[serde(rename = "processType")]
    pub process_type: String,
    #[serde(rename = "processStartupTime")]
    pub process_startup_time: i64,
    #[serde(rename = "processShutdownTime")]
    pub process_shutdown_time: Option<i64>,
    #[serde(rename = "registerTime")]
    pub register_time: i64,
    #[serde(rename = "unregisterTime")]
    pub unregister_time: Option<i64>,
    #[serde(rename = "pausedRanges")]
    pub paused_ranges: Vec<PausedRange>,
    #[serde(rename = "showMarkersInTimeline")]
    pub show_markers_in_timeline: bool,
    pub name: String,
    #[serde(rename = "isMainThread")]
    pub is_main_thread: bool,
    #[serde(rename = "processName")]
    pub process_name: String,
    pub pid: i64,
    pub tid: i64,
    pub samples: SamplesTable,
    pub markers: RawMarkerTable,
    #[serde(rename = "stackTable")]
    pub stack_table: StackTable,
    #[serde(rename = "frameTable")]
    pub frame_table: FrameTable,
    #[serde(rename = "stringArray")]
    pub string_array: Vec<String>,
    #[serde(rename = "funcTable")]
    pub func_table: FuncTable,
    #[serde(rename = "resourceTable")]
    pub resource_table: ResourceTable,
    #[serde(rename = "nativeSymbols")]
    pub native_symbols: NativeSymbolTable,
}

#[derive(Serialize, Default)]
pub struct PausedRange {}

#[derive(Serialize)]
pub struct SamplesTable {
    pub stack: Vec<()>,
    pub time: Vec<()>,
    pub weight: Option<()>,
    #[serde(rename = "weightType")]
    pub weight_type: String,
    pub length: usize,
}

impl Default for SamplesTable {
    fn default() -> Self {
        SamplesTable {
            stack: Vec::new(),
            time: Vec::new(),
            weight: None,
            weight_type: "samples".to_owned(),
            length: 0,
        }
    }
}

#[derive(Serialize, Default)]
pub struct StackTable {
    pub frame: Vec<()>,
    pub category: Vec<()>,
    pub subcategory: Vec<()>,
    pub prefix: Vec<()>,
    pub length: usize,
}

#[derive(Serialize, Default)]
pub struct FrameTable {
    pub address: Vec<()>,
    #[serde(rename = "inlineDepth")]
    pub inline_depth: Vec<()>,
    pub category: Vec<()>,
    pub subcategory: Vec<()>,
    pub func: Vec<()>,
    #[serde(rename = "nativeSymbol")]
    pub native_symbol: Vec<()>,
    #[serde(rename = "innerWindowID")]
    pub inner_window_id: Vec<()>,
    pub implementation: Vec<()>,
    pub line: Vec<()>,
    pub column: Vec<()>,
    pub length: usize,
}

#[derive(Serialize, Default)]
pub struct FuncTable {
    pub name: Vec<()>,
    #[serde(rename = "isJS")]
    pub is_js: Vec<()>,
    #[serde(rename = "relevantForJS")]
    pub relevant_for_js: Vec<()>,
    pub resource: Vec<()>,
    #[serde(rename = "fileName")]
    pub file_name: Vec<()>,
    #[serde(rename = "lineNumber")]
    pub line_number: Vec<()>,
    #[serde(rename = "columnNumber")]
    pub column_number: Vec<()>,
    pub length: usize,
}

#[derive(Serialize, Default)]
pub struct ResourceTable {
    pub length: usize,
    pub lib: Vec<()>,
    pub name: Vec<()>,
    pub host: Vec<()>,
    #[serde(rename = "type")]
    pub type_: Vec<()>,
}

#[derive(Serialize, Default)]
pub struct NativeSymbolTable {
    #[serde(rename = "libIndex")]
    pub lib_index: Vec<()>,
    pub address: Vec<()>,
    pub name: Vec<()>,
    #[serde(rename = "functionSize")]
    pub function_size: Vec<()>,
    pub length: usize,
}

#[derive(Serialize, Default)]
pub struct RawMarkerTable {
    pub data: Vec<Option<MarkerPayload>>,
    pub name: Vec<IndexIntoStringTable>,
    #[serde(rename = "startTime")]
    pub start_time: Vec<Option<f64>>,
    #[serde(rename = "endTime")]
    pub end_time: Vec<Option<f64>>,
    pub phase: Vec<MarkerPhase>,
    pub category: Vec<IndexIntoCategoryList>,
    pub length: usize,
}

pub type IndexIntoStringTable = usize;
pub type IndexIntoCategoryList = usize;

#[derive(Serialize)]
pub struct ProfileMeta {
    pub interval: i64,
    #[serde(rename = "startTime")]
    pub start_time: i64,
    #[serde(rename = "processType")]
    pub process_type: i64,
    pub product: String,
    pub stackwalk: u8,
    pub version: i64,
    #[serde(rename = "preprocessedProfileVersion")]
    pub preprocessed_profile_version: i64,
    #[serde(rename = "markerSchema")]
    pub marker_schema: Vec<MarkerSchema>,
    pub categories: Vec<Category>,
}

#[derive(Serialize)]
pub struct Category {
    pub name: String,
    pub color: String,
    pub subcategories: Vec<String>,
}

#[derive(Serialize)]
pub struct MarkerSchema {
    pub name: String,
    pub display: Vec<String>,
    pub data: Vec<MarkerSchemaData>,
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum MarkerSchemaData {
    Custom { key: String, label: String, format: String, searchable: bool },
    Static { label: String, value: String },
}

pub type MarkerPayload = HashMap<String, String>;
pub type MarkerPhase = u8;

pub struct StringTableBuilder {
    strings: Vec<String>,
    existing: HashMap<String, usize>,
}

impl StringTableBuilder {
    pub fn new() -> Self {
        StringTableBuilder { strings: Vec::new(), existing: HashMap::new() }
    }

    pub fn insert(&mut self, string: &str) -> usize {
        if let Some(&index) = self.existing.get(string) {
            return index;
        }

        let index = self.strings.len();
        self.strings.push(string.to_owned());
        self.existing.insert(string.to_owned(), index);
        index
    }

    pub fn build(self) -> Vec<String> {
        self.strings
    }
}

impl RawMarkerTable {
    pub fn add_marker(
        &mut self,
        string_table: &mut StringTableBuilder,
        name: &str,
        start_time: f64,
        end_time: f64,
        category: IndexIntoCategoryList,
        data: MarkerPayload,
    ) {
        self.name.push(string_table.insert(name));
        self.start_time.push(Some(start_time));
        self.end_time.push(Some(end_time));
        self.phase.push(1);
        self.category.push(category);
        self.data.push(Some(data));
        self.length += 1;
    }

    pub fn add_instant_marker(
        &mut self,
        string_table: &mut StringTableBuilder,
        name: &str,
        time: f64,
        category: IndexIntoCategoryList,
        data: MarkerPayload,
    ) {
        self.name.push(string_table.insert(name));
        self.start_time.push(Some(time));
        self.end_time.push(None);
        self.phase.push(0);
        self.category.push(category);
        self.data.push(Some(data));
        self.length += 1;
    }
}
