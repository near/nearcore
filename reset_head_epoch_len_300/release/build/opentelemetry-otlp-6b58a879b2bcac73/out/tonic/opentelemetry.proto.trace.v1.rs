/// A collection of InstrumentationLibrarySpans from a Resource.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceSpans {
    /// The resource for the spans in this message.
    /// If this field is not set then no resource info is known.
    #[prost(message, optional, tag="1")]
    pub resource: ::core::option::Option<super::super::resource::v1::Resource>,
    /// A list of InstrumentationLibrarySpans that originate from a resource.
    #[prost(message, repeated, tag="2")]
    pub instrumentation_library_spans: ::prost::alloc::vec::Vec<InstrumentationLibrarySpans>,
}
/// A collection of Spans produced by an InstrumentationLibrary.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstrumentationLibrarySpans {
    /// The instrumentation library information for the spans in this message.
    /// Semantically when InstrumentationLibrary isn't set, it is equivalent with
    /// an empty instrumentation library name (unknown).
    #[prost(message, optional, tag="1")]
    pub instrumentation_library: ::core::option::Option<super::super::common::v1::InstrumentationLibrary>,
    /// A list of Spans that originate from an instrumentation library.
    #[prost(message, repeated, tag="2")]
    pub spans: ::prost::alloc::vec::Vec<Span>,
}
/// Span represents a single operation within a trace. Spans can be
/// nested to form a trace tree. Spans may also be linked to other spans
/// from the same or different trace and form graphs. Often, a trace
/// contains a root span that describes the end-to-end latency, and one
/// or more subspans for its sub-operations. A trace can also contain
/// multiple root spans, or none at all. Spans do not need to be
/// contiguous - there may be gaps or overlaps between spans in a trace.
///
/// The next available field id is 17.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Span {
    /// A unique identifier for a trace. All spans from the same trace share
    /// the same `trace_id`. The ID is a 16-byte array. An ID with all zeroes
    /// is considered invalid.
    ///
    /// This field is semantically required. Receiver should generate new
    /// random trace_id if empty or invalid trace_id was received.
    ///
    /// This field is required.
    #[prost(bytes="vec", tag="1")]
    pub trace_id: ::prost::alloc::vec::Vec<u8>,
    /// A unique identifier for a span within a trace, assigned when the span
    /// is created. The ID is an 8-byte array. An ID with all zeroes is considered
    /// invalid.
    ///
    /// This field is semantically required. Receiver should generate new
    /// random span_id if empty or invalid span_id was received.
    ///
    /// This field is required.
    #[prost(bytes="vec", tag="2")]
    pub span_id: ::prost::alloc::vec::Vec<u8>,
    /// trace_state conveys information about request position in multiple distributed tracing graphs.
    /// It is a trace_state in w3c-trace-context format: <https://www.w3.org/TR/trace-context/#tracestate-header>
    /// See also <https://github.com/w3c/distributed-tracing> for more details about this field.
    #[prost(string, tag="3")]
    pub trace_state: ::prost::alloc::string::String,
    /// The `span_id` of this span's parent span. If this is a root span, then this
    /// field must be empty. The ID is an 8-byte array.
    #[prost(bytes="vec", tag="4")]
    pub parent_span_id: ::prost::alloc::vec::Vec<u8>,
    /// A description of the span's operation.
    ///
    /// For example, the name can be a qualified method name or a file name
    /// and a line number where the operation is called. A best practice is to use
    /// the same display name at the same call point in an application.
    /// This makes it easier to correlate spans in different traces.
    ///
    /// This field is semantically required to be set to non-empty string.
    /// When null or empty string received - receiver may use string "name"
    /// as a replacement. There might be smarted algorithms implemented by
    /// receiver to fix the empty span name.
    ///
    /// This field is required.
    #[prost(string, tag="5")]
    pub name: ::prost::alloc::string::String,
    /// Distinguishes between spans generated in a particular context. For example,
    /// two spans with the same name may be distinguished using `CLIENT` (caller)
    /// and `SERVER` (callee) to identify queueing latency associated with the span.
    #[prost(enumeration="span::SpanKind", tag="6")]
    pub kind: i32,
    /// start_time_unix_nano is the start time of the span. On the client side, this is the time
    /// kept by the local machine where the span execution starts. On the server side, this
    /// is the time when the server's application handler starts running.
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
    ///
    /// This field is semantically required and it is expected that end_time >= start_time.
    #[prost(fixed64, tag="7")]
    pub start_time_unix_nano: u64,
    /// end_time_unix_nano is the end time of the span. On the client side, this is the time
    /// kept by the local machine where the span execution ends. On the server side, this
    /// is the time when the server application handler stops running.
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
    ///
    /// This field is semantically required and it is expected that end_time >= start_time.
    #[prost(fixed64, tag="8")]
    pub end_time_unix_nano: u64,
    /// attributes is a collection of key/value pairs. The value can be a string,
    /// an integer, a double or the Boolean values `true` or `false`. Note, global attributes
    /// like server name can be set using the resource API. Examples of attributes:
    ///
    ///     "/http/user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
    ///     "/http/server_latency": 300
    ///     "abc.com/myattribute": true
    ///     "abc.com/score": 10.239
    #[prost(message, repeated, tag="9")]
    pub attributes: ::prost::alloc::vec::Vec<super::super::common::v1::KeyValue>,
    /// dropped_attributes_count is the number of attributes that were discarded. Attributes
    /// can be discarded because their keys are too long or because there are too many
    /// attributes. If this value is 0, then no attributes were dropped.
    #[prost(uint32, tag="10")]
    pub dropped_attributes_count: u32,
    /// events is a collection of Event items.
    #[prost(message, repeated, tag="11")]
    pub events: ::prost::alloc::vec::Vec<span::Event>,
    /// dropped_events_count is the number of dropped events. If the value is 0, then no
    /// events were dropped.
    #[prost(uint32, tag="12")]
    pub dropped_events_count: u32,
    /// links is a collection of Links, which are references from this span to a span
    /// in the same or different trace.
    #[prost(message, repeated, tag="13")]
    pub links: ::prost::alloc::vec::Vec<span::Link>,
    /// dropped_links_count is the number of dropped links after the maximum size was
    /// enforced. If this value is 0, then no links were dropped.
    #[prost(uint32, tag="14")]
    pub dropped_links_count: u32,
    /// An optional final status for this span. Semantically when Status isn't set, it means
    /// span's status code is unset, i.e. assume STATUS_CODE_UNSET (code = 0).
    #[prost(message, optional, tag="15")]
    pub status: ::core::option::Option<Status>,
}
/// Nested message and enum types in `Span`.
pub mod span {
    /// Event is a time-stamped annotation of the span, consisting of user-supplied
    /// text description and key-value pairs.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Event {
        /// time_unix_nano is the time the event occurred.
        #[prost(fixed64, tag="1")]
        pub time_unix_nano: u64,
        /// name of the event.
        /// This field is semantically required to be set to non-empty string.
        #[prost(string, tag="2")]
        pub name: ::prost::alloc::string::String,
        /// attributes is a collection of attribute key/value pairs on the event.
        #[prost(message, repeated, tag="3")]
        pub attributes: ::prost::alloc::vec::Vec<super::super::super::common::v1::KeyValue>,
        /// dropped_attributes_count is the number of dropped attributes. If the value is 0,
        /// then no attributes were dropped.
        #[prost(uint32, tag="4")]
        pub dropped_attributes_count: u32,
    }
    /// A pointer from the current span to another span in the same trace or in a
    /// different trace. For example, this can be used in batching operations,
    /// where a single batch handler processes multiple requests from different
    /// traces or when the handler receives a request from a different project.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Link {
        /// A unique identifier of a trace that this linked span is part of. The ID is a
        /// 16-byte array.
        #[prost(bytes="vec", tag="1")]
        pub trace_id: ::prost::alloc::vec::Vec<u8>,
        /// A unique identifier for the linked span. The ID is an 8-byte array.
        #[prost(bytes="vec", tag="2")]
        pub span_id: ::prost::alloc::vec::Vec<u8>,
        /// The trace_state associated with the link.
        #[prost(string, tag="3")]
        pub trace_state: ::prost::alloc::string::String,
        /// attributes is a collection of attribute key/value pairs on the link.
        #[prost(message, repeated, tag="4")]
        pub attributes: ::prost::alloc::vec::Vec<super::super::super::common::v1::KeyValue>,
        /// dropped_attributes_count is the number of dropped attributes. If the value is 0,
        /// then no attributes were dropped.
        #[prost(uint32, tag="5")]
        pub dropped_attributes_count: u32,
    }
    /// SpanKind is the type of span. Can be used to specify additional relationships between spans
    /// in addition to a parent/child relationship.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum SpanKind {
        /// Unspecified. Do NOT use as default.
        /// Implementations MAY assume SpanKind to be INTERNAL when receiving UNSPECIFIED.
        Unspecified = 0,
        /// Indicates that the span represents an internal operation within an application,
        /// as opposed to an operation happening at the boundaries. Default value.
        Internal = 1,
        /// Indicates that the span covers server-side handling of an RPC or other
        /// remote network request.
        Server = 2,
        /// Indicates that the span describes a request to some remote service.
        Client = 3,
        /// Indicates that the span describes a producer sending a message to a broker.
        /// Unlike CLIENT and SERVER, there is often no direct critical path latency relationship
        /// between producer and consumer spans. A PRODUCER span ends when the message was accepted
        /// by the broker while the logical processing of the message might span a much longer time.
        Producer = 4,
        /// Indicates that the span describes consumer receiving a message from a broker.
        /// Like the PRODUCER kind, there is often no direct critical path latency relationship
        /// between producer and consumer spans.
        Consumer = 5,
    }
}
/// The Status type defines a logical error model that is suitable for different
/// programming environments, including REST APIs and RPC APIs.
///
/// IMPORTANT: Backward compatibility notes:
///
/// To ensure any pair of senders and receivers continues to correctly signal and
/// interpret erroneous situations, the senders and receivers MUST follow these rules:
///
/// 1. Old senders and receivers that are not aware of `code` field will continue using
/// the `deprecated_code` field to signal and interpret erroneous situation.
///
/// 2. New senders, which are aware of the `code` field MUST set both the
/// `deprecated_code` and `code` fields according to the following rules:
///
///   if code==STATUS_CODE_UNSET then `deprecated_code` MUST be
///   set to DEPRECATED_STATUS_CODE_OK.
///
///   if code==STATUS_CODE_OK then `deprecated_code` MUST be
///   set to DEPRECATED_STATUS_CODE_OK.
///
///   if code==STATUS_CODE_ERROR then `deprecated_code` MUST be
///   set to DEPRECATED_STATUS_CODE_UNKNOWN_ERROR.
///
/// These rules allow old receivers to correctly interpret data received from new senders.
///
/// 3. New receivers MUST look at both the `code` and `deprecated_code` fields in order
/// to interpret the overall status:
///
///   If code==STATUS_CODE_UNSET then the value of `deprecated_code` is the
///   carrier of the overall status according to these rules:
///
///     if deprecated_code==DEPRECATED_STATUS_CODE_OK then the receiver MUST interpret
///     the overall status to be STATUS_CODE_UNSET.
///
///     if deprecated_code!=DEPRECATED_STATUS_CODE_OK then the receiver MUST interpret
///     the overall status to be STATUS_CODE_ERROR.
///
///   If code!=STATUS_CODE_UNSET then the value of `deprecated_code` MUST be
///   ignored, the `code` field is the sole carrier of the status.
///
/// These rules allow new receivers to correctly interpret data received from old senders.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Status {
    /// The deprecated status code. This is an optional field.
    ///
    /// This field is deprecated and is replaced by the `code` field below. See backward
    /// compatibility notes below. According to our stability guarantees this field
    /// will be removed in 12 months, on Oct 22, 2021. All usage of old senders and
    /// receivers that do not understand the `code` field MUST be phased out by then.
    #[deprecated]
    #[prost(enumeration="status::DeprecatedStatusCode", tag="1")]
    pub deprecated_code: i32,
    /// A developer-facing human readable error message.
    #[prost(string, tag="2")]
    pub message: ::prost::alloc::string::String,
    /// The status code.
    #[prost(enumeration="status::StatusCode", tag="3")]
    pub code: i32,
}
/// Nested message and enum types in `Status`.
pub mod status {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum DeprecatedStatusCode {
        Ok = 0,
        Cancelled = 1,
        UnknownError = 2,
        InvalidArgument = 3,
        DeadlineExceeded = 4,
        NotFound = 5,
        AlreadyExists = 6,
        PermissionDenied = 7,
        ResourceExhausted = 8,
        FailedPrecondition = 9,
        Aborted = 10,
        OutOfRange = 11,
        Unimplemented = 12,
        InternalError = 13,
        Unavailable = 14,
        DataLoss = 15,
        Unauthenticated = 16,
    }
    /// For the semantics of status codes see
    /// <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md#set-status>
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum StatusCode {
        /// The default status.
        Unset = 0,
        /// The Span has been validated by an Application developers or Operator to have
        /// completed successfully.
        Ok = 1,
        /// The Span contains an error.
        Error = 2,
    }
}
/// Global configuration of the trace service. All fields must be specified, or
/// the default (zero) values will be used for each type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TraceConfig {
    /// The global default max number of attributes per span.
    #[prost(int64, tag="4")]
    pub max_number_of_attributes: i64,
    /// The global default max number of annotation events per span.
    #[prost(int64, tag="5")]
    pub max_number_of_timed_events: i64,
    /// The global default max number of attributes per timed event.
    #[prost(int64, tag="6")]
    pub max_number_of_attributes_per_timed_event: i64,
    /// The global default max number of link entries per span.
    #[prost(int64, tag="7")]
    pub max_number_of_links: i64,
    /// The global default max number of attributes per span.
    #[prost(int64, tag="8")]
    pub max_number_of_attributes_per_link: i64,
    /// The global default sampler used to make decisions on span sampling.
    #[prost(oneof="trace_config::Sampler", tags="1, 2, 3")]
    pub sampler: ::core::option::Option<trace_config::Sampler>,
}
/// Nested message and enum types in `TraceConfig`.
pub mod trace_config {
    /// The global default sampler used to make decisions on span sampling.
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Sampler {
        #[prost(message, tag="1")]
        ConstantSampler(super::ConstantSampler),
        #[prost(message, tag="2")]
        TraceIdRatioBased(super::TraceIdRatioBased),
        #[prost(message, tag="3")]
        RateLimitingSampler(super::RateLimitingSampler),
    }
}
/// Sampler that always makes a constant decision on span sampling.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConstantSampler {
    #[prost(enumeration="constant_sampler::ConstantDecision", tag="1")]
    pub decision: i32,
}
/// Nested message and enum types in `ConstantSampler`.
pub mod constant_sampler {
    /// How spans should be sampled:
    /// - Always off
    /// - Always on
    /// - Always follow the parent Span's decision (off if no parent).
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ConstantDecision {
        AlwaysOff = 0,
        AlwaysOn = 1,
        AlwaysParent = 2,
    }
}
/// Sampler that tries to uniformly sample traces with a given ratio.
/// The ratio of sampling a trace is equal to that of the specified ratio.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TraceIdRatioBased {
    /// The desired ratio of sampling. Must be within [0.0, 1.0].
    #[prost(double, tag="1")]
    pub sampling_ratio: f64,
}
/// Sampler that tries to sample with a rate per time window.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RateLimitingSampler {
    /// Rate per second.
    #[prost(int64, tag="1")]
    pub qps: i64,
}
