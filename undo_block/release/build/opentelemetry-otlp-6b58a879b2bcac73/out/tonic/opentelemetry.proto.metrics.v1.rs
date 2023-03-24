/// A collection of InstrumentationLibraryMetrics from a Resource.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceMetrics {
    /// The resource for the metrics in this message.
    /// If this field is not set then no resource info is known.
    #[prost(message, optional, tag="1")]
    pub resource: ::core::option::Option<super::super::resource::v1::Resource>,
    /// A list of metrics that originate from a resource.
    #[prost(message, repeated, tag="2")]
    pub instrumentation_library_metrics: ::prost::alloc::vec::Vec<InstrumentationLibraryMetrics>,
}
/// A collection of Metrics produced by an InstrumentationLibrary.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstrumentationLibraryMetrics {
    /// The instrumentation library information for the metrics in this message.
    /// Semantically when InstrumentationLibrary isn't set, it is equivalent with
    /// an empty instrumentation library name (unknown).
    #[prost(message, optional, tag="1")]
    pub instrumentation_library: ::core::option::Option<super::super::common::v1::InstrumentationLibrary>,
    /// A list of metrics that originate from an instrumentation library.
    #[prost(message, repeated, tag="2")]
    pub metrics: ::prost::alloc::vec::Vec<Metric>,
}
/// Defines a Metric which has one or more timeseries.
///
/// The data model and relation between entities is shown in the
/// diagram below. Here, "DataPoint" is the term used to refer to any
/// one of the specific data point value types, and "points" is the term used
/// to refer to any one of the lists of points contained in the Metric.
///
/// - Metric is composed of a metadata and data.
/// - Metadata part contains a name, description, unit.
/// - Data is one of the possible types (Gauge, Sum, Histogram, etc.).
/// - DataPoint contains timestamps, labels, and one of the possible value type
///   fields.
///
///     Metric
///  +------------+
///  |name        |
///  |description |
///  |unit        |     +------------------------------------+
///  |data        |---> |Gauge, Sum, Histogram, Summary, ... |
///  +------------+     +------------------------------------+
///
///    Data [One of Gauge, Sum, Histogram, Summary, ...]
///  +-----------+
///  |...        |  // Metadata about the Data.
///  |points     |--+
///  +-----------+  |
///                 |      +---------------------------+
///                 |      |DataPoint 1                |
///                 v      |+------+------+   +------+ |
///              +-----+   ||label |label |...|label | |
///              |  1  |-->||value1|value2|...|valueN| |
///              +-----+   |+------+------+   +------+ |
///              |  .  |   |+-----+                    |
///              |  .  |   ||value|                    |
///              |  .  |   |+-----+                    |
///              |  .  |   +---------------------------+
///              |  .  |                   .
///              |  .  |                   .
///              |  .  |                   .
///              |  .  |   +---------------------------+
///              |  .  |   |DataPoint M                |
///              +-----+   |+------+------+   +------+ |
///              |  M  |-->||label |label |...|label | |
///              +-----+   ||value1|value2|...|valueN| |
///                        |+------+------+   +------+ |
///                        |+-----+                    |
///                        ||value|                    |
///                        |+-----+                    |
///                        +---------------------------+
///
/// All DataPoint types have three common fields:
/// - Labels zero or more key-value pairs associated with the data point.
/// - StartTimeUnixNano MUST be set to the start of the interval when the data's
///   type includes an AggregationTemporality. This field is not set otherwise.
/// - TimeUnixNano MUST be set to:
///   - the moment when an aggregation is reported (independent of the
///     aggregation temporality).
///   - the instantaneous time of the event.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Metric {
    /// name of the metric, including its DNS name prefix. It must be unique.
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// description of the metric, which can be used in documentation.
    #[prost(string, tag="2")]
    pub description: ::prost::alloc::string::String,
    /// unit in which the metric value is reported. Follows the format
    /// described by <http://unitsofmeasure.org/ucum.html.>
    #[prost(string, tag="3")]
    pub unit: ::prost::alloc::string::String,
    /// Data determines the aggregation type (if any) of the metric, what is the
    /// reported value type for the data points, as well as the relatationship to
    /// the time interval over which they are reported.
    #[prost(oneof="metric::Data", tags="4, 5, 6, 7, 8, 9, 11")]
    pub data: ::core::option::Option<metric::Data>,
}
/// Nested message and enum types in `Metric`.
pub mod metric {
    /// Data determines the aggregation type (if any) of the metric, what is the
    /// reported value type for the data points, as well as the relatationship to
    /// the time interval over which they are reported.
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Data {
        /// IntGauge and IntSum are deprecated and will be removed soon.
        /// 1. Old senders and receivers that are not aware of this change will
        /// continue using the `int_gauge` and `int_sum` fields.
        /// 2. New senders, which are aware of this change MUST send only `gauge`
        /// and `sum` fields.
        /// 3. New receivers, which are aware of this change MUST convert these into
        /// `gauge` and `sum` by using the provided as_int field in the oneof values.
        /// This field will be removed in ~3 months, on July 1, 2021.
        #[prost(message, tag="4")]
        IntGauge(super::IntGauge),
        #[prost(message, tag="5")]
        Gauge(super::Gauge),
        /// This field will be removed in ~3 months, on July 1, 2021.
        #[prost(message, tag="6")]
        IntSum(super::IntSum),
        #[prost(message, tag="7")]
        Sum(super::Sum),
        /// IntHistogram is deprecated and will be removed soon.
        /// 1. Old senders and receivers that are not aware of this change will
        /// continue using the `int_histogram` field.
        /// 2. New senders, which are aware of this change MUST send only `histogram`.
        /// 3. New receivers, which are aware of this change MUST convert this into
        /// `histogram` by simply converting all int64 values into float.
        /// This field will be removed in ~3 months, on July 1, 2021.
        #[prost(message, tag="8")]
        IntHistogram(super::IntHistogram),
        #[prost(message, tag="9")]
        Histogram(super::Histogram),
        #[prost(message, tag="11")]
        Summary(super::Summary),
    }
}
/// IntGauge is deprecated.  Use Gauge with an integer value in NumberDataPoint.
///
/// IntGauge represents the type of a int scalar metric that always exports the
/// "current value" for every data point. It should be used for an "unknown"
/// aggregation.
/// 
/// A Gauge does not support different aggregation temporalities. Given the
/// aggregation is unknown, points cannot be combined using the same
/// aggregation, regardless of aggregation temporalities. Therefore,
/// AggregationTemporality is not included. Consequently, this also means
/// "StartTimeUnixNano" is ignored for all data points.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntGauge {
    #[prost(message, repeated, tag="1")]
    pub data_points: ::prost::alloc::vec::Vec<IntDataPoint>,
}
/// Gauge represents the type of a double scalar metric that always exports the
/// "current value" for every data point. It should be used for an "unknown"
/// aggregation.
/// 
/// A Gauge does not support different aggregation temporalities. Given the
/// aggregation is unknown, points cannot be combined using the same
/// aggregation, regardless of aggregation temporalities. Therefore,
/// AggregationTemporality is not included. Consequently, this also means
/// "StartTimeUnixNano" is ignored for all data points.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Gauge {
    #[prost(message, repeated, tag="1")]
    pub data_points: ::prost::alloc::vec::Vec<NumberDataPoint>,
}
/// IntSum is deprecated.  Use Sum with an integer value in NumberDataPoint.
///
/// IntSum represents the type of a numeric int scalar metric that is calculated as
/// a sum of all reported measurements over a time interval.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntSum {
    #[prost(message, repeated, tag="1")]
    pub data_points: ::prost::alloc::vec::Vec<IntDataPoint>,
    /// aggregation_temporality describes if the aggregator reports delta changes
    /// since last report time, or cumulative changes since a fixed start time.
    #[prost(enumeration="AggregationTemporality", tag="2")]
    pub aggregation_temporality: i32,
    /// If "true" means that the sum is monotonic.
    #[prost(bool, tag="3")]
    pub is_monotonic: bool,
}
/// Sum represents the type of a numeric double scalar metric that is calculated
/// as a sum of all reported measurements over a time interval.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Sum {
    #[prost(message, repeated, tag="1")]
    pub data_points: ::prost::alloc::vec::Vec<NumberDataPoint>,
    /// aggregation_temporality describes if the aggregator reports delta changes
    /// since last report time, or cumulative changes since a fixed start time.
    #[prost(enumeration="AggregationTemporality", tag="2")]
    pub aggregation_temporality: i32,
    /// If "true" means that the sum is monotonic.
    #[prost(bool, tag="3")]
    pub is_monotonic: bool,
}
/// IntHistogram is deprecated, replaced by Histogram points using double-
/// valued exemplars.
///
/// This represents the type of a metric that is calculated by aggregating as a
/// Histogram of all reported int measurements over a time interval.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntHistogram {
    #[prost(message, repeated, tag="1")]
    pub data_points: ::prost::alloc::vec::Vec<IntHistogramDataPoint>,
    /// aggregation_temporality describes if the aggregator reports delta changes
    /// since last report time, or cumulative changes since a fixed start time.
    #[prost(enumeration="AggregationTemporality", tag="2")]
    pub aggregation_temporality: i32,
}
/// Histogram represents the type of a metric that is calculated by aggregating as a
/// Histogram of all reported double measurements over a time interval.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Histogram {
    #[prost(message, repeated, tag="1")]
    pub data_points: ::prost::alloc::vec::Vec<HistogramDataPoint>,
    /// aggregation_temporality describes if the aggregator reports delta changes
    /// since last report time, or cumulative changes since a fixed start time.
    #[prost(enumeration="AggregationTemporality", tag="2")]
    pub aggregation_temporality: i32,
}
/// Summary metric data are used to convey quantile summaries,
/// a Prometheus (see: <https://prometheus.io/docs/concepts/metric_types/#summary>)
/// and OpenMetrics (see: <https://github.com/OpenObservability/OpenMetrics/blob/4dbf6075567ab43296eed941037c12951faafb92/protos/prometheus.proto#L45>)
/// data type. These data points cannot always be merged in a meaningful way.
/// While they can be useful in some applications, histogram data points are
/// recommended for new applications.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Summary {
    #[prost(message, repeated, tag="1")]
    pub data_points: ::prost::alloc::vec::Vec<SummaryDataPoint>,
}
/// IntDataPoint is a single data point in a timeseries that describes the
/// time-varying values of a int64 metric.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntDataPoint {
    /// The set of labels that uniquely identify this timeseries.
    #[prost(message, repeated, tag="1")]
    pub labels: ::prost::alloc::vec::Vec<super::super::common::v1::StringKeyValue>,
    /// start_time_unix_nano is the last time when the aggregation value was reset
    /// to "zero". For some metric types this is ignored, see data types for more
    /// details.
    ///
    /// The aggregation value is over the time interval (start_time_unix_nano,
    /// time_unix_nano].
    /// 
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    ///
    /// Value of 0 indicates that the timestamp is unspecified. In that case the
    /// timestamp may be decided by the backend.
    #[prost(fixed64, tag="2")]
    pub start_time_unix_nano: u64,
    /// time_unix_nano is the moment when this aggregation value was reported.
    /// 
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="3")]
    pub time_unix_nano: u64,
    /// value itself.
    #[prost(sfixed64, tag="4")]
    pub value: i64,
    /// (Optional) List of exemplars collected from
    /// measurements that were used to form the data point
    #[prost(message, repeated, tag="5")]
    pub exemplars: ::prost::alloc::vec::Vec<IntExemplar>,
}
/// NumberDataPoint is a single data point in a timeseries that describes the
/// time-varying value of a double metric.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NumberDataPoint {
    /// The set of key/value pairs that uniquely identify the timeseries from 
    /// where this point belongs. The list may be empty (may contain 0 elements).
    #[prost(message, repeated, tag="7")]
    pub attributes: ::prost::alloc::vec::Vec<super::super::common::v1::KeyValue>,
    /// Labels is deprecated and will be removed soon.
    /// 1. Old senders and receivers that are not aware of this change will
    /// continue using the `labels` field.
    /// 2. New senders, which are aware of this change MUST send only `attributes`.
    /// 3. New receivers, which are aware of this change MUST convert this into
    /// `labels` by simply converting all int64 values into float.
    ///
    /// This field will be removed in ~3 months, on July 1, 2021.
    #[deprecated]
    #[prost(message, repeated, tag="1")]
    pub labels: ::prost::alloc::vec::Vec<super::super::common::v1::StringKeyValue>,
    /// start_time_unix_nano is the last time when the aggregation value was reset
    /// to "zero". For some metric types this is ignored, see data types for more
    /// details.
    ///
    /// The aggregation value is over the time interval (start_time_unix_nano,
    /// time_unix_nano].
    /// 
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    ///
    /// Value of 0 indicates that the timestamp is unspecified. In that case the
    /// timestamp may be decided by the backend.
    #[prost(fixed64, tag="2")]
    pub start_time_unix_nano: u64,
    /// time_unix_nano is the moment when this aggregation value was reported.
    /// 
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="3")]
    pub time_unix_nano: u64,
    /// (Optional) List of exemplars collected from
    /// measurements that were used to form the data point
    #[prost(message, repeated, tag="5")]
    pub exemplars: ::prost::alloc::vec::Vec<Exemplar>,
    /// The value itself.  A point is considered invalid when one of the recognized
    /// value fields is not present inside this oneof.
    #[prost(oneof="number_data_point::Value", tags="4, 6")]
    pub value: ::core::option::Option<number_data_point::Value>,
}
/// Nested message and enum types in `NumberDataPoint`.
pub mod number_data_point {
    /// The value itself.  A point is considered invalid when one of the recognized
    /// value fields is not present inside this oneof.
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(double, tag="4")]
        AsDouble(f64),
        #[prost(sfixed64, tag="6")]
        AsInt(i64),
    }
}
/// IntHistogramDataPoint is deprecated; use HistogramDataPoint.
///
/// This is a single data point in a timeseries that describes
/// the time-varying values of a Histogram of int values. A Histogram contains
/// summary statistics for a population of values, it may optionally contain
/// the distribution of those values across a set of buckets.
///
/// If the histogram contains the distribution of values, then both
/// "explicit_bounds" and "bucket counts" fields must be defined.   
/// If the histogram does not contain the distribution of values, then both
/// "explicit_bounds" and "bucket_counts" must be omitted and only "count" and
/// "sum" are known.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntHistogramDataPoint {
    /// The set of labels that uniquely identify this timeseries.
    #[prost(message, repeated, tag="1")]
    pub labels: ::prost::alloc::vec::Vec<super::super::common::v1::StringKeyValue>,
    /// start_time_unix_nano is the last time when the aggregation value was reset
    /// to "zero". For some metric types this is ignored, see data types for more
    /// details.
    ///
    /// The aggregation value is over the time interval (start_time_unix_nano,
    /// time_unix_nano].
    /// 
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    ///
    /// Value of 0 indicates that the timestamp is unspecified. In that case the
    /// timestamp may be decided by the backend.
    #[prost(fixed64, tag="2")]
    pub start_time_unix_nano: u64,
    /// time_unix_nano is the moment when this aggregation value was reported.
    /// 
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="3")]
    pub time_unix_nano: u64,
    /// count is the number of values in the population. Must be non-negative. This
    /// value must be equal to the sum of the "count" fields in buckets if a
    /// histogram is provided.
    #[prost(fixed64, tag="4")]
    pub count: u64,
    /// sum of the values in the population. If count is zero then this field
    /// must be zero. This value must be equal to the sum of the "sum" fields in
    /// buckets if a histogram is provided.
    #[prost(sfixed64, tag="5")]
    pub sum: i64,
    /// bucket_counts is an optional field contains the count values of histogram
    /// for each bucket.
    ///
    /// The sum of the bucket_counts must equal the value in the count field.
    ///
    /// The number of elements in bucket_counts array must be by one greater than
    /// the number of elements in explicit_bounds array.
    #[prost(fixed64, repeated, tag="6")]
    pub bucket_counts: ::prost::alloc::vec::Vec<u64>,
    /// explicit_bounds specifies buckets with explicitly defined bounds for values.
    ///
    /// This defines size(explicit_bounds) + 1 (= N) buckets. The boundaries for
    /// bucket at index i are:
    ///
    /// (-infinity, explicit_bounds\[i]\] for i == 0
    /// (explicit_bounds\[i-1\], explicit_bounds\[i]\] for 0 < i < N-1
    /// (explicit_bounds\[i\], +infinity) for i == N-1
    /// 
    /// The values in the explicit_bounds array must be strictly increasing.
    ///
    /// Histogram buckets are inclusive of their upper boundary, except the last
    /// bucket where the boundary is at infinity. This format is intentionally
    /// compatible with the OpenMetrics histogram definition.
    #[prost(double, repeated, tag="7")]
    pub explicit_bounds: ::prost::alloc::vec::Vec<f64>,
    /// (Optional) List of exemplars collected from
    /// measurements that were used to form the data point
    #[prost(message, repeated, tag="8")]
    pub exemplars: ::prost::alloc::vec::Vec<IntExemplar>,
}
/// HistogramDataPoint is a single data point in a timeseries that describes the
/// time-varying values of a Histogram of double values. A Histogram contains
/// summary statistics for a population of values, it may optionally contain the
/// distribution of those values across a set of buckets.
///
/// If the histogram contains the distribution of values, then both
/// "explicit_bounds" and "bucket counts" fields must be defined.   
/// If the histogram does not contain the distribution of values, then both
/// "explicit_bounds" and "bucket_counts" must be omitted and only "count" and
/// "sum" are known.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HistogramDataPoint {
    /// The set of key/value pairs that uniquely identify the timeseries from 
    /// where this point belongs. The list may be empty (may contain 0 elements).
    #[prost(message, repeated, tag="9")]
    pub attributes: ::prost::alloc::vec::Vec<super::super::common::v1::KeyValue>,
    /// Labels is deprecated and will be removed soon.
    /// 1. Old senders and receivers that are not aware of this change will
    /// continue using the `labels` field.
    /// 2. New senders, which are aware of this change MUST send only `attributes`.
    /// 3. New receivers, which are aware of this change MUST convert this into
    /// `labels` by simply converting all int64 values into float.
    ///
    /// This field will be removed in ~3 months, on July 1, 2021.
    #[deprecated]
    #[prost(message, repeated, tag="1")]
    pub labels: ::prost::alloc::vec::Vec<super::super::common::v1::StringKeyValue>,
    /// start_time_unix_nano is the last time when the aggregation value was reset
    /// to "zero". For some metric types this is ignored, see data types for more
    /// details.
    ///
    /// The aggregation value is over the time interval (start_time_unix_nano,
    /// time_unix_nano].
    /// 
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    ///
    /// Value of 0 indicates that the timestamp is unspecified. In that case the
    /// timestamp may be decided by the backend.
    #[prost(fixed64, tag="2")]
    pub start_time_unix_nano: u64,
    /// time_unix_nano is the moment when this aggregation value was reported.
    /// 
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="3")]
    pub time_unix_nano: u64,
    /// count is the number of values in the population. Must be non-negative. This
    /// value must be equal to the sum of the "count" fields in buckets if a
    /// histogram is provided.
    #[prost(fixed64, tag="4")]
    pub count: u64,
    /// sum of the values in the population. If count is zero then this field
    /// must be zero. This value must be equal to the sum of the "sum" fields in
    /// buckets if a histogram is provided.
    #[prost(double, tag="5")]
    pub sum: f64,
    /// bucket_counts is an optional field contains the count values of histogram
    /// for each bucket.
    ///
    /// The sum of the bucket_counts must equal the value in the count field.
    ///
    /// The number of elements in bucket_counts array must be by one greater than
    /// the number of elements in explicit_bounds array.
    #[prost(fixed64, repeated, tag="6")]
    pub bucket_counts: ::prost::alloc::vec::Vec<u64>,
    /// explicit_bounds specifies buckets with explicitly defined bounds for values.
    ///
    /// This defines size(explicit_bounds) + 1 (= N) buckets. The boundaries for
    /// bucket at index i are:
    ///
    /// (-infinity, explicit_bounds\[i]\] for i == 0
    /// (explicit_bounds\[i-1\], explicit_bounds\[i]\] for 0 < i < N-1
    /// (explicit_bounds\[i\], +infinity) for i == N-1
    /// 
    /// The values in the explicit_bounds array must be strictly increasing.
    ///
    /// Histogram buckets are inclusive of their upper boundary, except the last
    /// bucket where the boundary is at infinity. This format is intentionally
    /// compatible with the OpenMetrics histogram definition.
    #[prost(double, repeated, tag="7")]
    pub explicit_bounds: ::prost::alloc::vec::Vec<f64>,
    /// (Optional) List of exemplars collected from
    /// measurements that were used to form the data point
    #[prost(message, repeated, tag="8")]
    pub exemplars: ::prost::alloc::vec::Vec<Exemplar>,
}
/// SummaryDataPoint is a single data point in a timeseries that describes the
/// time-varying values of a Summary metric.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SummaryDataPoint {
    /// The set of key/value pairs that uniquely identify the timeseries from 
    /// where this point belongs. The list may be empty (may contain 0 elements).
    #[prost(message, repeated, tag="7")]
    pub attributes: ::prost::alloc::vec::Vec<super::super::common::v1::KeyValue>,
    /// Labels is deprecated and will be removed soon.
    /// 1. Old senders and receivers that are not aware of this change will
    /// continue using the `labels` field.
    /// 2. New senders, which are aware of this change MUST send only `attributes`.
    /// 3. New receivers, which are aware of this change MUST convert this into
    /// `labels` by simply converting all int64 values into float.
    ///
    /// This field will be removed in ~3 months, on July 1, 2021.
    #[deprecated]
    #[prost(message, repeated, tag="1")]
    pub labels: ::prost::alloc::vec::Vec<super::super::common::v1::StringKeyValue>,
    /// start_time_unix_nano is the last time when the aggregation value was reset
    /// to "zero". For some metric types this is ignored, see data types for more
    /// details.
    ///
    /// The aggregation value is over the time interval (start_time_unix_nano,
    /// time_unix_nano].
    ///
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    ///
    /// Value of 0 indicates that the timestamp is unspecified. In that case the
    /// timestamp may be decided by the backend.
    #[prost(fixed64, tag="2")]
    pub start_time_unix_nano: u64,
    /// time_unix_nano is the moment when this aggregation value was reported.
    ///
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="3")]
    pub time_unix_nano: u64,
    /// count is the number of values in the population. Must be non-negative.
    #[prost(fixed64, tag="4")]
    pub count: u64,
    /// sum of the values in the population. If count is zero then this field
    /// must be zero.
    #[prost(double, tag="5")]
    pub sum: f64,
    /// (Optional) list of values at different quantiles of the distribution calculated
    /// from the current snapshot. The quantiles must be strictly increasing.
    #[prost(message, repeated, tag="6")]
    pub quantile_values: ::prost::alloc::vec::Vec<summary_data_point::ValueAtQuantile>,
}
/// Nested message and enum types in `SummaryDataPoint`.
pub mod summary_data_point {
    /// Represents the value at a given quantile of a distribution.
    ///
    /// To record Min and Max values following conventions are used:
    /// - The 1.0 quantile is equivalent to the maximum value observed.
    /// - The 0.0 quantile is equivalent to the minimum value observed.
    ///
    /// See the following issue for more context:
    /// <https://github.com/open-telemetry/opentelemetry-proto/issues/125>
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ValueAtQuantile {
        /// The quantile of a distribution. Must be in the interval
        /// [0.0, 1.0].
        #[prost(double, tag="1")]
        pub quantile: f64,
        /// The value at the given quantile of a distribution.
        #[prost(double, tag="2")]
        pub value: f64,
    }
}
/// A representation of an exemplar, which is a sample input int measurement.
/// Exemplars also hold information about the environment when the measurement
/// was recorded, for example the span and trace ID of the active span when the
/// exemplar was recorded.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntExemplar {
    /// The set of labels that were filtered out by the aggregator, but recorded
    /// alongside the original measurement. Only labels that were filtered out
    /// by the aggregator should be included
    #[prost(message, repeated, tag="1")]
    pub filtered_labels: ::prost::alloc::vec::Vec<super::super::common::v1::StringKeyValue>,
    /// time_unix_nano is the exact time when this exemplar was recorded
    ///
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="2")]
    pub time_unix_nano: u64,
    /// Numerical int value of the measurement that was recorded.
    #[prost(sfixed64, tag="3")]
    pub value: i64,
    /// (Optional) Span ID of the exemplar trace.
    /// span_id may be missing if the measurement is not recorded inside a trace
    /// or if the trace is not sampled.
    #[prost(bytes="vec", tag="4")]
    pub span_id: ::prost::alloc::vec::Vec<u8>,
    /// (Optional) Trace ID of the exemplar trace.
    /// trace_id may be missing if the measurement is not recorded inside a trace
    /// or if the trace is not sampled.
    #[prost(bytes="vec", tag="5")]
    pub trace_id: ::prost::alloc::vec::Vec<u8>,
}
/// A representation of an exemplar, which is a sample input measurement.
/// Exemplars also hold information about the environment when the measurement
/// was recorded, for example the span and trace ID of the active span when the
/// exemplar was recorded.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Exemplar {
    /// The set of key/value pairs that were filtered out by the aggregator, but
    /// recorded alongside the original measurement. Only key/value pairs that were
    /// filtered out by the aggregator should be included
    #[prost(message, repeated, tag="7")]
    pub filtered_attributes: ::prost::alloc::vec::Vec<super::super::common::v1::KeyValue>,
    /// Labels is deprecated and will be removed soon.
    /// 1. Old senders and receivers that are not aware of this change will
    /// continue using the `filtered_labels` field.
    /// 2. New senders, which are aware of this change MUST send only
    /// `filtered_attributes`.
    /// 3. New receivers, which are aware of this change MUST convert this into
    /// `filtered_labels` by simply converting all int64 values into float.
    ///
    /// This field will be removed in ~3 months, on July 1, 2021.
    #[deprecated]
    #[prost(message, repeated, tag="1")]
    pub filtered_labels: ::prost::alloc::vec::Vec<super::super::common::v1::StringKeyValue>,
    /// time_unix_nano is the exact time when this exemplar was recorded
    ///
    /// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    /// 1970.
    #[prost(fixed64, tag="2")]
    pub time_unix_nano: u64,
    /// (Optional) Span ID of the exemplar trace.
    /// span_id may be missing if the measurement is not recorded inside a trace
    /// or if the trace is not sampled.
    #[prost(bytes="vec", tag="4")]
    pub span_id: ::prost::alloc::vec::Vec<u8>,
    /// (Optional) Trace ID of the exemplar trace.
    /// trace_id may be missing if the measurement is not recorded inside a trace
    /// or if the trace is not sampled.
    #[prost(bytes="vec", tag="5")]
    pub trace_id: ::prost::alloc::vec::Vec<u8>,
    /// Numerical value of the measurement that was recorded. An exemplar is
    /// considered invalid when one of the recognized value fields is not present
    /// inside this oneof.
    #[prost(oneof="exemplar::Value", tags="3, 6")]
    pub value: ::core::option::Option<exemplar::Value>,
}
/// Nested message and enum types in `Exemplar`.
pub mod exemplar {
    /// Numerical value of the measurement that was recorded. An exemplar is
    /// considered invalid when one of the recognized value fields is not present
    /// inside this oneof.
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(double, tag="3")]
        AsDouble(f64),
        #[prost(sfixed64, tag="6")]
        AsInt(i64),
    }
}
/// AggregationTemporality defines how a metric aggregator reports aggregated
/// values. It describes how those values relate to the time interval over
/// which they are aggregated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AggregationTemporality {
    /// UNSPECIFIED is the default AggregationTemporality, it MUST not be used.
    Unspecified = 0,
    /// DELTA is an AggregationTemporality for a metric aggregator which reports
    /// changes since last report time. Successive metrics contain aggregation of
    /// values from continuous and non-overlapping intervals.
    ///
    /// The values for a DELTA metric are based only on the time interval
    /// associated with one measurement cycle. There is no dependency on
    /// previous measurements like is the case for CUMULATIVE metrics.
    ///
    /// For example, consider a system measuring the number of requests that
    /// it receives and reports the sum of these requests every second as a
    /// DELTA metric:
    ///
    ///   1. The system starts receiving at time=t_0.
    ///   2. A request is received, the system measures 1 request.
    ///   3. A request is received, the system measures 1 request.
    ///   4. A request is received, the system measures 1 request.
    ///   5. The 1 second collection cycle ends. A metric is exported for the
    ///      number of requests received over the interval of time t_0 to
    ///      t_0+1 with a value of 3.
    ///   6. A request is received, the system measures 1 request.
    ///   7. A request is received, the system measures 1 request.
    ///   8. The 1 second collection cycle ends. A metric is exported for the
    ///      number of requests received over the interval of time t_0+1 to
    ///      t_0+2 with a value of 2.
    Delta = 1,
    /// CUMULATIVE is an AggregationTemporality for a metric aggregator which
    /// reports changes since a fixed start time. This means that current values
    /// of a CUMULATIVE metric depend on all previous measurements since the
    /// start time. Because of this, the sender is required to retain this state
    /// in some form. If this state is lost or invalidated, the CUMULATIVE metric
    /// values MUST be reset and a new fixed start time following the last
    /// reported measurement time sent MUST be used.
    ///
    /// For example, consider a system measuring the number of requests that
    /// it receives and reports the sum of these requests every second as a
    /// CUMULATIVE metric:
    ///
    ///   1. The system starts receiving at time=t_0.
    ///   2. A request is received, the system measures 1 request.
    ///   3. A request is received, the system measures 1 request.
    ///   4. A request is received, the system measures 1 request.
    ///   5. The 1 second collection cycle ends. A metric is exported for the
    ///      number of requests received over the interval of time t_0 to
    ///      t_0+1 with a value of 3.
    ///   6. A request is received, the system measures 1 request.
    ///   7. A request is received, the system measures 1 request.
    ///   8. The 1 second collection cycle ends. A metric is exported for the
    ///      number of requests received over the interval of time t_0 to
    ///      t_0+2 with a value of 5.
    ///   9. The system experiences a fault and loses state.
    ///   10. The system recovers and resumes receiving at time=t_1.
    ///   11. A request is received, the system measures 1 request.
    ///   12. The 1 second collection cycle ends. A metric is exported for the
    ///      number of requests received over the interval of time t_1 to
    ///      t_0+1 with a value of 1.
    ///
    /// Note: Even though, when reporting changes since last report time, using 
    /// CUMULATIVE is valid, it is not recommended. This may cause problems for
    /// systems that do not use start_time to determine when the aggregation
    /// value was reset (e.g. Prometheus).
    Cumulative = 2,
}
