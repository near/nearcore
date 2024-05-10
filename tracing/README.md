# Near Custom Tracing

The custom tracing setup involves the following:

* We configure neard nodes to send OpenTelemetry traces to the near-tracing collector (this crate);
* The near-tracing collector stores the traces into MongoDB;
* The near-tracing querier (also in this crate) accepts queries (a time interval) and returns
  tracing data in this interval as a Firefox profile;
* We can then visualize the traces in a forked version of the Firefox profiler.
