# Working with OpenTelemetry Traces

`neard` is instrumented in a few different ways. From the code perspective we have two major ways
of instrumenting code:

* Prometheus metrics – by computing various metrics in code and expoding them via the `prometheus`
  crate.
* Execution tracing – this shows up in the code as invocations of functionality provided by the
  `tracing` crate.

The focus of this document is to provide information on how to effectively work with the data
collected by the execution tracing approach to instrumentation.

## Gathering and Viewing the Traces

Tracing the execution of the code produces two distinct types of data: spans and events. These then
are exposed as either logs (representing mostly the events) seen in the standard output of the
`neard` process or sent onwards to an [opentelemetry collector].

[opentelemetry collector]: https://opentelemetry.io/docs/collector/

When deciding how to instrument a specific part of the code, consider the following decision tree:

1. Do I need execution timing information? If so, use a span; otherwise
2. Do I need call stack information? If so, use a span; otherwise
3. Do I need to preserve information about inputs or outputs to a specific section of the code? If
   so, use key-values on a pre-existing span or an event; otherwise
4. Use an event if it represents information applicable to a single point of execution trace.

As of writing (February 2024) our codebase uses spans somewhat sparsely and relies on events
heavily to expose information about the execution of the code. This is largely a historical
accident due to the fact that for a long time stdout logs were the only reasonable way to extract
information out of the running executable.

Today we have more tools available to us. In production environments and environments replicating
said environment (i.e. GCP environments such as mocknet) there's the ability to push this data to
Grafana Loki (for events) and [Tempo] (for spans and events alike), so long as the amount of data
is within reason. For that reason it is critical that the event and span levels are chosen
appropriately and in consideration with the frequency of invocations. In local environments
developers can use projects like [Jaeger], or set up the Grafana stack if they wish to use a
consistent interfaces.

It is still more straightforward to skip all the setup necessary for tracing, but relying
exclusively on logs only increases noise for the other developers and makes it ever so slightly
harder to extract signal in the future. Keep this trade off in mind.

[Tempo]: https://grafana.com/oss/tempo/
[Loki]: https://grafana.com/oss/loki/
[Jaeger]: https://www.jaegertracing.io/

## Configuration

[The OTLP documentation page in our terraform
repository](https://github.com/PagodaPlatform/tf-near-node/blob/main/doc/otlp.md) documents the
steps necessary to start moving the trace data from the node to our Grafana Cloud instance. Once
you set up your nodes, you can use the explore page to verify that the traces are coming through.

![Image displaying the Grafana explore page interacting with the grafana-nearinc-traces data
source, with Service Name filter set to
=~"neard:mocknet-mainnet-94194484-nagisa-10402-test-vzx2.near|neard:mocknet-mainnet-94194484-nagisa-10402-test-xdwp.near"
and showing some traces having been found](../../images/explore-traces.png)

If the traces are not coming through quite yet, consider using the ability to set logging
configuration at runtime. Create `$NEARD_HOME/log_config.json` file with the following contents:

```json
{ "opentelemetry": "info" }
```

Or optionally with `rust_log` setting to reduce logging on stdout:

```json
{ "opentelemetry": "info", "rust_log": "WARN" }
```

and invoke `sudo pkill -HUP neard`. Double check that the collector is running as well.

<blockquote style="background: rgba(255, 200, 0, 0.1); border: 5px solid rgba(255, 200, 0, 0.4);">

**Good to know**: You can modify the event/span/log targets you’re interested in just like when
setting the `RUST_LOG` environment variable.

For more information about the dynamic settings refer to `core/dyn-configs` code in the repository.

</blockquote>

### Local development

<blockquote style="background: rgba(255, 200, 0, 0.1); border: 5px solid rgba(255, 200, 0, 0.4);">

**TODO**: the setup is going to depend on whether one would like to use grafana stack or just
jaeger or something else. We shoud document setting either of these up, including the otel
collector and such for a full end-to-end setup. Success criteria: running integration tests should
allow you to see the traces in your grafana/jaeger. This may require code changes as well.

</blockquote>

Using the Grafana Stack here gives the benefit of all of the visualizations that are built-in. Any
dashboards you build are also portable between the local environment and the Grafana Cloud
instance. Jaeger may give a nicer interactive exploration ability. You can also set up both if you
wish.

## Visualization

Now that the data is arriving into the databases, it is time to visualize the data to determine
what you want to know about the node. The only general advise I have here is to check that the data
source is indeed tempo or loki. Try out visualizations other than time series. For example the
author was interested in checking the execution speed before and after a change in a component.
To make the comparison visual, the span of interest was graphed using the histogram visualization
in order to obtain the following result. Note: you can hover your mouse on the image to see the
visualization of the baseline performance.

<div id="image-comparison">
<img src="../../images/compile-and-load-before.png" class="before" />
<img src="../../images/compile-and-load-after.png" class="after" />
</div>
<style>
#image-comparison {
    position: relative;
}
#image-comparison>.before {
    position: absolute;
    top: 0;
    left: 0;
    z-index: 1;
    opacity: 0;
    transition: opacity 250ms;
}
#image-comparison>.before:hover {
    opacity: 1;
}
</style>

You can also add a panel that shows all the trace events in a log-like representation using the log
visualization.
