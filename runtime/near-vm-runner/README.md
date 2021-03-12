# near-vm-runner

An engine that run smart contracts compiled to Wasm by exposing
them `near-vm-logic` through the host functions. Currently is using Wasmer and singlepass compiler.

Can be used for benchmarks of smart contracts.


## Dev Notes

### Entry Point

The entry point is the `runner::run` function.
It is exposed as a stand-alone binary for testing purposes in the `near-vm-runner-standalone` crate.

### Profiling

`tracing` crate is used to collect Rust code profile data via manual instrumentation.
If you want to know how long a particular function executes, use the following pattern:

```rust
fn compute_thing() {
    let _span = tracing::debug_span!("compute_thing").entered();
    for i in 0..99 {
        do_work()
    }
}
```

This will record when the `_span` object is created and dropped, including the time diff between the two events.

To get a human readable output out of this events, you can use the built-in tracing subscriber:

```rust
tracing_subscriber::fmt::Subscriber::builder()
    .with_max_level(tracing::level_filters::LevelFilter::DEBUG)
    .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
    .init();

code_to_profile_here();
```

Alternatively, in the `standalone` crate we have a more compact hierarchical subscriber

```rust
crate::tracing_timings::enable();

code_to_profile_here();
```

The result would look like this:

```
      112.33ms deserialize_wasmer
      2.64ms run_wasmer/instantiate
      96.34µs run_wasmer/call
    123.15ms run_wasmer
  123.17ms run_vm
```
