# near-vm-runner

An engine that runs smart contracts compiled to Wasm.
This is the main crate of the "contract runtime" part of nearcore.

"Running smart contracts" is:

- Wasm instrumentation for gas metering and various safety checks (`prepare.rs`).
- Compiling Wasm to a particular VM representation (`cache.rs`).
- Exposing blockchain-specific functionality to Wasm code. That is, defining a corresponding host
  function for each function in `near-vm-logic` (`imports.rs`).
- Actual code execution (`wasmer_runner.rs`).

The particular runtime used for Wasm execution is an implementation detail.  At the moment we support
Wasmer 0.x, Wasmer 2.0 and Wasmtime, with Wasmer 2.0 being default.

The primary client of Wasm execution services is the blockchain proper. The second client is the
contract sdk tooling. vm-runner provides additional API for contract developers to, for example,
get a gas costs breakdown.

See the [FAQ][./faq.md] document for high-level design constraints discussion.

## Entry Point

The entry point is the `runner::run` function. 

## Testing

There are a bunch of unit-tests in this crate. You can run them with

```console
$ cargo t -p near-vm-runner --features wasmer0_vm,wasmer2_vm,wasmtime_vm,near_vm
```

The tests use either a short wasm snippet specified inline, or a couple of
larger test contracts from the `near-test-contracts` crate.

We also have a fuzzing setup:

```console
$ cd runtime/near-vm-runner && RUSTC_BOOTSTRAP=1 cargo fuzz run runner
```

## Profiling

`tracing` crate is used to collect Rust code profile data via manual instrumentation.
If you want to know how long a particular function executes, use the following pattern:

```ignore
fn compute_thing() {
    let _span = tracing::debug_span!(target: "vm", "compute_thing").entered();
    for i in 0..99 {
        do_work()
    }
}
```

This will record when the `_span` object is created and dropped, including the time diff between
the two events.

To get human-readable output out of these events, you can use the built-in tracing subscriber:

```ignore
tracing_subscriber::fmt::Subscriber::builder()
    .with_max_level(tracing::level_filters::LevelFilter::DEBUG)
    .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
    .init();

code_to_profile_here();
```

Alternatively, there's an alternative hierarchical profiler

```ignore
tracing_span_tree::span_tree().enable();

code_to_profile_here();
```

The result would look like this:

```text
      112.33ms deserialize_wasmer
      2.64ms run_wasmer/instantiate
      96.34µs run_wasmer/call
    123.15ms run_wasmer
  123.17ms run_vm
```
