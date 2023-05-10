# The NearVM runtime crates

This crate set is a fork of Wasmer. A significant number of things
changed, but the documentation is not up-to-date yet.

The philosophy of Wasmer was to be very modular by design. It's
composed of a set of crates. We can group them as follows:

* `api` — The public Rust API exposes everything a user needs to use Wasmer programatically through
  the `wasmer` crate,
* `cache` — The traits and types to cache compiled WebAssembly modules,
* `cli` — The Wasmer CLI itself,
* `compiler` — The base for the compiler implementations, it defines
  the framework for the compilers and provides everything they need:
  * `compiler-singlepass` — A WebAssembly compiler based on our own compilation infrastructure;
    recommended for compilation-time speed performance.
* `derive` — A set of procedural macros used inside Wasmer,
* `engine` — The general abstraction for creating an engine, which is responsible of leading the
  compiling and running flow. Using the same compiler, the runtime performance will be
  approximately the same, however the way it stores and loads the executable code will differ:
  * `engine-universal` — stores the code in a custom file format, and loads it in memory,
* `types` — The basic structures to use WebAssembly,
* `vm` — The Wasmer VM runtime library, the low-level base of
  everything.

This is no longer a case for near-vm which is moving away from this to being specialized for
nearcore needs.
