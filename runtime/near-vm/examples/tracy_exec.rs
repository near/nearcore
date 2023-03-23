//! This is a simple example introducing the core concepts of the Wasmer API.
//!
//! You can run the example directly by executing the following in the Wasmer root:
//!
//! ```shell
//! cargo run --example hello-world --release --features "cranelift"
//! ```

use anyhow::Context;
use tracing_subscriber::layer::SubscriberExt;
use wasmer::{imports, wat2wasm, Function, Instance, Module, Singlepass, Store};
use wasmer_engine_universal::Universal;

// Note: the below makes the code under profiling _much much_ slower (several orders of magnitude)
/*
#[global_allocator]
static GLOBAL: tracy_client::ProfiledAllocator<std::alloc::System> =
    tracy_client::ProfiledAllocator::new(std::alloc::System, 100);
*/

fn main() -> anyhow::Result<()> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::Layer::new())
            .with(tracing_tracy::TracyLayer::new()),
    )
    .expect("Failed setting tracing subscriber");

    // Prepare rayon so that we don't see a huge 3-4ms for the first rayon usage
    tracing::info_span!("init_rayon").in_scope(|| {
        rayon::ThreadPoolBuilder::new().build_global().unwrap();
    });

    // Load the wasm
    let args = std::env::args().collect::<Vec<String>>();
    let input = std::fs::read(args.get(1).context("Usage: tracy_exec <WASM_FILE>")?)
        .context("Failed reading input file")?;
    let wasm_bytes = wat2wasm(&input).context("Failed parsing the input as wasm")?;

    // Configure a Store
    let store = Store::new(&Universal::new(Singlepass::new()).engine());

    // Compile a Module
    let module = Module::new(&store, wasm_bytes)?;

    // Link in an Instance with minimal functions
    fn gas(_foo: i32) {}
    let import_object = imports! {
        "env" => {
            "gas" => Function::new_native(&store, gas),
        }
    };
    let _instance = Instance::new(&module, &import_object)?;

    // Then everything is ready and compiled, so there is nothing more to trace.

    // Sleep a bit for Tracy to have enough time to gather everything
    tracy_client::non_continuous_frame!("tracy cleanup sleep");
    std::thread::sleep(std::time::Duration::from_millis(100));

    Ok(())
}
