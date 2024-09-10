mod arena;
mod construction;
pub(crate) mod flexible_data;
mod freelist;
pub mod iter;
pub mod loading;
mod lookup;
pub mod mem_tries;
pub mod metrics;
pub mod node;
mod parallel_loader;
pub mod updating;

/// Check this, because in the code we conveniently assume usize is 8 bytes.
/// In-memory trie can't possibly work under 32-bit anyway.
#[cfg(not(target_pointer_width = "64"))]
compile_error!("In-memory trie requires a 64 bit platform");
