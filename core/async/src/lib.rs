pub use near_async_derive::{MultiSend, MultiSendMessage, MultiSenderFrom};

pub mod actix;
pub mod actix_wrapper;
pub mod break_apart;
#[cfg(test)]
mod examples;
mod functional;
pub mod futures;
pub mod messaging;
pub mod test_loop;

// FIXME: near_time re-export is not optimal solution, but it would require to change time in many places
pub use near_time as time;
