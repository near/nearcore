#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate bs58;

mod nightshade;
mod verifier;
pub mod nightshade_task;

#[cfg(test)]
mod fake_network;
