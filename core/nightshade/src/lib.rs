#[macro_use]
extern crate serde_derive;
extern crate bs58;
extern crate serde;

pub mod nightshade;
pub mod nightshade_task;
mod verifier;
#[macro_use]
mod testing_utils;

#[cfg(test)]
mod fake_network;
