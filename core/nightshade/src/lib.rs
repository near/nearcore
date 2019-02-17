#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate bs58;

pub mod nightshade;
pub mod nightshade_task;

#[cfg(test)]
pub mod fake_network;
