#![feature(const_vec_new)]

#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

mod account;
mod agent;
mod asset;
#[macro_use]
mod macros;
mod mission_control;
mod rate;
