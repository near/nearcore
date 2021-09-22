#![no_std]
#![allow(non_snake_case)]

#[cfg(feature = "small")]
mod small;
#[cfg(not(feature = "small"))]
mod full;
