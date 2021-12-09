#![doc = include_str!("../README.md")]
pub(crate) mod framed_read;
pub use framed_read::{ThrottleController, ThrottleToken, ThrottledFrameRead};
