//! An implementation of the [BLAKE2][1] hash functions.
//!
//! # Usage
//!
//! `Blake2b` can be used in the following way:
//!
//! ```rust
//! use blake2::{Blake2b, Blake2s, Digest};
//! use hex_literal::hex;
//!
//! // create a Blake2b object
//! let mut hasher = Blake2b::new();
//!
//! // write input message
//! hasher.update(b"hello world");
//!
//! // read hash digest and consume hasher
//! let res = hasher.finalize();
//! assert_eq!(res[..], hex!("
//!     021ced8799296ceca557832ab941a50b4a11f83478cf141f51f933f653ab9fbc
//!     c05a037cddbed06e309bf334942c4e58cdf1a46e237911ccd7fcf9787cbc7fd0
//! ")[..]);
//!
//! // same example for `Blake2s`:
//! let mut hasher = Blake2s::new();
//! hasher.update(b"hello world");
//! let res = hasher.finalize();
//! assert_eq!(res[..], hex!("
//!     9aec6806794561107e594b1f6a8a6b0c92a0cba9acf5e5e93cca06f781813b0b
//! ")[..]);
//! ```
//!
//! Also see [RustCrypto/hashes](https://github.com/RustCrypto/hashes) readme.
//!
//! ## Variable output size
//!
//! If you need variable sized output you can use `VarBlake2b` and `VarBlake2s`
//! which support variable output sizes through `VariableOutput` trait. `Update`
//! trait has to be imported as well.
//!
//! ```rust
//! use blake2::VarBlake2b;
//! use blake2::digest::{Update, VariableOutput};
//!
//! let mut hasher = VarBlake2b::new(10).unwrap();
//! hasher.update(b"my_input");
//! hasher.finalize_variable(|res| {
//!     assert_eq!(res, [44, 197, 92, 132, 228, 22, 146, 78, 100, 0])
//! })
//! ```
//!
//! ## Message Authentication Code (MAC)
//!
//! BLAKE2 can be used as a MAC without any additional constructs:
//!
//! ```rust
//! use blake2::Blake2b;
//! use blake2::crypto_mac::{Mac, NewMac};
//!
//! let mut hasher = Blake2b::new_varkey(b"my key").unwrap();
//! hasher.update(b"hello world");
//!
//! // `result` has type `crypto_mac::Output` which is a thin wrapper around
//! // a byte array and provides a constant time equality check
//! let result = hasher.finalize();
//! // To get underlying array use the `into_bytes` method, but be careful,
//! // since incorrect use of the code value may permit timing attacks which
//! // defeat the security provided by the `crypto_mac::Output`
//! let code_bytes = result.into_bytes();
//!
//! // To verify the message it's recommended to use `verify` method
//! let mut hasher = Blake2b::new_varkey(b"my key").unwrap();
//! hasher.update(b"hello world");
//! // `verify` return `Ok(())` if code is correct, `Err(MacError)` otherwise
//! hasher.verify(&code_bytes).unwrap();
//! ```
//!
//! # Acknowledgment
//! Based on the [blake2-rfc][2] crate.
//!
//! [1]: https://en.wikipedia.org/wiki/BLAKE_(hash_function)#BLAKE2
//! [2]: https://github.com/cesarb/blake2-rfc

#![no_std]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/RustCrypto/meta/master/logo.svg",
    html_favicon_url = "https://raw.githubusercontent.com/RustCrypto/meta/master/logo.svg"
)]
#![warn(missing_docs, rust_2018_idioms)]
#![cfg_attr(feature = "simd", feature(platform_intrinsics, repr_simd))]
#![cfg_attr(feature = "simd_asm", feature(asm))]

#[cfg(feature = "std")]
extern crate std;

mod as_bytes;
mod consts;

mod simd;

#[macro_use]
mod blake2;

mod blake2b;
mod blake2s;

pub use crypto_mac;
pub use digest::{self, Digest};

pub use crate::blake2b::{f as blake2b_f, Blake2b, VarBlake2b};
pub use crate::blake2s::{f as blake2s_f, Blake2s, VarBlake2s};
