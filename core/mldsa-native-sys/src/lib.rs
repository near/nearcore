//! FFI bindings to the vendored mldsa-native (FIPS 204) ML-DSA-65 implementation.
#![no_std]

use core::ffi::c_int;

pub const SEED_BYTES: usize = 32;
pub const RND_BYTES: usize = 32;
pub const PUBLIC_KEY_BYTES: usize = 1952;
pub const SECRET_KEY_BYTES: usize = 4032;
pub const SIGNATURE_BYTES: usize = 3309;

/// FIPS 204 pure ML-DSA domain separation prefix with an empty context: `0x00 || 0x00`.
const PURE_EMPTY_CONTEXT_PREFIX: [u8; 2] = [0, 0];

pub mod ffi {
    use core::ffi::c_int;
    unsafe extern "C" {
        pub fn mldsa65_keypair_internal(pk: *mut u8, sk: *mut u8, seed: *const u8) -> c_int;
        pub fn mldsa65_signature_internal(
            sig: *mut u8,
            m: *const u8,
            mlen: usize,
            pre: *const u8,
            prelen: usize,
            rnd: *const u8,
            sk: *const u8,
            externalmu: c_int,
        ) -> c_int;
        pub fn mldsa65_verify(
            sig: *const u8,
            m: *const u8,
            mlen: usize,
            ctx: *const u8,
            ctxlen: usize,
            pk: *const u8,
        ) -> c_int;
        pub fn mldsa65_pk_from_sk(pk: *mut u8, sk: *const u8) -> c_int;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Error(pub c_int);

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "mldsa-native error {}", self.0)
    }
}

fn check(rc: c_int) -> Result<(), Error> {
    if rc == 0 { Ok(()) } else { Err(Error(rc)) }
}

/// FIPS 204 ML-DSA.KeyGen_internal.
pub fn keypair_from_seed(
    seed: &[u8; SEED_BYTES],
    pk: &mut [u8; PUBLIC_KEY_BYTES],
    sk: &mut [u8; SECRET_KEY_BYTES],
) -> Result<(), Error> {
    // SAFETY: fixed-size buffers matching the C prototypes; no aliasing.
    check(unsafe { ffi::mldsa65_keypair_internal(pk.as_mut_ptr(), sk.as_mut_ptr(), seed.as_ptr()) })
}

/// Pure ML-DSA signing with an empty context. `rnd` all zeros is the FIPS 204
/// deterministic variant. Does not validate `sk`; see [`public_key_from_secret_key`].
pub fn sign(
    sk: &[u8; SECRET_KEY_BYTES],
    msg: &[u8],
    rnd: &[u8; RND_BYTES],
    sig: &mut [u8; SIGNATURE_BYTES],
) -> Result<(), Error> {
    // SAFETY: fixed-size buffers matching the C prototypes; msg pointer/len from a slice.
    check(unsafe {
        ffi::mldsa65_signature_internal(
            sig.as_mut_ptr(),
            msg.as_ptr(),
            msg.len(),
            PURE_EMPTY_CONTEXT_PREFIX.as_ptr(),
            PURE_EMPTY_CONTEXT_PREFIX.len(),
            rnd.as_ptr(),
            sk.as_ptr(),
            0,
        )
    })
}

/// FIPS 204 ML-DSA.Verify with an empty context.
pub fn verify(pk: &[u8; PUBLIC_KEY_BYTES], msg: &[u8], sig: &[u8; SIGNATURE_BYTES]) -> bool {
    // SAFETY: fixed-size buffers matching the C prototypes; msg pointer/len from a slice.
    unsafe {
        ffi::mldsa65_verify(sig.as_ptr(), msg.as_ptr(), msg.len(), core::ptr::null(), 0, pk.as_ptr())
            == 0
    }
}

/// Validates `sk` (coefficient ranges, t0 and tr consistency) and derives its public key.
pub fn public_key_from_secret_key(
    sk: &[u8; SECRET_KEY_BYTES],
    pk: &mut [u8; PUBLIC_KEY_BYTES],
) -> Result<(), Error> {
    // SAFETY: fixed-size buffers matching the C prototypes; no aliasing.
    check(unsafe { ffi::mldsa65_pk_from_sk(pk.as_mut_ptr(), sk.as_ptr()) })
}

#[cfg(all(target_arch = "x86_64", any(target_os = "linux", target_os = "macos")))]
mod cpu {
    extern crate std;
    use core::sync::atomic::{AtomicU8, Ordering};

    static AVX2: AtomicU8 = AtomicU8::new(u8::MAX);

    /// Called from C (`mld_sys_check_capability`) to dispatch to the AVX2 backend.
    #[unsafe(no_mangle)]
    pub extern "C" fn near_mldsa_native_x86_64_has_avx2() -> core::ffi::c_int {
        match AVX2.load(Ordering::Relaxed) {
            u8::MAX => {
                let has = std::arch::is_x86_feature_detected!("avx2") as u8;
                AVX2.store(has, Ordering::Relaxed);
                has as core::ffi::c_int
            }
            v => v as core::ffi::c_int,
        }
    }
}
