use crate::consts::BLAKE2S_IV;
use digest::generic_array::typenum::{U32, U64};

blake2_impl!(
    VarBlake2s,
    Blake2s,
    u32,
    u32x4,
    U32,
    U64,
    16,
    12,
    8,
    7,
    BLAKE2S_IV,
    "Blake2s instance with a variable output.",
    "Blake2s instance with a fixed output.",
);
