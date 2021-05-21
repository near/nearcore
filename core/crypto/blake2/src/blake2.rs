macro_rules! blake2_impl {
    (
        $state:ident, $fix_state:ident, $word:ident, $vec:ident, $bytes:ident,
        $block_size:ident, $R1:expr, $R2:expr, $R3:expr, $R4:expr, $IV:expr,
        $vardoc:expr, $doc:expr,
    ) => {
        use $crate::as_bytes::AsBytes;
        use $crate::error::Error;
        use $crate::simd::{$vec, Vector4};

        use core::{cmp, convert::TryInto, ops::Div};
        use crypto_mac::{InvalidKeyLength, Mac, NewMac};
        use digest::generic_array::typenum::{Unsigned, U4};
        use digest::generic_array::GenericArray;
        use digest::InvalidOutputSize;
        use digest::{BlockInput, FixedOutputDirty, Reset, Update, VariableOutputDirty};

        type Output = GenericArray<u8, $bytes>;

        #[derive(Clone)]
        #[doc=$vardoc]
        pub struct $state {
            m: [$word; 16],
            h: [$vec; 2],
            t: u64,
            n: usize,

            h0: [$vec; 2],
            m0: [$word; 16],
            t0: u64,

            rounds: u32,
        }

        // Can't make this a const, else it would be great as that.
        fn max_rounds() -> u32 {
            if $bytes::to_u8() == 64 {
                12
            } else {
                10
            }
        }

        #[inline(always)]
        fn iv0() -> $vec {
            $vec::new($IV[0], $IV[1], $IV[2], $IV[3])
        }
        #[inline(always)]
        fn iv1() -> $vec {
            $vec::new($IV[4], $IV[5], $IV[6], $IV[7])
        }

        #[inline(always)]
        fn quarter_round(v: &mut [$vec; 4], rd: u32, rb: u32, m: $vec) {
            v[0] = v[0].wrapping_add(v[1]).wrapping_add(m.from_le());
            v[3] = (v[3] ^ v[0]).rotate_right_const(rd);
            v[2] = v[2].wrapping_add(v[3]);
            v[1] = (v[1] ^ v[2]).rotate_right_const(rb);
        }

        #[inline(always)]
        fn shuffle(v: &mut [$vec; 4]) {
            v[1] = v[1].shuffle_left_1();
            v[2] = v[2].shuffle_left_2();
            v[3] = v[3].shuffle_left_3();
        }

        #[inline(always)]
        fn unshuffle(v: &mut [$vec; 4]) {
            v[1] = v[1].shuffle_right_1();
            v[2] = v[2].shuffle_right_2();
            v[3] = v[3].shuffle_right_3();
        }

        #[inline(always)]
        fn round(v: &mut [$vec; 4], m: &[$word; 16], s: &[usize; 16]) {
            quarter_round(v, $R1, $R2, $vec::gather(m, s[0], s[2], s[4], s[6]));
            quarter_round(v, $R3, $R4, $vec::gather(m, s[1], s[3], s[5], s[7]));

            shuffle(v);
            quarter_round(v, $R1, $R2, $vec::gather(m, s[8], s[10], s[12], s[14]));
            quarter_round(v, $R3, $R4, $vec::gather(m, s[9], s[11], s[13], s[15]));
            unshuffle(v);
        }

        impl $state {
            /// Creates a new hashing context with a key.
            ///
            /// **WARNING!** If you plan to use it for variable output MAC, then
            /// make sure to compare codes in constant time! It can be done
            /// for example by using `subtle` crate.
            pub fn new_keyed(key: &[u8], output_size: usize) -> Self {
                Self::with_params(key, &[], &[], output_size)
            }

            /// Creates a new hashing context with the full set of sequential-mode parameters.
            pub fn with_params(
                key: &[u8],
                salt: &[u8],
                persona: &[u8],
                output_size: usize,
            ) -> Self {
                let kk = key.len();
                assert!(kk <= $bytes::to_usize());
                assert!(output_size <= $bytes::to_usize());

                // The number of bytes needed to express two words.
                let length = $bytes::to_usize() / 4;
                assert!(salt.len() <= length);
                assert!(persona.len() <= length);

                // Build a parameter block
                let mut p = [0 as $word; 8];
                p[0] = 0x0101_0000 ^ ((kk as $word) << 8) ^ (output_size as $word);

                // salt is two words long
                if salt.len() < length {
                    let mut padded_salt =
                        GenericArray::<u8, <$bytes as Div<U4>>::Output>::default();
                    for i in 0..salt.len() {
                        padded_salt[i] = salt[i];
                    }
                    p[4] = $word::from_le_bytes(padded_salt[0..length / 2].try_into().unwrap());
                    p[5] = $word::from_le_bytes(
                        padded_salt[length / 2..padded_salt.len()].try_into().unwrap(),
                    );
                } else {
                    p[4] = $word::from_le_bytes(salt[0..salt.len() / 2].try_into().unwrap());
                    p[5] =
                        $word::from_le_bytes(salt[salt.len() / 2..salt.len()].try_into().unwrap());
                }

                // persona is also two words long
                if persona.len() < length {
                    let mut padded_persona =
                        GenericArray::<u8, <$bytes as Div<U4>>::Output>::default();
                    for i in 0..persona.len() {
                        padded_persona[i] = persona[i];
                    }
                    p[6] = $word::from_le_bytes(padded_persona[0..length / 2].try_into().unwrap());
                    p[7] = $word::from_le_bytes(
                        padded_persona[length / 2..padded_persona.len()].try_into().unwrap(),
                    );
                } else {
                    p[6] = $word::from_le_bytes(persona[0..length / 2].try_into().unwrap());
                    p[7] = $word::from_le_bytes(
                        persona[length / 2..persona.len()].try_into().unwrap(),
                    );
                }

                let mut state = Self::with_parameter_block(&p);

                if kk > 0 {
                    copy(key, state.m.as_mut_bytes());
                    state.t = 2 * $bytes::to_u64();
                }

                state.t0 = state.t;
                state.m0 = state.m;
                state
            }

            #[doc(hidden)]
            pub fn with_parameter_block(p: &[$word; 8]) -> Self {
                let nn = p[0] as u8 as usize;
                let kk = (p[0] >> 8) as u8 as usize;
                assert!(nn >= 1 && nn <= $bytes::to_usize());
                assert!(kk <= $bytes::to_usize());

                let h0 = [
                    iv0() ^ $vec::new(p[0], p[1], p[2], p[3]),
                    iv1() ^ $vec::new(p[4], p[5], p[6], p[7]),
                ];

                $state {
                    m: [0; 16],
                    h: h0,
                    t: 0,
                    n: nn,

                    t0: 0,
                    m0: [0; 16],
                    h0,

                    rounds: max_rounds(),
                }
            }

            /// Constructs a new hashing context with a given state.
            ///
            /// This enables continued hashing of a pre-hashed state.
            ///
            /// **Warning**: The user of this method is responsible for the
            /// initialization of the vectors for the first round.
            pub fn with_state(rounds: u32, state: [$word; 8], t: u64) -> Result<Self, Error> {
                if rounds > 12 {
                    return Err(Error::TooManyRounds { max: max_rounds(), actual: rounds });
                }

                let h0 = [
                    $vec::new(state[0], state[1], state[2], state[3]),
                    $vec::new(state[4], state[5], state[6], state[7]),
                ];
                let nn = $bytes::to_u8() as usize;

                Ok($state { m: [0; 16], h: h0, t, n: nn, t0: t, m0: [0; 16], h0, rounds })
            }

            /// Updates the hashing context with more data.
            pub fn update_inner(&mut self, data: &[u8]) -> Result<(), Error> {
                let mut rest = data;

                let block = 2 * $bytes::to_usize();

                let off = self.t as usize % block;
                if off != 0 || self.t == 0 {
                    let len = cmp::min(block - off, rest.len());

                    let part = &rest[..len];
                    rest = &rest[part.len()..];

                    copy(part, &mut self.m.as_mut_bytes()[off..]);
                    self.t = match self.t.checked_add(part.len() as u64) {
                        Some(v) => v,
                        None => return Err(Error::HashDataOverflow),
                    }
                }

                while rest.len() >= block {
                    self.compress(0, 0);

                    let part = &rest[..block];
                    rest = &rest[part.len()..];

                    copy(part, &mut self.m.as_mut_bytes());
                    self.t = match self.t.checked_add(part.len() as u64) {
                        Some(v) => v,
                        None => return Err(Error::HashDataOverflow),
                    }
                }

                let n = rest.len();
                if n > 0 {
                    self.compress(0, 0);

                    copy(rest, &mut self.m.as_mut_bytes());
                    self.t = match self.t.checked_add(rest.len() as u64) {
                        Some(v) => v,
                        None => return Err(Error::HashDataOverflow),
                    }
                }

                Ok(())
            }

            #[doc(hidden)]
            pub fn finalize_last_node(mut self) -> Output {
                self.finalize_with_flag(!0)
            }

            fn finalize_with_flag(&mut self, f1: $word) -> Output {
                let off = self.t as usize % (2 * $bytes::to_usize());
                if off != 0 {
                    self.m.as_mut_bytes()[off..].iter_mut().for_each(|b| *b = 0);
                }

                self.compress(!0, f1);

                self.output()
            }

            /// Compression `F` function.
            pub fn compress(&mut self, f0: $word, f1: $word) {
                use $crate::consts::SIGMA;

                let m = &self.m;
                let h = &mut self.h;

                let t0 = self.t as $word;
                let t1 = match $bytes::to_u8() {
                    64 => 0,
                    32 => (self.t >> 32) as $word,
                    _ => unreachable!(),
                };

                let mut v = [h[0], h[1], iv0(), iv1() ^ $vec::new(t0, t1, f0, f1)];

                for x in 1..=self.rounds {
                    let x = if x > 10 { x - 11 } else { x - 1 };
                    round(&mut v, &m, &SIGMA[x as usize]);
                }

                h[0] = h[0] ^ (v[0] ^ v[2]);
                h[1] = h[1] ^ (v[1] ^ v[3]);
            }

            /// Returns the current count value `t`.
            pub fn counter(&self) -> u64 {
                self.t
            }

            /// Returns the current hashed state.
            pub fn output(&self) -> Output {
                let buf = [self.h[0].to_le(), self.h[1].to_le()];

                let mut out = GenericArray::default();
                copy(buf.as_bytes(), &mut out);
                out
            }
        }

        impl Default for $state {
            fn default() -> Self {
                Self::new_keyed(&[], $bytes::to_usize())
            }
        }

        impl BlockInput for $state {
            type BlockSize = $block_size;
        }

        impl Update for $state {
            fn update(&mut self, data: impl AsRef<[u8]>) {
                self.update_inner(data.as_ref()).unwrap();
            }
        }

        impl VariableOutputDirty for $state {
            fn new(output_size: usize) -> Result<Self, InvalidOutputSize> {
                if output_size == 0 || output_size > $bytes::to_usize() {
                    return Err(InvalidOutputSize);
                }
                Ok(Self::new_keyed(&[], output_size))
            }

            fn output_size(&self) -> usize {
                self.n
            }

            fn finalize_variable_dirty(&mut self, f: impl FnOnce(&[u8])) {
                let n = self.n;
                let res = self.finalize_with_flag(0);
                f(&res[..n]);
            }
        }

        impl Reset for $state {
            fn reset(&mut self) {
                self.t = self.t0;
                self.m = self.m0;
                self.h = self.h0;
            }
        }

        opaque_debug::implement!($state);
        digest::impl_write!($state);

        #[derive(Clone)]
        #[doc=$doc]
        pub struct $fix_state {
            state: $state,
        }

        impl $fix_state {
            /// Creates a new hashing context with the full set of sequential-mode parameters.
            pub fn with_params(key: &[u8], salt: &[u8], persona: &[u8]) -> Self {
                let state = $state::with_params(key, salt, persona, $bytes::to_usize());
                Self { state }
            }
        }

        impl Default for $fix_state {
            fn default() -> Self {
                let state = $state::new_keyed(&[], $bytes::to_usize());
                Self { state }
            }
        }

        impl BlockInput for $fix_state {
            type BlockSize = $block_size;
        }

        impl Update for $fix_state {
            fn update(&mut self, data: impl AsRef<[u8]>) {
                self.state.update_inner(data.as_ref()).unwrap();
            }
        }

        impl FixedOutputDirty for $fix_state {
            type OutputSize = $bytes;

            fn finalize_into_dirty(&mut self, out: &mut Output) {
                out.copy_from_slice(&self.state.finalize_with_flag(0));
            }
        }

        impl Reset for $fix_state {
            fn reset(&mut self) {
                self.state.reset()
            }
        }

        impl NewMac for $fix_state {
            type KeySize = $bytes;

            fn new(key: &GenericArray<u8, $bytes>) -> Self {
                let state = $state::new_keyed(key, $bytes::to_usize());
                Self { state }
            }

            fn new_varkey(key: &[u8]) -> Result<Self, InvalidKeyLength> {
                if key.len() > $bytes::to_usize() {
                    Err(InvalidKeyLength)
                } else {
                    let state = $state::new_keyed(key, $bytes::to_usize());
                    Ok(Self { state })
                }
            }
        }

        impl Mac for $fix_state {
            type OutputSize = $bytes;

            fn update(&mut self, data: &[u8]) {
                self.state.update_inner(data).unwrap();
            }

            fn reset(&mut self) {
                <Self as Reset>::reset(self)
            }

            fn finalize(mut self) -> crypto_mac::Output<Self> {
                crypto_mac::Output::new(self.state.finalize_with_flag(0))
            }
        }

        opaque_debug::implement!($fix_state);
        digest::impl_write!($fix_state);

        fn copy(src: &[u8], dst: &mut [u8]) {
            assert!(dst.len() >= src.len());
            unsafe {
                core::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), src.len());
            }
        }
    };
}
