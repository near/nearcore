use std::{
    cmp::{max, min},
    io::{self, Cursor, Read},
    mem::size_of,
};

use crate::pricer::{
    AltBn128PairingPrice, AltBn128PairingPricer, Bls12ConstOperations, Linear, ModexpPricer,
    Pricer, Pricing,
};
use crate::utils::ecrecover_address;
use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};

use ethereum_types::{Address, U256};
use near_runtime_fees::EvmPrecompileCostConfig;
use num_bigint::BigUint;
use num_traits::{FromPrimitive, One, ToPrimitive, Zero};
use parity_bytes::BytesRef;
use vm::{MessageCallResult, ReturnData};

#[derive(Primitive)]
enum Precompile {
    EcRecover = 1,
    Sha256 = 2,
    Ripemd160 = 3,
    Identity = 4,
    ModexpImpl = 5,
    Bn128AddImpl = 6,
    Bn128MulImpl = 7,
    Bn128PairingImpl = 8,
    Blake2FImpl = 9,
    LastPrecompile = 10,
}

pub fn is_precompile(addr: &Address) -> bool {
    *addr < Address::from_low_u64_be(Precompile::LastPrecompile.to_u64().unwrap())
}

pub fn precompile(id: u64) -> Result<Box<dyn Impl>, String> {
    Ok(match Precompile::from_u64(id) {
        Some(Precompile::EcRecover) => Box::new(EcRecover) as Box<dyn Impl>,
        Some(Precompile::Sha256) => Box::new(Sha256) as Box<dyn Impl>,
        Some(Precompile::Ripemd160) => Box::new(Ripemd160) as Box<dyn Impl>,
        Some(Precompile::Identity) => Box::new(Identity) as Box<dyn Impl>,
        Some(Precompile::ModexpImpl) => Box::new(ModexpImpl) as Box<dyn Impl>,
        Some(Precompile::Bn128AddImpl) => Box::new(Bn128AddImpl) as Box<dyn Impl>,
        Some(Precompile::Bn128MulImpl) => Box::new(Bn128MulImpl) as Box<dyn Impl>,
        Some(Precompile::Bn128PairingImpl) => Box::new(Bn128PairingImpl) as Box<dyn Impl>,
        Some(Precompile::Blake2FImpl) => Box::new(Blake2FImpl) as Box<dyn Impl>,
        _ => return Err(format!("Invalid builtin ID: {}", id)),
    })
}

pub fn process_precompile(
    addr: &Address,
    input: &[u8],
    gas: &U256,
    precompile_costs: &EvmPrecompileCostConfig,
) -> MessageCallResult {
    let f = match precompile(addr.to_low_u64_be()) {
        Ok(f) => f,
        Err(_) => return MessageCallResult::Failed,
    };
    let mut bytes = vec![];
    let mut output = parity_bytes::BytesRef::Flexible(&mut bytes);
    let cost = f.gas(input, precompile_costs);

    if cost > *gas {
        return MessageCallResult::Failed;
    }

    // mutates bytes
    match f.execute(input, &mut output) {
        Ok(()) => {}
        Err(_) => return MessageCallResult::Failed,
    };
    let size = bytes.len();

    MessageCallResult::Success(*gas - cost, ReturnData::new(bytes, 0, size))
}

/** the following is copied from ethcore/src/builtin.rs **/

// Copyright 2015-2019 Parity Technologies (UK) Ltd.
// This file is part of Parity Ethereum.

// Parity Ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Parity Ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Parity Ethereum.  If not, see <http://www.gnu.org/licenses/>.

/// Execution error.
#[derive(Debug)]
pub struct Error(pub &'static str);

impl From<&'static str> for Error {
    fn from(val: &'static str) -> Self {
        Error(val)
    }
}

impl Into<vm::Error> for Error {
    fn into(self) -> ::vm::Error {
        vm::Error::BuiltIn(self.0)
    }
}
#[derive(Debug)]
struct EcRecover;

#[derive(Debug)]
struct Sha256;

#[derive(Debug)]
struct Ripemd160;

#[derive(Debug)]
struct Identity;

#[derive(Debug)]
struct ModexpImpl;

#[derive(Debug)]
struct Bn128AddImpl;

#[derive(Debug)]
struct Bn128MulImpl;

#[derive(Debug)]
struct Bn128PairingImpl;

#[derive(Debug)]
pub struct Blake2FImpl;

/// Native implementation of a built-in contract.
pub trait Impl: Send + Sync {
    /// execute this built-in on the given input, writing to the given output.
    fn execute(&self, input: &[u8], output: &mut BytesRef) -> Result<(), Error>;
    // how many evm gas will cost
    fn gas(&self, _input: &[u8], _evm_gas_config: &EvmPrecompileCostConfig) -> U256;
}

impl Impl for Identity {
    fn execute(&self, input: &[u8], output: &mut BytesRef) -> Result<(), Error> {
        output.write(0, input);
        Ok(())
    }
    fn gas(&self, input: &[u8], precompile_costs: &EvmPrecompileCostConfig) -> U256 {
        Pricing::Linear(Linear {
            base: precompile_costs.identity_cost.base,
            word: precompile_costs.identity_cost.word,
        })
        .cost(input)
    }
}

impl Impl for EcRecover {
    fn execute(&self, i: &[u8], output: &mut BytesRef) -> Result<(), Error> {
        let len = min(i.len(), 128);

        let mut input = [0; 128];
        input[..len].copy_from_slice(&i[..len]);
        let mut hash = [0; 32];
        hash.copy_from_slice(&input[..32]);
        let mut signature = [0; 65];
        signature.copy_from_slice(&input[63..]);

        let result = ecrecover_address(&hash, &signature);
        output.write(0, &[0, 12]);
        output.write(12, &result.0);

        Ok(())
    }

    fn gas(&self, input: &[u8], precompile_costs: &EvmPrecompileCostConfig) -> U256 {
        Pricing::Linear(Linear {
            base: precompile_costs.ecrecover_cost.base,
            word: precompile_costs.ecrecover_cost.word,
        })
        .cost(input)
    }
}

impl Impl for Sha256 {
    fn execute(&self, input: &[u8], output: &mut BytesRef) -> Result<(), Error> {
        use sha2::Digest;
        let d = sha2::Sha256::digest(input);
        output.write(0, &*d);
        Ok(())
    }

    fn gas(&self, input: &[u8], precompile_costs: &EvmPrecompileCostConfig) -> U256 {
        Pricing::Linear(Linear {
            base: precompile_costs.sha256_cost.base,
            word: precompile_costs.sha256_cost.word,
        })
        .cost(input)
    }
}

impl Impl for Ripemd160 {
    fn execute(&self, input: &[u8], output: &mut BytesRef) -> Result<(), Error> {
        use ripemd160::Digest;
        let hash = ripemd160::Ripemd160::digest(input);
        output.write(0, &[0; 12][..]);
        output.write(12, &hash);
        Ok(())
    }

    fn gas(&self, input: &[u8], precompile_costs: &EvmPrecompileCostConfig) -> U256 {
        Pricing::Linear(Linear {
            base: precompile_costs.ripemd160_cost.base,
            word: precompile_costs.ripemd160_cost.word,
        })
        .cost(input)
    }
}

// calculate modexp: left-to-right binary exponentiation to keep multiplicands lower
fn modexp(mut base: BigUint, exp: Vec<u8>, modulus: BigUint) -> BigUint {
    const BITS_PER_DIGIT: usize = 8;

    // n^m % 0 || n^m % 1
    if modulus <= BigUint::one() {
        return BigUint::zero();
    }

    // normalize exponent
    let mut exp = exp.into_iter().skip_while(|d| *d == 0).peekable();

    // n^0 % m
    if exp.peek().is_none() {
        return BigUint::one();
    }

    // 0^n % m, n > 0
    if base.is_zero() {
        return BigUint::zero();
    }

    base %= &modulus;

    // Fast path for base divisible by modulus.
    if base.is_zero() {
        return BigUint::zero();
    }

    // Left-to-right binary exponentiation (Handbook of Applied Cryptography - Algorithm 14.79).
    // http://www.cacr.math.uwaterloo.ca/hac/about/chap14.pdf
    let mut result = BigUint::one();

    for digit in exp {
        let mut mask = 1 << (BITS_PER_DIGIT - 1);

        for _ in 0..BITS_PER_DIGIT {
            result = &result * &result % &modulus;

            if digit & mask > 0 {
                result = result * &base % &modulus;
            }

            mask >>= 1;
        }
    }

    result
}

impl Impl for ModexpImpl {
    fn execute(&self, input: &[u8], output: &mut BytesRef) -> Result<(), Error> {
        let mut reader = input.chain(io::repeat(0));
        let mut buf = [0; 32];

        // read lengths as usize.
        // ignoring the first 24 bytes might technically lead us to fall out of consensus,
        // but so would running out of addressable memory!
        let mut read_len = |reader: &mut io::Chain<&[u8], io::Repeat>| {
            reader
                .read_exact(&mut buf[..])
                .expect("reading from zero-extended memory cannot fail; qed");
            BigEndian::read_u64(&buf[24..]) as usize
        };

        let base_len = read_len(&mut reader);
        let exp_len = read_len(&mut reader);
        let mod_len = read_len(&mut reader);

        // Gas formula allows arbitrary large exp_len when base and modulus are empty, so we need to handle empty base first.
        let r = if base_len == 0 && mod_len == 0 {
            BigUint::zero()
        } else {
            // read the numbers themselves.
            let mut buf = vec![0; max(mod_len, max(base_len, exp_len))];
            let mut read_num = |reader: &mut io::Chain<&[u8], io::Repeat>, len: usize| {
                reader
                    .read_exact(&mut buf[..len])
                    .expect("reading from zero-extended memory cannot fail; qed");
                BigUint::from_bytes_be(&buf[..len])
            };

            let base = read_num(&mut reader, base_len);

            let mut exp_buf = vec![0; exp_len];
            reader
                .read_exact(&mut exp_buf[..exp_len])
                .expect("reading from zero-extended memory cannot fail; qed");

            let modulus = read_num(&mut reader, mod_len);

            modexp(base, exp_buf, modulus)
        };

        // write output to given memory, left padded and same length as the modulus.
        let bytes = r.to_bytes_be();

        // always true except in the case of zero-length modulus, which leads to
        // output of length and value 1.
        if bytes.len() <= mod_len {
            let res_start = mod_len - bytes.len();
            output.write(res_start, &bytes);
        }

        Ok(())
    }

    fn gas(&self, input: &[u8], precompile_costs: &EvmPrecompileCostConfig) -> U256 {
        Pricing::Modexp(ModexpPricer { divisor: precompile_costs.modexp_cost.divisor }).cost(input)
    }
}

fn read_fr(reader: &mut io::Chain<&[u8], io::Repeat>) -> Result<::bn::Fr, Error> {
    let mut buf = [0u8; 32];

    reader.read_exact(&mut buf[..]).expect("reading from zero-extended memory cannot fail; qed");
    ::bn::Fr::from_slice(&buf[0..32]).map_err(|_| Error::from("Invalid field element"))
}

fn read_point(reader: &mut io::Chain<&[u8], io::Repeat>) -> Result<::bn::G1, Error> {
    use bn::{AffineG1, Fq, Group, G1};

    let mut buf = [0u8; 32];

    reader.read_exact(&mut buf[..]).expect("reading from zero-extended memory cannot fail; qed");
    let px = Fq::from_slice(&buf[0..32]).map_err(|_| Error::from("Invalid point x coordinate"))?;

    reader.read_exact(&mut buf[..]).expect("reading from zero-extended memory cannot fail; qed");
    let py = Fq::from_slice(&buf[0..32]).map_err(|_| Error::from("Invalid point y coordinate"))?;
    Ok(if px == Fq::zero() && py == Fq::zero() {
        G1::zero()
    } else {
        AffineG1::new(px, py).map_err(|_| Error::from("Invalid curve point"))?.into()
    })
}

impl Impl for Bn128AddImpl {
    // Can fail if any of the 2 points does not belong the bn128 curve
    fn execute(&self, input: &[u8], output: &mut BytesRef) -> Result<(), Error> {
        use bn::AffineG1;

        let mut padded_input = input.chain(io::repeat(0));
        let p1 = read_point(&mut padded_input)?;
        let p2 = read_point(&mut padded_input)?;

        let mut write_buf = [0u8; 64];
        if let Some(sum) = AffineG1::from_jacobian(p1 + p2) {
            // point not at infinity
            sum.x()
                .to_big_endian(&mut write_buf[0..32])
                .expect("Cannot fail since 0..32 is 32-byte length");
            sum.y()
                .to_big_endian(&mut write_buf[32..64])
                .expect("Cannot fail since 32..64 is 32-byte length");
        }
        output.write(0, &write_buf);

        Ok(())
    }

    fn gas(&self, input: &[u8], precompile_costs: &EvmPrecompileCostConfig) -> U256 {
        Pricing::Bls12ConstOperations(Bls12ConstOperations {
            price: precompile_costs.bn128_add_cost.price,
        })
        .cost(input)
    }
}

impl Impl for Bn128MulImpl {
    // Can fail if first paramter (bn128 curve point) does not actually belong to the curve
    fn execute(&self, input: &[u8], output: &mut BytesRef) -> Result<(), Error> {
        use bn::AffineG1;

        let mut padded_input = input.chain(io::repeat(0));
        let p = read_point(&mut padded_input)?;
        let fr = read_fr(&mut padded_input)?;

        let mut write_buf = [0u8; 64];
        if let Some(sum) = AffineG1::from_jacobian(p * fr) {
            // point not at infinity
            sum.x()
                .to_big_endian(&mut write_buf[0..32])
                .expect("Cannot fail since 0..32 is 32-byte length");
            sum.y()
                .to_big_endian(&mut write_buf[32..64])
                .expect("Cannot fail since 32..64 is 32-byte length");
        }
        output.write(0, &write_buf);
        Ok(())
    }

    fn gas(&self, input: &[u8], precompile_costs: &EvmPrecompileCostConfig) -> U256 {
        Pricing::Bls12ConstOperations(Bls12ConstOperations {
            price: precompile_costs.bn128_mul_cost.price,
        })
        .cost(input)
    }
}

impl Impl for Bn128PairingImpl {
    /// Can fail if:
    ///     - input length is not a multiple of 192
    ///     - any of odd points does not belong to bn128 curve
    ///     - any of even points does not belong to the twisted bn128 curve over the field F_p^2 = F_p[i] / (i^2 + 1)
    fn execute(&self, input: &[u8], output: &mut BytesRef) -> Result<(), Error> {
        if input.len() % 192 != 0 {
            return Err("Invalid input length, must be multiple of 192 (3 * (32*2))".into());
        }

        self.execute_with_error(input, output)
    }

    fn gas(&self, input: &[u8], precompile_costs: &EvmPrecompileCostConfig) -> U256 {
        Pricing::AltBn128Pairing(AltBn128PairingPricer {
            price: AltBn128PairingPrice {
                base: precompile_costs.bn128_pairing_cost.base,
                pair: precompile_costs.bn128_pairing_cost.pair,
            },
        })
        .cost(input)
    }
}

impl Bn128PairingImpl {
    fn execute_with_error(&self, input: &[u8], output: &mut BytesRef) -> Result<(), Error> {
        use bn::{pairing, AffineG1, AffineG2, Fq, Fq2, Group, Gt, G1, G2};

        let elements = input.len() / 192; // (a, b_a, b_b - each 64-byte affine coordinates)
        let ret_val = if elements == 0 {
            U256::one()
        } else {
            let mut vals = Vec::new();
            for idx in 0..elements {
                let a_x = Fq::from_slice(&input[idx * 192..idx * 192 + 32])
                    .map_err(|_| Error::from("Invalid a argument x coordinate"))?;

                let a_y = Fq::from_slice(&input[idx * 192 + 32..idx * 192 + 64])
                    .map_err(|_| Error::from("Invalid a argument y coordinate"))?;

                let b_a_y = Fq::from_slice(&input[idx * 192 + 64..idx * 192 + 96])
                    .map_err(|_| Error::from("Invalid b argument imaginary coeff x coordinate"))?;

                let b_a_x = Fq::from_slice(&input[idx * 192 + 96..idx * 192 + 128])
                    .map_err(|_| Error::from("Invalid b argument imaginary coeff y coordinate"))?;

                let b_b_y = Fq::from_slice(&input[idx * 192 + 128..idx * 192 + 160])
                    .map_err(|_| Error::from("Invalid b argument real coeff x coordinate"))?;

                let b_b_x = Fq::from_slice(&input[idx * 192 + 160..idx * 192 + 192])
                    .map_err(|_| Error::from("Invalid b argument real coeff y coordinate"))?;

                let b_a = Fq2::new(b_a_x, b_a_y);
                let b_b = Fq2::new(b_b_x, b_b_y);
                let b = if b_a.is_zero() && b_b.is_zero() {
                    G2::zero()
                } else {
                    G2::from(
                        AffineG2::new(b_a, b_b)
                            .map_err(|_| Error::from("Invalid b argument - not on curve"))?,
                    )
                };
                let a = if a_x.is_zero() && a_y.is_zero() {
                    G1::zero()
                } else {
                    G1::from(
                        AffineG1::new(a_x, a_y)
                            .map_err(|_| Error::from("Invalid a argument - not on curve"))?,
                    )
                };
                vals.push((a, b));
            }

            let mul = vals.into_iter().fold(Gt::one(), |s, (a, b)| s * pairing(a, b));

            if mul == Gt::one() {
                U256::one()
            } else {
                U256::zero()
            }
        };

        let mut buf = [0u8; 32];
        ret_val.to_big_endian(&mut buf);
        output.write(0, &buf);

        Ok(())
    }
}

/// The precomputed values for BLAKE2b [from the spec](https://tools.ietf.org/html/rfc7693#section-2.7)
/// There are 10 16-byte arrays - one for each round
/// the entries are calculated from the sigma constants.
const SIGMA: [[usize; 16]; 10] = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
    [11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4],
    [7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8],
    [9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13],
    [2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9],
    [12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11],
    [13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10],
    [6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5],
    [10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0],
];

/// IV is the initialization vector for BLAKE2b. See https://tools.ietf.org/html/rfc7693#section-2.6
/// for details.
const IV: [u64; 8] = [
    0x6a09e667f3bcc908,
    0xbb67ae8584caa73b,
    0x3c6ef372fe94f82b,
    0xa54ff53a5f1d36f1,
    0x510e527fade682d1,
    0x9b05688c2b3e6c1f,
    0x1f83d9abfb41bd6b,
    0x5be0cd19137e2179,
];

#[inline(always)]
#[allow(clippy::many_single_char_names)]
fn g(v: &mut [u64], a: usize, b: usize, c: usize, d: usize, x: u64, y: u64) {
    v[a] = v[a].wrapping_add(v[b]).wrapping_add(x);
    v[d] = (v[d] ^ v[a]).rotate_right(32);
    v[c] = v[c].wrapping_add(v[d]);
    v[b] = (v[b] ^ v[c]).rotate_right(24);

    v[a] = v[a].wrapping_add(v[b]).wrapping_add(y);
    v[d] = (v[d] ^ v[a]).rotate_right(16);
    v[c] = v[c].wrapping_add(v[d]);
    v[b] = (v[b] ^ v[c]).rotate_right(63);
}

/// The Blake2b compression function F. See https://tools.ietf.org/html/rfc7693#section-3.2
/// Takes as an argument the state vector `h`, message block vector `m`, offset counter `t`, final
/// block indicator flag `f`, and number of rounds `rounds`. The state vector provided as the first
/// parameter is modified by the function.
#[allow(clippy::many_single_char_names)]
pub fn compress(h: &mut [u64; 8], m: [u64; 16], t: [u64; 2], f: bool, rounds: usize) {
    let mut v = [0u64; 16];
    v[..8].copy_from_slice(h); // First half from state.
    v[8..].copy_from_slice(&IV); // Second half from IV.

    v[12] ^= t[0];
    v[13] ^= t[1];

    if f {
        v[14] = !v[14]; // Invert all bits if the last-block-flag is set.
    }

    for i in 0..rounds {
        // Message word selection permutation for this round.
        let s = &SIGMA[i % 10];
        g(&mut v, 0, 4, 8, 12, m[s[0]], m[s[1]]);
        g(&mut v, 1, 5, 9, 13, m[s[2]], m[s[3]]);
        g(&mut v, 2, 6, 10, 14, m[s[4]], m[s[5]]);
        g(&mut v, 3, 7, 11, 15, m[s[6]], m[s[7]]);

        g(&mut v, 0, 5, 10, 15, m[s[8]], m[s[9]]);
        g(&mut v, 1, 6, 11, 12, m[s[10]], m[s[11]]);
        g(&mut v, 2, 7, 8, 13, m[s[12]], m[s[13]]);
        g(&mut v, 3, 4, 9, 14, m[s[14]], m[s[15]]);
    }

    for i in 0..8 {
        h[i] ^= v[i] ^ v[i + 8];
    }
}

impl Impl for Blake2FImpl {
    /// Format of `input`:
    /// [4 bytes for rounds][64 bytes for h][128 bytes for m][8 bytes for t_0][8 bytes for t_1][1 byte for f]
    fn execute(&self, input: &[u8], output: &mut BytesRef) -> Result<(), Error> {
        const BLAKE2_F_ARG_LEN: usize = 213;
        const PROOF: &str = "Checked the length of the input above; qed";

        if input.len() != BLAKE2_F_ARG_LEN {
            return Err(Error("input length for Blake2 F precompile should be exactly 213 bytes"));
        }

        let mut cursor = Cursor::new(input);
        let rounds = cursor.read_u32::<BigEndian>().expect(PROOF);

        // state vector, h
        let mut h = [0u64; 8];
        for state_word in &mut h {
            *state_word = cursor.read_u64::<byteorder::LittleEndian>().expect(PROOF);
        }

        // message block vector, m
        let mut m = [0u64; 16];
        for msg_word in &mut m {
            *msg_word = cursor.read_u64::<LittleEndian>().expect(PROOF);
        }

        // 2w-bit offset counter, t
        let t = [
            cursor.read_u64::<LittleEndian>().expect(PROOF),
            cursor.read_u64::<LittleEndian>().expect(PROOF),
        ];

        // final block indicator flag, "f"
        let f = match input.last() {
            Some(1) => true,
            Some(0) => false,
            _ => {
                return Err(Error("incorrect final block indicator flag"));
            }
        };

        compress(&mut h, m, t, f, rounds as usize);

        let mut output_buf = [0u8; 8 * size_of::<u64>()];
        for (i, state_word) in h.iter().enumerate() {
            output_buf[i * 8..(i + 1) * 8].copy_from_slice(&state_word.to_le_bytes());
        }
        output.write(0, &output_buf[..]);
        Ok(())
    }

    fn gas(&self, input: &[u8], precompile_costs: &EvmPrecompileCostConfig) -> U256 {
        Pricing::Blake2F(precompile_costs.blake2f_cost).cost(input)
    }
}
