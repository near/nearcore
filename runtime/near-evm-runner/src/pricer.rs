// Below is part of Open Ethereum:
// Copyright 2015-2020 Parity Technologies (UK) Ltd.

// Open Ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Open Ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Open Ethereum.  If not, see <http://www.gnu.org/licenses/>.

use ethereum_types::U256;
use std::convert::TryInto;
use std::{
    cmp::{max, min},
    io::{self, Read},
};

const SCALAR_BYTE_LENGTH: usize = 32;
const SERIALIZED_FP_BYTE_LENGTH: usize = 64;
const SERIALIZED_G1_POINT_BYTE_LENGTH: usize = SERIALIZED_FP_BYTE_LENGTH * 2;
const SERIALIZED_FP2_BYTE_LENGTH: usize = SERIALIZED_FP_BYTE_LENGTH * 2;
const SERIALIZED_G2_POINT_BYTE_LENGTH: usize = SERIALIZED_FP2_BYTE_LENGTH * 2;

/// A gas pricing scheme for built-in contracts.
pub trait Pricer: Send + Sync {
    /// The gas cost of running this built-in for the given input data at block number `at`
    fn cost(&self, input: &[u8]) -> U256;
}

/// Pricing for the Blake2 compression function (aka "F").
/// Computes the price as a fixed cost per round where the number of rounds is part of the input
/// byte slice.
pub type Blake2FPricer = u64;

impl Pricer for Blake2FPricer {
    fn cost(&self, input: &[u8]) -> U256 {
        const FOUR: usize = std::mem::size_of::<u32>();
        // Returning zero if the conversion fails is fine because `execute()` will check the length
        // and bail with the appropriate error.
        if input.len() < FOUR {
            return U256::zero();
        }
        let (rounds_bytes, _) = input.split_at(FOUR);
        let rounds = u32::from_be_bytes(rounds_bytes.try_into().unwrap_or([0u8; FOUR]));
        U256::from(*self as u128 * rounds as u128)
    }
}

/// Pricing model
#[allow(dead_code)]
#[derive(Debug)]
pub enum Pricing {
    AltBn128Pairing(AltBn128PairingPricer),
    AltBn128ConstOperations(AltBn128ConstOperations),
    Blake2F(Blake2FPricer),
    Linear(Linear),
    Modexp(ModexpPricer),
    Bls12Pairing(Bls12PairingPricer),
    Bls12ConstOperations(Bls12ConstOperations),
    Bls12MultiexpG1(Bls12MultiexpPricerG1),
    Bls12MultiexpG2(Bls12MultiexpPricerG2),
}

impl Pricer for Pricing {
    fn cost(&self, input: &[u8]) -> U256 {
        match self {
            Pricing::AltBn128Pairing(inner) => inner.cost(input),
            Pricing::AltBn128ConstOperations(inner) => inner.cost(input),
            Pricing::Blake2F(inner) => inner.cost(input),
            Pricing::Linear(inner) => inner.cost(input),
            Pricing::Modexp(inner) => inner.cost(input),
            Pricing::Bls12Pairing(inner) => inner.cost(input),
            Pricing::Bls12ConstOperations(inner) => inner.cost(input),
            Pricing::Bls12MultiexpG1(inner) => inner.cost(input),
            Pricing::Bls12MultiexpG2(inner) => inner.cost(input),
        }
    }
}

/// A linear pricing model. This computes a price using a base cost and a cost per-word.
#[derive(Debug)]
pub struct Linear {
    pub base: u64,
    pub word: u64,
}

/// A special pricing model for modular exponentiation.
#[derive(Debug)]
pub struct ModexpPricer {
    pub divisor: u64,
}

impl Pricer for Linear {
    fn cost(&self, input: &[u8]) -> U256 {
        U256::from(self.base) + U256::from(self.word) * U256::from((input.len() + 31) / 32)
    }
}

/// alt_bn128 pairing price
#[derive(Debug, Copy, Clone)]
pub struct AltBn128PairingPrice {
    pub base: u64,
    pub pair: u64,
}

/// alt_bn128_pairing pricing model. This computes a price using a base cost and a cost per pair.
#[derive(Debug)]
pub struct AltBn128PairingPricer {
    pub price: AltBn128PairingPrice,
}

/// Pricing for constant alt_bn128 operations (ECADD and ECMUL)
#[derive(Debug, Copy, Clone)]
pub struct AltBn128ConstOperations {
    /// Fixed price.
    pub price: u64,
}

impl Pricer for AltBn128ConstOperations {
    fn cost(&self, _input: &[u8]) -> U256 {
        self.price.into()
    }
}

impl Pricer for AltBn128PairingPricer {
    fn cost(&self, input: &[u8]) -> U256 {
        U256::from(self.price.base) + U256::from(self.price.pair) * U256::from(input.len() / 192)
    }
}

impl Pricer for ModexpPricer {
    fn cost(&self, input: &[u8]) -> U256 {
        let mut reader = input.chain(io::repeat(0));
        let mut buf = [0; 32];

        // read lengths as U256 here for accurate gas calculation.
        let mut read_len = || {
            reader
                .read_exact(&mut buf[..])
                .expect("reading from zero-extended memory cannot fail; qed");
            U256::from_big_endian(&buf[..])
        };
        let base_len = read_len();
        let exp_len = read_len();
        let mod_len = read_len();

        if mod_len.is_zero() && base_len.is_zero() {
            return U256::zero();
        }

        let max_len = U256::from(u32::max_value() / 2);
        if base_len > max_len || mod_len > max_len || exp_len > max_len {
            return U256::max_value();
        }
        let (base_len, exp_len, mod_len) =
            (base_len.low_u64(), exp_len.low_u64(), mod_len.low_u64());

        let m = max(mod_len, base_len);
        // read fist 32-byte word of the exponent.
        let exp_low = if base_len + 96 >= input.len() as u64 {
            U256::zero()
        } else {
            buf.iter_mut().for_each(|b| *b = 0);
            let mut reader = input[(96 + base_len as usize)..].chain(io::repeat(0));
            let len = min(exp_len, 32) as usize;
            reader
                .read_exact(&mut buf[(32 - len)..])
                .expect("reading from zero-extended memory cannot fail; qed");
            U256::from_big_endian(&buf[..])
        };

        let adjusted_exp_len = Self::adjusted_exp_len(exp_len, exp_low);

        let (gas, overflow) = Self::mult_complexity(m).overflowing_mul(max(adjusted_exp_len, 1));
        if overflow {
            return U256::max_value();
        }
        (gas / self.divisor as u64).into()
    }
}

impl ModexpPricer {
    fn adjusted_exp_len(len: u64, exp_low: U256) -> u64 {
        let bit_index = if exp_low.is_zero() { 0 } else { (255 - exp_low.leading_zeros()) as u64 };
        if len <= 32 {
            bit_index
        } else {
            8 * (len - 32) + bit_index
        }
    }

    fn mult_complexity(x: u64) -> u64 {
        match x {
            x if x <= 64 => x * x,
            x if x <= 1024 => (x * x) / 4 + 96 * x - 3072,
            x => (x * x) / 16 + 480 * x - 199_680,
        }
    }
}

/// Bls12 pairing price
#[derive(Debug, Copy, Clone)]
pub struct Bls12PairingPrice {
    pub base: u64,
    pub pair: u64,
}

/// bls12_pairing pricing model. This computes a price using a base cost and a cost per pair.
#[derive(Debug)]
pub struct Bls12PairingPricer {
    pub price: Bls12PairingPrice,
}

/// Pricing for constant Bls12 operations (ADD and MUL in G1 and G2, as well as mappings)
#[derive(Debug, Copy, Clone)]
pub struct Bls12ConstOperations {
    /// Fixed price.
    pub price: u64,
}

/// Discount table for multiexponentiation (Peppinger algorithm)
/// Later on is normalized using the divisor
pub const BLS12_MULTIEXP_DISCOUNTS_TABLE: [[u64; 2]; BLS12_MULTIEXP_PAIRS_FOR_MAX_DISCOUNT] = [
    [1, 1200],
    [2, 888],
    [3, 764],
    [4, 641],
    [5, 594],
    [6, 547],
    [7, 500],
    [8, 453],
    [9, 438],
    [10, 423],
    [11, 408],
    [12, 394],
    [13, 379],
    [14, 364],
    [15, 349],
    [16, 334],
    [17, 330],
    [18, 326],
    [19, 322],
    [20, 318],
    [21, 314],
    [22, 310],
    [23, 306],
    [24, 302],
    [25, 298],
    [26, 294],
    [27, 289],
    [28, 285],
    [29, 281],
    [30, 277],
    [31, 273],
    [32, 269],
    [33, 268],
    [34, 266],
    [35, 265],
    [36, 263],
    [37, 262],
    [38, 260],
    [39, 259],
    [40, 257],
    [41, 256],
    [42, 254],
    [43, 253],
    [44, 251],
    [45, 250],
    [46, 248],
    [47, 247],
    [48, 245],
    [49, 244],
    [50, 242],
    [51, 241],
    [52, 239],
    [53, 238],
    [54, 236],
    [55, 235],
    [56, 233],
    [57, 232],
    [58, 231],
    [59, 229],
    [60, 228],
    [61, 226],
    [62, 225],
    [63, 223],
    [64, 222],
    [65, 221],
    [66, 220],
    [67, 219],
    [68, 219],
    [69, 218],
    [70, 217],
    [71, 216],
    [72, 216],
    [73, 215],
    [74, 214],
    [75, 213],
    [76, 213],
    [77, 212],
    [78, 211],
    [79, 211],
    [80, 210],
    [81, 209],
    [82, 208],
    [83, 208],
    [84, 207],
    [85, 206],
    [86, 205],
    [87, 205],
    [88, 204],
    [89, 203],
    [90, 202],
    [91, 202],
    [92, 201],
    [93, 200],
    [94, 199],
    [95, 199],
    [96, 198],
    [97, 197],
    [98, 196],
    [99, 196],
    [100, 195],
    [101, 194],
    [102, 193],
    [103, 193],
    [104, 192],
    [105, 191],
    [106, 191],
    [107, 190],
    [108, 189],
    [109, 188],
    [110, 188],
    [111, 187],
    [112, 186],
    [113, 185],
    [114, 185],
    [115, 184],
    [116, 183],
    [117, 182],
    [118, 182],
    [119, 181],
    [120, 180],
    [121, 179],
    [122, 179],
    [123, 178],
    [124, 177],
    [125, 176],
    [126, 176],
    [127, 175],
    [128, 174],
];

/// Max discount allowed
pub const BLS12_MULTIEXP_MAX_DISCOUNT: u64 = 174;
/// Max discount is reached at this number of pairs
pub const BLS12_MULTIEXP_PAIRS_FOR_MAX_DISCOUNT: usize = 128;
/// Divisor for discounts table
pub const BLS12_MULTIEXP_DISCOUNT_DIVISOR: u64 = 1000;
/// Length of single G1 + G2 points pair for pairing operation
pub const BLS12_G1_AND_G2_PAIR_LEN: usize =
    SERIALIZED_G1_POINT_BYTE_LENGTH + SERIALIZED_G2_POINT_BYTE_LENGTH;

/// Marter trait for length of input per one pair (point + scalar)
pub trait PointScalarLength: Copy + Clone + std::fmt::Debug + Send + Sync {
    /// Length itself
    const LENGTH: usize;
}
/// Marker trait that indicated that we perform operations in G1
#[derive(Clone, Copy, Debug)]
pub struct G1Marker;
impl PointScalarLength for G1Marker {
    const LENGTH: usize = SERIALIZED_G1_POINT_BYTE_LENGTH + SCALAR_BYTE_LENGTH;
}
/// Marker trait that indicated that we perform operations in G2
#[derive(Clone, Copy, Debug)]
pub struct G2Marker;
impl PointScalarLength for G2Marker {
    const LENGTH: usize = SERIALIZED_G2_POINT_BYTE_LENGTH + SCALAR_BYTE_LENGTH;
}

/// Pricing for constant Bls12 operations (ADD and MUL in G1 and G2, as well as mappings)
#[derive(Debug, Copy, Clone)]
pub struct Bls12MultiexpPricer<P: PointScalarLength> {
    /// Base const of the operation (G1 or G2 multiplication)
    pub base_price: Bls12ConstOperations,

    _marker: std::marker::PhantomData<P>,
}

impl Pricer for Bls12ConstOperations {
    fn cost(&self, _input: &[u8]) -> U256 {
        self.price.into()
    }
}

impl Pricer for Bls12PairingPricer {
    fn cost(&self, input: &[u8]) -> U256 {
        U256::from(self.price.base)
            + U256::from(self.price.pair) * U256::from(input.len() / BLS12_G1_AND_G2_PAIR_LEN)
    }
}

impl<P: PointScalarLength> Pricer for Bls12MultiexpPricer<P> {
    fn cost(&self, input: &[u8]) -> U256 {
        let num_pairs = input.len() / P::LENGTH;
        if num_pairs == 0 {
            return U256::zero();
        }
        let discount = if num_pairs > BLS12_MULTIEXP_PAIRS_FOR_MAX_DISCOUNT {
            BLS12_MULTIEXP_MAX_DISCOUNT
        } else {
            let table_entry = BLS12_MULTIEXP_DISCOUNTS_TABLE[num_pairs - 1];
            table_entry[1]
        };
        U256::from(self.base_price.price) * U256::from(num_pairs) * U256::from(discount)
            / U256::from(BLS12_MULTIEXP_DISCOUNT_DIVISOR)
    }
}

/// Multiexp pricer in G1
pub type Bls12MultiexpPricerG1 = Bls12MultiexpPricer<G1Marker>;

/// Multiexp pricer in G2
pub type Bls12MultiexpPricerG2 = Bls12MultiexpPricer<G2Marker>;
