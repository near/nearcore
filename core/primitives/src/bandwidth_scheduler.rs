use std::collections::BTreeMap;
use std::num::NonZeroU64;

use bitvec::order::Lsb0;
use bitvec::slice::BitSlice;
use borsh::{BorshDeserialize, BorshSerialize};
use near_parameters::RuntimeConfig;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{ProtocolVersion, ShardId};
use near_primitives_core::version::ProtocolFeature;
use near_schema_checker_lib::ProtocolSchema;

/// Represents size of receipts, in the context of cross-shard bandwidth, in bytes.
/// TODO(bandwidth_scheduler) - consider using ByteSize
pub type Bandwidth = u64;

/// A list of shard's bandwidth requests.
/// Describes how much the shard would like to send to other shards.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    ProtocolSchema,
)]
pub enum BandwidthRequests {
    V1(BandwidthRequestsV1),
}

impl BandwidthRequests {
    pub fn empty() -> BandwidthRequests {
        BandwidthRequests::V1(BandwidthRequestsV1 { requests: Vec::new() })
    }

    pub fn default_for_protocol_version(
        protocol_version: ProtocolVersion,
    ) -> Option<BandwidthRequests> {
        if ProtocolFeature::BandwidthScheduler.enabled(protocol_version) {
            Some(BandwidthRequests::empty())
        } else {
            None
        }
    }
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Default,
    Debug,
    Clone,
    PartialEq,
    Eq,
    ProtocolSchema,
)]
pub struct BandwidthRequestsV1 {
    pub requests: Vec<BandwidthRequest>,
}

/// `BandwidthRequest` describes the size of receipts that a shard would like to send to another shard.
/// When a shard wants to send a lot of receipts to another shard, it needs to create a request and wait
/// for a bandwidth grant from the bandwidth scheduler.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    ProtocolSchema,
)]
pub struct BandwidthRequest {
    /// Requesting bandwidth to this shard.
    pub to_shard: u16,
    /// Bitmap which describes what values of bandwidth are requested.
    pub requested_values_bitmap: BandwidthRequestBitmap,
}

impl BandwidthRequest {
    /// Creates a bandwidth request based on the sizes of receipts in the outgoing buffer.
    /// Returns None when a request is not needed (receipt size below base bandwidth).
    pub fn make_from_receipt_sizes<E>(
        to_shard: ShardId,
        receipt_sizes: impl Iterator<Item = Result<u64, E>>,
        params: &BandwidthSchedulerParams,
    ) -> Result<Option<BandwidthRequest>, E> {
        let values = BandwidthRequestValues::new(params).values;
        let mut bitmap = BandwidthRequestBitmap::new();

        // For every receipt find out how much bandwidth would be needed to send out
        // all the receipts up to this one. Then find the value that is at least as
        // large as the required bandwidth and request it in the request bitmap.
        let mut total_size: u64 = 0;
        let mut cur_value_idx: usize = 0;
        for receipt_size_res in receipt_sizes {
            let receipt_size = receipt_size_res?;
            total_size = total_size.checked_add(receipt_size).expect(
                "Total size of receipts doesn't fit in u64, are there exabytes of receipts?",
            );

            if total_size <= params.base_bandwidth {
                continue;
            }

            // Find a value that is at least as big as the total_size
            while cur_value_idx < values.len() && values[cur_value_idx] < total_size {
                cur_value_idx += 1;
            }

            if cur_value_idx == values.len() {
                // There is no value to request this much, stop the loop.
                break;
            }

            // Request the value that is at least as large as total_size
            bitmap.set_bit(cur_value_idx, true);
        }

        if bitmap.is_all_zeros() {
            // No point in making a bandwidth request that doesn't request anything
            return Ok(None);
        }

        Ok(Some(BandwidthRequest { to_shard: to_shard.into(), requested_values_bitmap: bitmap }))
    }

    /// Create a basic bandwidth request when receipt sizes are not available.
    /// It'll request a single value - max_receipt_size. Bandwidth scheduler will
    /// grant all the bandwidth to one of the shards that requests max_receipt_size.
    /// The resulting behaviour will be similar to the previous approach with allowed shard.
    /// It is used only during the protocol upgrade while the outgoing buffer metadata
    /// is not built for receipts that were buffered before the upgrade.
    pub fn make_max_receipt_size_request(
        to_shard: ShardId,
        params: &BandwidthSchedulerParams,
    ) -> BandwidthRequest {
        let mut bitmap = BandwidthRequestBitmap::new();
        let values = BandwidthRequestValues::new(params).values;

        let max_receipt_size_value_pos = values
            .iter()
            .position(|&value| value == params.max_receipt_size)
            .expect("max_receipt_size should be in the values list");
        bitmap.set_bit(max_receipt_size_value_pos, true);

        BandwidthRequest { to_shard: to_shard.into(), requested_values_bitmap: bitmap }
    }
}

/// There are this many predefined values of bandwidth that can be requested in a BandwidthRequest.
pub const BANDWIDTH_REQUEST_VALUES_NUM: usize = 40;

/// Values of bandwidth that can be requested in a bandwidth request.
/// When the nth bit is set in a request bitmap, it means that a shard is requesting the nth value from this list.
/// The list is sorted, from smallest to largest values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BandwidthRequestValues {
    pub values: [Bandwidth; BANDWIDTH_REQUEST_VALUES_NUM],
}

/// Performs linear interpolation between min and max.
/// interpolate(100, 200, 0, 10) = 100
/// interpolate(100, 200, 5, 10) = 150
/// interpolate(100, 200, 10, 10) = 200
fn interpolate(min: u64, max: u64, i: u64, n: u64) -> u64 {
    min + (max - min) * i / n
}

impl BandwidthRequestValues {
    pub fn new(params: &BandwidthSchedulerParams) -> BandwidthRequestValues {
        // values[-1] = base_bandwidth
        // values[values.len() - 1] = max_shard_bandwidth
        // values[i] = linear interpolation between values[-1] and values[values.len() - 1]
        // TODO(bandwidth_scheduler) - consider using exponential interpolation.
        let mut values = [0; BANDWIDTH_REQUEST_VALUES_NUM];

        let values_len: u64 =
            values.len().try_into().expect("Converting usize to u64 shouldn't fail");
        for i in 0..values.len() {
            let i_u64: u64 = i.try_into().expect("Converting usize to u64 shouldn't fail");

            values[i] = interpolate(
                params.base_bandwidth,
                params.max_shard_bandwidth,
                i_u64 + 1,
                values_len,
            );
        }

        // The value that is closest to max_receipt_size is set to max_receipt_size.
        // Without this we could end up in a situation where one shard wants to send
        // a maximum size receipt to another and requests the corresponding value from the list,
        // but the sum of this value and base bandwidth exceeds max_shard_bandwidth.
        // There's a guarantee that (num_shards - 1) * base_bandwidth + max_receipt_size <= max_shard_bandwidth,
        // but there's no guarantee that (num_shards - 1) * base_bandwidth + request_value <= max_shard_bandwidth.
        let mut closest_to_max: Bandwidth = 0;
        for value in &values {
            if value.abs_diff(params.max_receipt_size)
                < closest_to_max.abs_diff(params.max_receipt_size)
            {
                closest_to_max = *value;
            }
        }
        for value in values.iter_mut() {
            if *value == closest_to_max {
                *value = params.max_receipt_size;
            }
        }

        BandwidthRequestValues { values }
    }
}

/// Bitmap which describes which values from the predefined list are being requested.
/// The nth bit is set to 1 when the nth value from the list is being requested.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    PartialEq,
    Eq,
    ProtocolSchema,
)]
pub struct BandwidthRequestBitmap {
    pub data: [u8; BANDWIDTH_REQUEST_BITMAP_SIZE],
}

pub const BANDWIDTH_REQUEST_BITMAP_SIZE: usize = BANDWIDTH_REQUEST_VALUES_NUM / 8;
const _: () = assert!(
    BANDWIDTH_REQUEST_VALUES_NUM % 8 == 0,
    "Every bit in the bitmap should be used. It's wasteful to have unused bits.
    And having unused bits would require extra validation logic"
);

impl std::fmt::Debug for BandwidthRequestBitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BandwidthRequestBitmap(")?;
        for i in 0..self.len() {
            if self.get_bit(i) {
                write!(f, "1")?;
            } else {
                write!(f, "0")?;
            }
        }
        write!(f, ")")
    }
}

impl BandwidthRequestBitmap {
    pub fn new() -> BandwidthRequestBitmap {
        BandwidthRequestBitmap { data: [0u8; BANDWIDTH_REQUEST_BITMAP_SIZE] }
    }

    pub fn get_bit(&self, idx: usize) -> bool {
        assert!(idx < self.len());

        let bit_slice = BitSlice::<_, Lsb0>::from_slice(self.data.as_slice());
        *bit_slice.get(idx).unwrap()
    }

    pub fn set_bit(&mut self, idx: usize, val: bool) {
        assert!(idx < self.len());

        let bit_slice = BitSlice::<_, Lsb0>::from_slice_mut(self.data.as_mut_slice());
        bit_slice.set(idx, val);
    }

    pub fn len(&self) -> usize {
        BANDWIDTH_REQUEST_VALUES_NUM
    }

    pub fn is_all_zeros(&self) -> bool {
        self.data == [0u8; BANDWIDTH_REQUEST_BITMAP_SIZE]
    }
}

/// `BandwidthRequests` from all chunks in a block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockBandwidthRequests {
    /// For every shard - all the bandwidth requests generated by this shard.
    pub shards_bandwidth_requests: BTreeMap<ShardId, BandwidthRequests>,
}

impl BlockBandwidthRequests {
    pub fn empty() -> BlockBandwidthRequests {
        BlockBandwidthRequests { shards_bandwidth_requests: BTreeMap::new() }
    }
}

/// Persistent state used by the bandwidth scheduler.
/// It is kept in the shard trie.
/// The state should be the same on all shards. All shards start with the same state
/// and apply the same bandwidth scheduler algorithm at the same heights, so the resulting
/// scheduler state stays the same.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, ProtocolSchema)]
pub enum BandwidthSchedulerState {
    V1(BandwidthSchedulerStateV1),
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, ProtocolSchema)]
pub struct BandwidthSchedulerStateV1 {
    /// Allowance for every pair of (sender, receiver). Used in the scheduler algorithm.
    /// Bandwidth scheduler updates the allowances on every run.
    pub link_allowances: Vec<LinkAllowance>,
    /// Sanity check hash to assert that all shards run bandwidth scheduler in the exact same way.
    /// Hash of previous scheduler state and (some) scheduler inputs.
    pub sanity_check_hash: CryptoHash,
}

/// Allowance for a (sender, receiver) pair of shards.
/// Used in bandwidth scheduler.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, ProtocolSchema)]
pub struct LinkAllowance {
    /// Sender shard
    pub sender: ShardId,
    /// Receiver shard
    pub receiver: ShardId,
    /// Link allowance, determines priority for granting bandwidth.
    /// See the bandwidth scheduler module-level comment for a more
    /// detailed description.
    pub allowance: Bandwidth,
}

/// Parameters used in the bandwidth scheduler algorithm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BandwidthSchedulerParams {
    /// This much bandwidth is granted by default.
    pub base_bandwidth: Bandwidth,
    /// The maximum amount of data that a shard can send or receive at a single height.
    pub max_shard_bandwidth: Bandwidth,
    /// Maximum size of a single receipt.
    pub max_receipt_size: Bandwidth,
    /// Maximum bandwidth allowance that a link can accumulate.
    pub max_allowance: Bandwidth,
}

impl BandwidthSchedulerParams {
    /// Calculate values of scheduler params based on the current configuration
    pub fn new(num_shards: NonZeroU64, runtime_config: &RuntimeConfig) -> BandwidthSchedulerParams {
        // TODO(bandwidth_scheduler) - put these parameters in RuntimeConfig.
        let max_shard_bandwidth: Bandwidth = 4_500_000;
        let max_allowance = max_shard_bandwidth;
        let max_base_bandwidth = 100_000;

        let max_receipt_size = runtime_config.wasm_config.limit_config.max_receipt_size;

        if max_shard_bandwidth < max_receipt_size {
            panic!(
                "max_shard_bandwidth is smaller than max_receipt_size! ({} < {}).
                Invalid configuration - it'll be impossible to send a maximum size receipt.",
                max_shard_bandwidth, max_receipt_size
            );
        }

        // A receipt with maximum size must still be able to get through
        // after base bandwidth is granted to everyone. We have to ensure that:
        // base_bandwidth * (num_shards - 1) + max_receipt_size <= max_shard_bandwidth
        let available_bandwidth = max_shard_bandwidth - max_receipt_size;
        let mut base_bandwidth = available_bandwidth / std::cmp::max(1, num_shards.get() - 1);
        if base_bandwidth > max_base_bandwidth {
            base_bandwidth = max_base_bandwidth;
        }

        BandwidthSchedulerParams {
            base_bandwidth,
            max_shard_bandwidth,
            max_receipt_size,
            max_allowance,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::ops::Deref;
    use std::sync::Arc;

    use near_parameters::RuntimeConfig;
    use rand::{Rng, SeedableRng};

    use crate::bandwidth_scheduler::{interpolate, BANDWIDTH_REQUEST_VALUES_NUM};
    use crate::shard_layout::ShardUId;

    use super::{
        BandwidthRequest, BandwidthRequestBitmap, BandwidthRequestValues, BandwidthSchedulerParams,
    };
    use rand_chacha::ChaCha20Rng;

    fn make_runtime_config(max_receipt_size: u64) -> RuntimeConfig {
        let mut runtime_config = RuntimeConfig::test();

        // wasm_config is in Arc, need to clone, modify and set new Arc to modify parameter
        let mut wasm_config = runtime_config.wasm_config.deref().clone();
        wasm_config.limit_config.max_receipt_size = max_receipt_size;
        runtime_config.wasm_config = Arc::new(wasm_config);

        runtime_config
    }

    /// Ensure that a maximum size receipt can still be sent after granting everyone
    /// base bandwidth without going over the max_shard_bandwidth limit.
    fn assert_max_size_can_get_through(params: &BandwidthSchedulerParams, num_shards: u64) {
        assert!(
            (num_shards - 1) * params.base_bandwidth + params.max_receipt_size
                <= params.max_shard_bandwidth
        )
    }

    #[test]
    fn test_scheduler_params_one_shard() {
        let max_receipt_size = 4 * 1024 * 1024;
        let num_shards = 1;

        let runtime_config = make_runtime_config(max_receipt_size);
        let scheduler_params =
            BandwidthSchedulerParams::new(NonZeroU64::new(num_shards).unwrap(), &runtime_config);
        let expected = BandwidthSchedulerParams {
            base_bandwidth: 100_000,
            max_shard_bandwidth: 4_500_000,
            max_receipt_size,
            max_allowance: 4_500_000,
        };
        assert_eq!(scheduler_params, expected);
        assert_max_size_can_get_through(&scheduler_params, num_shards);
    }

    #[test]
    fn test_scheduler_params_six_shards() {
        let max_receipt_size = 4 * 1024 * 1024;
        let num_shards = 6;

        let runtime_config = make_runtime_config(max_receipt_size);
        let scheduler_params =
            BandwidthSchedulerParams::new(NonZeroU64::new(num_shards).unwrap(), &runtime_config);
        let expected = BandwidthSchedulerParams {
            base_bandwidth: (4_500_000 - max_receipt_size) / 5,
            max_shard_bandwidth: 4_500_000,
            max_receipt_size,
            max_allowance: 4_500_000,
        };
        assert_eq!(scheduler_params, expected);
        assert_max_size_can_get_through(&scheduler_params, num_shards);
    }

    /// max_receipt_size is larger than max_shard_bandwidth - incorrect configuration
    #[test]
    #[should_panic]
    fn test_scheduler_params_invalid_config() {
        let max_receipt_size = 40 * 1024 * 1024;
        let num_shards = 6;
        let runtime_config = make_runtime_config(max_receipt_size);
        BandwidthSchedulerParams::new(NonZeroU64::new(num_shards).unwrap(), &runtime_config);
    }

    #[test]
    fn test_bandwidth_request_bitmap() {
        let mut bitmap = BandwidthRequestBitmap::new();
        assert_eq!(bitmap.len(), BANDWIDTH_REQUEST_VALUES_NUM);

        let mut fake_bitmap = [false; BANDWIDTH_REQUEST_VALUES_NUM];
        let mut rng = ChaCha20Rng::from_seed([0u8; 32]);

        for _ in 0..(BANDWIDTH_REQUEST_VALUES_NUM * 5) {
            let random_index = rng.gen_range(0..BANDWIDTH_REQUEST_VALUES_NUM);
            let value = rng.gen_bool(0.5);

            bitmap.set_bit(random_index, value);
            fake_bitmap[random_index] = value;

            for i in 0..BANDWIDTH_REQUEST_VALUES_NUM {
                assert_eq!(bitmap.get_bit(i), fake_bitmap[i]);
            }
        }
    }

    #[test]
    fn test_bandwidth_request_values() {
        let max_receipt_size = 4 * 1024 * 1024;

        let params = BandwidthSchedulerParams::new(
            NonZeroU64::new(6).unwrap(),
            &make_runtime_config(max_receipt_size),
        );
        let values = BandwidthRequestValues::new(&params);

        assert!(values.values[0] > params.base_bandwidth);
        assert_eq!(values.values[BANDWIDTH_REQUEST_VALUES_NUM - 1], params.max_shard_bandwidth);
        assert!(values.values.contains(&max_receipt_size));

        assert_eq!(params.base_bandwidth, 61139);
        assert_eq!(
            values.values,
            [
                172110, 283082, 394053, 505025, 615996, 726968, 837939, 948911, 1059882, 1170854,
                1281825, 1392797, 1503768, 1614740, 1725711, 1836683, 1947654, 2058626, 2169597,
                2280569, 2391541, 2502512, 2613484, 2724455, 2835427, 2946398, 3057370, 3168341,
                3279313, 3390284, 3501256, 3612227, 3723199, 3834170, 3945142, 4056113, 4194304,
                4278056, 4389028, 4500000
            ]
        );
    }

    // Make a bandwidth request to shard 0 with a bitmap which has ones at the specified indices.
    fn make_request_with_ones(ones_indexes: &[usize]) -> BandwidthRequest {
        let mut req = BandwidthRequest {
            to_shard: ShardUId::single_shard().shard_id().into(),
            requested_values_bitmap: BandwidthRequestBitmap::new(),
        };
        for i in ones_indexes {
            req.requested_values_bitmap.set_bit(*i, true);
        }
        req
    }

    fn make_sizes_iter<'a>(
        sizes: &'a [u64],
    ) -> impl Iterator<Item = Result<u64, std::convert::Infallible>> + 'a {
        sizes.iter().map(|&size| Ok(size))
    }

    #[test]
    fn test_make_bandwidth_request_from_receipt_sizes() {
        let max_receipt_size = 4 * 1024 * 1024;
        let params = BandwidthSchedulerParams::new(
            NonZeroU64::new(6).unwrap(),
            &make_runtime_config(max_receipt_size),
        );
        let values = BandwidthRequestValues::new(&params).values;

        let get_request = |receipt_sizes: &[u64]| -> Option<BandwidthRequest> {
            BandwidthRequest::make_from_receipt_sizes(
                ShardUId::single_shard().shard_id(),
                make_sizes_iter(receipt_sizes),
                &params,
            )
            .unwrap()
        };

        // No receipts - no bandwidth request.
        assert_eq!(get_request(&[]), None);

        // Receipts with total size smaller than base_bandwidth don't need a bandwidth request.
        let below_base_bandwidth_receipts = [10_000, 20, 999, 2362, 3343, 232, 22];
        assert!(below_base_bandwidth_receipts.iter().sum::<u64>() < params.base_bandwidth);
        assert_eq!(get_request(&below_base_bandwidth_receipts), None);

        // Receipts with total size equal to base_bandwidth don't need a bandwidth_request
        let equal_to_base_bandwidth_receipts = [10_000, 20_000, params.base_bandwidth - 30_000];
        assert_eq!(equal_to_base_bandwidth_receipts.iter().sum::<u64>(), params.base_bandwidth);
        assert_eq!(get_request(&equal_to_base_bandwidth_receipts), None);

        // Receipts with total size barely larger than base_bandwidth need a bandwidth request.
        // Only the first bit in the bitmap should be set to 1.
        let above_base_bandwidth_receipts = [10_000, 20_000, params.base_bandwidth - 30_000, 1];
        assert_eq!(above_base_bandwidth_receipts.iter().sum::<u64>(), params.base_bandwidth + 1);
        assert_eq!(get_request(&above_base_bandwidth_receipts), Some(make_request_with_ones(&[0])));

        // A single receipt which is slightly larger than base_bandwidth needs a bandwidth request.
        let above_base_bandwidth_one_receipt = [params.base_bandwidth + 1];
        assert_eq!(
            get_request(&above_base_bandwidth_one_receipt),
            Some(make_request_with_ones(&[0]))
        );

        // When requesting bandwidth that is between two values on the list, the request
        // should ask for the first value that is bigger than the needed bandwidth.
        let inbetween_value = (values[values.len() / 2] + values[values.len() / 2 + 1]) / 2;
        assert!(!values.contains(&inbetween_value));
        let inbetween_size_receipt = [inbetween_value];
        assert_eq!(
            get_request(&inbetween_size_receipt),
            Some(make_request_with_ones(&[values.len() / 2 + 1]))
        );

        // A single max size receipt should have the corresponding value set to one.
        let max_size_receipt = [max_receipt_size];
        let max_size_receipt_value_idx =
            values.iter().position(|v| *v == max_receipt_size).unwrap();
        assert_eq!(
            get_request(&max_size_receipt),
            Some(make_request_with_ones(&[max_size_receipt_value_idx]))
        );

        // Two max size receipts should produce the same bandwidth request as one max size receipt.
        // 2 * max_size_receipt > max_shard_bandwidth, so it doesn't make sense to request more bandwidth.
        assert!(2 * params.max_receipt_size > params.max_shard_bandwidth);
        let two_max_size_receipts = [max_receipt_size, max_receipt_size];
        assert_eq!(
            get_request(&two_max_size_receipts),
            Some(make_request_with_ones(&[max_size_receipt_value_idx]))
        );

        // A ton of small receipts should cause all bits to be set to one.
        // 10_000 receipts, each with size 1000. More than a shard can send out at a single height.
        let lots_of_small_receipts: Vec<u64> = (0..10_000).into_iter().map(|_| 1_000).collect();
        assert!(lots_of_small_receipts.iter().sum::<u64>() > params.max_shard_bandwidth);
        let all_bitmap_indices: Vec<usize> = (0..BANDWIDTH_REQUEST_VALUES_NUM).collect();
        assert_eq!(
            get_request(&lots_of_small_receipts),
            Some(make_request_with_ones(&all_bitmap_indices))
        );
    }

    /// Generate random receipt sizes and create a bandwidth request from them.
    /// Compare the created bandwidth request with a request created using simpler logic.
    #[test]
    fn test_make_bandwidth_request_random() {
        let mut rng = ChaCha20Rng::from_seed([0u8; 32]);
        let max_receipt_size = 4 * 1024 * 1024;
        let params = BandwidthSchedulerParams::new(
            NonZeroU64::new(6).unwrap(),
            &make_runtime_config(max_receipt_size),
        );

        let min_receipt_size = 5_000;
        let max_receipts_num = params.max_shard_bandwidth / min_receipt_size * 3 / 2;

        for _test_idx in 0..100 {
            let num_receipts = rng.gen_range(0..=max_receipts_num);
            let receipt_sizes: Vec<u64> = (0..num_receipts)
                .map(|_| rng.gen_range(min_receipt_size..=max_receipt_size))
                .collect();

            let request = BandwidthRequest::make_from_receipt_sizes(
                ShardUId::single_shard().shard_id(),
                make_sizes_iter(&receipt_sizes),
                &params,
            )
            .unwrap();

            let expected_request =
                make_bandwidth_request_slow(receipt_sizes.iter().copied(), &params);
            assert_eq!(request, expected_request);
        }
    }

    /// A more naive implementation of bandwidth request generation.
    /// For every total_size find the value that is at least this large and request it.
    fn make_bandwidth_request_slow(
        receipt_sizes: impl Iterator<Item = u64>,
        params: &BandwidthSchedulerParams,
    ) -> Option<BandwidthRequest> {
        let mut request = BandwidthRequest {
            to_shard: ShardUId::single_shard().shard_id().into(),
            requested_values_bitmap: BandwidthRequestBitmap::new(),
        };
        let values = BandwidthRequestValues::new(params).values;

        let mut total_size = 0;
        for receipt_size in receipt_sizes {
            total_size += receipt_size;

            for i in 0..values.len() {
                if values[i] >= total_size {
                    request.requested_values_bitmap.set_bit(i, true);
                    break;
                }
            }
        }

        if request.requested_values_bitmap.is_all_zeros() {
            return None;
        }

        Some(request)
    }

    #[test]
    fn test_interpolate() {
        assert_eq!(interpolate(100, 200, 0, 10), 100);
        assert_eq!(interpolate(100, 200, 5, 10), 150);
        assert_eq!(interpolate(100, 200, 10, 10), 200);
    }
}
