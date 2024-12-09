use bytesize::ByteSize;
use rand::seq::SliceRandom;
use rand::Rng;
use rand_chacha::ChaCha20Rng;

const MAX_RECEIPT_SIZE: u64 = 4 * 1024 * 1024;

/// Get a random receipt size for testing.
/// The sizes are sampled from a reasonable distribution where most receipts are small,
/// some are medium sized, and a few are large.
/// See `RandomReceiptSizeGenerator` for the exact implementation.
pub fn get_random_receipt_size_for_test(rng: &mut ChaCha20Rng) -> u64 {
    RandomReceiptSizeGenerator.generate_receipt_size(rng).as_u64()
}

/// Objects with this trait are responsible for generating receipt sizes that are used in testing.
/// Each implementation generates different distribution of sizes.
pub trait ReceiptSizeGenerator: std::fmt::Debug {
    fn generate_receipt_size(&mut self, rng: &mut ChaCha20Rng) -> ByteSize;
}

/// Generates small receipt sizes (< 4kB)
#[derive(Debug)]
pub struct SmallReceiptSizeGenerator;

impl ReceiptSizeGenerator for SmallReceiptSizeGenerator {
    fn generate_receipt_size(&mut self, rng: &mut ChaCha20Rng) -> ByteSize {
        ByteSize::b(rng.gen_range(200..4_000))
    }
}

/// Generates medium receipt sizes.
#[derive(Debug)]
pub struct MediumReceiptSizeGenerator;

impl ReceiptSizeGenerator for MediumReceiptSizeGenerator {
    fn generate_receipt_size(&mut self, rng: &mut ChaCha20Rng) -> ByteSize {
        ByteSize::b(rng.gen_range(4_000..300_000))
    }
}

/// Generates large receipt sizes (> 300kB)
#[derive(Debug)]
pub struct LargeReceiptSizeGenerator;

impl ReceiptSizeGenerator for LargeReceiptSizeGenerator {
    fn generate_receipt_size(&mut self, rng: &mut ChaCha20Rng) -> ByteSize {
        ByteSize::b(rng.gen_range(300_000..=MAX_RECEIPT_SIZE))
    }
}

/// Always generates a maximum size receipt.
#[derive(Debug)]
pub struct MaxReceiptSizeGenerator;

impl ReceiptSizeGenerator for MaxReceiptSizeGenerator {
    fn generate_receipt_size(&mut self, _rng: &mut ChaCha20Rng) -> ByteSize {
        ByteSize::b(MAX_RECEIPT_SIZE)
    }
}

/// Generates random receipt sizes from a reasonable distribution.
/// Most receipts are small, some are medium sized, and a few are large.
/// This is more similar to the real-life distribution of receipts.
/// And small receipts are more interesting than large ones, we need
/// a lot of small receipts to properly test receipt groups.
/// Naive sampling of sizes between 0 and 4MB would result in 97.5% of receipts
/// being larger than 100kB, which is undesirable.
#[derive(Debug)]
pub struct RandomReceiptSizeGenerator;

impl ReceiptSizeGenerator for RandomReceiptSizeGenerator {
    fn generate_receipt_size(&mut self, rng: &mut ChaCha20Rng) -> ByteSize {
        let weighted_sizes = [
            (SmallReceiptSizeGenerator.generate_receipt_size(rng), 70), // 70% of receipts are small
            (MediumReceiptSizeGenerator.generate_receipt_size(rng), 20), // 20% of receipts are medium
            (LargeReceiptSizeGenerator.generate_receipt_size(rng), 8),   // 8% of receipts are large
            (MaxReceiptSizeGenerator.generate_receipt_size(rng), 2), // 2% of receipts are max size
        ];
        weighted_sizes.choose_weighted(rng, |item| item.1).unwrap().0
    }
}
