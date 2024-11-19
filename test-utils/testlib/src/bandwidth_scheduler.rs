use rand::seq::SliceRandom;
use rand::Rng;

/// Generate a random receipt size for bandwidth scheduler testing.
/// Most receipts are small, some are medium sized, and a few are large.
/// This is more similar to the real-life distribution of receipts.
/// And small receipts are more interesting than large ones, we need
/// a lot of small receipts to properly test receipt groups.
/// Naive sampling of sizes between 0 and 4MB would result in 97.5% of receipts
/// being larger than 100kB, which is undesirable.
pub fn get_random_receipt_size_for_test(rng: &mut impl Rng) -> u64 {
    let min_receipt_size: u64 = 100;
    let max_receipt_size: u64 = 4 * 1024 * 1024;

    let small_receipt_size_range = min_receipt_size..4_000;
    let medium_receipt_size_range = 4_000..300_000;
    let large_receipt_size_range = 300_000..=max_receipt_size;

    let weighted_sizes = [
        (rng.gen_range(small_receipt_size_range), 70), // 70% of receipts are small
        (rng.gen_range(medium_receipt_size_range), 20), // 20% of receipts are medium
        (rng.gen_range(large_receipt_size_range), 5),  // 5% of receipts are large
        (max_receipt_size, 5),                         // 5% of receipts are max size
    ];
    weighted_sizes.choose_weighted(rng, |item| item.1).unwrap().0
}
