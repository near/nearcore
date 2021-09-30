#[macro_use]
extern crate bencher;

use bencher::Bencher;

use near_network::ibf_peer_set::SlotMapId;
use near_network::ibf_set::IbfSet;

#[allow(dead_code)]
fn test_measure_adding_edges_to_ibf(bench: &mut Bencher) {
    bench.iter(|| {
        let mut a = IbfSet::<u64>::new(12);
        for i in 0..40 * 8 * 3 {
            a.add_edge(&(i as u64), (i + 1000000) as SlotMapId);
        }
    });
}

benchmark_group!(benches, test_measure_adding_edges_to_ibf);

benchmark_main!(benches);
