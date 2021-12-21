#[macro_use]
extern crate bencher;

use bencher::Bencher;
use lru::LruCache;
use near_cache::SyncLruCache;

fn bench_lru(bench: &mut Bencher) {
    bench.iter(|| {
        let mut cache = LruCache::new(10000);
        for _x in 0..1000000 {
            let a = rand::random::<u64>();
            let b = rand::random::<u64>();
            cache.put(a, b);
        }
    });
}

fn bench_lru_cache(bench: &mut Bencher) {
    bench.iter(|| {
        let cache = SyncLruCache::new(10000);
        for _x in 0..1000000 {
            let a = rand::random::<u64>();
            let b = rand::random::<u64>();
            cache.put(a, b);
        }
    });
}

benchmark_group!(benches, bench_lru, bench_lru_cache);

benchmark_main!(benches);

// test bench_sized_cache ... bench:   6,709,170 ns/iter (+/- 3,536,860)
// test bench_lru         ... bench:  35,469,761 ns/iter (+/- 1,045,064)
// test bench_sized_cache ... bench:  47,299,971 ns/iter (+/- 1,446,543)

// LruCache with `cached`
// test bench_lru_cache   ... bench:  51,420,781 ns/iter (+/- 912,557)

// LruCache with `lru`
// test bench_lru_cache   ... bench:  40,837,052 ns/iter (+/- 747,426)
