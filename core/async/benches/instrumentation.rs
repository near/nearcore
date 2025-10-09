use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::sync::Arc;
use time::Duration;

use near_async::instrumentation::{InstrumentedThread, InstrumentedThreadWriter, WINDOW_SIZE_NS};
use near_time::FakeClock;

const NUM_WINDOWS: usize = 1000;
const MESSAGE_TYPES: [&str; 10] = [
    "Message0", "Message1", "Message2", "Message3", "Message4", "Message5", "Message6", "Message7",
    "Message8", "Message9",
];

struct BenchSetup {
    clock: FakeClock,
    writer: InstrumentedThreadWriter,
}

impl BenchSetup {
    fn new() -> Self {
        let fake_clock = FakeClock::default();
        let clock = fake_clock.clock();
        let reference_instant = fake_clock.now();

        let target = Arc::new(InstrumentedThread::new(
            "bench_thread".to_string(),
            "bench_actor".to_string(),
            0,
        ));

        let writer = InstrumentedThreadWriter::new(clock, reference_instant, target);

        Self { clock: fake_clock, writer }
    }
}

fn benchmark_writer_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("writer_overhead");

    // Test different numbers of events per window
    for events_per_window in [1, 10, 100] {
        group.bench_with_input(
            format!("num_windows={}/events_per_window={}", NUM_WINDOWS, events_per_window),
            &events_per_window,
            |b, &events_per_window| {
                b.iter(|| {
                    let BenchSetup { clock, mut writer, .. } = BenchSetup::new();

                    for _window in 0..NUM_WINDOWS {
                        // Generate events for this window
                        for event in 0..events_per_window {
                            let message_type = MESSAGE_TYPES[event % MESSAGE_TYPES.len()];
                            writer.start_event(black_box(message_type), black_box(1_000_000));
                            clock.advance(Duration::microseconds(1));
                            writer.end_event(black_box(message_type));
                        }

                        clock.advance(Duration::nanoseconds(WINDOW_SIZE_NS as i64));
                        writer.advance_window_if_needed();
                    }
                });
            },
        );
    }

    group.finish();
}

fn benchmark_message_type_registration(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_type_registration");

    // Test the overhead of registering new message types
    group.bench_function("register_10_types", |b| {
        b.iter(|| {
            let BenchSetup { mut writer, .. } = BenchSetup::new();

            // Register all message types by using them
            for message_type in MESSAGE_TYPES.iter() {
                writer.start_event(black_box(message_type), black_box(1_000_000));
                writer.end_event(black_box(message_type));
            }
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_writer_overhead, benchmark_message_type_registration);
criterion_main!(benches);
