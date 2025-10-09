use std::sync::Arc;
use time::Duration;

use near_time::FakeClock;

use crate::instrumentation::NUM_WINDOWS;
use crate::instrumentation::queue::InstrumentedQueue;
use crate::instrumentation::writer::InstrumentedThreadWriterSharedPart;
use crate::instrumentation::{
    WINDOW_SIZE_NS, data::InstrumentedThread, writer::InstrumentedThreadWriter,
};

const TEST_WINDOW_SIZE_MS: i64 = WINDOW_SIZE_NS as i64 / 1_000_000; // More convenient for tests to use milliseconds

struct TestSetup {
    clock: FakeClock,
    reference_instant: near_time::Instant,
    target: Arc<InstrumentedThread>,
    writer: InstrumentedThreadWriter,
}

impl Default for TestSetup {
    fn default() -> Self {
        let fake_clock = FakeClock::default();
        let clock = fake_clock.clock();
        let reference_instant = fake_clock.now();

        let queue = InstrumentedQueue::new("test_actor");
        let target = Arc::new(InstrumentedThread::new(
            "test_thread".to_string(),
            "test_actor".to_string(),
            queue.clone(),
            0,
        ));
        let shared = InstrumentedThreadWriterSharedPart::new_for_test(
            "test_actor".to_string(),
            clock,
            reference_instant,
            queue,
        );

        let writer = shared.new_writer_for_test(target.clone());

        Self { clock: fake_clock, reference_instant, target, writer }
    }
}

#[test]
fn test_basic_event_tracking() {
    let TestSetup { clock, reference_instant, target, mut writer } = Default::default();

    // Record a simple event
    writer.start_event("TestMessage", 1_000_000); // 1ms dequeue time
    clock.advance(Duration::milliseconds(50)); // Process for 50ms
    writer.end_event("TestMessage");

    // Move time forward to complete the window
    clock.advance(Duration::milliseconds(TEST_WINDOW_SIZE_MS));
    writer.advance_window_if_needed();

    // Get the view and verify the event was recorded
    let view = target.to_view(clock.now().duration_since(reference_instant).as_nanos() as u64);

    assert_eq!(view.thread_name, "test_thread");
    assert_eq!(view.message_types.len(), 1);
    assert_eq!(view.message_types[0], "TestMessage");
    assert_eq!(view.windows.len(), 1);

    let window = &view.windows[0];
    assert_eq!(window.events.len(), 2); // start and end events
    assert!(!window.events_overfilled);

    // Check that the summary contains our message processing time
    assert!(!window.summary.message_stats_by_type.is_empty());
    let message_stats = &window.summary.message_stats_by_type[0];
    assert_eq!(message_stats.message_type, 0); // First message type gets ID 0
    assert_eq!(message_stats.count, 1);
    assert!(message_stats.total_time_ns >= 50_000_000); // At least 50ms

    // Check dequeue summary
    assert!(!window.dequeue_summary.message_stats_by_type.is_empty());
    let dequeue_stats = &window.dequeue_summary.message_stats_by_type[0];
    assert_eq!(dequeue_stats.message_type, 0);
    assert_eq!(dequeue_stats.count, 1);
    assert_eq!(dequeue_stats.total_time_ns, 1_000_000); // 1ms dequeue time
}

#[test]
fn test_multiple_message_types() {
    let TestSetup { clock, reference_instant, target, mut writer } = Default::default();

    // Record events for different message types
    writer.start_event("MessageA", 500_000);
    clock.advance(Duration::milliseconds(10));
    writer.end_event("MessageA");

    writer.start_event("MessageB", 750_000);
    clock.advance(Duration::milliseconds(20));
    writer.end_event("MessageB");

    writer.start_event("MessageA", 1_000_000);
    clock.advance(Duration::milliseconds(15));
    writer.end_event("MessageA");

    // Move time forward to complete the window
    clock.advance(Duration::milliseconds(TEST_WINDOW_SIZE_MS));
    writer.advance_window_if_needed();

    let view = target.to_view(clock.now().duration_since(reference_instant).as_nanos() as u64);

    assert_eq!(view.message_types.len(), 2);
    assert!(view.message_types.contains(&"MessageA".to_string()));
    assert!(view.message_types.contains(&"MessageB".to_string()));

    let window = &view.windows[0];
    assert_eq!(window.events.len(), 6); // 3 start + 3 end events

    // Should have stats for both message types
    assert!(window.summary.message_stats_by_type.len() >= 2);

    // Find MessageA stats (should have 2 events)
    let message_a_stats = window
        .summary
        .message_stats_by_type
        .iter()
        .find(|s| s.count == 2)
        .expect("MessageA should have 2 events");
    assert!(message_a_stats.total_time_ns >= 25_000_000); // At least 25ms (10ms + 15ms)

    // Find MessageB stats (should have 1 event)
    let message_b_stats = window
        .summary
        .message_stats_by_type
        .iter()
        .find(|s| s.count == 1)
        .expect("MessageB should have 1 event");
    assert!(message_b_stats.total_time_ns >= 20_000_000); // At least 20ms
}

#[test]
fn test_window_wrapping() {
    let TestSetup { clock, reference_instant, target, mut writer } = Default::default();

    // Record events across multiple windows to test buffer wrapping
    // We'll create more than NUM_WINDOWS to see wrapping behavior
    for _i in 0..NUM_WINDOWS + 5 {
        // Start an event
        writer.start_event("TestMessage", 100_000);
        clock.advance(Duration::milliseconds(10));
        writer.end_event("TestMessage");

        // Advance time to the next window
        clock.advance(Duration::milliseconds(TEST_WINDOW_SIZE_MS - 10));
        writer.advance_window_if_needed();
    }

    let view = target.to_view(clock.now().duration_since(reference_instant).as_nanos() as u64);

    // Should only keep the most recent NUM_WINDOWS windows due to buffer wrapping
    assert!(
        view.windows.len() <= NUM_WINDOWS,
        "Should not exceed NUM_WINDOWS due to buffer wrapping"
    );

    // Each window should have our test event
    for window in &view.windows {
        if !window.summary.message_stats_by_type.is_empty() {
            assert_eq!(window.summary.message_stats_by_type[0].count, 1);
            assert!(window.summary.message_stats_by_type[0].total_time_ns >= 10_000_000); // At least 10ms
        }
    }
}

#[test]
fn test_cross_window_event() {
    let TestSetup { clock, reference_instant, target, mut writer } = Default::default();

    // Start an event near the end of a window
    clock.advance(Duration::milliseconds(TEST_WINDOW_SIZE_MS - 50)); // 450ms into first window
    writer.start_event("CrossWindowMessage", 200_000);

    // Continue processing into the next window
    clock.advance(Duration::milliseconds(100)); // This should cross into the next window
    writer.end_event("CrossWindowMessage");

    // Move time forward to complete the window
    clock.advance(Duration::milliseconds(TEST_WINDOW_SIZE_MS));
    writer.advance_window_if_needed();

    let view = target.to_view(clock.now().duration_since(reference_instant).as_nanos() as u64);

    // Should have 2 windows now
    assert_eq!(view.windows.len(), 2);

    // The event should be split across windows
    // First window should have partial time (50ms)
    let first_window = &view.windows[1]; // Windows are in reverse order (most recent first)
    if !first_window.summary.message_stats_by_type.is_empty() {
        assert!(first_window.summary.message_stats_by_type[0].total_time_ns <= 50_000_000); // Should be <= 50ms
    }

    // Second window should have the remaining time (50ms)
    let second_window = &view.windows[0];
    if !second_window.summary.message_stats_by_type.is_empty() {
        assert!(second_window.summary.message_stats_by_type[0].total_time_ns >= 50_000_000); // Should be >= 50ms
    }
}

#[test]
fn test_active_event_tracking() {
    let TestSetup { clock, reference_instant, target, mut writer } = Default::default();

    // Start an event but don't end it
    writer.start_event("ActiveMessage", 300_000);
    clock.advance(Duration::milliseconds(75)); // Let it run for 75ms

    let view = target.to_view(clock.now().duration_since(reference_instant).as_nanos() as u64);

    // Should show an active event
    assert!(view.active_event.is_some());
    let active_event = view.active_event.unwrap();
    assert_eq!(active_event.message_type, 0); // First message type
    assert!(active_event.active_for_ns >= 75_000_000); // At least 75ms

    // Now end the event
    writer.end_event("ActiveMessage");

    // Move time forward to complete the window
    clock.advance(Duration::milliseconds(TEST_WINDOW_SIZE_MS));
    writer.advance_window_if_needed();

    let view = target.to_view(clock.now().duration_since(reference_instant).as_nanos() as u64);

    // Should no longer show an active event
    assert!(view.active_event.is_none());

    // Should have recorded the complete event
    let window = &view.windows[0];
    assert_eq!(window.summary.message_stats_by_type[0].count, 1);
    assert!(window.summary.message_stats_by_type[0].total_time_ns >= 75_000_000);
}
