# Code Review: `reimpl-soft-realtime-thread-pool`

## Summary of Changes

Replaces the oneshot-channel-based soft-realtime thread pool with a shared-queue + condvar
design. Changes the thread limit from soft (warn-only) to hard cap. Adds graceful shutdown,
new queue-size metric, and improved tests.

## Bug: Panic Safety — `total_threads` Leak

**File**: `chain/chain/src/soft_realtime_thread_pool.rs`, `run_worker` function

If `job()` panics, the stack unwinds through `run_worker`. The lock was already dropped
before calling `job()` (line 192), so the mutex isn't poisoned. However,
`state_guard.dec_total_threads()` at the end of `run_worker` is **never executed**,
permanently inflating `total_threads` and reducing effective pool capacity by 1 per panic.

The old code handled this with `WorkerCounterGuard` (RAII pattern). The new code needs
equivalent protection — e.g., a guard struct whose `Drop` impl calls `dec_total_threads()`,
or wrapping `job()` in `std::panic::catch_unwind`.

## Minor: Race Between Condvar Timeout and Notification

A narrow TOCTOU race can orphan a queued job:

1. Idle thread's `wait_for` times out but hasn't reacquired the lock.
2. `spawn_boxed` acquires lock, enqueues job, sees `idle_threads > 0`, calls `notify_one()`,
   does NOT spawn a new thread.
3. Timed-out thread reacquires lock, sees `timed_out() == true`, exits.

Result: job sits in queue until next `spawn_boxed` or another thread finishes its work.
Practically negligible with 30s timeout, but fixable by checking queue before exiting on timeout.

## Design Note: Queued Jobs Dropped on Shutdown

When `ThreadPool` is dropped, idle threads are woken and exit. Busy threads check `shutdown`
after finishing their current job and break before draining the queue. Any remaining queued
jobs are silently dropped. The old design had no queue so this wasn't possible.

## Design Note: Drop Doesn't Join Threads

The `Drop` impl signals shutdown but doesn't wait for threads to finish. In-flight jobs
continue post-drop. This matches the old behavior. No correctness issue, but worth noting.

## Thread Limit Values

- `num_shards * 3` for apply-chunks: reasonable given soft→hard limit change.
- `PartialWitnessValidationThreadPool` 2→96: seems very high — worth validating.
- `WitnessCreationThreadPool` 1→6: reasonable for multi-shard concurrency.

## Verdict

Core design is clean and correct. The panic safety bug should be fixed before merge.
