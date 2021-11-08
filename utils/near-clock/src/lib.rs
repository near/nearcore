thread_local! {
    static TIME_OVERRIDE: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
}

pub fn get_time_override() -> Arc<AtomicUsize> {
    TIME_OVERRIDE.with(|x| x.clone())
}

pub fn now() -> NearClock {
    NearClock { time: SystemTime::now() }
}

struct NearClock {
    time: SystemTime,
}
