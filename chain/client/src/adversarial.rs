#[cfg(feature = "test_features")]
mod adv {
    use std::sync::atomic::Ordering;

    #[derive(Default)]
    struct Inner {
        disable_header_sync: std::sync::atomic::AtomicBool,
        disable_doomslug: std::sync::atomic::AtomicBool,
        // Negative values mean None, non-negative values mean Some(sync_height
        // as u64).  This is only for testig so we can live with not supporting
        // values over i64::MAX.
        sync_height: std::sync::atomic::AtomicI64,
        is_archival: bool,
    }

    #[derive(Default, Clone)]
    pub struct Controls(std::sync::Arc<Inner>);

    impl Controls {
        pub fn new(is_archival: bool) -> Self {
            Self(std::sync::Arc::new(Inner { is_archival, ..Inner::default() }))
        }

        pub fn disable_header_sync(&self) -> bool {
            self.0.disable_header_sync.load(Ordering::SeqCst)
        }

        pub fn set_disable_header_sync(&mut self, value: bool) {
            self.0.disable_header_sync.store(value, Ordering::SeqCst);
        }

        pub fn disable_doomslug(&self) -> bool {
            self.0.disable_doomslug.load(Ordering::SeqCst)
        }

        pub fn set_disable_doomslug(&self, value: bool) {
            self.0.disable_doomslug.store(value, Ordering::SeqCst);
        }

        pub fn sync_height(&self) -> Option<u64> {
            let value = self.0.sync_height.load(Ordering::SeqCst);
            if value < 0 {
                None
            } else {
                Some(value as u64)
            }
        }

        pub fn set_sync_height(&self, height: u64) {
            let value: i64 = height.try_into().unwrap();
            self.0.sync_height.store(value, Ordering::SeqCst);
        }

        pub fn is_archival(&self) -> bool {
            self.0.is_archival
        }
    }
}

#[cfg(not(feature = "test_features"))]
mod adv {
    #[derive(Default, Clone)]
    pub struct Controls;

    impl Controls {
        pub const fn new(_is_archival: bool) -> Self {
            Self
        }

        pub const fn disable_header_sync(&self) -> bool {
            false
        }

        pub const fn disable_doomslug(&self) -> bool {
            false
        }

        pub const fn sync_height(&self) -> Option<u64> {
            None
        }
    }
}

pub use adv::Controls;
