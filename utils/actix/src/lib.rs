use std::sync::Once;

static SET_PANIC_HOOK: Once = Once::new();

/// This is a workaround to make actix/tokio runtime stop when a task panics.
pub fn init_stop_on_panic() {
    SET_PANIC_HOOK.call_once(|| {
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            default_hook(info);
            if actix::System::is_set() {
                actix::System::with_current(|sys| sys.stop_with_code(1));
            }
        }));
    })
}
