use crate::concurrency::ctx::Ctx;
use crate::concurrency::scope;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// drop a trivial future without completion => panic (in debug mode at least).
#[tokio::test]
#[should_panic]
async fn must_complete_should_panic() {
    let _ = scope::must_complete(async move { 6 });
}

// run a trivial future until completion => OK
#[tokio::test]
async fn must_complete_ok() {
    assert_eq!(5, scope::must_complete(async move { 5 }).await);
}

#[tokio::test]
async fn test_drop_service() {
    abort_on_panic();
    let res = scope::run!(&Ctx::inf(), |s| move |ctx: Ctx| async move {
        let service = Arc::new(s.new_service());
        service
            .spawn(|ctx: Ctx| async move {
                ctx.canceled().await;
                Err(2)
            })
            .unwrap();
        // Both scope task and service task await context cancellation.
        // However, scope task drops the only reference to service before
        // awaiting, which should cause service to cancel.
        // The canceled service task returns an error which should cancel
        // the whole scope.
        drop(service);
        ctx.canceled().await;
        Ok(())
    });
    assert_eq!(Err(2), res);
}

/// Initializes the test logger and then, iff the current process is executed under
/// nextest in process-per-test mode, changes the behavior of the process to [panic=abort].
/// In particular it doesn't enable [panic=abort] when run via "cargo test".
/// Note that (unfortunately) some tests may expect a panic, so we cannot apply blindly
/// [panic=abort] in compilation time to all tests.
// TODO: investigate whether "-Zpanic-abort-tests" could replace this function once the flag
// becomes stable: https://github.com/rust-lang/rust/issues/67650, so we don't use it.
fn abort_on_panic() {
    // I don't know a way to set panic=abort for nextest builds in compilation time, so we set it
    // in runtime. https://nexte.st/book/env-vars.html#environment-variables-nextest-sets
    let Ok(nextest) = std::env::var("NEXTEST") else { return };
    let Ok(nextest_execution_mode) = std::env::var("NEXTEST_EXECUTION_MODE") else { return };
    if nextest != "1" || nextest_execution_mode != "process-per-test" {
        return;
    }
    println!("[panic=abort] enabled");
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);
        std::process::abort();
    }));
}

// TODO(gprusak): test spawning after service/scope cancellation.
// TODO(gprusak): test service termination.
// TODO(gprusak): test spawning after termination.
// TODO(gprusak): test nested services.
// TODO(gprusak): test nested scopes.

#[tokio::test]
async fn test_service_cancel() {
    abort_on_panic();
    scope::run!(&Ctx::inf(), |s| |_| async {
        let service = Arc::new(s.new_service());
        service
            .spawn({
                let service = service.clone();
                |ctx: Ctx| async move {
                    let _service = service;
                    ctx.canceled().await;
                    Ok(())
                }
            })
            .unwrap();
        Result::<(), ()>::Ok(())
    })
    .unwrap();
}

#[tokio::test]
async fn test_service_error_before_cancel() {
    abort_on_panic();
    let res = scope::run!(&Ctx::inf(), |s| move |ctx: Ctx| async move {
        let service: Arc<scope::Service<usize>> = Arc::new(s.new_service());

        service
            .spawn({
                let service = service.clone();
                |_| async move {
                    let _service = service;
                    Err(1)
                }
            })
            .unwrap();
        ctx.canceled().await;
        Ok(())
    });
    assert_eq!(Err(1), res);
}

#[tokio::test]
async fn test_service_error_after_cancel() {
    abort_on_panic();
    let res = scope::run!(&Ctx::inf(), |s| move |_: Ctx| async move {
        let service: Arc<scope::Service<usize>> = Arc::new(s.new_service());

        service
            .spawn({
                let service = service.clone();
                |ctx: Ctx| async move {
                    let _service = service;
                    ctx.canceled().await;
                    Err(2)
                }
            })
            .unwrap();
        Ok(())
    });
    assert_eq!(Err(2), res);
}

#[tokio::test]
async fn test_scope_error() {
    abort_on_panic();
    let res = scope::run!(&Ctx::inf(), |s| move |_ctx: Ctx| async move {
        let service: Arc<scope::Service<usize>> = Arc::new(s.new_service());

        service
            .spawn({
                let service = service.clone();
                |ctx: Ctx| async move {
                    let service = service;
                    ctx.canceled().await;
                    Ok(())
                }
            })
            .unwrap();
        Err(2)
    });
    assert_eq!(Err(2), res);
}

#[tokio::test]
async fn test_scope_error_nonoverridable() {
    abort_on_panic();
    let res = scope::run!(&Ctx::inf(), |s| move |_ctx: Ctx| async move {
        let service: Arc<scope::Service<usize>> = Arc::new(s.new_service());

        service
            .spawn({
                let service = service.clone();
                |ctx: Ctx| async move {
                    let _service = service;
                    ctx.canceled().await;
                    Err(3)
                }
            })
            .unwrap();
        Err(2)
    });
    assert_eq!(Err(2), res);
}

#[tokio::test]
async fn test1() {
    abort_on_panic();
    let a = AtomicU64::new(0);
    let a = &a;
    scope::run!(&Ctx::inf(), |s| |_ctx| async {
        s.spawn(|ctx| async move {
            scope::run!(&ctx, |s| |_ctx| async {
                s.spawn(|_| async {
                    a.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                });
                Ok(())
            })
        });

        s.spawn(|_| async {
            s.spawn(|_| async {
                a.fetch_add(1, Ordering::Relaxed);
                let res: Result<(), ()> = Ok(());
                res
            });
            a.fetch_add(1, Ordering::Relaxed);
            Ok(())
        });
        a.fetch_add(1, Ordering::Relaxed);
        s.spawn(|_| async {
            a.fetch_add(1, Ordering::Relaxed);
            Ok(())
        });
        a.fetch_add(1, Ordering::Relaxed);
        Ok(())
    })
    .unwrap();
    assert_eq!(6, a.load(Ordering::Relaxed));
}
