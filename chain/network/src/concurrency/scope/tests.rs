use crate::concurrency::ctx::Ctx;
use crate::concurrency::scope;
use crate::testonly::abort_on_panic;
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

#[tokio::test]
async fn test_spawn_after_cancelling_scope() {
    abort_on_panic();
    let res = scope::run!(&Ctx::inf(), |s| move |ctx: Ctx| async move {
        s.spawn(|_| async { Err(7) });
        ctx.canceled().await;
        s.spawn(|_| async { Err(3) });
        Ok(())
    });
    assert_eq!(Err(7), res);
}

#[tokio::test]
async fn test_spawn_after_dropping_service() {
    abort_on_panic();
    let res = scope::run!(&Ctx::inf(), |s| move |ctx: Ctx| async move {
        let service = Arc::new(s.new_service());
        service
            .spawn({
                let service = service.clone();
                |ctx: Ctx| async move {
                    ctx.canceled().await;
                    // Even though the service has been cancelled, you can spawn more tasks on it
                    // until it is actually terminated. So it is always OK to spawn new tasks on
                    // a service from another task running on this service.
                    service.spawn(|_| async { Err(5) }).unwrap();
                    Ok(())
                }
            })
            .unwrap();
        // terminate() may get cancelled, because we expect the service to return an error.
        // Error may or may not get propagated before terminate completes.
        let _ = service.terminate(&ctx).await;
        Ok(())
    });
    assert_eq!(Err(5), res);
}

#[tokio::test]
async fn test_service_termination() {
    let res = scope::run!(&Ctx::inf(), |s| move |ctx| async move {
        let service = Arc::new(s.new_service());
        service
            .spawn(|ctx: Ctx| async move {
                ctx.canceled().await;
                Ok(())
            })
            .unwrap();
        service
            .spawn(|ctx: Ctx| async move {
                ctx.canceled().await;
                Ok(())
            })
            .unwrap();
        service.terminate(&ctx).await.unwrap();
        Result::<_, ()>::Ok(())
    });
    assert_eq!(Ok(()), res);
}

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
                    let _service = service;
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
async fn test_access_to_vars_outside_of_scope() {
    abort_on_panic();
    // Lifetime of a is larger than scope's lifetime,
    // so it should be accessible from scope's tasks.
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
