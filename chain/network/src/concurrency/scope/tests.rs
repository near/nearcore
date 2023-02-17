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

type R<E> = Result<(), E>;

#[tokio::test]
async fn test_drop_service() {
    abort_on_panic();
    let res = scope::run!(|s| async {
        let service = Arc::new(s.new_service());
        service
            .spawn(async {
                Ctx::canceled().await;
                R::Err(2)
            })
            .unwrap();
        // Both scope task and service task await context cancellation.
        // However, scope task drops the only reference to service before
        // awaiting, which should cause service to cancel.
        // The canceled service task returns an error which should cancel
        // the whole scope.
        drop(service);
        Ctx::canceled().await;
        Ok(())
    });
    assert_eq!(Err(2), res);
}

#[tokio::test]
async fn test_spawn_after_cancelling_scope() {
    abort_on_panic();
    let res = scope::run!(|s| async {
        s.spawn(async { R::Err(7) });
        Ctx::canceled().await;
        s.spawn(async { R::Err(3) });
        Ok(())
    });
    assert_eq!(R::Err(7), res);
}

#[tokio::test]
async fn test_spawn_after_dropping_service() {
    abort_on_panic();
    let res = scope::run!(|s| async {
        let service = Arc::new(s.new_service());
        service
            .spawn({
                let service = service.clone();
                async move {
                    Ctx::canceled().await;
                    // Even though the service has been cancelled, you can spawn more tasks on it
                    // until it is actually terminated. So it is always OK to spawn new tasks on
                    // a service from another task running on this service.
                    service.spawn(async { R::Err(5) }).unwrap();
                    Ok(())
                }
            })
            .unwrap();
        // terminate() may get cancelled, because we expect the service to return an error.
        // Error may or may not get propagated before terminate completes.
        let _ = service.terminate().await;
        Ok(())
    });
    assert_eq!(Err(5), res);
}

#[tokio::test]
async fn test_service_termination() {
    let res = scope::run!(|s| async {
        let service = Arc::new(s.new_service());
        service.spawn(async { Ok(Ctx::canceled().await) }).unwrap();
        service.spawn(async { Ok(Ctx::canceled().await) }).unwrap();
        service.terminate().await.unwrap();
        // Spawning after service termination should fail.
        assert!(service.spawn(async { R::Err(1) }).is_err());
        Ok(())
    });
    assert_eq!(Ok(()), res);
}

#[tokio::test]
async fn test_nested_service() {
    let res = scope::run!(|s| async {
        let outer = Arc::new(s.new_service());
        outer
            .spawn({
                let outer = outer.clone();
                async move {
                    let inner = outer.new_service().unwrap();
                    inner
                        .spawn(async {
                            Ctx::canceled().await;
                            R::Err(9)
                        })
                        .unwrap();
                    Ctx::canceled().await;
                    Ok(())
                }
            })
            .unwrap();
        // Scope (and all the transitive subservices) get cancelled once this task completes.
        // Scope won't be terminated until all the transitive subservices terminate.
        Ok(())
    });
    assert_eq!(Err(9), res);
}

#[tokio::test]
async fn test_nested_scopes() {
    let res = scope::run!(|s| async {
        s.spawn(async {
            scope::run!(|s| async move {
                s.spawn(async { scope::run!(|_| async { R::Err(8) }) });
                Ok(())
            })
        });
        Ok(())
    });
    assert_eq!(Err(8), res);
}

/*
#[tokio::test]
async fn test_external_cancel() {
    let external_ctx = Ctx::inf().with_cancel();
    let external_ctx = &external_ctx;
    let res = scope::run!(&external_ctx, |s| move |ctx: Ctx| async move {
        s.spawn(|ctx: Ctx| async move {
            ctx.canceled().await;
            Ok(())
        });
        // We cancel the external context manually.
        // It should just cancel the scope as usual, but it
        // doesn't imply that Scope encountered an error.
        external_ctx.cancel();
        ctx.canceled().await;
        Result::<_, ()>::Ok(())
    });
    assert!(res.is_ok());
}*/

#[tokio::test]
async fn test_service_cancel() {
    abort_on_panic();
    scope::run!(|s| async {
        let service = Arc::new(s.new_service());
        service
            .spawn({
                let service = service.clone();
                async move {
                    let _service = service;
                    Ctx::canceled().await;
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
    let res = scope::run!(|s| async {
        let service: Arc<scope::Service<usize>> = Arc::new(s.new_service());

        service
            .spawn({
                let service = service.clone();
                async move {
                    let _service = service;
                    R::Err(1)
                }
            })
            .unwrap();
        Ctx::canceled().await;
        Ok(())
    });
    assert_eq!(Err(1), res);
}

#[tokio::test]
async fn test_service_error_after_cancel() {
    abort_on_panic();
    let res = scope::run!(|s| async {
        let service: Arc<scope::Service<usize>> = Arc::new(s.new_service());

        service
            .spawn({
                let service = service.clone();
                async move {
                    let _service = service;
                    Ctx::canceled().await;
                    R::Err(2)
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
    let res = scope::run!(|s| async {
        let service: Arc<scope::Service<usize>> = Arc::new(s.new_service());

        service
            .spawn({
                let service = service.clone();
                async move {
                    let _service = service;
                    Ctx::canceled().await;
                    Ok(())
                }
            })
            .unwrap();
        R::Err(2)
    });
    assert_eq!(Err(2), res);
}

#[tokio::test]
async fn test_scope_error_nonoverridable() {
    abort_on_panic();
    let res = scope::run!(|s| async {
        let service: Arc<scope::Service<usize>> = Arc::new(s.new_service());
        service
            .spawn({
                let service = service.clone();
                async {
                    let _service = service;
                    Ctx::canceled().await;
                    R::Err(3)
                }
            })
            .unwrap();
        R::Err(2)
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
    scope::run!(|s| async {
        s.spawn(async move {
            scope::run!(|s| async {
                s.spawn(async {
                    a.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                });
                Ok(())
            })
        });

        s.spawn(async {
            s.spawn(async {
                a.fetch_add(1, Ordering::Relaxed);
                let res: Result<(), ()> = Ok(());
                res
            });
            a.fetch_add(1, Ordering::Relaxed);
            Ok(())
        });
        a.fetch_add(1, Ordering::Relaxed);
        s.spawn(async {
            a.fetch_add(1, Ordering::Relaxed);
            Ok(())
        });
        a.fetch_add(1, Ordering::Relaxed);
        Ok(())
    })
    .unwrap();
    assert_eq!(6, a.load(Ordering::Relaxed));
}
