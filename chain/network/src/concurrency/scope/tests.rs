use crate::concurrency::ctx;
use crate::concurrency::scope;
use crate::concurrency::signal;
use crate::testonly::abort_on_panic;
use std::sync::atomic::{AtomicU64, Ordering};

// run a trivial future until completion => OK
#[tokio::test]
async fn must_complete_ok() {
    assert_eq!(5, scope::must_complete(async move { 5 }).await);
}

struct S;
impl scope::ServiceTrait for S {
    type E = usize;
}

type R = Result<(), usize>;

#[tokio::test]
async fn test_spawn_after_cancelling_scope() {
    abort_on_panic();
    let res = scope::run!(|s| async {
        s.spawn(async { R::Err(7) });
        ctx::canceled().await;
        s.spawn(async { R::Err(3) });
        Ok(())
    });
    assert_eq!(R::Err(7), res);
}

#[tokio::test]
async fn test_service_termination() {
    abort_on_panic();
    let res = scope::run!(|s| async {
        let service = s.new_service(S);
        scope::try_spawn!(&service, |_| async { Ok(ctx::canceled().await) }).unwrap();
        scope::try_spawn!(&service, |_| async { Ok(ctx::canceled().await) }).unwrap();
        service.terminate();
        service.terminated().await.unwrap().unwrap();
        // Spawning after service termination should fail.
        assert!(scope::try_spawn!(&service, |_| async { R::Err(1) }).is_err());
        R::Ok(())
    });
    assert_eq!(Ok(()), res);
}

#[tokio::test]
async fn test_nested_service() {
    abort_on_panic();
    let inner_spawned = signal::Once::new();
    scope::run!(|s| async {
        let outer = s.new_service(S);
        let inner = scope::try_spawn!(&outer, |s| async { Ok(s.new_service(S)) })
            .unwrap()
            .join()
            .await
            .unwrap()
            .unwrap();
        {
            let inner = inner.clone();
            let inner_spawned = inner_spawned.clone();
            scope::try_spawn!(&outer, |_| async move {
                let x = scope::try_spawn!(&inner, |_| async move {
                    inner_spawned.send();
                    // inner service task waits for cancelation, then returns an error.
                    ctx::canceled().await;
                    R::Err(8)
                })
                .unwrap()
                .join_err()
                .await
                .unwrap()
                .err()
                .unwrap();
                // outer service task waits for inner task to return an error,
                // then it returns its own error.
                R::Err(x + 1)
            })
            .unwrap();
        }
        // Wait for the inner task to get spawned.
        inner_spawned.recv().await;
        // Terminate the inner service.
        inner.terminate();
        // Await the inner service error.
        assert_eq!(Err(8), inner.terminated().await.unwrap());
        // Expect the outer service to return an error by itself.
        assert_eq!(Err(9), outer.terminated().await.unwrap());
        R::Ok(())
    })
    .unwrap();
}

#[tokio::test]
async fn test_nested_scopes() {
    abort_on_panic();
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

#[tokio::test]
async fn test_already_canceled() {
    abort_on_panic();
    let res = ctx::run_canceled(async {
        // scope::run! should start a task,
        // even though the task has been already canceled.
        scope::run!(|s| async {
            s.spawn(async {
                ctx::canceled().await;
                R::Err(4)
            });
            Ok(())
        })
    })
    .await;
    assert_eq!(Err(4), res);
}

#[tokio::test]
async fn test_service_error_collected() {
    abort_on_panic();
    let res = scope::run!(|s| async {
        let service = s.new_service(S);
        scope::try_spawn!(&service, |_| async { R::Err(1) }).unwrap();
        service.terminated().await.unwrap()
    });
    assert_eq!(Err(1), res);
}

#[tokio::test]
async fn test_service_spawn_while_terminating() {
    abort_on_panic();
    let res = scope::run!(|s| async {
        let service = s.new_service(S);
        scope::try_spawn!(&service, |s| async {
            ctx::canceled().await;
            // Until service is terminated, you are still
            // able to spawn new tasks, but you cannot await them,
            // because the context is canceled at this point.
            //
            // `join()` and `join_err()` await the termination of the whole
            // service if the task returns an error. The fact that context
            // gets cancelled prevents a deadlock.
            let res = scope::spawn!(s, |_| async { R::Err(7) }).join_err().await;
            assert_eq!(Err(ctx::ErrCanceled), res);
            Ok(())
        })
        .unwrap();
        service.terminate();
        assert_eq!(Err(7), service.terminated().await.unwrap());
        // Service error doesn't affect the scope. We can return a different error, or even
        // success.
        R::Err(3)
    });
    assert_eq!(Err(3), res);
}

#[tokio::test]
async fn test_service_join() {
    abort_on_panic();
    scope::run!(|s| async {
        let service = s.new_service(S);
        // Task which returns a success doesn't terminate the service (using `join_err`).
        let res =
            scope::try_spawn!(&service, |_| async { Ok(3) }).unwrap().join_err().await.unwrap();
        assert_eq!(Ok(3), res);
        // Task which returns a success doesn't terminate the service (using `join`).
        let res = scope::try_spawn!(&service, |_| async { Ok(2) }).unwrap().join().await.unwrap();
        assert_eq!(Ok(2), res);
        // Task which returns an error terminates the service.
        let res =
            scope::try_spawn!(&service, |_| async { R::Err(7) }).unwrap().join_err().await.unwrap();
        assert_eq!(Err(7), res);
        // After the service is terminated, no new task can be spawned.
        let res = scope::try_spawn!(&service, |_| async {
            panic!("shouldn't be executed");
            #[allow(unreachable_code)]
            Ok(())
        });
        assert_eq!(scope::ErrTerminated, res.err().unwrap());
        R::Ok(())
    })
    .unwrap();
}

// We treat all tasks within a service as a single error domain.
// If more that one task returns an error, the first error is returned in every join.
#[tokio::test]
async fn test_service_join_error_override() {
    abort_on_panic();
    scope::run!(|s| async {
        let s = s.new_service(S);
        let t1 = scope::try_spawn!(&s, |_| async {
            ctx::canceled().await;
            R::Err(1)
        })
        .unwrap();
        let t2 = scope::try_spawn!(&s, |_| async {
            ctx::canceled().await;
            R::Err(2)
        })
        .unwrap();
        s.terminate();
        let res = s.terminated().await.unwrap();
        assert!(res.is_err());
        assert_eq!(res, t1.join_err().await.unwrap());
        assert_eq!(res, t2.join_err().await.unwrap());
        R::Ok(())
    })
    .unwrap();
}

#[tokio::test]
async fn test_scope_error() {
    abort_on_panic();
    let res = scope::run!(|s| async {
        let service = s.new_service(S);
        scope::try_spawn!(&service, |_| async {
            ctx::canceled().await;
            R::Ok(())
        })
        .unwrap();
        R::Err(2)
    });
    assert_eq!(Err(2), res);
}

// After all main tasks complete succesfully, the scope gets canceled.
// Background tasks of the scope should still be able to spawn more tasks
// both via `Scope::spawn()` and `Scope::spawn_bg` (although after scope
// cancelation they behave exactly the same).
#[tokio::test]
async fn test_spawn_from_spawn_bg() {
    abort_on_panic();
    let res = scope::run!(|s| async {
        s.spawn_bg(async {
            ctx::canceled().await;
            s.spawn(async {
                assert!(ctx::is_canceled());
                R::Err(3)
            });
            // Service can be spawned, but we cannot do anything about it
            // (within the canceled scope).
            s.new_service(S);
            Ok(())
        });
        Ok(())
    });
    assert_eq!(Err(3), res);
}

#[tokio::test]
async fn test_service_outside_of_the_scope() {
    let service = scope::run!(|s| async { Result::<_, ()>::Ok(s.new_service(S)) }).unwrap();
    // If the service object outlives the scope it is nested in, it should be already terminated.
    assert!(service.is_terminated());
    // Therefore spawning should fail.
    assert_eq!(
        scope::ErrTerminated,
        scope::try_spawn!(&service, |_| async {
            #[allow(unreachable_code)]
            Ok(panic!("unreachable"))
        })
        .err()
        .unwrap()
    );
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
                    R::Ok(())
                });
                R::Ok(())
            })
        });

        s.spawn(async {
            s.spawn(async {
                a.fetch_add(1, Ordering::Relaxed);
                R::Ok(())
            });
            a.fetch_add(1, Ordering::Relaxed);
            R::Ok(())
        });
        a.fetch_add(1, Ordering::Relaxed);
        s.spawn(async {
            a.fetch_add(1, Ordering::Relaxed);
            R::Ok(())
        });
        a.fetch_add(1, Ordering::Relaxed);
        R::Ok(())
    })
    .unwrap();
    assert_eq!(6, a.load(Ordering::Relaxed));
}
