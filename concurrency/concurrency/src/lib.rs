use std::future::Future;

pub use near_concurrency_macro::must_complete;

struct MustComplete; 

impl Drop for MustComplete {
    fn drop(&mut self) {
        panic!("dropped a future before completion");
    }
}

pub async fn must_complete<Fut:Future>(fut:Fut) -> Fut::Output {
    let guard = MustComplete;
    let res = fut.await;
    let _ = std::mem::ManuallyDrop::new(guard); 
    res
}
