use crate::futures::DelayedActionRunner;

pub trait Handler<M, R = ()> {
    fn handle(&mut self, msg: M) -> R;
}

// TODO: Long term vision is to eventually get rid of DelayedActionRunner context parameter
pub trait HandlerWithContext<M, R = ()> {
    fn handle(&mut self, msg: M, ctx: &mut dyn DelayedActionRunner<Self>) -> R;
}

// TODO: Long term vision is to eventually get rid of DelayedActionRunner context parameter
impl<A, M, R> HandlerWithContext<M, R> for A
where
    A: Handler<M, R>,
{
    fn handle(&mut self, msg: M, _ctx: &mut dyn DelayedActionRunner<Self>) -> R {
        Handler::<M, R>::handle(self, msg)
    }
}
