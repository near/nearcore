use std::time::Duration;

pub fn spawn<F>(_class_name: &'static str, _file: &'static str, _line: u32, f: F)
where
    F: futures::Future<Output = ()> + 'static,
{
    actix_rt::spawn(f);
}

pub fn run_later<F, A, B>(
    ctx: &mut B,
    _file: &'static str,
    _line: u32,
    dur: Duration,
    f: F,
) -> actix::SpawnHandle
where
    B: actix::AsyncContext<A>,
    A: actix::Actor<Context = B>,
    F: FnOnce(&mut A, &mut A::Context) + 'static,
{
    ctx.run_later(dur, f)
}
