use std::time::Duration;

pub fn measure_performance<F, Message, Result>(
    _class_name: &'static str,
    msg: Message,
    f: F,
) -> Result
where
    F: FnOnce(Message) -> Result,
{
    f(msg)
}

pub fn measure_performance_with_debug<F, Message, Result>(
    _class_name: &'static str,
    msg: Message,
    f: F,
) -> Result
where
    F: FnOnce(Message) -> Result,
    for<'a> &'a Message: Into<&'static str>,
{
    f(msg)
}

pub fn print_performance_stats(_sleep_time: Duration) {}
