use super::message::Message;

struct Pipeline {
    middlewares: Vec<Middleware<_, _>>
}

trait Middleware<T: Iterator, U: Iterator> {
    fn pipe(&mut self, messages: T<Message>) -> U<Message>;

    fn pipe_one(&mut self, message: Message) -> Iterable<Message>;
}