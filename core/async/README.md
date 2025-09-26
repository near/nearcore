# Core Async Helpers

This crate contains helpers related to common asynchronous programming patterns
used in nearcore:

* messaging: common interfaces for sending messages between components.
* test_loop: an event-loop-based test framework that can test multiple components
  together in a synchronous way.


## Messaging

`Sender<T>` and `AsyncSender<T>` are abstractions of our actor interfaces. When
a component needs to send a message to another component, the component should
keep a `Sender<T>` as a field and require it during construction, like:

```rust
struct MyComponent {
  downstream_component: Sender<DownstreamMessage>,
}

impl MyComponent {
  pub fn new(downstream_component: Sender<DownstreamMessage>) -> Self { ... }
}
```

The sender can then be used to send messages:
```rust
impl MyComponent {
  fn do_something(&mut self, args: ...) {
    self.downstream_component.send(DownstreamMessage::DataReady(...));
  }
}
```

To create a `Sender<T>`, we need any implementation of `CanSend<T>`. One way is
to use an `TokioRuntimeHandle<T>` or `MultithreadRuntimeHandle<T>`.
```rust
impl Handler<DownstreamMessage> for DownstreamActor {...}

fn setup_system(actor_system: ActorSystem) {
  let addr = actor_system.spawn_tokio_actor(DownstreamActor());
  let my_component = MyComponent::new(addr.into_sender());
}
```

In tests, the `TestLoopBuilder` provides the `sender()` function which also
implements `CanSend`, see the examples directory under this crate.

`AsyncSender<T>` is similar, except that calling `send_async` returns a future
that carries the response to the message.
