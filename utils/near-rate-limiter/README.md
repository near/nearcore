# `near-rate-limiter` Overview:

Currently, in `near` we use `Actix` `Actors` multi-threaded framework for running tasks on different threads as well as 
inter-thread communication.
For the purpose of communicating with the outside world, we use `std::tokio::TcpStream` streams.
Typically, we open a new connection, we define handles in actors, and `tokio` handles communication between raw Tcp sockets, and `Actix` `Actors` framework.
Encoding/decoding messages, as well as splitting messages into frames, is done by `FramedRead`, `FramedWrite` pair from `tokio`.

However, neither `tokio`, nor `actix` solve the issue of load-balancing. 
`near-rate-limiter` provides a `ThrottleFramedRead` as a replacement for `FramedRead` with a couple of extra features.

## Implemented features:
- Stopping/resuming reading from tcp socket at will.
- `ThrottleFramedRead` can be controlled with a helper data structure `ThrottleController`.
- For example, can be based on total memory usage, cpu, or other metrics, `ThrottleFramedRead` can stop or resume reading from socket.
- An `ActixMessageWrapper`, wraps around `Actix` messages, can be used to track, the number, and size of all messages
that originated from `TcpSocket`, and are still alive, and/or being transported inside `Actix` mailboxes, etc.
The full design, needs its own separate section. TODO(#5672)
- Throttling based on size/count of all actix messages

## Planned features:
- Throttling based on bandwidth used
- Throttling based on size of read/write buffers
- Throttling based on other metrics, like cpu, thread fairness, total memory used by process.
- Stats, needed to choose right limits.

## Structure

### `ThrottleFramedRead`
- Manages a read `TcpSocket`. Is controlled by `ThrottleController` helper data structure.
- Holds a copy of `ThrottleController`.

### `ThrottleController`
- Holds a counter of number of messages/total size of messages tracked (`atomics`).
- Is created with thresholds, defining, maximum values of the counters above, before read throttling starts happening.
- Has a semaphore, which can be used to wake up the `ThrottleFramedRead`.
- Provides `is_ready` method, for determining, whenever `TcpStream` is ready to be read.
That method can be accessed from any thread.

### `ThrottleToken`
- Is associated with given `ThrottleController`.
- Holds size of message, which is tracked by `ThrottleController`
- Gets created at the time tracking starts, and increases the right counters.
- When gets dropped, decreases `ThrottleController` counters.

### `ActixMessageWrapper`
- Currently, in `near-network`, will be moved to this crate.
- A wrapper around `Actix` messages.

### `ActixMessageWrapperReponse`
- Currently, in `near-network`, will be moved to this crate.
- A wrapper around `Actix` message responses.
