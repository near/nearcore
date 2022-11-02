mod connection;

pub use connection::{ConnectError, Connection, ReceivedMessage};

#[cfg(test)]
mod tests;
