mod connection;

pub use connection::{ConnectError, Connection, DirectMessage, Message, RoutedMessage};

#[cfg(test)]
mod tests;
