use protobuf::Message;

pub mod block;
pub mod transaction;
pub mod message;
pub mod txflow;


pub fn encode<T: Message>(m: &T) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    m.write_to_writer(&mut bytes).map_err(|_| "Protobuf write failed")?;
    Ok(bytes)
}

pub fn decode<T: Message>(bytes: &[u8]) -> Result<T, String> {
    protobuf::parse_from_bytes(bytes).map_err(|_| "Protobuf merge from bytes failed".to_string())
}
