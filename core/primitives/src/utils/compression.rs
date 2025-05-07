use super::io::CountingRead;
use crate::utils::io::CountingWrite;
use borsh::{BorshDeserialize, BorshSerialize};
use bytes::{Buf, BufMut};
use bytesize::ByteSize;

/// Helper trait for implementing a compressed structure for networking messages.
/// The reason this is not a struct is because some derives do not work well on
/// structs that have generics; e.g. ProtocolSchema.
pub trait CompressedData<T, const MAX_UNCOMPRESSED_SIZE: u64, const COMPRESSION_LEVEL: i32>
where
    T: BorshSerialize + BorshDeserialize,
    Self: From<Box<[u8]>> + AsRef<Box<[u8]>>,
{
    /// Only use this if you are sure that the data is already encoded.
    fn from_boxed_slice(data: Box<[u8]>) -> Self {
        Self::from(data)
    }

    /// Borsh-serialize and compress the given data.
    /// Returns compressed data along with the raw (uncompressed) serialized data size.
    fn encode(uncompressed: &T) -> std::io::Result<(Self, usize)> {
        // Flow of data: Original --> Borsh serialization --> Counting write --> zstd compression --> Bytes.
        // CountingWrite will count the number of bytes for the Borsh-serialized data, before compression.
        let mut counting_write =
            CountingWrite::new(zstd::stream::Encoder::new(Vec::new().writer(), COMPRESSION_LEVEL)?);
        borsh::to_writer(&mut counting_write, uncompressed)?;

        let borsh_bytes_len = counting_write.bytes_written();
        let encoded_bytes = counting_write.into_inner().finish()?.into_inner();

        Ok((Self::from(encoded_bytes.into()), borsh_bytes_len.as_u64() as usize))
    }

    /// Decompress and borsh-deserialize the compressed data.
    /// Returns decompressed and deserialized data along with the raw (uncompressed) serialized data size.
    fn decode(&self) -> std::io::Result<(T, usize)> {
        // We want to limit the size of decompressed data to address "Zip bomb" attack.
        self.decode_with_limit(ByteSize(MAX_UNCOMPRESSED_SIZE))
    }

    /// Decompress and borsh-deserialize the compressed data.
    /// Returns decompressed and deserialized data along with the raw (uncompressed) serialized data size.
    fn decode_with_limit(&self, limit: ByteSize) -> std::io::Result<(T, usize)> {
        // Flow of data: Bytes --> zstd decompression --> Counting read --> Borsh deserialization --> Original.
        // CountingRead will count the number of bytes for the Borsh-deserialized data, after decompression.
        let mut counting_read = CountingRead::new_with_limit(
            zstd::stream::Decoder::new(self.as_ref().reader())?,
            limit,
        );

        match borsh::from_reader(&mut counting_read) {
            Err(err) => {
                // If decompressed data exceeds the limit then CountingRead will return a WriteZero error.
                // Here we convert it to a more descriptive error to make debugging easier.
                let err = if err.kind() == std::io::ErrorKind::WriteZero {
                    std::io::Error::other(format!(
                        "Decompressed data exceeded limit of {limit}: {err}"
                    ))
                } else {
                    err
                };
                Err(err)
            }
            Ok(deserialized) => {
                Ok((deserialized, counting_read.bytes_read().as_u64().try_into().unwrap()))
            }
        }
    }

    fn size_bytes(&self) -> usize {
        self.as_ref().len()
    }

    fn as_slice(&self) -> &[u8] {
        &self.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::compression::CompressedData;
    use borsh::{BorshDeserialize, BorshSerialize};
    use std::io::ErrorKind;

    #[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug)]
    struct MyData(Vec<u8>);

    #[derive(derive_more::From, derive_more::AsRef)]
    struct CompressedMyData(Box<[u8]>);

    impl super::CompressedData<MyData, 1000, 3> for CompressedMyData {}

    #[test]
    fn encode_decode_within_limit() {
        let data = MyData(vec![42; 100]);
        let (compressed, uncompressed_size) = CompressedMyData::encode(&data).unwrap();
        let (decompressed, decompressed_size) = compressed.decode().unwrap();
        assert_eq!(&decompressed, &data);
        assert_eq!(uncompressed_size, decompressed_size);
        assert_eq!(borsh::object_length(&data).unwrap(), uncompressed_size);
    }

    #[test]
    fn encode_exceeding_limit() {
        // Encode exceeding limit is OK.
        let data = MyData(vec![42; 2000]);
        let (_, uncompressed_size) = CompressedMyData::encode(&data).unwrap();
        assert_eq!(borsh::object_length(&data).unwrap(), uncompressed_size);
    }

    #[test]
    fn decode_exceeding_limit() {
        let data = MyData(vec![42; 2000]);
        let (compressed, _) = CompressedMyData::encode(&data).unwrap();
        let error = compressed.decode().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Other);
        assert_eq!(
            error.to_string(),
            "Decompressed data exceeded limit of 1.0 KB: Exceeded the limit of 1000 bytes"
        );
    }

    #[test]
    fn decode_invalid_data() {
        let invalid_data = [0; 10];
        let error =
            CompressedMyData::from_boxed_slice(Box::new(invalid_data)).decode().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Other);
    }
}
