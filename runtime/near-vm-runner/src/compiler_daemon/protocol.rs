//! IPC protocol: length-prefixed borsh frames over stdin/stdout.

use borsh::{BorshDeserialize, BorshSerialize};
use std::io::{Read, Write};

/// Prepared-code payload that makes a test daemon abort, exercising the
/// parent-side handling of machine-local compiler failures.
#[cfg(feature = "test_features")]
pub const TEST_ABORT_REQUEST: &[u8] = b"near-vm-runner compiler daemon test abort";

#[cfg(all(target_os = "linux", feature = "test_features"))]
pub const TEST_LANDLOCK_PROBE_REQUEST: &[u8] = b"near-vm-runner compiler daemon landlock probe";

#[cfg(all(target_os = "linux", feature = "test_features"))]
pub const TEST_LANDLOCK_PROBE_RESPONSE: &[u8] =
    b"near-vm-runner compiler daemon landlock probe passed";

#[derive(BorshSerialize, BorshDeserialize)]
pub enum DaemonStartup {
    Ready,
    Err(String),
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct CompileRequest {
    pub prepared_code: Vec<u8>,
    pub max_memory_pages: u32,
    pub max_tables_per_contract: Option<u32>,
    pub max_elements_per_contract_table: Option<u64>,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub enum CompileResponse {
    Ok(Vec<u8>),
    Err(String),
}

pub fn write_frame(w: &mut impl Write, data: &[u8]) -> std::io::Result<()> {
    w.write_all(&(data.len() as u32).to_le_bytes())?;
    w.write_all(data)?;
    w.flush()
}

/// Maximum frame size. Compiled modules are typically a few MiB;
/// this limit prevents a buggy daemon from causing OOM in the parent.
const MAX_FRAME_SIZE: usize = 128 * 1024 * 1024;

pub fn read_frame(r: &mut impl Read) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_FRAME_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame too large: {len} bytes (max {MAX_FRAME_SIZE})"),
        ));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}
