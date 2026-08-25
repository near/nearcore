//! IPC protocol: length-prefixed borsh frames over stdin/stdout.

// cspell:words landlock

use borsh::{BorshDeserialize, BorshSerialize, from_slice, to_vec};
use std::io::{self, ErrorKind, Read, Write};

/// Test-only behavior requested from a compiler worker.
#[cfg(feature = "test_features")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum TestAction {
    Abort,
    Timeout,
    EngineCreationFailure,
    #[cfg(target_os = "linux")]
    LandlockProbe,
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum IsolationStatus {
    LinuxLandlock { abi: u32 },
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct DaemonStatus {
    /// Hash supplied by Wasmtime for deciding whether serialized artifacts can
    /// be loaded by another engine.
    pub compiler_compatibility_hash: u64,
    pub isolation: IsolationStatus,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub enum DaemonStartup {
    Ready(DaemonStatus),
    Err(String),
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct CompileRequest {
    pub prepared_code: Vec<u8>,
    pub max_memory_pages: u32,
    #[cfg(feature = "test_features")]
    pub test_action: Option<TestAction>,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub enum CompileResponse {
    /// Followed by raw artifact frames and an empty terminating frame.
    Ok {
        artifact_size: u32,
    },
    Err(String),
}

pub fn write_frame(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    let len = u32::try_from(data.len())
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "frame length exceeds u32"))?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(data)?;
    w.flush()
}

/// Maximum size of an individual protocol frame. Compilation artifacts are
/// split into smaller frames, so this primarily bounds request and control
/// message allocation.
const MAX_FRAME_SIZE: usize = 128 * 1024 * 1024;

const ARTIFACT_CHUNK_SIZE: usize = 8 * 1024 * 1024;

pub fn read_frame(r: &mut impl Read) -> io::Result<Vec<u8>> {
    read_frame_with_limit(r, MAX_FRAME_SIZE)
}

fn read_frame_with_limit(r: &mut impl Read, max_size: usize) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > max_size {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("frame too large: {len} bytes (max {max_size})"),
        ));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

/// Write a compilation result without placing the whole artifact in one frame.
pub fn write_compile_response(w: &mut impl Write, response: Result<&[u8], &str>) -> io::Result<()> {
    match response {
        Ok(artifact) => {
            // u32 is the deliberate protocol limit for a compiled artifact's total size.
            // Allowing larger artifacts could exhaust the parent's memory if the child is buggy.
            let artifact_size = u32::try_from(artifact.len()).map_err(|_| {
                io::Error::new(ErrorKind::InvalidData, "artifact length exceeds u32")
            })?;
            write_frame(w, &to_vec(&CompileResponse::Ok { artifact_size })?)?;
            for chunk in artifact.chunks(ARTIFACT_CHUNK_SIZE) {
                write_frame(w, chunk)?;
            }
            write_frame(w, &[])
        }
        Err(err) => write_frame(w, &to_vec(&CompileResponse::Err(err.to_owned()))?),
    }
}

/// Read a chunked compilation result.
pub fn read_compile_response(r: &mut impl Read) -> io::Result<Result<Vec<u8>, String>> {
    let header = read_frame_with_limit(r, ARTIFACT_CHUNK_SIZE)?;
    let response: CompileResponse =
        from_slice(&header).map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?;
    match response {
        CompileResponse::Err(err) => Ok(Err(err)),
        CompileResponse::Ok { artifact_size } => {
            let mut artifact = Vec::new();
            let mut received = 0u32;
            loop {
                let chunk = read_frame_with_limit(r, ARTIFACT_CHUNK_SIZE)?;
                if chunk.is_empty() {
                    if received != artifact_size {
                        return Err(io::Error::new(
                            ErrorKind::InvalidData,
                            format!(
                                "artifact length mismatch: received {received} bytes, expected {artifact_size}"
                            ),
                        ));
                    }
                    return Ok(Ok(artifact));
                }
                let chunk_len = u32::try_from(chunk.len()).map_err(|_| {
                    io::Error::new(ErrorKind::InvalidData, "artifact chunk length exceeds u32")
                })?;
                received = received.checked_add(chunk_len).ok_or_else(|| {
                    io::Error::new(ErrorKind::InvalidData, "artifact length exceeds u32")
                })?;
                if received > artifact_size {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "artifact length exceeds announced size: received {received} bytes, expected {artifact_size}"
                        ),
                    ));
                }
                artifact.try_reserve(chunk.len()).map_err(|err| {
                    io::Error::other(format!("failed to allocate artifact buffer: {err}"))
                })?;
                artifact.extend_from_slice(&chunk);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ARTIFACT_CHUNK_SIZE, CompileResponse, ErrorKind, read_compile_response, to_vec,
        write_compile_response, write_frame,
    };

    #[test]
    fn compile_artifact_uses_multiple_frames() {
        let artifact = vec![0x5a; ARTIFACT_CHUNK_SIZE + 1];
        let mut wire = Vec::new();
        write_compile_response(&mut wire, Ok(&artifact)).unwrap();

        assert_eq!(read_compile_response(&mut wire.as_slice()).unwrap(), Ok(artifact));
    }

    #[test]
    fn compile_error_round_trips() {
        let mut wire = Vec::new();
        write_compile_response(&mut wire, Err("compilation failed")).unwrap();

        assert_eq!(
            read_compile_response(&mut wire.as_slice()).unwrap(),
            Err("compilation failed".to_owned())
        );
    }

    #[test]
    fn announced_artifact_size_is_enforced() {
        let mut wire = Vec::new();
        let response = CompileResponse::Ok { artifact_size: 4 };
        write_frame(&mut wire, &to_vec(&response).unwrap()).unwrap();
        write_frame(&mut wire, &[1, 2, 3]).unwrap();
        write_frame(&mut wire, &[4, 5]).unwrap();
        write_frame(&mut wire, &[]).unwrap();

        let err = read_compile_response(&mut wire.as_slice()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(
            err.to_string(),
            "artifact length exceeds announced size: received 5 bytes, expected 4"
        );
    }
}
