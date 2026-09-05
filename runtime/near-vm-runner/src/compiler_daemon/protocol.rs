//! IPC protocol: length-prefixed borsh frames over stdin/stdout.

use borsh::{BorshDeserialize, BorshSerialize, from_slice, to_vec};
use std::borrow::Cow;
use std::io::{self, ErrorKind, Read, Write};

/// Environment contract used by the parent to configure a worker process.
pub const COMPILER_DAEMON_THREADS_ENV: &str = "NEAR_COMPILER_DAEMON_THREADS";
pub const COMPILER_DAEMON_STACK_SIZE_ENV: &str = "NEAR_COMPILER_DAEMON_STACK_SIZE_BYTES";

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct WorkerConfig {
    pub threads: u32,
    pub thread_stack_size_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct DaemonStatus {
    /// Hash supplied by Wasmtime for deciding whether serialized artifacts can
    /// be loaded by another engine.
    pub compiler_compatibility_hash: u64,
    pub isolation: IsolationStatus,
    /// Effective worker settings, echoed so the parent can verify that the
    /// child implementation honored its process configuration.
    pub worker_config: WorkerConfig,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub enum DaemonStartup {
    Ready(DaemonStatus),
    Err(String),
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct CompileRequest<'a> {
    pub prepared_code: Cow<'a, [u8]>,
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

/// Maximum serialized size of a compilation error response.
const MAX_COMPILE_ERROR_SIZE: usize = 1024 * 1024;

pub fn read_frame(r: &mut impl Read) -> io::Result<Vec<u8>> {
    read_frame_with_limit(r, MAX_FRAME_SIZE)
}

fn read_frame_length(r: &mut impl Read, max_size: usize) -> io::Result<usize> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > max_size {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("frame too large: {len} bytes (max {max_size})"),
        ));
    }
    Ok(len)
}

fn read_frame_with_limit(r: &mut impl Read, max_size: usize) -> io::Result<Vec<u8>> {
    let len = read_frame_length(r, max_size)?;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

/// Append one frame directly to `destination`, avoiding an intermediate frame
/// allocation. The caller reserves capacity for the complete artifact first.
fn read_frame_into(
    r: &mut impl Read,
    destination: &mut Vec<u8>,
    max_frame_size: usize,
    max_total_size: usize,
) -> io::Result<usize> {
    let frame_len = read_frame_length(r, max_frame_size)?;
    let old_len = destination.len();
    let new_len = old_len.checked_add(frame_len).ok_or_else(|| {
        io::Error::new(ErrorKind::InvalidData, "artifact length exceeds address space")
    })?;
    if new_len > max_total_size {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "artifact length exceeds announced size: received {new_len} bytes, expected {max_total_size}"
            ),
        ));
    }
    destination.resize(new_len, 0);
    if let Err(err) = r.read_exact(&mut destination[old_len..]) {
        destination.truncate(old_len);
        return Err(err);
    }
    Ok(frame_len)
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
    let header = read_frame_with_limit(r, MAX_COMPILE_ERROR_SIZE)?;
    let response: CompileResponse =
        from_slice(&header).map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?;
    match response {
        CompileResponse::Err(err) => Ok(Err(err)),
        CompileResponse::Ok { artifact_size } => {
            let artifact_size = artifact_size as usize;
            let mut artifact = Vec::new();
            artifact.try_reserve_exact(artifact_size).map_err(|err| {
                io::Error::other(format!("failed to allocate artifact buffer: {err}"))
            })?;
            loop {
                let chunk_len =
                    read_frame_into(r, &mut artifact, ARTIFACT_CHUNK_SIZE, artifact_size)?;
                if chunk_len == 0 {
                    if artifact.len() != artifact_size {
                        return Err(io::Error::new(
                            ErrorKind::InvalidData,
                            format!(
                                "artifact length mismatch: received {} bytes, expected {artifact_size}",
                                artifact.len()
                            ),
                        ));
                    }
                    return Ok(Ok(artifact));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ARTIFACT_CHUNK_SIZE, CompileResponse, ErrorKind, MAX_COMPILE_ERROR_SIZE,
        read_compile_response, to_vec, write_compile_response, write_frame,
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
    fn compile_error_size_is_bounded() {
        let wire = Vec::from(((MAX_COMPILE_ERROR_SIZE + 1) as u32).to_le_bytes());

        let err = read_compile_response(&mut wire.as_slice()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
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
