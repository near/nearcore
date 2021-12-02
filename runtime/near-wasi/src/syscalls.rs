#![allow(unused_variables)]

use near_vm_errors::HostError;
use near_vm_logic::{VMLogic, VMLogicError};

pub type Pointer = u32;
pub type Errno = u16;

pub const WASI_EBADF: u16 = 8; // Bad file descriptor
pub const WASI_EFAULT: u16 = 21; // Bad address
pub const WASI_EFBIG: u16 = 22; // File too large
pub const WASI_EINVAL: u16 = 28; // Invalid argument
pub const WASI_EIO: u16 = 29; // I/O Error
pub const WASI_ENOMEM: u16 = 48; // Not enough space
pub const WASI_ENXIO: u16 = 60; // No such device or address
pub const WASI_ENOTCAPABLE: u16 = 76; // WASI Extension: Capabilities insufficient

// All "wasi_snapshot_preview1" syscalls
// https://github.com/WebAssembly/WASI/blob/main/phases/snapshot/witx/wasi_snapshot_preview1.witx

/// "args_get"
/// Read command-line argument data.
pub fn args_get(vm_logic: &mut VMLogic, argv: Pointer, argv_buf: Pointer) -> Errno {
    0
}

/// "args_sizes_get"
/// Return command-line argument data sizes.
pub fn args_sizes_get(vm_logic: &mut VMLogic, argc: Pointer, argv_buf_size: Pointer) -> Errno {
    0
}

pub const CLOCK_REALTIME: u32 = 0;
pub const CLOCK_MONOTONIC: u32 = 1;
pub const CLOCK_PROCESS_CPUTIME_ID: u32 = 2;
pub const CLOCK_THREAD_CPUTIME_ID: u32 = 3;
// Other CLOCK IDs are Linux specific

fn to_errno(result: Result<(), VMLogicError>) -> Errno {
    match result {
        Ok(_) => 0,
        Err(e) => err_to_errno(e),
    }
}

fn err_to_errno(e: VMLogicError) -> Errno {
    match e {
        // Validator error or storage error, both can be considered as "physical io device error"
        VMLogicError::ExternalError(_) => WASI_EIO,
        VMLogicError::InconsistentStateError(_) => WASI_EIO,
        VMLogicError::HostError(e) => match e {
            HostError::MemoryAccessViolation => WASI_EFAULT,
            HostError::GasExceeded | HostError::GasLimitExceeded => WASI_ENOMEM,
            HostError::TotalLogLengthExceeded { .. } | HostError::NumberOfLogsExceeded { .. } => {
                WASI_EFBIG
            }
            HostError::BadUTF8 => WASI_EINVAL,
            _ => WASI_EIO,
        },
    }
}

/// "clock_res_get"
/// Get the resolution of the specified clock
pub fn clock_res_get(vm_logic: &mut VMLogic, clock_id: u32, resolution: Pointer) -> Errno {
    // Experimented in MacOS and Linux, choose lowest common multiples.
    if clock_id == CLOCK_REALTIME
        || clock_id == CLOCK_MONOTONIC
        || clock_id == CLOCK_PROCESS_CPUTIME_ID
    {
        // This can error due to run out of gas or memory access out of bound. in both case, it should
        // return control to the runner, e.g. Wasmer. Wasmer 2 does this automatically when the return
        // impls std::error::Error. However, here Errno is just a type alias and cannot impls Error trait/
        // So this must return an error code and let Wasm side code handle it. If it's the case memory
        // access out of bounds, caller received WASI_EFAULT knows how to handle it. If it's the case of
        // run out of gas, it will return WASI_ENOMEM, and Wasm side caller will try to handle this error,
        // and it will hit out of gas error in next injected `gas()` call.
        to_errno(vm_logic.memory_set_u64(resolution as u64, 1000))
    } else if clock_id == CLOCK_THREAD_CPUTIME_ID {
        to_errno(vm_logic.memory_set_u64(resolution as u64, 42))
    } else {
        WASI_EINVAL
    }
}

/// "clock_time_get"
/// Get the time of the specified clock
pub fn clock_time_get(
    vm_logic: &mut VMLogic,
    clock_id: u32,
    precision: u64,
    time: Pointer,
) -> Errno {
    if clock_id == CLOCK_REALTIME {
        match vm_logic.block_timestamp() {
            Ok(t) => vm_logic.memory_set_u64(time as u64, t).map_or(WASI_EINVAL, |_| 0),
            Err(e) => err_to_errno(e),
        }
    } else {
        WASI_EINVAL
    }
}

/// "environ_get"
/// Read environment variable data.
pub fn environ_get(vm_logic: &mut VMLogic, environ: Pointer, environ_buf: Pointer) -> Errno {
    WASI_ENOTCAPABLE
}

/// "environ_sizes_get"
/// Return command-line argument data sizes.
pub fn environ_sizes_get(
    vm_logic: &mut VMLogic,
    environ_count: Pointer,
    environ_buf_size: Pointer,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_advise"
/// Advise the system about how a file will be used
pub fn fd_advise(vm_logic: &mut VMLogic, fd: u32, offset: u64, len: u64, advice: u8) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_allocate"
/// Allocate extra space for a file descriptor
pub fn fd_allocate(vm_logic: &mut VMLogic, fd: u32, offset: u64, len: u64) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_close"
/// Close an open file descriptor
pub fn fd_close(vm_logic: &mut VMLogic, fd: u32) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_datasync"
/// Synchronize the file data to disk
pub fn fd_datasync(vm_logic: &mut VMLogic, fd: u32) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_fdstat_get"
/// Get metadata of a file descriptor
pub fn fd_fdstat_get(vm_logic: &mut VMLogic, fd: u32, buf_ptr: Pointer) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_fdstat_set_flags"
/// Set file descriptor flags for a file descriptor
pub fn fd_fdstat_set_flags(vm_logic: &mut VMLogic, fd: u32, flags: u16) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_fdstat_set_rights"
/// Set the rights of a file descriptor.  This can only be used to remove rights
pub fn fd_fdstat_set_rights(
    vm_logic: &mut VMLogic,
    fd: u32,
    fs_rights_base: u64,
    fs_rights_inheriting: u64,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_filestat_get"
/// Get the metadata of an open file
pub fn fd_filestat_get(vm_logic: &mut VMLogic, fd: u32, buf: Pointer) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_filestat_set_size"
/// Change the size of an open file, zeroing out any new bytes
pub fn fd_filestat_set_size(vm_logic: &mut VMLogic, fd: u32, st_size: u64) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_filestat_set_times"
/// Set timestamp metadata on a file
pub fn fd_filestat_set_times(
    vm_logic: &mut VMLogic,
    fd: u32,
    st_atim: u64,
    st_mtim: u64,
    fst_flags: u16,
) -> Errno {
    WASI_ENOTCAPABLE
}

use std::io::Read;
fn read_stdin(
    vm_logic: &mut VMLogic,
    iovs: Pointer,
    iovs_len: u32,
    offset: u64,
    nread: Pointer,
) -> Result<(), Errno> {
    let mut bytes_read = 0;
    let mut raw_bytes: Vec<u8> = vec![0; 1024];
    let mut iovs_arr = vec![];

    let input = vm_logic.internal_input();
    let mut input_slice = if offset < input.len() as u64 {
        &input[(offset as usize)..]
    } else {
        // POSIX read use lseek to read from offset, which returns ENXIO when the file offset is beyond
        // the end of the file.
        return Err(WASI_ENXIO);
    };

    for i in 0..(iovs_len / 2) {
        let buf = vm_logic.memory_get_u32((iovs + i * 8) as u64).map_err(err_to_errno)?;
        let buf_len = vm_logic.memory_get_u32((iovs + i * 8 + 4) as u64).map_err(err_to_errno)?;
        iovs_arr.push(WasiIovec { buf, buf_len })
    }

    for iov in iovs_arr {
        raw_bytes.clear();
        raw_bytes.resize(iov.buf_len as usize, 0);

        bytes_read += input_slice.read(&mut raw_bytes).expect("Read from a slice will never fail");
        vm_logic.memory_set_slice(iov.buf as u64, &raw_bytes).map_err(err_to_errno)?;
    }
    vm_logic.memory_set_u64(nread as u64, bytes_read as u64).map_err(err_to_errno)
}

pub const WASI_STDIN_FILENO: u32 = 0;
pub const WASI_STDOUT_FILENO: u32 = 1;

/// "fd_pread"
/// Read from the file at the given offset without updating the file cursor.
pub fn fd_pread(
    vm_logic: &mut VMLogic,
    fd: u32,
    iovs: Pointer,
    iovs_len: u32,
    offset: u64,
    nread: Pointer,
) -> Errno {
    // we can only read from vm_logic.input(), which considered as stdin
    if fd != WASI_STDIN_FILENO {
        return WASI_EBADF;
    }

    read_stdin(vm_logic, iovs, iovs_len, offset, nread).map_or_else(|e| e, |_| 0)
}

#[repr(C)]
pub struct WasiIovec {
    pub buf: u32,
    pub buf_len: u32,
}

/// "fd_prestat_get"
/// Get metadata about a preopened file descriptor
pub fn fd_prestat_get(vm_logic: &mut VMLogic, fd: u32, buf: Pointer) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_prestat_dir_name"
/// Get dirname a preopened file descriptor of a directory
pub fn fd_prestat_dir_name(vm_logic: &mut VMLogic, fd: u32, path: Pointer, path_len: u32) -> Errno {
    WASI_ENOTCAPABLE
}

fn write_stdout(
    vm_logic: &mut VMLogic,
    iovs: Pointer,
    iovs_len: u32,
    nwritten: Pointer,
) -> Result<(), Errno> {
    let mut bytes_written = 0;

    for i in 0..(iovs_len / 2) {
        let buf = vm_logic.memory_get_u32((iovs + i * 8) as u64).map_err(err_to_errno)?;
        let buf_len = vm_logic.memory_get_u32((iovs + i * 8 + 4) as u64).map_err(err_to_errno)?;
        let bytes = vm_logic.log_utf8(buf as u64, buf_len as u64).map_err(err_to_errno)?;
        bytes_written += buf_len;
    }

    vm_logic.memory_set_u64(nwritten as u64, bytes_written as u64).map_err(err_to_errno)
}

/// "fd_pwrite"
/// Write to a file without adjusting its offset
pub fn fd_pwrite(
    vm_logic: &mut VMLogic,
    fd: u32,
    iovs: Pointer,
    iovs_len: u32,
    offset: u64,
    nwritten: Pointer,
) -> Errno {
    if fd == WASI_STDOUT_FILENO {
        return WASI_EBADF;
    }

    write_stdout(vm_logic, iovs, iovs_len, nwritten).map_or_else(|e| e, |_| 0)
}

/// "fd_read"
/// Read data from file descriptor
pub fn fd_read(
    vm_logic: &mut VMLogic,
    fd: u32,
    iovs: Pointer,
    iovs_len: u32,
    nread: Pointer,
) -> Errno {
    // we can only read from vm_logic.input(), which considered as stdin
    if fd != WASI_STDIN_FILENO {
        return WASI_EBADF;
    }

    read_stdin(vm_logic, iovs, iovs_len, 0, nread).map_or_else(|e| e, |_| 0)
}

/// "fd_readdir"
/// Read data from directory specified by file descriptor
pub fn fd_readdir(
    vm_logic: &mut VMLogic,
    fd: u32,
    buf: Pointer,
    buf_len: u32,
    cookie: u64,
    bufused: Pointer,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_renumber"
/// Atomically copy file descriptor
pub fn fd_renumber(vm_logic: &mut VMLogic, from: u32, to: u32) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_seek"
/// Update file descriptor offset
pub fn fd_seek(
    vm_logic: &mut VMLogic,
    fd: u32,
    offset: i64,
    whence: u8,
    newoffset: Pointer,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_sync"
/// Synchronize file and metadata to disk
pub fn fd_sync(vm_logic: &mut VMLogic, fd: u32) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_tell"
/// Get the offset of the file descriptor
pub fn fd_tell(vm_logic: &mut VMLogic, fd: u32, offset: Pointer) -> Errno {
    WASI_ENOTCAPABLE
}

/// "fd_write"
/// Write data to the file descriptor
pub fn fd_write(
    vm_logic: &mut VMLogic,
    fd: u32,
    iovs: Pointer,
    iovs_len: u32,
    nwritten: Pointer,
) -> Errno {
    if fd == WASI_STDOUT_FILENO {
        return WASI_EBADF;
    }

    write_stdout(vm_logic, iovs, iovs_len, nwritten).map_or_else(|e| e, |_| 0)
}

/// "path_create_directory"
/// Create Directory at a path
pub fn path_create_directory(
    vm_logic: &mut VMLogic,
    fd: u32,
    path: Pointer,
    path_len: u32,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "path_filestat_get"
/// Access metadata about a file or directory
pub fn path_filestat_get(
    vm_logic: &mut VMLogic,
    fd: u32,
    flags: u32,
    path: Pointer,
    path_len: u32,
    buf: Pointer,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "path_filestat_set_times"
/// Update time metadata on a file or directory
pub fn path_filestat_set_times(
    vm_logic: &mut VMLogic,
    fd: u32,
    flags: u32,
    path: Pointer,
    path_len: u32,
    st_atim: u64,
    st_mtim: u64,
    fst_flags: u16,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "path_link"
/// Create a hard link
pub fn path_link(
    vm_logic: &mut VMLogic,
    old_fd: u32,
    old_flags: u32,
    old_path: Pointer,
    old_path_len: u32,
    new_fd: u32,
    new_path: Pointer,
    new_path_len: u32,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "path_open"
/// Open file located at the given path
pub fn path_open(
    vm_logic: &mut VMLogic,
    dirfd: u32,
    dirflags: u32,
    path: Pointer,
    path_len: u32,
    o_flags: u16,
    fs_rights_base: u64,
    fs_rights_inheriting: u64,
    fs_flags: u16,
    fd: Pointer,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "path_readlink"
/// Open file located at the given path
pub fn path_readlink(
    vm_logic: &mut VMLogic,
    dirfd: u8,
    path: Pointer,
    path_len: u32,
    buf: Pointer,
    buf_len: u32,
    buf_used: Pointer,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "path_remove_directory"
/// Remove the directory if it's empty
pub fn path_remove_directory(
    vm_logic: &mut VMLogic,
    fd: u32,
    path: Pointer,
    path_len: u32,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "path_rename"
/// Rename a file or directory
pub fn path_rename(
    vm_logic: &mut VMLogic,
    old_fd: u32,
    old_path: Pointer,
    old_path_len: u32,
    new_fd: u32,
    new_path: Pointer,
    new_path_len: u32,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "path_symlink"
/// Create a symlink
pub fn path_symlink(
    vm_logic: &mut VMLogic,
    old_path: Pointer,
    old_path_len: u32,
    fd: u32,
    new_path: Pointer,
    new_path_len: Pointer,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "path_unlink_file"
/// Unlink a file, deleting if the number of hardlinks is 1
pub fn path_unlink_file(vm_logic: &mut VMLogic, fd: u32, path: Pointer, path_len: u32) -> Errno {
    WASI_ENOTCAPABLE
}

/// "poll_oneoff"
/// Concurrently poll for a set of events
pub fn poll_oneoff(
    vm_logic: &mut VMLogic,
    in_: Pointer,
    out_: Pointer,
    nsubscriptions: u32,
    nevents: Pointer,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "proc_exit"
/// Exit the process
pub fn proc_exit(vm_logic: &mut VMLogic, code: u32) {
    // We have no way to let the wasmer or runtime exit here, GuestPanic will be ignored since this
    // function doesn't return std::error::Error to trap the runtime. So it's just noop.
}

/// "proc_raise"
/// Raise the given signal
pub fn proc_raise(vm_logic: &mut VMLogic, sig: u8) -> Errno {
    WASI_ENOTCAPABLE
}

/// "random_get"
/// Fill buffer with high-quality random data.  This function may be slow and block
pub fn random_get(vm_logic: &mut VMLogic, buf: Pointer, buf_len: u32) -> Errno {
    let near_seed = vm_logic.internal_random_seed();
    let mut seed = vec![0u8; buf_len as usize];
    for i in 0..buf_len {
        seed[i as usize] = near_seed[i as usize % near_seed.len()];
    }
    vm_logic.memory_set_slice(buf as u64, &seed).map_or(0, |_| WASI_EFAULT)
}

/// "sched_yield"
/// Yields execution of the thread
pub fn sched_yield(vm_logic: &mut VMLogic) -> Errno {
    // This is meaningless in single thread runtime and shouldn't let host yield execution of
    // the runtime. So just noop.
    0
}

/// "sock_recv"
/// Receive a message from a socket
pub fn sock_recv(
    vm_logic: &mut VMLogic,
    sock: u32,
    ri_data: Pointer,
    ri_data_len: u32,
    ri_flags: u16,
    ro_datalen: Pointer,
    ro_flags: Pointer,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "sock_send"
/// Send a message on a socket
pub fn sock_send(
    vm_logic: &mut VMLogic,
    sock: u32,
    si_data: Pointer,
    si_data_len: u32,
    si_flags: u16,
    so_datalen: Pointer,
) -> Errno {
    WASI_ENOTCAPABLE
}

/// "sock_shutdown"
pub fn sock_shutdown(vm_logic: &mut VMLogic, sock: u32, how: u8) -> Errno {
    WASI_ENOTCAPABLE
}
