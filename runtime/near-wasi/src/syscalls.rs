#[allow(unused_variables)]

use near_vm_logic::VMLogic;

type Pointer<T> = u32;
type Errno = u16;

const WASI_EBADF: u16 = 8;

// All "wasi_snapshot_preview1" syscalls
// https://github.com/WebAssembly/WASI/blob/main/phases/snapshot/witx/wasi_snapshot_preview1.witx

/// "args_get"
/// Read command-line argument data.
pub fn args_get(vm_logic: &mut VMLogic, argv: Pointer<Pointer<u8>>, argv_buf: Pointer<u8>) -> Errno {
    0
}

/// "args_sizes_get"
/// Return command-line argument data sizes.
pub fn args_sizes_get(vm_logic: &mut VMLogic, argc: Pointer<u32>, argv_buf_size: Pointer<u32>) -> Errno {
    0
}

/// "clock_res_get"
/// Get the resolution of the specified clock
pub fn clock_res_get(vm_logic: &mut VMLogic, clock_id: u32, resolution: Pointer<u64>) -> Errno {
    // vm_logic.memory_set_u64(resolution as u64, TODO: what to put, 1ns in c time ns? check several host systems)
    0
}

/// "clock_time_get"
/// Get the time of the specified clock
pub fn clock_time_get(vm_logic: &mut VMLogic, clock_id: u32, precision: u64, time: Pointer<u64>) -> Errno {
    // TODO: use current block timestamp
    0
}

/// "environ_get"
/// Read environment variable data.
pub fn environ_get(vm_logic: &mut VMLogic, environ: Pointer<Pointer<u8>>, environ_buf: Pointer<u8>) -> Errno {
    0
}

/// "environ_sizes_get"
/// Return command-line argument data sizes.
pub fn environ_sizes_get(vm_logic: &mut VMLogic, environ_count: Pointer<u32>, environ_buf_size: Pointer<u32>) -> Errno {
    0
}

/// "fd_advise"
/// Advise the system about how a file will be used
pub fn fd_advise(vm_logic: &mut VMLogic, fd: u32, offset: u64, len: u64, advice: u8) -> Errno {
    WASI_EBADF
}

/// "fd_allocate"
/// Allocate extra space for a file descriptor
pub fn fd_allocate(vm_logic: &mut VMLogic, fd: u32, offset: u64, len: u64) -> Errno {
    WASI_EBADF
}

/// "fd_close"
/// Close an open file descriptor
pub fn fd_close(vm_logic: &mut VMLogic, fd: u32) -> Errno {
    WASI_EBADF
}

/// "fd_datasync"
/// Synchronize the file data to disk
pub fn fd_datasync(vm_logic: &mut VMLogic, fd: u32) -> Errno {
    WASI_EBADF
}

// "fd_fdstat_get"
// "fd_fdstat_set_flags"
// "fd_fdstat_set_rights"
// "fd_filestat_get"
// "fd_filestat_set_size"
// "fd_filestat_set_times"
// "fd_pread"
// "fd_prestat_get"
// "fd_prestat_dir_name"
// "fd_pwrite"
// "fd_read"
// "fd_readdir"
// "fd_renumber"
// "fd_seek"
// "fd_sync"
// "fd_tell"
// "fd_write"
// "path_create_directory"
// "path_filestat_get"
// "path_filestat_set_times"
// "path_link"
// "path_open"
// "path_readlink"
// "path_remove_directory"
// "path_rename"
// "path_symlink"
// "path_unlink_file"
// "poll_oneoff"
// "proc_exit"
// "proc_raise"
// "random_get"
// "sched_yield"
// "sock_recv"
// "sock_send"
// "sock_shutdown"
