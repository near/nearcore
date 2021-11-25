#[allow(unused_variables)]

use near_vm_logic::VMLogic;

type Pointer<T> = u32;
type Errno = u16;

const WASI_EBADF: u16 = 8;
const WASI_ENOTSUP: u16 = 58;

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

pub struct Unused;

/// "fd_fdstat_get"
/// Get metadata of a file descriptor
pub fn fd_fdstat_get(vm_logic: &mut VMLogic, fd: u32, buf_ptr: Pointer<Unused>) -> Errno {
    WASI_EBADF
}

/// "fd_fdstat_set_flags"
/// Set file descriptor flags for a file descriptor
pub fn fd_fdstat_set_flags(vm_logic: &mut VMLogic, fd: u32, flags: u16) -> Errno {
    WASI_EBADF
}

/// "fd_fdstat_set_rights"
/// Set the rights of a file descriptor.  This can only be used to remove rights
pub fn fd_fstat_set_rights(vm_logic: &mut VMLogic, fd: u32, fs_rights_base: u64, fs_rights_inheriting: u64) -> Errno {
    WASI_EBADF
}

/// "fd_filestat_get"
/// Get the metadata of an open file
pub fn fn_filestat_get(vm_logic: &mut VMLogic, fd: u32, buf: Pointer<Unused>) -> Errno {
    WASI_EBADF
}

/// "fd_filestat_set_size"
/// Change the size of an open file, zeroing out any new bytes
pub fn fd_filestat_set_size(vm_logic: &mut VMLogic, fd: u32, st_size: u64) -> Errno {
    WASI_EBADF
}

/// "fd_filestat_set_times"
/// Set timestamp metadata on a file
pub fn fd_filestat_set_times(vm_logic: &mut VMLogic, fd: u32, st_atim: u64, st_mtim: u64, fst_flags: u16) -> Errno {
    WASI_EBADF
}

/// "fd_pread"
/// Read from the file at the given offset without updating the file cursor.
pub fn fd_pread(vm_logic: &mut VMLogic, fd: u32, iovs: Pointer<Unused>, iovs_len: u32, offset: u64, nread: Pointer<u32>) -> Errno {
    WASI_EBADF
}

/// "fd_prestat_get"
/// Get metadata about a preopened file descriptor
pub fn fd_prestat_get(vm_logic: &mut VMLogic, fd: u32, buf: Pointer<Unused>) -> Errno {
    WASI_EBADF
}

/// "fd_prestat_dir_name"
/// Get dirname a preopened file descriptor of a directory
pub fn fd_prestat_dir_name(vm_logic: &mut VMLogic, fd: u32, path: Pointer<u8>, path_len: u32) -> Errno {
    WASI_EBADF
}

/// "fd_pwrite"
/// Write to a file without adjusting its offset
pub fn fd_pwrite(vm_logic: &mut VMLogic, fd: u32, iovs: Pointer<Unused>, iovs_len: u32, offset: u64, nwritten: Pointer<u32>) -> Errno {
    // TODO: if write to stderr, can be considered as vm_logic.log()
    WASI_EBADF
}

/// "fd_read"
/// Read data from file descriptor
pub fn fd_read(vm_logic: &mut VMLogic, fd: u32, iovs: Pointer<Unused>, iovs_len: u32, nread: Pointer<u32>) -> Errno {
    WASI_EBADF
}

/// "fd_readdir"
/// Read data from directory specified by file descriptor
pub fn fd_readdir(vm_logic: &mut VMLogic, fd: u32, buf: WasmPtr<u8>, buf_len: u32, cookie: u64, bufused: Pointer<u32>) -> Errno {
    WASI_EBADF
}

/// "fd_renumber"
/// Atomically copy file descriptor
pub fn fd_renumber(vm_logic: &mut VMLogic, from: u32, to: u32) -> Errno {
    WASI_EBADF
}

/// "fd_seek"
/// Update file descriptor offset
pub fn fd_seek(vm_logic: &mut VMLogic, offset: i64, whence: u8, newoffset: Pointer<u64>) -> Errno {
    WASI_EBADF
}

/// "fd_sync"
/// Synchronize file and metadata to disk (TODO: expand upon what this means in our system)
pub fn fd_sync(vm_logic: &mut VMLogic, fd: u32) -> Errno {
    WASI_EBADF
}

/// "fd_tell"
/// Get the offset of the file descriptor
pub fn fd_tell(vm_logic: &mut VMLogic, fd: u32, offset: Pointer<u64>) -> Errno {
    WASI_EBADF
}

/// "fd_write"
/// Write data to the file descriptor
pub fn fd_write(vm_logic: &mut VMLogic, fd: u32, iovs: Pointer<Unused>, iovs_len: u32, nwritten: Pointer<u32>) -> Errno {
    WASI_EBADF
}

/// "path_create_directory"
/// Create Directory at a path
pub fn path_create_directory(vm_logic: &mut VMLogic, fd: u32, path: Pointer<u8>, path_len: u32) -> Errno {
    WASI_EBADF
}

/// "path_filestat_get"
/// Access metadata about a file or directory
pub fn path_filestat_get(vm_logic: &mut VMLogic, fd: u32, flags: u32, path: Pointer<u8>, path_len: u32, buf: Pointer<Unused>) -> Errno {
    WASI_EBADF
}

/// "path_filestat_set_times"
/// Update time metadata on a file or directory
pub fn path_filestat_set_times(vm_logic: &mut VMLogic, fd: u32, flags: u32, path: Pointer<u8>, path_len: u32, st_atim: u64, st_mtim: u64, fst_flags: u16) -> Errno {
    WASI_EBADF
}

/// "path_link"
/// Create a hard link
pub fn path_link(vm_logic: &mut VMLogic, old_fd: u32, old_flags: u32, old_path: Pointer<u8>, old_path_len: u32, new_fd: u32, new_path: Pointer<u8>, new_path_len: u32) -> Errno {
    WASI_EBADF
}

/// "path_open"
/// Open file located at the given path
pub fn path_open(vm_logic: &mut VMLogic, dirfd: u32, dirflags: u32, path: Pointer<u8>, path_len: u32, o_flags: u16, fs_rights_base: u64, fs_rights_inheriting: u64, fs_flags: u16, fd: Pointer<u32>) -> Errno {
    WASI_EBADF
}

/// "path_readlink"
/// Open file located at the given path
pub fn path_readlink(vm_logic: &mut VMLogic, dirfd: u8, dirflags: u32, path: Pointer<u8>, path_len: u32, o_flags: u16, fs_rights_base: u64, fs_rights_inheriting: u64, fs_flags: u16, fd: Pointer<u32>) -> Errno {
    WASI_EBADF
}

/// "path_remove_directory"
/// Remove the directory if it's empty
pub fn path_remove_directory(vm_logic: &mut VMLogic, fd: u32, path: Pointer<u8>, path_len: u32) -> Errno {
    WASI_EBADF
}

/// "path_rename"
/// Rename a file or directory
pub fn path_rename(vm_logic: &mut VMLogic, old_fd: u32, old_path: Pointer<u8>, old_path_len: u32, new_fd: u32, new_path: Pointer<u8>, new_path_len: u32) -> Errno {
    WASI_EBADF
}

/// "path_symlink"
/// Create a symlink
pub fn path_symlink(vm_logic: &mut VMLogic, old_path: Pointer<u8>, old_path_len: u32, fd: u32, new_path: Pointer<u8>, new_path_len: Pointer<u8>) -> Errno {
    WASI_EBADF
}

/// "path_unlink_file"
/// Unlink a file, deleting if the number of hardlinks is 1
pub fn path_unlink_file(vm_logic: &mut VMLogic, fd: u32, path: Pointer<u8>, path_len: u32) -> Errno {
    WASI_EBADF
}

/// "poll_oneoff"
/// Concurrently poll for a set of events
pub fn poll_oneoff(vm_logic: &mut VMLogic, in_: Pointer<Unused>, Pointer<Unused>, nsubscriptions: u32, nevents: Pointer<u32>) -> Errno {
    WASI_EBADF
}

/// "proc_exit"
/// Exit the process
pub fn proc_exit(vm_logic: &mut VMLogic, code: u32) {}

/// "proc_raise"
/// Raise the given signal
pub fn proc_raise(vm_logic: &mut VMLogic, sig: u8) {}

/// "random_get"
/// Fill buffer with high-quality random data.  This function may be slow and block
pub fn random_get(vm_logic: &mut VMLogic, buf: Pointer<Unused>, buf_len: u32) -> Errno {
    // TODO: use near's random seed
    WASI_EBADF
}

/// "sched_yield"
/// Yields execution of the thread
pub fn sched_yield(vm_logic: &mut VMLogic) -> Errno {
    // TODO: use std::thread::yield_now()?
    0
}

/// "sock_recv"
/// Receive a message from a socket
pub fn sock_recv(vm_logic: &mut VMLogic, sock: u32, ri_data: Pointer<Unused>, ri_data_len: u32, ri_flags: u16, ro_datalen: Pointer<<u32>, ro_flags: Pointer<u16>) -> Errno {
    WASI_ENOTSUP
}

/// "sock_send"
/// Send a message on a socket
pub fn sock_send(vm_logic: &mut VMLogic, sock: u32, si_data: Pointer<Unused>, si_data_len: u32, si_flags: u16, so_datalen: Pointer<u32>) -> Errno {
    WASI_ENOTSUP
}

/// "sock_shutdown"
pub fn sock_shutdown(vm_logic: &mut VMLogic, sock: u32, how: u8) -> Errno {
    WASI_ENOTSUP
}
