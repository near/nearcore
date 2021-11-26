mod syscalls;

use near_vm_logic::VMLogic;
use std::ffi::c_void;

// TODO: to avoid cyclic reference, cannot use ImportReference and NearWasmerEnv from near-vm-runner,
// refactor them to a common depenency, or near-vm-runner use the def from near-wasi
#[derive(Clone, Copy)]
pub struct ImportReference(pub *mut c_void);
unsafe impl Send for ImportReference {}
unsafe impl Sync for ImportReference {}

use wasmer::{Memory, WasmerEnv};

#[derive(WasmerEnv, Clone)]
pub struct NearWasmerEnv {
    pub memory: Memory,
    pub logic: ImportReference,
}

macro_rules! generate_import_near_wasi {
    ($($func:ident ( $( $arg_name:ident : $arg_type:ident ),* ) $(-> $returns:ident )?  ; )*) => {
        pub mod wasmer2_ext {
            use crate::syscalls::{Pointer, Errno};
            use crate::NearWasmerEnv;
            use near_vm_logic::VMLogic;
        $(
            #[allow(unused_parens)]
            pub fn $func(env: &NearWasmerEnv, $( $arg_name: $arg_type ),* ) $(->  $returns)? {
                let logic: &mut VMLogic = unsafe { &mut *(env.logic.0 as *mut VMLogic<'_>) };
                crate::syscalls::$func( logic, $($arg_name, )* )
            }
        )*
        }

        pub fn generate_import_wasmer2(store: &wasmer::Store, memory: wasmer::Memory, logic: &mut VMLogic<'_>) -> wasmer::ImportObject {
            let mut import_object = wasmer::ImportObject::new();
            let mut namespace = wasmer::Exports::new();
            let env = NearWasmerEnv {logic: ImportReference(logic as * mut _ as * mut c_void), memory: memory.clone()};
            $(
            namespace.insert(stringify!($func), wasmer::Function::new_native_with_env(&store, env.clone(), wasmer2_ext::$func));
            )*
            import_object.register("wasi_snapshot_preview1", namespace);
            import_object
        }
    }
}


generate_import_near_wasi! {
    args_get(argv: Pointer, argv_buf: Pointer) -> Errno;
    args_sizes_get(argc: Pointer, argv_buf_size: Pointer) -> Errno;
    clock_res_get(clock_id: u32, resolution: Pointer) -> Errno;
    clock_time_get(clock_id: u32, precision: u64, time: Pointer) -> Errno;
    environ_get(environ: Pointer, environ_buf: Pointer) -> Errno;
    environ_sizes_get(environ_count: Pointer, environ_buf_size: Pointer) -> Errno;
    fd_advise(fd: u32, offset: u64, len: u64, advice: u8) -> Errno;
    fd_allocate(fd: u32, offset: u64, len: u64) -> Errno;
    fd_close(fd: u32) -> Errno;
    fd_datasync(fd: u32) -> Errno;
    fd_fdstat_get(fd: u32, buf_ptr: Pointer) -> Errno;
    fd_fdstat_set_flags(fd: u32, flags: u16) -> Errno;
    fd_fdstat_set_rights(fd: u32, fs_rights_base: u64, fs_rights_inheriting: u64) -> Errno;
    fd_filestat_get(fd: u32, buf: Pointer) -> Errno;
    fd_filestat_set_size(fd: u32, st_size: u64) -> Errno;
    fd_filestat_set_times(fd: u32, st_atim: u64, st_mtim: u64, fst_flags: u16) -> Errno;
    fd_pread(fd: u32, iovs: Pointer, iovs_len: u32, offset: u64, nread: Pointer) -> Errno;
    fd_prestat_get(fd: u32, buf: Pointer) -> Errno;
    fd_prestat_dir_name(fd: u32, path: Pointer, path_len: u32) -> Errno;
    fd_pwrite(fd: u32, iovs: Pointer, iovs_len: u32, offset: u64, nwritten: Pointer) -> Errno;
    fd_read(fd: u32, iovs: Pointer, iovs_len: u32, nread: Pointer) -> Errno;
    fd_readdir(fd: u32, buf: Pointer, buf_len: u32, cookie: u64, bufused: Pointer) -> Errno;
    fd_renumber(from: u32, to: u32) -> Errno;
    fd_seek(fd: u32, offset: i64, whence: u8, newoffset: Pointer) -> Errno;
    fd_sync(fd: u32) -> Errno;
    fd_tell(fd: u32, offset: Pointer) -> Errno;
    fd_write(fd: u32, iovs: Pointer, iovs_len: u32, nwritten: Pointer) -> Errno;
    path_create_directory(fd: u32, path: Pointer, path_len: u32) -> Errno;
    path_filestat_get(fd: u32, flags: u32, path: Pointer, path_len: u32, buf: Pointer) -> Errno;
    path_filestat_set_times(fd: u32, flags: u32, path: Pointer, path_len: u32, st_atim: u64, st_mtim: u64, fst_flags: u16) -> Errno;
    path_link(old_fd: u32, old_flags: u32, old_path: Pointer, old_path_len: u32, new_fd: u32, new_path: Pointer, new_path_len: u32) -> Errno;
    path_open(dirfd: u32, dirflags: u32, path: Pointer, path_len: u32, o_flags: u16, fs_rights_base: u64, fs_rights_inheriting: u64, fs_flags: u16, fd: Pointer) -> Errno;
    path_readlink(dirfd: u8, path: Pointer, path_len: u32, buf: Pointer, buf_len: u32, buf_used: Pointer) -> Errno;
    path_remove_directory(fd: u32, path: Pointer, path_len: u32) -> Errno;
    path_rename(old_fd: u32, old_path: Pointer, old_path_len: u32, new_fd: u32, new_path: Pointer, new_path_len: u32) -> Errno;
    path_symlink(old_path: Pointer, old_path_len: u32, fd: u32, new_path: Pointer, new_path_len: Pointer) -> Errno;
    path_unlink_file(fd: u32, path: Pointer, path_len: u32) -> Errno;
    poll_oneoff(in_: Pointer, out_: Pointer, nsubscriptions: u32, nevents: Pointer) -> Errno;
    proc_exit(code: u32);
    proc_raise(sig: u8) -> Errno;
    random_get(buf: Pointer, buf_len: u32) -> Errno;
    sched_yield() -> Errno;
    sock_recv(sock: u32, ri_data: Pointer, ri_data_len: u32, ri_flags: u16, ro_datalen: Pointer, ro_flags: Pointer) -> Errno;
    sock_send(sock: u32, si_data: Pointer, si_data_len: u32, si_flags: u16, so_datalen: Pointer) -> Errno;
    sock_shutdown(sock: u32, how: u8) -> Errno;
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
