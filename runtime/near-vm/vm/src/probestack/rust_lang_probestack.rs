// cspell:words rustlang uefi UEFI fortanix pushsection popsection globl startproc endproc pushq rvalues stackprobe
// This file is copied from [rust-lang/compiler-builtins] with a few changes:
//
// 1) rename `__rust_probestack` to `__private_rust_probestack` to avoid any
//    potential linker conflicts with older Rust versions that still have the symbol.
// 2) remove cfg branches for architectures: "uefi", ("sgx" + "fortanix")
// 3) remove the `#![cfg(not(feature = "mangled-names"))]` because we don't have that feature
//
// This is a temporary solution to facilitate the Rust toolchain upgrade. Once
// protocol versions with NearVM are no longer supported, this file can go.
//
// [rust-lang/compiler-builtins]:
//     https://github.com/rust-lang/compiler-builtins/blob/compiler_builtins-v0.1.124/src/probestack.rs

// Copyright 2017 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! This module defines the `__private_rust_probestack` intrinsic which is used in the
//! implementation of "stack probes" on certain platforms.
//!
//! The purpose of a stack probe is to provide a static guarantee that if a
//! thread has a guard page then a stack overflow is guaranteed to hit that
//! guard page. If a function did not have a stack probe then there's a risk of
//! having a stack frame *larger* than the guard page, so a function call could
//! skip over the guard page entirely and then later hit maybe the heap or
//! another thread, possibly leading to security vulnerabilities such as [The
//! Stack Clash], for example.
//!
//! [The Stack Clash]: https://blog.qualys.com/securitylabs/2017/06/19/the-stack-clash
//!
//! The `__private_rust_probestack` is called in the prologue of functions whose stack
//! size is larger than the guard page, for example larger than 4096 bytes on
//! x86. This function is then responsible for "touching" all pages relevant to
//! the stack to ensure that that if any of them are the guard page we'll hit
//! them guaranteed.
//!
//! The precise ABI for how this function operates is defined by LLVM. There's
//! no real documentation as to what this is, so you'd basically need to read
//! the LLVM source code for reference. Often though the test cases can be
//! illuminating as to the ABI that's generated, or just looking at the output
//! of `llc`.
//!
//! Note that `#[naked]` is typically used here for the stack probe because the
//! ABI corresponds to no actual ABI.
//!
//! Finally it's worth noting that at the time of this writing LLVM only has
//! support for stack probes on x86 and x86_64. There's no support for stack
//! probes on any other architecture like ARM or PowerPC64. LLVM I'm sure would
//! be more than welcome to accept such a change!

// Windows already has builtins to do this.
#![cfg(not(windows))]
// All these builtins require assembly
#![cfg(not(feature = "no-asm"))]
// We only define stack probing for these architectures today.
#![cfg(any(target_arch = "x86_64", target_arch = "x86"))]

unsafe extern "C" {
    pub fn __private_rust_probestack();
}

// A wrapper for our implementation of __private_rust_probestack, which allows us to
// keep the assembly inline while controlling all CFI directives in the assembly
// emitted for the function.
//
// This is the ELF version.
#[cfg(not(any(target_vendor = "apple", target_os = "uefi")))]
macro_rules! define_rust_probestack {
    ($body: expr) => {
        concat!(
            "
            .pushsection .text.__private_rust_probestack
            .globl __private_rust_probestack
            .type  __private_rust_probestack, @function
            .hidden __private_rust_probestack
        __private_rust_probestack:
            ",
            $body,
            "
            .size __private_rust_probestack, . - __private_rust_probestack
            .popsection
            "
        )
    };
}

// Same as above, but for Mach-O. Note that the triple underscore
// is deliberate
#[cfg(target_vendor = "apple")]
macro_rules! define_rust_probestack {
    ($body: expr) => {
        concat!(
            "
            .globl ___private_rust_probestack
        ___private_rust_probestack:
            ",
            $body
        )
    };
}

// Our goal here is to touch each page between %rsp+8 and %rsp+8-%rax,
// ensuring that if any pages are unmapped we'll make a page fault.
//
// The ABI here is that the stack frame size is located in `%rax`. Upon
// return we're not supposed to modify `%rsp` or `%rax`.
//
// Any changes to this function should be replicated to the SGX version below.
#[cfg(all(target_arch = "x86_64", not(all(target_env = "sgx", target_vendor = "fortanix"))))]
core::arch::global_asm!(
    define_rust_probestack!(
        "
    .cfi_startproc
    pushq  %rbp
    .cfi_adjust_cfa_offset 8
    .cfi_offset %rbp, -16
    movq   %rsp, %rbp
    .cfi_def_cfa_register %rbp

    mov    %rax,%r11        // duplicate %rax as we're clobbering %r11

    // Main loop, taken in one page increments. We're decrementing rsp by
    // a page each time until there's less than a page remaining. We're
    // guaranteed that this function isn't called unless there's more than a
    // page needed.
    //
    // Note that we're also testing against `8(%rsp)` to account for the 8
    // bytes pushed on the stack originally with our return address. Using
    // `8(%rsp)` simulates us testing the stack pointer in the caller's
    // context.

    // It's usually called when %rax >= 0x1000, but that's not always true.
    // Dynamic stack allocation, which is needed to implement unsized
    // rvalues, triggers stackprobe even if %rax < 0x1000.
    // Thus we have to check %r11 first to avoid segfault.
    cmp    $0x1000,%r11
    jna    3f
2:
    sub    $0x1000,%rsp
    test   %rsp,8(%rsp)
    sub    $0x1000,%r11
    cmp    $0x1000,%r11
    ja     2b

3:
    // Finish up the last remaining stack space requested, getting the last
    // bits out of r11
    sub    %r11,%rsp
    test   %rsp,8(%rsp)

    // Restore the stack pointer to what it previously was when entering
    // this function. The caller will readjust the stack pointer after we
    // return.
    add    %rax,%rsp

    leave
    .cfi_def_cfa_register %rsp
    .cfi_adjust_cfa_offset -8
    ret
    .cfi_endproc
    "
    ),
    options(att_syntax)
);

#[cfg(all(target_arch = "x86", not(target_os = "uefi")))]
// This is the same as x86_64 above, only translated for 32-bit sizes. Note
// that on Unix we're expected to restore everything as it was, this
// function basically can't tamper with anything.
//
// The ABI here is the same as x86_64, except everything is 32-bits large.
core::arch::global_asm!(
    define_rust_probestack!(
        "
    .cfi_startproc
    push   %ebp
    .cfi_adjust_cfa_offset 4
    .cfi_offset %ebp, -8
    mov    %esp, %ebp
    .cfi_def_cfa_register %ebp
    push   %ecx
    mov    %eax,%ecx

    cmp    $0x1000,%ecx
    jna    3f
2:
    sub    $0x1000,%esp
    test   %esp,8(%esp)
    sub    $0x1000,%ecx
    cmp    $0x1000,%ecx
    ja     2b

3:
    sub    %ecx,%esp
    test   %esp,8(%esp)

    add    %eax,%esp
    pop    %ecx
    leave
    .cfi_def_cfa_register %esp
    .cfi_adjust_cfa_offset -4
    ret
    .cfi_endproc
    "
    ),
    options(att_syntax)
);
