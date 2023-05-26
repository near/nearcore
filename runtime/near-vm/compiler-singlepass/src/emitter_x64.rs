pub(crate) use crate::x64_decl::{GPR, XMM};
use dynasm::dynasm;
use dynasmrt::{
    x64::X64Relocation, AssemblyOffset, DynamicLabel, DynasmApi, DynasmLabelApi, VecAssembler,
};
use enumset::EnumSet;
use near_vm_compiler::CpuFeature;

type Assembler = VecAssembler<X64Relocation>;

/// Force `dynasm!` to use the correct arch (x64) when cross-compiling.
/// `dynasm!` proc-macro tries to auto-detect it by default by looking at the
/// `target_arch`, but it sees the `target_arch` of the proc-macro itself, which
/// is always equal to host, even when cross-compiling.
macro_rules! dynasm {
    ($a:expr ; $($tt:tt)*) => {
        dynasm::dynasm!(
            $a
            ; .arch x64
            ; $($tt)*
        )
    };
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) enum Location {
    Imm8(u8),
    Imm32(u32),
    Imm64(u64),
    // Imm128(u128),
    GPR(GPR),
    XMM(XMM),
    Memory(GPR, i32),
    MemoryAddTriple(GPR, GPR, i32),
}

#[allow(dead_code)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Condition {
    None,
    Above,
    AboveEqual,
    Below,
    BelowEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    Equal,
    NotEqual,
    Signed,
    Carry,
    Overflow,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum Size {
    S8,
    S16,
    S32,
    S64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) enum XMMOrMemory {
    XMM(XMM),
    Memory(GPR, i32),
}

#[derive(Copy, Clone, Debug)]
#[allow(dead_code)]
pub(crate) enum GPROrMemory {
    GPR(GPR),
    Memory(GPR, i32),
}

pub(crate) trait Emitter {
    type Label;
    type Offset;

    fn get_label(&mut self) -> Self::Label;
    fn get_offset(&self) -> Self::Offset;
    fn get_jmp_instr_size(&self) -> u8;

    fn finalize_function(&mut self) {}

    fn emit_u64(&mut self, x: u64);
    fn emit_bytes(&mut self, bytes: &[u8]);

    fn emit_label(&mut self, label: Self::Label);

    fn emit_nop(&mut self);

    /// A high-level assembler method. Emits an instruction sequence of length `n` that is functionally
    /// equivalent to a `nop` instruction, without guarantee about the underlying implementation.
    fn emit_nop_n(&mut self, n: usize);

    fn emit_mov(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_lea(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_lea_label(&mut self, label: Self::Label, dst: Location);
    fn emit_cdq(&mut self);
    fn emit_cqo(&mut self);
    fn emit_xor(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_jmp(&mut self, condition: Condition, label: Self::Label);
    fn emit_jmp_location(&mut self, loc: Location);
    fn emit_set(&mut self, condition: Condition, dst: GPR);
    fn emit_push(&mut self, sz: Size, src: Location);
    fn emit_pop(&mut self, sz: Size, dst: Location);
    fn emit_cmp(&mut self, sz: Size, left: Location, right: Location);
    fn emit_add(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_sub(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_neg(&mut self, sz: Size, value: Location);
    fn emit_imul(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_imul_imm32_gpr64(&mut self, src: u32, dst: GPR);
    fn emit_div(&mut self, sz: Size, divisor: Location);
    fn emit_idiv(&mut self, sz: Size, divisor: Location);
    fn emit_shl(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_shr(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_sar(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_rol(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_ror(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_and(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_or(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_bsr(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_bsf(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_popcnt(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_movzx(&mut self, sz_src: Size, src: Location, sz_dst: Size, dst: Location);
    fn emit_movsx(&mut self, sz_src: Size, src: Location, sz_dst: Size, dst: Location);
    fn emit_xchg(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_lock_xadd(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_lock_cmpxchg(&mut self, sz: Size, src: Location, dst: Location);
    fn emit_rep_stosq(&mut self);

    fn emit_btc_gpr_imm8_32(&mut self, src: u8, dst: GPR);
    fn emit_btc_gpr_imm8_64(&mut self, src: u8, dst: GPR);

    fn emit_cmovae_gpr_32(&mut self, src: GPR, dst: GPR);
    fn emit_cmovae_gpr_64(&mut self, src: GPR, dst: GPR);

    fn emit_vmovaps(&mut self, src: XMMOrMemory, dst: XMMOrMemory);
    fn emit_vmovapd(&mut self, src: XMMOrMemory, dst: XMMOrMemory);
    fn emit_vxorps(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vxorpd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);

    fn emit_vaddss(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vaddsd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vsubss(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vsubsd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vmulss(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vmulsd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vdivss(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vdivsd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vmaxss(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vmaxsd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vminss(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vminsd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);

    fn emit_vcmpeqss(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vcmpeqsd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);

    fn emit_vcmpneqss(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vcmpneqsd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);

    fn emit_vcmpltss(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vcmpltsd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);

    fn emit_vcmpless(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vcmplesd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);

    fn emit_vcmpgtss(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vcmpgtsd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);

    fn emit_vcmpgess(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vcmpgesd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);

    fn emit_vcmpunordss(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vcmpunordsd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);

    fn emit_vcmpordss(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vcmpordsd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);

    fn emit_vsqrtss(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vsqrtsd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);

    fn emit_vroundss_nearest(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vroundss_floor(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vroundss_ceil(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vroundss_trunc(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vroundsd_nearest(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vroundsd_floor(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vroundsd_ceil(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vroundsd_trunc(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);

    fn emit_vcvtss2sd(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);
    fn emit_vcvtsd2ss(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM);

    fn emit_ucomiss(&mut self, src: XMMOrMemory, dst: XMM);
    fn emit_ucomisd(&mut self, src: XMMOrMemory, dst: XMM);

    fn emit_cvttss2si_32(&mut self, src: XMMOrMemory, dst: GPR);
    fn emit_cvttss2si_64(&mut self, src: XMMOrMemory, dst: GPR);
    fn emit_cvttsd2si_32(&mut self, src: XMMOrMemory, dst: GPR);
    fn emit_cvttsd2si_64(&mut self, src: XMMOrMemory, dst: GPR);

    fn emit_vcvtsi2ss_32(&mut self, src1: XMM, src2: GPROrMemory, dst: XMM);
    fn emit_vcvtsi2ss_64(&mut self, src1: XMM, src2: GPROrMemory, dst: XMM);
    fn emit_vcvtsi2sd_32(&mut self, src1: XMM, src2: GPROrMemory, dst: XMM);
    fn emit_vcvtsi2sd_64(&mut self, src1: XMM, src2: GPROrMemory, dst: XMM);

    fn emit_vblendvps(&mut self, src1: XMM, src2: XMMOrMemory, mask: XMM, dst: XMM);
    fn emit_vblendvpd(&mut self, src1: XMM, src2: XMMOrMemory, mask: XMM, dst: XMM);

    fn emit_test_gpr_64(&mut self, reg: GPR);

    fn emit_ud2(&mut self);
    fn emit_ret(&mut self);
    fn emit_call_label(&mut self, label: Self::Label);
    fn emit_call_location(&mut self, loc: Location);

    fn emit_call_register(&mut self, reg: GPR);

    fn emit_bkpt(&mut self);

    fn emit_host_redirection(&mut self, target: GPR);

    fn arch_has_itruncf(&self) -> bool {
        false
    }
    fn arch_emit_i32_trunc_sf32(&mut self, _src: XMM, _dst: GPR) {
        unimplemented!()
    }
    fn arch_emit_i32_trunc_sf64(&mut self, _src: XMM, _dst: GPR) {
        unimplemented!()
    }
    fn arch_emit_i32_trunc_uf32(&mut self, _src: XMM, _dst: GPR) {
        unimplemented!()
    }
    fn arch_emit_i32_trunc_uf64(&mut self, _src: XMM, _dst: GPR) {
        unimplemented!()
    }
    fn arch_emit_i64_trunc_sf32(&mut self, _src: XMM, _dst: GPR) {
        unimplemented!()
    }
    fn arch_emit_i64_trunc_sf64(&mut self, _src: XMM, _dst: GPR) {
        unimplemented!()
    }
    fn arch_emit_i64_trunc_uf32(&mut self, _src: XMM, _dst: GPR) {
        unimplemented!()
    }
    fn arch_emit_i64_trunc_uf64(&mut self, _src: XMM, _dst: GPR) {
        unimplemented!()
    }

    fn arch_has_fconverti(&self) -> bool {
        false
    }
    fn arch_emit_f32_convert_si32(&mut self, _src: GPR, _dst: XMM) {
        unimplemented!()
    }
    fn arch_emit_f32_convert_si64(&mut self, _src: GPR, _dst: XMM) {
        unimplemented!()
    }
    fn arch_emit_f32_convert_ui32(&mut self, _src: GPR, _dst: XMM) {
        unimplemented!()
    }
    fn arch_emit_f32_convert_ui64(&mut self, _src: GPR, _dst: XMM) {
        unimplemented!()
    }
    fn arch_emit_f64_convert_si32(&mut self, _src: GPR, _dst: XMM) {
        unimplemented!()
    }
    fn arch_emit_f64_convert_si64(&mut self, _src: GPR, _dst: XMM) {
        unimplemented!()
    }
    fn arch_emit_f64_convert_ui32(&mut self, _src: GPR, _dst: XMM) {
        unimplemented!()
    }
    fn arch_emit_f64_convert_ui64(&mut self, _src: GPR, _dst: XMM) {
        unimplemented!()
    }

    fn arch_has_fneg(&self) -> bool {
        false
    }
    fn arch_emit_f32_neg(&mut self, _src: XMM, _dst: XMM) {
        unimplemented!()
    }
    fn arch_emit_f64_neg(&mut self, _src: XMM, _dst: XMM) {
        unimplemented!()
    }

    fn arch_has_xzcnt(&self, _cpu: &EnumSet<CpuFeature>) -> bool {
        false
    }
    fn arch_emit_lzcnt(&mut self, _sz: Size, _src: Location, _dst: Location) {
        unimplemented!()
    }
    fn arch_emit_tzcnt(&mut self, _sz: Size, _src: Location, _dst: Location) {
        unimplemented!()
    }

    fn arch_supports_canonicalize_nan(&self) -> bool {
        true
    }

    fn arch_requires_indirect_call_trampoline(&self) -> bool {
        false
    }

    fn arch_emit_indirect_call_with_trampoline(&mut self, _loc: Location) {
        unimplemented!()
    }

    // Emits entry trampoline just before the real function.
    fn arch_emit_entry_trampoline(&mut self) {}

    // Byte offset from the beginning of a `mov Imm64, GPR` instruction to the imm64 value.
    // Required to support emulation on Aarch64.
    fn arch_mov64_imm_offset(&self) -> usize {
        unimplemented!()
    }
}

macro_rules! unop_gpr {
    ($ins:ident, $assembler:tt, $sz:expr, $loc:expr, $otherwise:block) => {
        match ($sz, $loc) {
            (Size::S32, Location::GPR(loc)) => {
                dynasm!($assembler ; $ins Rd(loc as u8));
            },
            (Size::S64, Location::GPR(loc)) => {
                dynasm!($assembler ; $ins Rq(loc as u8));
            },
            _ => $otherwise
        }
    };
}

macro_rules! unop_mem {
    ($ins:ident, $assembler:tt, $sz:expr, $loc:expr, $otherwise:block) => {
        match ($sz, $loc) {
            (Size::S32, Location::Memory(loc, disp)) => {
                dynasm!($assembler ; $ins DWORD [Rq(loc as u8) + disp] );
            },
            (Size::S64, Location::Memory(loc, disp)) => {
                dynasm!($assembler ; $ins QWORD [Rq(loc as u8) + disp] );
            },
            _ => $otherwise
        }
    };
}

macro_rules! unop_gpr_or_mem {
    ($ins:ident, $assembler:tt, $sz:expr, $loc:expr, $otherwise:block) => {
        unop_gpr!($ins, $assembler, $sz, $loc, {
            unop_mem!($ins, $assembler, $sz, $loc, $otherwise)
        })
    };
}

macro_rules! binop_imm32_gpr {
    ($ins:ident, $assembler:tt, $sz:expr, $src:expr, $dst:expr, $otherwise:block) => {
        match ($sz, $src, $dst) {
            (Size::S32, Location::Imm32(src), Location::GPR(dst)) => {
                dynasm!($assembler ; $ins Rd(dst as u8), src as i32); // IMM32_2GPR
            },
            (Size::S64, Location::Imm32(src), Location::GPR(dst)) => {
                dynasm!($assembler ; $ins Rq(dst as u8), src as i32); // IMM32_2GPR
            },
            _ => $otherwise
        }
    };
}

macro_rules! binop_imm32_mem {
    ($ins:ident, $assembler:tt, $sz:expr, $src:expr, $dst:expr, $otherwise:block) => {
        match ($sz, $src, $dst) {
            (Size::S32, Location::Imm32(src), Location::Memory(dst, disp)) => {
                dynasm!($assembler ; $ins DWORD [Rq(dst as u8) + disp], src as i32);
            },
            (Size::S64, Location::Imm32(src), Location::Memory(dst, disp)) => {
                dynasm!($assembler ; $ins QWORD [Rq(dst as u8) + disp], src as i32);
            },
            _ => $otherwise
        }
    };
}

macro_rules! binop_imm64_gpr {
    ($ins:ident, $assembler:tt, $sz:expr, $src:expr, $dst:expr, $otherwise:block) => {
        match ($sz, $src, $dst) {
            (Size::S64, Location::Imm64(src), Location::GPR(dst)) => {
                dynasm!($assembler ; $ins Rq(dst as u8), QWORD src as i64); // IMM32_2GPR
            },
            _ => $otherwise
        }
    };
}

macro_rules! binop_gpr_gpr {
    ($ins:ident, $assembler:tt, $sz:expr, $src:expr, $dst:expr, $otherwise:block) => {
        match ($sz, $src, $dst) {
            (Size::S32, Location::GPR(src), Location::GPR(dst)) => {
                dynasm!($assembler ; $ins Rd(dst as u8), Rd(src as u8)); // GPR2GPR
            },
            (Size::S64, Location::GPR(src), Location::GPR(dst)) => {
                dynasm!($assembler ; $ins Rq(dst as u8), Rq(src as u8)); // GPR2GPR
            },
            _ => $otherwise
        }
    };
}

macro_rules! binop_gpr_mem {
    ($ins:ident, $assembler:tt, $sz:expr, $src:expr, $dst:expr, $otherwise:block) => {
        match ($sz, $src, $dst) {
            (Size::S32, Location::GPR(src), Location::Memory(dst, disp)) => {
                dynasm!($assembler ; $ins [Rq(dst as u8) + disp], Rd(src as u8)); // GPR2MEM
            },
            (Size::S64, Location::GPR(src), Location::Memory(dst, disp)) => {
                dynasm!($assembler ; $ins [Rq(dst as u8) + disp], Rq(src as u8)); // GPR2MEM
            },
            _ => $otherwise
        }
    };
}

macro_rules! binop_mem_gpr {
    ($ins:ident, $assembler:tt, $sz:expr, $src:expr, $dst:expr, $otherwise:block) => {
        match ($sz, $src, $dst) {
            (Size::S32, Location::Memory(src, disp), Location::GPR(dst)) => {
                dynasm!($assembler ; $ins Rd(dst as u8), [Rq(src as u8) + disp]); // MEM2GPR
            },
            (Size::S64, Location::Memory(src, disp), Location::GPR(dst)) => {
                dynasm!($assembler ; $ins Rq(dst as u8), [Rq(src as u8) + disp]); // MEM2GPR
            },
            _ => $otherwise
        }
    };
}

macro_rules! binop_all_nofp {
    ($ins:ident, $assembler:tt, $sz:expr, $src:expr, $dst:expr, $otherwise:block) => {
        binop_imm32_gpr!($ins, $assembler, $sz, $src, $dst, {
            binop_imm32_mem!($ins, $assembler, $sz, $src, $dst, {
                binop_gpr_gpr!($ins, $assembler, $sz, $src, $dst, {
                    binop_gpr_mem!($ins, $assembler, $sz, $src, $dst, {
                        binop_mem_gpr!($ins, $assembler, $sz, $src, $dst, $otherwise)
                    })
                })
            })
        })
    };
}

macro_rules! binop_shift {
    ($ins:ident, $assembler:tt, $sz:expr, $src:expr, $dst:expr, $otherwise:block) => {
        match ($sz, $src, $dst) {
            (Size::S32, Location::GPR(GPR::RCX), Location::GPR(dst)) => {
                dynasm!($assembler ; $ins Rd(dst as u8), cl);
            },
            (Size::S32, Location::GPR(GPR::RCX), Location::Memory(dst, disp)) => {
                dynasm!($assembler ; $ins DWORD [Rq(dst as u8) + disp], cl);
            },
            (Size::S32, Location::Imm8(imm), Location::GPR(dst)) => {
                dynasm!($assembler ; $ins Rd(dst as u8), imm as i8);
            },
            (Size::S32, Location::Imm8(imm), Location::Memory(dst, disp)) => {
                dynasm!($assembler ; $ins DWORD [Rq(dst as u8) + disp], imm as i8);
            },
            (Size::S64, Location::GPR(GPR::RCX), Location::GPR(dst)) => {
                dynasm!($assembler ; $ins Rq(dst as u8), cl);
            },
            (Size::S64, Location::GPR(GPR::RCX), Location::Memory(dst, disp)) => {
                dynasm!($assembler ; $ins QWORD [Rq(dst as u8) + disp], cl);
            },
            (Size::S64, Location::Imm8(imm), Location::GPR(dst)) => {
                dynasm!($assembler ; $ins Rq(dst as u8), imm as i8);
            },
            (Size::S64, Location::Imm8(imm), Location::Memory(dst, disp)) => {
                dynasm!($assembler ; $ins QWORD [Rq(dst as u8) + disp], imm as i8);
            },
            _ => $otherwise
        }
    }
}

macro_rules! jmp_op {
    ($ins:ident, $assembler:tt, $label:ident) => {
        dynasm!($assembler ; $ins =>$label)
    }
}

macro_rules! avx_fn {
    ($ins:ident, $name:ident) => {
        fn $name(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM) {
            // Dynasm bug: AVX instructions are not encoded correctly.
            match src2 {
                XMMOrMemory::XMM(x) => match src1 {
                    XMM::XMM0 => dynasm!(self ; $ins Rx((dst as u8)), xmm0, Rx((x as u8))),
                    XMM::XMM1 => dynasm!(self ; $ins Rx((dst as u8)), xmm1, Rx((x as u8))),
                    XMM::XMM2 => dynasm!(self ; $ins Rx((dst as u8)), xmm2, Rx((x as u8))),
                    XMM::XMM3 => dynasm!(self ; $ins Rx((dst as u8)), xmm3, Rx((x as u8))),
                    XMM::XMM4 => dynasm!(self ; $ins Rx((dst as u8)), xmm4, Rx((x as u8))),
                    XMM::XMM5 => dynasm!(self ; $ins Rx((dst as u8)), xmm5, Rx((x as u8))),
                    XMM::XMM6 => dynasm!(self ; $ins Rx((dst as u8)), xmm6, Rx((x as u8))),
                    XMM::XMM7 => dynasm!(self ; $ins Rx((dst as u8)), xmm7, Rx((x as u8))),
                    XMM::XMM8 => dynasm!(self ; $ins Rx((dst as u8)), xmm8, Rx((x as u8))),
                    XMM::XMM9 => dynasm!(self ; $ins Rx((dst as u8)), xmm9, Rx((x as u8))),
                    XMM::XMM10 => dynasm!(self ; $ins Rx((dst as u8)), xmm10, Rx((x as u8))),
                    XMM::XMM11 => dynasm!(self ; $ins Rx((dst as u8)), xmm11, Rx((x as u8))),
                    XMM::XMM12 => dynasm!(self ; $ins Rx((dst as u8)), xmm12, Rx((x as u8))),
                    XMM::XMM13 => dynasm!(self ; $ins Rx((dst as u8)), xmm13, Rx((x as u8))),
                    XMM::XMM14 => dynasm!(self ; $ins Rx((dst as u8)), xmm14, Rx((x as u8))),
                    XMM::XMM15 => dynasm!(self ; $ins Rx((dst as u8)), xmm15, Rx((x as u8))),
                },
                XMMOrMemory::Memory(base, disp) => match src1 {
                    XMM::XMM0 => dynasm!(self ; $ins Rx((dst as u8)), xmm0, [Rq((base as u8)) + disp]),
                    XMM::XMM1 => dynasm!(self ; $ins Rx((dst as u8)), xmm1, [Rq((base as u8)) + disp]),
                    XMM::XMM2 => dynasm!(self ; $ins Rx((dst as u8)), xmm2, [Rq((base as u8)) + disp]),
                    XMM::XMM3 => dynasm!(self ; $ins Rx((dst as u8)), xmm3, [Rq((base as u8)) + disp]),
                    XMM::XMM4 => dynasm!(self ; $ins Rx((dst as u8)), xmm4, [Rq((base as u8)) + disp]),
                    XMM::XMM5 => dynasm!(self ; $ins Rx((dst as u8)), xmm5, [Rq((base as u8)) + disp]),
                    XMM::XMM6 => dynasm!(self ; $ins Rx((dst as u8)), xmm6, [Rq((base as u8)) + disp]),
                    XMM::XMM7 => dynasm!(self ; $ins Rx((dst as u8)), xmm7, [Rq((base as u8)) + disp]),
                    XMM::XMM8 => dynasm!(self ; $ins Rx((dst as u8)), xmm8, [Rq((base as u8)) + disp]),
                    XMM::XMM9 => dynasm!(self ; $ins Rx((dst as u8)), xmm9, [Rq((base as u8)) + disp]),
                    XMM::XMM10 => dynasm!(self ; $ins Rx((dst as u8)), xmm10, [Rq((base as u8)) + disp]),
                    XMM::XMM11 => dynasm!(self ; $ins Rx((dst as u8)), xmm11, [Rq((base as u8)) + disp]),
                    XMM::XMM12 => dynasm!(self ; $ins Rx((dst as u8)), xmm12, [Rq((base as u8)) + disp]),
                    XMM::XMM13 => dynasm!(self ; $ins Rx((dst as u8)), xmm13, [Rq((base as u8)) + disp]),
                    XMM::XMM14 => dynasm!(self ; $ins Rx((dst as u8)), xmm14, [Rq((base as u8)) + disp]),
                    XMM::XMM15 => dynasm!(self ; $ins Rx((dst as u8)), xmm15, [Rq((base as u8)) + disp]),
                },
            }
        }
    }
}

macro_rules! avx_i2f_64_fn {
    ($ins:ident, $name:ident) => {
        fn $name(&mut self, src1: XMM, src2: GPROrMemory, dst: XMM) {
            match src2 {
                GPROrMemory::GPR(x) => match src1 {
                    XMM::XMM0 => dynasm!(self ; $ins Rx((dst as u8)), xmm0, Rq((x as u8))),
                    XMM::XMM1 => dynasm!(self ; $ins Rx((dst as u8)), xmm1, Rq((x as u8))),
                    XMM::XMM2 => dynasm!(self ; $ins Rx((dst as u8)), xmm2, Rq((x as u8))),
                    XMM::XMM3 => dynasm!(self ; $ins Rx((dst as u8)), xmm3, Rq((x as u8))),
                    XMM::XMM4 => dynasm!(self ; $ins Rx((dst as u8)), xmm4, Rq((x as u8))),
                    XMM::XMM5 => dynasm!(self ; $ins Rx((dst as u8)), xmm5, Rq((x as u8))),
                    XMM::XMM6 => dynasm!(self ; $ins Rx((dst as u8)), xmm6, Rq((x as u8))),
                    XMM::XMM7 => dynasm!(self ; $ins Rx((dst as u8)), xmm7, Rq((x as u8))),
                    XMM::XMM8 => dynasm!(self ; $ins Rx((dst as u8)), xmm8, Rq((x as u8))),
                    XMM::XMM9 => dynasm!(self ; $ins Rx((dst as u8)), xmm9, Rq((x as u8))),
                    XMM::XMM10 => dynasm!(self ; $ins Rx((dst as u8)), xmm10, Rq((x as u8))),
                    XMM::XMM11 => dynasm!(self ; $ins Rx((dst as u8)), xmm11, Rq((x as u8))),
                    XMM::XMM12 => dynasm!(self ; $ins Rx((dst as u8)), xmm12, Rq((x as u8))),
                    XMM::XMM13 => dynasm!(self ; $ins Rx((dst as u8)), xmm13, Rq((x as u8))),
                    XMM::XMM14 => dynasm!(self ; $ins Rx((dst as u8)), xmm14, Rq((x as u8))),
                    XMM::XMM15 => dynasm!(self ; $ins Rx((dst as u8)), xmm15, Rq((x as u8))),
                },
                GPROrMemory::Memory(base, disp) => match src1 {
                    XMM::XMM0 => dynasm!(self ; $ins Rx((dst as u8)), xmm0, QWORD [Rq((base as u8)) + disp]),
                    XMM::XMM1 => dynasm!(self ; $ins Rx((dst as u8)), xmm1, QWORD [Rq((base as u8)) + disp]),
                    XMM::XMM2 => dynasm!(self ; $ins Rx((dst as u8)), xmm2, QWORD [Rq((base as u8)) + disp]),
                    XMM::XMM3 => dynasm!(self ; $ins Rx((dst as u8)), xmm3, QWORD [Rq((base as u8)) + disp]),
                    XMM::XMM4 => dynasm!(self ; $ins Rx((dst as u8)), xmm4, QWORD [Rq((base as u8)) + disp]),
                    XMM::XMM5 => dynasm!(self ; $ins Rx((dst as u8)), xmm5, QWORD [Rq((base as u8)) + disp]),
                    XMM::XMM6 => dynasm!(self ; $ins Rx((dst as u8)), xmm6, QWORD [Rq((base as u8)) + disp]),
                    XMM::XMM7 => dynasm!(self ; $ins Rx((dst as u8)), xmm7, QWORD [Rq((base as u8)) + disp]),
                    XMM::XMM8 => dynasm!(self ; $ins Rx((dst as u8)), xmm8, QWORD [Rq((base as u8)) + disp]),
                    XMM::XMM9 => dynasm!(self ; $ins Rx((dst as u8)), xmm9, QWORD [Rq((base as u8)) + disp]),
                    XMM::XMM10 => dynasm!(self ; $ins Rx((dst as u8)), xmm10, QWORD [Rq((base as u8)) + disp]),
                    XMM::XMM11 => dynasm!(self ; $ins Rx((dst as u8)), xmm11, QWORD [Rq((base as u8)) + disp]),
                    XMM::XMM12 => dynasm!(self ; $ins Rx((dst as u8)), xmm12, QWORD [Rq((base as u8)) + disp]),
                    XMM::XMM13 => dynasm!(self ; $ins Rx((dst as u8)), xmm13, QWORD [Rq((base as u8)) + disp]),
                    XMM::XMM14 => dynasm!(self ; $ins Rx((dst as u8)), xmm14, QWORD [Rq((base as u8)) + disp]),
                    XMM::XMM15 => dynasm!(self ; $ins Rx((dst as u8)), xmm15, QWORD [Rq((base as u8)) + disp]),
                },
            }
        }
    }
}

macro_rules! avx_i2f_32_fn {
    ($ins:ident, $name:ident) => {
        fn $name(&mut self, src1: XMM, src2: GPROrMemory, dst: XMM) {
            match src2 {
                GPROrMemory::GPR(x) => match src1 {
                    XMM::XMM0 => dynasm!(self ; $ins Rx((dst as u8)), xmm0, Rd((x as u8))),
                    XMM::XMM1 => dynasm!(self ; $ins Rx((dst as u8)), xmm1, Rd((x as u8))),
                    XMM::XMM2 => dynasm!(self ; $ins Rx((dst as u8)), xmm2, Rd((x as u8))),
                    XMM::XMM3 => dynasm!(self ; $ins Rx((dst as u8)), xmm3, Rd((x as u8))),
                    XMM::XMM4 => dynasm!(self ; $ins Rx((dst as u8)), xmm4, Rd((x as u8))),
                    XMM::XMM5 => dynasm!(self ; $ins Rx((dst as u8)), xmm5, Rd((x as u8))),
                    XMM::XMM6 => dynasm!(self ; $ins Rx((dst as u8)), xmm6, Rd((x as u8))),
                    XMM::XMM7 => dynasm!(self ; $ins Rx((dst as u8)), xmm7, Rd((x as u8))),
                    XMM::XMM8 => dynasm!(self ; $ins Rx((dst as u8)), xmm8, Rd((x as u8))),
                    XMM::XMM9 => dynasm!(self ; $ins Rx((dst as u8)), xmm9, Rd((x as u8))),
                    XMM::XMM10 => dynasm!(self ; $ins Rx((dst as u8)), xmm10, Rd((x as u8))),
                    XMM::XMM11 => dynasm!(self ; $ins Rx((dst as u8)), xmm11, Rd((x as u8))),
                    XMM::XMM12 => dynasm!(self ; $ins Rx((dst as u8)), xmm12, Rd((x as u8))),
                    XMM::XMM13 => dynasm!(self ; $ins Rx((dst as u8)), xmm13, Rd((x as u8))),
                    XMM::XMM14 => dynasm!(self ; $ins Rx((dst as u8)), xmm14, Rd((x as u8))),
                    XMM::XMM15 => dynasm!(self ; $ins Rx((dst as u8)), xmm15, Rd((x as u8))),
                },
                GPROrMemory::Memory(base, disp) => match src1 {
                    XMM::XMM0 => dynasm!(self ; $ins Rx((dst as u8)), xmm0, DWORD [Rq((base as u8)) + disp]),
                    XMM::XMM1 => dynasm!(self ; $ins Rx((dst as u8)), xmm1, DWORD [Rq((base as u8)) + disp]),
                    XMM::XMM2 => dynasm!(self ; $ins Rx((dst as u8)), xmm2, DWORD [Rq((base as u8)) + disp]),
                    XMM::XMM3 => dynasm!(self ; $ins Rx((dst as u8)), xmm3, DWORD [Rq((base as u8)) + disp]),
                    XMM::XMM4 => dynasm!(self ; $ins Rx((dst as u8)), xmm4, DWORD [Rq((base as u8)) + disp]),
                    XMM::XMM5 => dynasm!(self ; $ins Rx((dst as u8)), xmm5, DWORD [Rq((base as u8)) + disp]),
                    XMM::XMM6 => dynasm!(self ; $ins Rx((dst as u8)), xmm6, DWORD [Rq((base as u8)) + disp]),
                    XMM::XMM7 => dynasm!(self ; $ins Rx((dst as u8)), xmm7, DWORD [Rq((base as u8)) + disp]),
                    XMM::XMM8 => dynasm!(self ; $ins Rx((dst as u8)), xmm8, DWORD [Rq((base as u8)) + disp]),
                    XMM::XMM9 => dynasm!(self ; $ins Rx((dst as u8)), xmm9, DWORD [Rq((base as u8)) + disp]),
                    XMM::XMM10 => dynasm!(self ; $ins Rx((dst as u8)), xmm10, DWORD [Rq((base as u8)) + disp]),
                    XMM::XMM11 => dynasm!(self ; $ins Rx((dst as u8)), xmm11, DWORD [Rq((base as u8)) + disp]),
                    XMM::XMM12 => dynasm!(self ; $ins Rx((dst as u8)), xmm12, DWORD [Rq((base as u8)) + disp]),
                    XMM::XMM13 => dynasm!(self ; $ins Rx((dst as u8)), xmm13, DWORD [Rq((base as u8)) + disp]),
                    XMM::XMM14 => dynasm!(self ; $ins Rx((dst as u8)), xmm14, DWORD [Rq((base as u8)) + disp]),
                    XMM::XMM15 => dynasm!(self ; $ins Rx((dst as u8)), xmm15, DWORD [Rq((base as u8)) + disp]),
                },
            }
        }
    }
}

macro_rules! avx_round_fn {
    ($ins:ident, $name:ident, $mode:expr) => {
        fn $name(&mut self, src1: XMM, src2: XMMOrMemory, dst: XMM) {
            match src2 {
                XMMOrMemory::XMM(x) => dynasm!(self ; $ins Rx((dst as u8)), Rx((src1 as u8)), Rx((x as u8)), $mode),
                XMMOrMemory::Memory(base, disp) => dynasm!(self ; $ins Rx((dst as u8)), Rx((src1 as u8)), [Rq((base as u8)) + disp], $mode),
            }
        }
    }
}

impl Emitter for Assembler {
    type Label = DynamicLabel;
    type Offset = AssemblyOffset;

    fn get_label(&mut self) -> DynamicLabel {
        self.new_dynamic_label()
    }

    fn get_offset(&self) -> AssemblyOffset {
        self.offset()
    }

    fn get_jmp_instr_size(&self) -> u8 {
        5
    }

    fn finalize_function(&mut self) {
        dynasm!(
            self
            ; const_neg_one_32:
            ; .dword -1
            ; const_zero_32:
            ; .dword 0
            ; const_pos_one_32:
            ; .dword 1
        );
    }

    fn emit_u64(&mut self, x: u64) {
        self.push_u64(x);
    }

    fn emit_bytes(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.push(b);
        }
    }

    fn emit_label(&mut self, label: Self::Label) {
        dynasm!(self ; => label);
    }

    fn emit_nop(&mut self) {
        dynasm!(self ; nop);
    }

    fn emit_nop_n(&mut self, mut n: usize) {
        /*
            1      90H                            NOP
            2      66 90H                         66 NOP
            3      0F 1F 00H                      NOP DWORD ptr [EAX]
            4      0F 1F 40 00H                   NOP DWORD ptr [EAX + 00H]
            5      0F 1F 44 00 00H                NOP DWORD ptr [EAX + EAX*1 + 00H]
            6      66 0F 1F 44 00 00H             NOP DWORD ptr [AX + AX*1 + 00H]
            7      0F 1F 80 00 00 00 00H          NOP DWORD ptr [EAX + 00000000H]
            8      0F 1F 84 00 00 00 00 00H       NOP DWORD ptr [AX + AX*1 + 00000000H]
            9      66 0F 1F 84 00 00 00 00 00H    NOP DWORD ptr [AX + AX*1 + 00000000H]
        */
        while n >= 9 {
            n -= 9;
            self.emit_bytes(&[0x66, 0x0f, 0x1f, 0x84, 0x00, 0x00, 0x00, 0x00, 0x00]);
            // 9-byte nop
        }
        let seq: &[u8] = match n {
            0 => &[],
            1 => &[0x90],
            2 => &[0x66, 0x90],
            3 => &[0x0f, 0x1f, 0x00],
            4 => &[0x0f, 0x1f, 0x40, 0x00],
            5 => &[0x0f, 0x1f, 0x44, 0x00, 0x00],
            6 => &[0x66, 0x0f, 0x1f, 0x44, 0x00, 0x00],
            7 => &[0x0f, 0x1f, 0x80, 0x00, 0x00, 0x00, 0x00],
            8 => &[0x0f, 0x1f, 0x84, 0x00, 0x00, 0x00, 0x00, 0x00],
            _ => unreachable!(),
        };
        self.emit_bytes(seq);
    }

    fn emit_mov(&mut self, sz: Size, src: Location, dst: Location) {
        // fast path
        if let (Location::Imm32(0), Location::GPR(x)) = (src, dst) {
            dynasm!(self ; xor Rd(x as u8), Rd(x as u8));
            return;
        }

        binop_all_nofp!(mov, self, sz, src, dst, {
            binop_imm64_gpr!(mov, self, sz, src, dst, {
                match (sz, src, dst) {
                    (Size::S8, Location::GPR(src), Location::Memory(dst, disp)) => {
                        dynasm!(self ; mov [Rq(dst as u8) + disp], Rb(src as u8));
                    }
                    (Size::S8, Location::Memory(src, disp), Location::GPR(dst)) => {
                        dynasm!(self ; mov Rb(dst as u8), [Rq(src as u8) + disp]);
                    }
                    (Size::S8, Location::Imm32(src), Location::GPR(dst)) => {
                        dynasm!(self ; mov Rb(dst as u8), src as i8);
                    }
                    (Size::S8, Location::Imm64(src), Location::GPR(dst)) => {
                        dynasm!(self ; mov Rb(dst as u8), src as i8);
                    }
                    (Size::S8, Location::Imm32(src), Location::Memory(dst, disp)) => {
                        dynasm!(self ; mov BYTE [Rq(dst as u8) + disp], src as i8);
                    }
                    (Size::S8, Location::Imm64(src), Location::Memory(dst, disp)) => {
                        dynasm!(self ; mov BYTE [Rq(dst as u8) + disp], src as i8);
                    }
                    (Size::S16, Location::GPR(src), Location::Memory(dst, disp)) => {
                        dynasm!(self ; mov [Rq(dst as u8) + disp], Rw(src as u8));
                    }
                    (Size::S16, Location::Memory(src, disp), Location::GPR(dst)) => {
                        dynasm!(self ; mov Rw(dst as u8), [Rq(src as u8) + disp]);
                    }
                    (Size::S16, Location::Imm32(src), Location::GPR(dst)) => {
                        dynasm!(self ; mov Rw(dst as u8), src as i16);
                    }
                    (Size::S16, Location::Imm64(src), Location::GPR(dst)) => {
                        dynasm!(self ; mov Rw(dst as u8), src as i16);
                    }
                    (Size::S16, Location::Imm32(src), Location::Memory(dst, disp)) => {
                        dynasm!(self ; mov WORD [Rq(dst as u8) + disp], src as i16);
                    }
                    (Size::S16, Location::Imm64(src), Location::Memory(dst, disp)) => {
                        dynasm!(self ; mov WORD [Rq(dst as u8) + disp], src as i16);
                    }
                    (Size::S32, Location::Imm64(src), Location::GPR(dst)) => {
                        dynasm!(self ; mov Rd(dst as u8), src as i32);
                    }
                    (Size::S32, Location::Imm64(src), Location::Memory(dst, disp)) => {
                        dynasm!(self ; mov DWORD [Rq(dst as u8) + disp], src as i32);
                    }
                    (Size::S32, Location::GPR(src), Location::XMM(dst)) => {
                        dynasm!(self ; movd Rx(dst as u8), Rd(src as u8));
                    }
                    (Size::S32, Location::XMM(src), Location::GPR(dst)) => {
                        dynasm!(self ; movd Rd(dst as u8), Rx(src as u8));
                    }
                    (Size::S32, Location::Memory(src, disp), Location::XMM(dst)) => {
                        dynasm!(self ; movd Rx(dst as u8), [Rq(src as u8) + disp]);
                    }
                    (Size::S32, Location::XMM(src), Location::Memory(dst, disp)) => {
                        dynasm!(self ; movd [Rq(dst as u8) + disp], Rx(src as u8));
                    }

                    (Size::S64, Location::GPR(src), Location::XMM(dst)) => {
                        dynasm!(self ; movq Rx(dst as u8), Rq(src as u8));
                    }
                    (Size::S64, Location::XMM(src), Location::GPR(dst)) => {
                        dynasm!(self ; movq Rq(dst as u8), Rx(src as u8));
                    }
                    (Size::S64, Location::Memory(src, disp), Location::XMM(dst)) => {
                        dynasm!(self ; movq Rx(dst as u8), [Rq(src as u8) + disp]);
                    }
                    (Size::S64, Location::XMM(src), Location::Memory(dst, disp)) => {
                        dynasm!(self ; movq [Rq(dst as u8) + disp], Rx(src as u8));
                    }
                    (_, Location::XMM(src), Location::XMM(dst)) => {
                        dynasm!(self ; movq Rx(dst as u8), Rx(src as u8));
                    }

                    _ => panic!("singlepass can't emit MOV {:?} {:?} {:?}", sz, src, dst),
                }
            })
        });
    }
    fn emit_lea(&mut self, sz: Size, src: Location, dst: Location) {
        match (sz, src, dst) {
            (Size::S32, Location::Memory(src, disp), Location::GPR(dst)) => {
                dynasm!(self ; lea Rd(dst as u8), [Rq(src as u8) + disp]);
            }
            (Size::S64, Location::Memory(src, disp), Location::GPR(dst)) => {
                dynasm!(self ; lea Rq(dst as u8), [Rq(src as u8) + disp]);
            }
            (Size::S32, Location::MemoryAddTriple(src1, src2, disp), Location::GPR(dst)) => {
                dynasm!(self ; lea Rd(dst as u8), [Rq(src1 as u8) + Rq(src2 as u8) + disp]);
            }
            (Size::S64, Location::MemoryAddTriple(src1, src2, disp), Location::GPR(dst)) => {
                dynasm!(self ; lea Rq(dst as u8), [Rq(src1 as u8) + Rq(src2 as u8) + disp]);
            }
            _ => panic!("singlepass can't emit LEA {:?} {:?} {:?}", sz, src, dst),
        }
    }
    fn emit_lea_label(&mut self, label: Self::Label, dst: Location) {
        match dst {
            Location::GPR(x) => {
                dynasm!(self ; lea Rq(x as u8), [=>label]);
            }
            _ => panic!("singlepass can't emit LEA label={:?} {:?}", label, dst),
        }
    }
    fn emit_cdq(&mut self) {
        dynasm!(self ; cdq);
    }
    fn emit_cqo(&mut self) {
        dynasm!(self ; cqo);
    }
    fn emit_xor(&mut self, sz: Size, src: Location, dst: Location) {
        binop_all_nofp!(xor, self, sz, src, dst, {
            panic!("singlepass can't emit XOR {:?} {:?} {:?}", sz, src, dst)
        });
    }
    fn emit_jmp(&mut self, condition: Condition, label: Self::Label) {
        match condition {
            Condition::None => jmp_op!(jmp, self, label),
            Condition::Above => jmp_op!(ja, self, label),
            Condition::AboveEqual => jmp_op!(jae, self, label),
            Condition::Below => jmp_op!(jb, self, label),
            Condition::BelowEqual => jmp_op!(jbe, self, label),
            Condition::Greater => jmp_op!(jg, self, label),
            Condition::GreaterEqual => jmp_op!(jge, self, label),
            Condition::Less => jmp_op!(jl, self, label),
            Condition::LessEqual => jmp_op!(jle, self, label),
            Condition::Equal => jmp_op!(je, self, label),
            Condition::NotEqual => jmp_op!(jne, self, label),
            Condition::Signed => jmp_op!(js, self, label),
            Condition::Carry => jmp_op!(jc, self, label),
            Condition::Overflow => jmp_op!(jo, self, label),
        }
    }
    fn emit_jmp_location(&mut self, loc: Location) {
        match loc {
            Location::GPR(x) => dynasm!(self ; jmp Rq(x as u8)),
            Location::Memory(base, disp) => dynasm!(self ; jmp QWORD [Rq(base as u8) + disp]),
            _ => panic!("singlepass can't emit JMP {:?}", loc),
        }
    }
    fn emit_set(&mut self, condition: Condition, dst: GPR) {
        match condition {
            Condition::Above => dynasm!(self ; seta Rb(dst as u8)),
            Condition::AboveEqual => dynasm!(self ; setae Rb(dst as u8)),
            Condition::Below => dynasm!(self ; setb Rb(dst as u8)),
            Condition::BelowEqual => dynasm!(self ; setbe Rb(dst as u8)),
            Condition::Greater => dynasm!(self ; setg Rb(dst as u8)),
            Condition::GreaterEqual => dynasm!(self ; setge Rb(dst as u8)),
            Condition::Less => dynasm!(self ; setl Rb(dst as u8)),
            Condition::LessEqual => dynasm!(self ; setle Rb(dst as u8)),
            Condition::Equal => dynasm!(self ; sete Rb(dst as u8)),
            Condition::NotEqual => dynasm!(self ; setne Rb(dst as u8)),
            Condition::Signed => dynasm!(self ; sets Rb(dst as u8)),
            Condition::Carry => dynasm!(self ; setc Rb(dst as u8)),
            Condition::Overflow => dynasm!(self ; seto Rb(dst as u8)),
            _ => panic!("singlepass can't emit SET {:?} {:?}", condition, dst),
        }
    }
    fn emit_push(&mut self, sz: Size, src: Location) {
        match (sz, src) {
            (Size::S64, Location::Imm32(src)) => dynasm!(self ; push src as i32),
            (Size::S64, Location::GPR(src)) => dynasm!(self ; push Rq(src as u8)),
            (Size::S64, Location::Memory(src, disp)) => {
                dynasm!(self ; push QWORD [Rq(src as u8) + disp])
            }
            _ => panic!("singlepass can't emit PUSH {:?} {:?}", sz, src),
        }
    }
    fn emit_pop(&mut self, sz: Size, dst: Location) {
        match (sz, dst) {
            (Size::S64, Location::GPR(dst)) => dynasm!(self ; pop Rq(dst as u8)),
            (Size::S64, Location::Memory(dst, disp)) => {
                dynasm!(self ; pop QWORD [Rq(dst as u8) + disp])
            }
            _ => panic!("singlepass can't emit POP {:?} {:?}", sz, dst),
        }
    }
    fn emit_cmp(&mut self, sz: Size, left: Location, right: Location) {
        // Constant elimination for comparision between consts.
        //
        // Only needed for `emit_cmp`, since other binary operators actually write to `right` and `right` must
        // be a writable location for them.
        let consts = match (left, right) {
            (Location::Imm32(x), Location::Imm32(y)) => Some((x as i32 as i64, y as i32 as i64)),
            (Location::Imm32(x), Location::Imm64(y)) => Some((x as i32 as i64, y as i64)),
            (Location::Imm64(x), Location::Imm32(y)) => Some((x as i64, y as i32 as i64)),
            (Location::Imm64(x), Location::Imm64(y)) => Some((x as i64, y as i64)),
            _ => None,
        };
        use std::cmp::Ordering;
        match consts {
            Some((x, y)) => match x.cmp(&y) {
                Ordering::Less => dynasm!(self ; cmp DWORD [>const_neg_one_32], 0),
                Ordering::Equal => dynasm!(self ; cmp DWORD [>const_zero_32], 0),
                Ordering::Greater => dynasm!(self ; cmp DWORD [>const_pos_one_32], 0),
            },
            None => binop_all_nofp!(cmp, self, sz, left, right, {
                panic!("singlepass can't emit CMP {:?} {:?} {:?}", sz, left, right);
            }),
        }
    }
    fn emit_add(&mut self, sz: Size, src: Location, dst: Location) {
        // Fast path
        if let Location::Imm32(0) = src {
            return;
        }
        binop_all_nofp!(add, self, sz, src, dst, {
            panic!("singlepass can't emit ADD {:?} {:?} {:?}", sz, src, dst)
        });
    }
    fn emit_sub(&mut self, sz: Size, src: Location, dst: Location) {
        // Fast path
        if let Location::Imm32(0) = src {
            return;
        }
        binop_all_nofp!(sub, self, sz, src, dst, {
            panic!("singlepass can't emit SUB {:?} {:?} {:?}", sz, src, dst)
        });
    }
    fn emit_neg(&mut self, sz: Size, value: Location) {
        match (sz, value) {
            (Size::S8, Location::GPR(value)) => dynasm!(self ; neg Rb(value as u8)),
            (Size::S8, Location::Memory(value, disp)) => {
                dynasm!(self ; neg [Rq(value as u8) + disp])
            }
            (Size::S16, Location::GPR(value)) => dynasm!(self ; neg Rw(value as u8)),
            (Size::S16, Location::Memory(value, disp)) => {
                dynasm!(self ; neg [Rq(value as u8) + disp])
            }
            (Size::S32, Location::GPR(value)) => dynasm!(self ; neg Rd(value as u8)),
            (Size::S32, Location::Memory(value, disp)) => {
                dynasm!(self ; neg [Rq(value as u8) + disp])
            }
            (Size::S64, Location::GPR(value)) => dynasm!(self ; neg Rq(value as u8)),
            (Size::S64, Location::Memory(value, disp)) => {
                dynasm!(self ; neg [Rq(value as u8) + disp])
            }
            _ => panic!("singlepass can't emit NEG {:?} {:?}", sz, value),
        }
    }
    fn emit_imul(&mut self, sz: Size, src: Location, dst: Location) {
        binop_gpr_gpr!(imul, self, sz, src, dst, {
            binop_mem_gpr!(imul, self, sz, src, dst, {
                panic!("singlepass can't emit IMUL {:?} {:?} {:?}", sz, src, dst)
            })
        });
    }
    fn emit_imul_imm32_gpr64(&mut self, src: u32, dst: GPR) {
        dynasm!(self ; imul Rq(dst as u8), Rq(dst as u8), src as i32);
    }
    fn emit_div(&mut self, sz: Size, divisor: Location) {
        unop_gpr_or_mem!(div, self, sz, divisor, {
            panic!("singlepass can't emit DIV {:?} {:?}", sz, divisor)
        });
    }
    fn emit_idiv(&mut self, sz: Size, divisor: Location) {
        unop_gpr_or_mem!(idiv, self, sz, divisor, {
            panic!("singlepass can't emit IDIV {:?} {:?}", sz, divisor)
        });
    }
    fn emit_shl(&mut self, sz: Size, src: Location, dst: Location) {
        binop_shift!(shl, self, sz, src, dst, {
            panic!("singlepass can't emit SHL {:?} {:?} {:?}", sz, src, dst)
        });
    }
    fn emit_shr(&mut self, sz: Size, src: Location, dst: Location) {
        binop_shift!(shr, self, sz, src, dst, {
            panic!("singlepass can't emit SHR {:?} {:?} {:?}", sz, src, dst)
        });
    }
    fn emit_sar(&mut self, sz: Size, src: Location, dst: Location) {
        binop_shift!(sar, self, sz, src, dst, {
            panic!("singlepass can't emit SAR {:?} {:?} {:?}", sz, src, dst)
        });
    }
    fn emit_rol(&mut self, sz: Size, src: Location, dst: Location) {
        binop_shift!(rol, self, sz, src, dst, {
            panic!("singlepass can't emit ROL {:?} {:?} {:?}", sz, src, dst)
        });
    }
    fn emit_ror(&mut self, sz: Size, src: Location, dst: Location) {
        binop_shift!(ror, self, sz, src, dst, {
            panic!("singlepass can't emit ROR {:?} {:?} {:?}", sz, src, dst)
        });
    }
    fn emit_and(&mut self, sz: Size, src: Location, dst: Location) {
        binop_all_nofp!(and, self, sz, src, dst, {
            panic!("singlepass can't emit AND {:?} {:?} {:?}", sz, src, dst)
        });
    }
    fn emit_or(&mut self, sz: Size, src: Location, dst: Location) {
        binop_all_nofp!(or, self, sz, src, dst, {
            panic!("singlepass can't emit OR {:?} {:?} {:?}", sz, src, dst)
        });
    }
    fn emit_bsr(&mut self, sz: Size, src: Location, dst: Location) {
        binop_gpr_gpr!(bsr, self, sz, src, dst, {
            binop_mem_gpr!(bsr, self, sz, src, dst, {
                panic!("singlepass can't emit BSR {:?} {:?} {:?}", sz, src, dst)
            })
        });
    }
    fn emit_bsf(&mut self, sz: Size, src: Location, dst: Location) {
        binop_gpr_gpr!(bsf, self, sz, src, dst, {
            binop_mem_gpr!(bsf, self, sz, src, dst, {
                panic!("singlepass can't emit BSF {:?} {:?} {:?}", sz, src, dst)
            })
        });
    }
    fn emit_popcnt(&mut self, sz: Size, src: Location, dst: Location) {
        binop_gpr_gpr!(popcnt, self, sz, src, dst, {
            binop_mem_gpr!(popcnt, self, sz, src, dst, {
                panic!("singlepass can't emit POPCNT {:?} {:?} {:?}", sz, src, dst)
            })
        });
    }
    fn emit_movzx(&mut self, sz_src: Size, src: Location, sz_dst: Size, dst: Location) {
        match (sz_src, src, sz_dst, dst) {
            (Size::S8, Location::GPR(src), Size::S32, Location::GPR(dst)) => {
                dynasm!(self ; movzx Rd(dst as u8), Rb(src as u8));
            }
            (Size::S16, Location::GPR(src), Size::S32, Location::GPR(dst)) => {
                dynasm!(self ; movzx Rd(dst as u8), Rw(src as u8));
            }
            (Size::S8, Location::Memory(src, disp), Size::S32, Location::GPR(dst)) => {
                dynasm!(self ; movzx Rd(dst as u8), BYTE [Rq(src as u8) + disp]);
            }
            (Size::S16, Location::Memory(src, disp), Size::S32, Location::GPR(dst)) => {
                dynasm!(self ; movzx Rd(dst as u8), WORD [Rq(src as u8) + disp]);
            }
            (Size::S8, Location::GPR(src), Size::S64, Location::GPR(dst)) => {
                dynasm!(self ; movzx Rq(dst as u8), Rb(src as u8));
            }
            (Size::S16, Location::GPR(src), Size::S64, Location::GPR(dst)) => {
                dynasm!(self ; movzx Rq(dst as u8), Rw(src as u8));
            }
            (Size::S8, Location::Memory(src, disp), Size::S64, Location::GPR(dst)) => {
                dynasm!(self ; movzx Rq(dst as u8), BYTE [Rq(src as u8) + disp]);
            }
            (Size::S16, Location::Memory(src, disp), Size::S64, Location::GPR(dst)) => {
                dynasm!(self ; movzx Rq(dst as u8), WORD [Rq(src as u8) + disp]);
            }
            _ => {
                panic!("singlepass can't emit MOVZX {:?} {:?} {:?} {:?}", sz_src, src, sz_dst, dst)
            }
        }
    }
    fn emit_movsx(&mut self, sz_src: Size, src: Location, sz_dst: Size, dst: Location) {
        match (sz_src, src, sz_dst, dst) {
            (Size::S8, Location::GPR(src), Size::S32, Location::GPR(dst)) => {
                dynasm!(self ; movsx Rd(dst as u8), Rb(src as u8));
            }
            (Size::S16, Location::GPR(src), Size::S32, Location::GPR(dst)) => {
                dynasm!(self ; movsx Rd(dst as u8), Rw(src as u8));
            }
            (Size::S8, Location::Memory(src, disp), Size::S32, Location::GPR(dst)) => {
                dynasm!(self ; movsx Rd(dst as u8), BYTE [Rq(src as u8) + disp]);
            }
            (Size::S16, Location::Memory(src, disp), Size::S32, Location::GPR(dst)) => {
                dynasm!(self ; movsx Rd(dst as u8), WORD [Rq(src as u8) + disp]);
            }
            (Size::S8, Location::GPR(src), Size::S64, Location::GPR(dst)) => {
                dynasm!(self ; movsx Rq(dst as u8), Rb(src as u8));
            }
            (Size::S16, Location::GPR(src), Size::S64, Location::GPR(dst)) => {
                dynasm!(self ; movsx Rq(dst as u8), Rw(src as u8));
            }
            (Size::S32, Location::GPR(src), Size::S64, Location::GPR(dst)) => {
                dynasm!(self ; movsx Rq(dst as u8), Rd(src as u8));
            }
            (Size::S8, Location::Memory(src, disp), Size::S64, Location::GPR(dst)) => {
                dynasm!(self ; movsx Rq(dst as u8), BYTE [Rq(src as u8) + disp]);
            }
            (Size::S16, Location::Memory(src, disp), Size::S64, Location::GPR(dst)) => {
                dynasm!(self ; movsx Rq(dst as u8), WORD [Rq(src as u8) + disp]);
            }
            (Size::S32, Location::Memory(src, disp), Size::S64, Location::GPR(dst)) => {
                dynasm!(self ; movsx Rq(dst as u8), DWORD [Rq(src as u8) + disp]);
            }
            _ => {
                panic!("singlepass can't emit MOVSX {:?} {:?} {:?} {:?}", sz_src, src, sz_dst, dst)
            }
        }
    }

    fn emit_xchg(&mut self, sz: Size, src: Location, dst: Location) {
        match (sz, src, dst) {
            (Size::S8, Location::GPR(src), Location::GPR(dst)) => {
                dynasm!(self ; xchg Rb(dst as u8), Rb(src as u8));
            }
            (Size::S16, Location::GPR(src), Location::GPR(dst)) => {
                dynasm!(self ; xchg Rw(dst as u8), Rw(src as u8));
            }
            (Size::S32, Location::GPR(src), Location::GPR(dst)) => {
                dynasm!(self ; xchg Rd(dst as u8), Rd(src as u8));
            }
            (Size::S64, Location::GPR(src), Location::GPR(dst)) => {
                dynasm!(self ; xchg Rq(dst as u8), Rq(src as u8));
            }
            (Size::S8, Location::Memory(src, disp), Location::GPR(dst)) => {
                dynasm!(self ; xchg Rb(dst as u8), [Rq(src as u8) + disp]);
            }
            (Size::S8, Location::GPR(src), Location::Memory(dst, disp)) => {
                dynasm!(self ; xchg [Rq(dst as u8) + disp], Rb(src as u8));
            }
            (Size::S16, Location::Memory(src, disp), Location::GPR(dst)) => {
                dynasm!(self ; xchg Rw(dst as u8), [Rq(src as u8) + disp]);
            }
            (Size::S16, Location::GPR(src), Location::Memory(dst, disp)) => {
                dynasm!(self ; xchg [Rq(dst as u8) + disp], Rw(src as u8));
            }
            (Size::S32, Location::Memory(src, disp), Location::GPR(dst)) => {
                dynasm!(self ; xchg Rd(dst as u8), [Rq(src as u8) + disp]);
            }
            (Size::S32, Location::GPR(src), Location::Memory(dst, disp)) => {
                dynasm!(self ; xchg [Rq(dst as u8) + disp], Rd(src as u8));
            }
            (Size::S64, Location::Memory(src, disp), Location::GPR(dst)) => {
                dynasm!(self ; xchg Rq(dst as u8), [Rq(src as u8) + disp]);
            }
            (Size::S64, Location::GPR(src), Location::Memory(dst, disp)) => {
                dynasm!(self ; xchg [Rq(dst as u8) + disp], Rq(src as u8));
            }
            _ => panic!("singlepass can't emit XCHG {:?} {:?} {:?}", sz, src, dst),
        }
    }

    fn emit_lock_xadd(&mut self, sz: Size, src: Location, dst: Location) {
        match (sz, src, dst) {
            (Size::S8, Location::GPR(src), Location::Memory(dst, disp)) => {
                dynasm!(self ; lock xadd [Rq(dst as u8) + disp], Rb(src as u8));
            }
            (Size::S16, Location::GPR(src), Location::Memory(dst, disp)) => {
                dynasm!(self ; lock xadd [Rq(dst as u8) + disp], Rw(src as u8));
            }
            (Size::S32, Location::GPR(src), Location::Memory(dst, disp)) => {
                dynasm!(self ; lock xadd [Rq(dst as u8) + disp], Rd(src as u8));
            }
            (Size::S64, Location::GPR(src), Location::Memory(dst, disp)) => {
                dynasm!(self ; lock xadd [Rq(dst as u8) + disp], Rq(src as u8));
            }
            _ => panic!("singlepass can't emit LOCK XADD {:?} {:?} {:?}", sz, src, dst),
        }
    }

    fn emit_lock_cmpxchg(&mut self, sz: Size, src: Location, dst: Location) {
        match (sz, src, dst) {
            (Size::S8, Location::GPR(src), Location::Memory(dst, disp)) => {
                dynasm!(self ; lock cmpxchg [Rq(dst as u8) + disp], Rb(src as u8));
            }
            (Size::S16, Location::GPR(src), Location::Memory(dst, disp)) => {
                dynasm!(self ; lock cmpxchg [Rq(dst as u8) + disp], Rw(src as u8));
            }
            (Size::S32, Location::GPR(src), Location::Memory(dst, disp)) => {
                dynasm!(self ; lock cmpxchg [Rq(dst as u8) + disp], Rd(src as u8));
            }
            (Size::S64, Location::GPR(src), Location::Memory(dst, disp)) => {
                dynasm!(self ; lock cmpxchg [Rq(dst as u8) + disp], Rq(src as u8));
            }
            _ => panic!("singlepass can't emit LOCK CMPXCHG {:?} {:?} {:?}", sz, src, dst),
        }
    }

    fn emit_rep_stosq(&mut self) {
        dynasm!(self ; rep stosq);
    }
    fn emit_btc_gpr_imm8_32(&mut self, src: u8, dst: GPR) {
        dynasm!(self ; btc Rd(dst as u8), BYTE src as i8);
    }

    fn emit_btc_gpr_imm8_64(&mut self, src: u8, dst: GPR) {
        dynasm!(self ; btc Rq(dst as u8), BYTE src as i8);
    }

    fn emit_cmovae_gpr_32(&mut self, src: GPR, dst: GPR) {
        dynasm!(self ; cmovae Rd(dst as u8), Rd(src as u8));
    }

    fn emit_cmovae_gpr_64(&mut self, src: GPR, dst: GPR) {
        dynasm!(self ; cmovae Rq(dst as u8), Rq(src as u8));
    }

    fn emit_vmovaps(&mut self, src: XMMOrMemory, dst: XMMOrMemory) {
        match (src, dst) {
            (XMMOrMemory::XMM(src), XMMOrMemory::XMM(dst)) => {
                dynasm!(self ; movaps Rx(dst as u8), Rx(src as u8))
            }
            (XMMOrMemory::Memory(base, disp), XMMOrMemory::XMM(dst)) => {
                dynasm!(self ; movaps Rx(dst as u8), [Rq(base as u8) + disp])
            }
            (XMMOrMemory::XMM(src), XMMOrMemory::Memory(base, disp)) => {
                dynasm!(self ; movaps [Rq(base as u8) + disp], Rx(src as u8))
            }
            _ => panic!("singlepass can't emit VMOVAPS {:?} {:?}", src, dst),
        };
    }

    fn emit_vmovapd(&mut self, src: XMMOrMemory, dst: XMMOrMemory) {
        match (src, dst) {
            (XMMOrMemory::XMM(src), XMMOrMemory::XMM(dst)) => {
                dynasm!(self ; movapd Rx(dst as u8), Rx(src as u8))
            }
            (XMMOrMemory::Memory(base, disp), XMMOrMemory::XMM(dst)) => {
                dynasm!(self ; movapd Rx(dst as u8), [Rq(base as u8) + disp])
            }
            (XMMOrMemory::XMM(src), XMMOrMemory::Memory(base, disp)) => {
                dynasm!(self ; movapd [Rq(base as u8) + disp], Rx(src as u8))
            }
            _ => panic!("singlepass can't emit VMOVAPD {:?} {:?}", src, dst),
        };
    }

    avx_fn!(vxorps, emit_vxorps);
    avx_fn!(vxorpd, emit_vxorpd);

    avx_fn!(vaddss, emit_vaddss);
    avx_fn!(vaddsd, emit_vaddsd);

    avx_fn!(vsubss, emit_vsubss);
    avx_fn!(vsubsd, emit_vsubsd);

    avx_fn!(vmulss, emit_vmulss);
    avx_fn!(vmulsd, emit_vmulsd);

    avx_fn!(vdivss, emit_vdivss);
    avx_fn!(vdivsd, emit_vdivsd);

    avx_fn!(vmaxss, emit_vmaxss);
    avx_fn!(vmaxsd, emit_vmaxsd);

    avx_fn!(vminss, emit_vminss);
    avx_fn!(vminsd, emit_vminsd);

    avx_fn!(vcmpeqss, emit_vcmpeqss);
    avx_fn!(vcmpeqsd, emit_vcmpeqsd);

    avx_fn!(vcmpneqss, emit_vcmpneqss);
    avx_fn!(vcmpneqsd, emit_vcmpneqsd);

    avx_fn!(vcmpltss, emit_vcmpltss);
    avx_fn!(vcmpltsd, emit_vcmpltsd);

    avx_fn!(vcmpless, emit_vcmpless);
    avx_fn!(vcmplesd, emit_vcmplesd);

    avx_fn!(vcmpgtss, emit_vcmpgtss);
    avx_fn!(vcmpgtsd, emit_vcmpgtsd);

    avx_fn!(vcmpgess, emit_vcmpgess);
    avx_fn!(vcmpgesd, emit_vcmpgesd);

    avx_fn!(vcmpunordss, emit_vcmpunordss);
    avx_fn!(vcmpunordsd, emit_vcmpunordsd);

    avx_fn!(vcmpordss, emit_vcmpordss);
    avx_fn!(vcmpordsd, emit_vcmpordsd);

    avx_fn!(vsqrtss, emit_vsqrtss);
    avx_fn!(vsqrtsd, emit_vsqrtsd);

    avx_fn!(vcvtss2sd, emit_vcvtss2sd);
    avx_fn!(vcvtsd2ss, emit_vcvtsd2ss);

    avx_round_fn!(vroundss, emit_vroundss_nearest, 0);
    avx_round_fn!(vroundss, emit_vroundss_floor, 1);
    avx_round_fn!(vroundss, emit_vroundss_ceil, 2);
    avx_round_fn!(vroundss, emit_vroundss_trunc, 3);
    avx_round_fn!(vroundsd, emit_vroundsd_nearest, 0);
    avx_round_fn!(vroundsd, emit_vroundsd_floor, 1);
    avx_round_fn!(vroundsd, emit_vroundsd_ceil, 2);
    avx_round_fn!(vroundsd, emit_vroundsd_trunc, 3);

    avx_i2f_32_fn!(vcvtsi2ss, emit_vcvtsi2ss_32);
    avx_i2f_32_fn!(vcvtsi2sd, emit_vcvtsi2sd_32);
    avx_i2f_64_fn!(vcvtsi2ss, emit_vcvtsi2ss_64);
    avx_i2f_64_fn!(vcvtsi2sd, emit_vcvtsi2sd_64);

    fn emit_vblendvps(&mut self, src1: XMM, src2: XMMOrMemory, mask: XMM, dst: XMM) {
        match src2 {
            XMMOrMemory::XMM(src2) => {
                dynasm!(self ; vblendvps Rx(dst as u8), Rx(mask as u8), Rx(src2 as u8), Rx(src1 as u8))
            }
            XMMOrMemory::Memory(base, disp) => {
                dynasm!(self ; vblendvps Rx(dst as u8), Rx(mask as u8), [Rq(base as u8) + disp], Rx(src1 as u8))
            }
        }
    }

    fn emit_vblendvpd(&mut self, src1: XMM, src2: XMMOrMemory, mask: XMM, dst: XMM) {
        match src2 {
            XMMOrMemory::XMM(src2) => {
                dynasm!(self ; vblendvpd Rx(dst as u8), Rx(mask as u8), Rx(src2 as u8), Rx(src1 as u8))
            }
            XMMOrMemory::Memory(base, disp) => {
                dynasm!(self ; vblendvpd Rx(dst as u8), Rx(mask as u8), [Rq(base as u8) + disp], Rx(src1 as u8))
            }
        }
    }

    fn emit_ucomiss(&mut self, src: XMMOrMemory, dst: XMM) {
        match src {
            XMMOrMemory::XMM(x) => dynasm!(self ; ucomiss Rx(dst as u8), Rx(x as u8)),
            XMMOrMemory::Memory(base, disp) => {
                dynasm!(self ; ucomiss Rx(dst as u8), [Rq(base as u8) + disp])
            }
        }
    }

    fn emit_ucomisd(&mut self, src: XMMOrMemory, dst: XMM) {
        match src {
            XMMOrMemory::XMM(x) => dynasm!(self ; ucomisd Rx(dst as u8), Rx(x as u8)),
            XMMOrMemory::Memory(base, disp) => {
                dynasm!(self ; ucomisd Rx(dst as u8), [Rq(base as u8) + disp])
            }
        }
    }

    fn emit_cvttss2si_32(&mut self, src: XMMOrMemory, dst: GPR) {
        match src {
            XMMOrMemory::XMM(x) => dynasm!(self ; cvttss2si Rd(dst as u8), Rx(x as u8)),
            XMMOrMemory::Memory(base, disp) => {
                dynasm!(self ; cvttss2si Rd(dst as u8), [Rq(base as u8) + disp])
            }
        }
    }

    fn emit_cvttss2si_64(&mut self, src: XMMOrMemory, dst: GPR) {
        match src {
            XMMOrMemory::XMM(x) => dynasm!(self ; cvttss2si Rq(dst as u8), Rx(x as u8)),
            XMMOrMemory::Memory(base, disp) => {
                dynasm!(self ; cvttss2si Rq(dst as u8), [Rq(base as u8) + disp])
            }
        }
    }

    fn emit_cvttsd2si_32(&mut self, src: XMMOrMemory, dst: GPR) {
        match src {
            XMMOrMemory::XMM(x) => dynasm!(self ; cvttsd2si Rd(dst as u8), Rx(x as u8)),
            XMMOrMemory::Memory(base, disp) => {
                dynasm!(self ; cvttsd2si Rd(dst as u8), [Rq(base as u8) + disp])
            }
        }
    }

    fn emit_cvttsd2si_64(&mut self, src: XMMOrMemory, dst: GPR) {
        match src {
            XMMOrMemory::XMM(x) => dynasm!(self ; cvttsd2si Rq(dst as u8), Rx(x as u8)),
            XMMOrMemory::Memory(base, disp) => {
                dynasm!(self ; cvttsd2si Rq(dst as u8), [Rq(base as u8) + disp])
            }
        }
    }

    fn emit_test_gpr_64(&mut self, reg: GPR) {
        dynasm!(self ; test Rq(reg as u8), Rq(reg as u8));
    }

    fn emit_ud2(&mut self) {
        dynasm!(self ; ud2);
    }
    fn emit_ret(&mut self) {
        dynasm!(self ; ret);
    }

    fn emit_call_label(&mut self, label: Self::Label) {
        dynasm!(self ; call =>label);
    }
    fn emit_call_location(&mut self, loc: Location) {
        match loc {
            Location::GPR(x) => dynasm!(self ; call Rq(x as u8)),
            Location::Memory(base, disp) => dynasm!(self ; call QWORD [Rq(base as u8) + disp]),
            _ => panic!("singlepass can't emit CALL {:?}", loc),
        }
    }

    fn emit_call_register(&mut self, reg: GPR) {
        dynasm!(self ; call Rq(reg as u8));
    }

    fn emit_bkpt(&mut self) {
        dynasm!(self ; int3);
    }

    fn emit_host_redirection(&mut self, target: GPR) {
        self.emit_jmp_location(Location::GPR(target));
    }

    fn arch_has_xzcnt(&self, cpu: &EnumSet<CpuFeature>) -> bool {
        cpu.contains(CpuFeature::BMI1) && cpu.contains(CpuFeature::LZCNT)
    }

    fn arch_emit_lzcnt(&mut self, sz: Size, src: Location, dst: Location) {
        binop_gpr_gpr!(lzcnt, self, sz, src, dst, {
            binop_mem_gpr!(lzcnt, self, sz, src, dst, {
                panic!("singlepass can't emit LZCNT {:?} {:?} {:?}", sz, src, dst)
            })
        })
    }

    fn arch_emit_tzcnt(&mut self, sz: Size, src: Location, dst: Location) {
        binop_gpr_gpr!(tzcnt, self, sz, src, dst, {
            binop_mem_gpr!(tzcnt, self, sz, src, dst, {
                panic!("singlepass can't emit TZCNT {:?} {:?} {:?}", sz, src, dst)
            })
        })
    }

    fn arch_mov64_imm_offset(&self) -> usize {
        2
    }
}
