/*
 * Copyright (c) The mlkem-native project authors
 * Copyright (c) The mldsa-native project authors
 * SPDX-License-Identifier: Apache-2.0 OR ISC OR MIT
 */

#ifndef MLD_FIPS202_NATIVE_X86_64_SRC_FIPS202_NATIVE_X86_64_H
#define MLD_FIPS202_NATIVE_X86_64_SRC_FIPS202_NATIVE_X86_64_H

#include "../../../../cbmc.h"
#include "../../../../common.h"

/* TODO: Reconsider whether this check is needed -- x86_64 is always
 * little-endian, so the backend selection already implies this. */
#ifndef MLD_SYS_LITTLE_ENDIAN
#error Expecting a little-endian platform
#endif

#define mld_keccakf1600_round_constants \
  MLD_NAMESPACE(keccakf1600_round_constants)
MLD_INTERNAL_DATA_DECLARATION const uint64_t
    mld_keccakf1600_round_constants[24];

#define mld_keccak_rho8 MLD_NAMESPACE(keccak_rho8)
MLD_INTERNAL_DATA_DECLARATION const uint64_t mld_keccak_rho8[4];

#define mld_keccak_rho56 MLD_NAMESPACE(keccak_rho56)
MLD_INTERNAL_DATA_DECLARATION const uint64_t mld_keccak_rho56[4];

#define mld_keccak_f1600_x4_avx2_asm MLD_NAMESPACE(keccak_f1600_x4_avx2_asm)
MLD_SYSV_ABI
void mld_keccak_f1600_x4_avx2_asm(uint64_t states[100], const uint64_t rc[24],
                                  const uint64_t rho8[4],
                                  const uint64_t rho56[4])
/* This must be kept in sync with the HOL-Light specification
 * in proofs/hol_light/x86_64/proofs/keccak_f1600_x4_avx2_asm.ml */
__contract__(
  requires(memory_no_alias(states, sizeof(uint64_t) * 25 * 4))
  requires(rc == mld_keccakf1600_round_constants)
  requires(rho8 == mld_keccak_rho8)
  requires(rho56 == mld_keccak_rho56)
  assigns(memory_slice(states, sizeof(uint64_t) * 25 * 4))
);

#endif /* !MLD_FIPS202_NATIVE_X86_64_SRC_FIPS202_NATIVE_X86_64_H */
