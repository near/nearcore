/*
 * Copyright (c) The mlkem-native project authors
 * Copyright (c) The mldsa-native project authors
 * SPDX-License-Identifier: Apache-2.0 OR ISC OR MIT
 */
#ifndef MLD_FIPS202_NATIVE_AARCH64_SRC_FIPS202_NATIVE_AARCH64_H
#define MLD_FIPS202_NATIVE_AARCH64_SRC_FIPS202_NATIVE_AARCH64_H


#include "../../../../cbmc.h"
#include "../../../../common.h"


#define mld_keccakf1600_round_constants \
  MLD_NAMESPACE(keccakf1600_round_constants)
MLD_INTERNAL_DATA_DECLARATION const uint64_t
    mld_keccakf1600_round_constants[24];

#define mld_keccak_f1600_x1_scalar_aarch64_asm \
  MLD_NAMESPACE(keccak_f1600_x1_scalar_aarch64_asm)
void mld_keccak_f1600_x1_scalar_aarch64_asm(uint64_t state[25],
                                            const uint64_t rc[24])
__contract__(
  requires(memory_no_alias(state, sizeof(uint64_t) * 25 * 1))
  requires(rc == mld_keccakf1600_round_constants)
  assigns(memory_slice(state, sizeof(uint64_t) * 25 * 1))
);

#define mld_keccak_f1600_x1_v84a_aarch64_asm \
  MLD_NAMESPACE(keccak_f1600_x1_v84a_aarch64_asm)
void mld_keccak_f1600_x1_v84a_aarch64_asm(uint64_t state[25],
                                          const uint64_t rc[24])
__contract__(
  requires(memory_no_alias(state, sizeof(uint64_t) * 25 * 1))
  requires(rc == mld_keccakf1600_round_constants)
  assigns(memory_slice(state, sizeof(uint64_t) * 25 * 1))
);

#define mld_keccak_f1600_x2_v84a_aarch64_asm \
  MLD_NAMESPACE(keccak_f1600_x2_v84a_aarch64_asm)
void mld_keccak_f1600_x2_v84a_aarch64_asm(uint64_t state[50],
                                          const uint64_t rc[24])
__contract__(
  requires(memory_no_alias(state, sizeof(uint64_t) * 25 * 2))
  requires(rc == mld_keccakf1600_round_constants)
  assigns(memory_slice(state, sizeof(uint64_t) * 25 * 2))
);

#define mld_keccak_f1600_x4_v8a_scalar_hybrid_aarch64_asm \
  MLD_NAMESPACE(keccak_f1600_x4_v8a_scalar_hybrid_aarch64_asm)
void mld_keccak_f1600_x4_v8a_scalar_hybrid_aarch64_asm(uint64_t state[100],
                                                       const uint64_t rc[24])
__contract__(
  requires(memory_no_alias(state, sizeof(uint64_t) * 25 * 4))
  requires(rc == mld_keccakf1600_round_constants)
  assigns(memory_slice(state, sizeof(uint64_t) * 25 * 4))
);

#define mld_keccak_f1600_x4_v8a_v84a_scalar_hybrid_aarch64_asm \
  MLD_NAMESPACE(keccak_f1600_x4_v8a_v84a_scalar_hybrid_aarch64_asm)
void mld_keccak_f1600_x4_v8a_v84a_scalar_hybrid_aarch64_asm(
    uint64_t state[100], const uint64_t rc[24])
__contract__(
  requires(memory_no_alias(state, sizeof(uint64_t) * 25 * 4))
  requires(rc == mld_keccakf1600_round_constants)
  assigns(memory_slice(state, sizeof(uint64_t) * 25 * 4))
);

#endif /* !MLD_FIPS202_NATIVE_AARCH64_SRC_FIPS202_NATIVE_AARCH64_H */
