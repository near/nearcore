/*
 * Copyright (c) The mlkem-native project authors
 * Copyright (c) The mldsa-native project authors
 * SPDX-License-Identifier: Apache-2.0 OR ISC OR MIT
 */
#ifndef MLD_FIPS202_NATIVE_ARMV81M_SRC_FIPS202_NATIVE_ARMV81M_H
#define MLD_FIPS202_NATIVE_ARMV81M_SRC_FIPS202_NATIVE_ARMV81M_H

#include "../../../../common.h"

/* Keccak round constants in bit-interleaved form */
#define mld_keccakf1600_round_constants \
  MLD_NAMESPACE(keccakf1600_round_constants)
MLD_INTERNAL_DATA_DECLARATION const uint32_t
    mld_keccakf1600_round_constants[48];

#define mld_keccak_f1600_x4_mve_asm MLD_NAMESPACE(keccak_f1600_x4_mve_asm)
void mld_keccak_f1600_x4_mve_asm(uint64_t state[100], uint64_t tmpstate[100],
                                 const uint32_t rc[48]);

#define mld_keccak_f1600_x4_state_xor_bytes_asm \
  MLD_NAMESPACE(keccak_f1600_x4_state_xor_bytes_asm)
void mld_keccak_f1600_x4_state_xor_bytes_asm(void *state, const uint8_t *d0,
                                             const uint8_t *d1,
                                             const uint8_t *d2,
                                             const uint8_t *d3, unsigned offset,
                                             unsigned length);

#define mld_keccak_f1600_x4_state_extract_bytes_asm \
  MLD_NAMESPACE(keccak_f1600_x4_state_extract_bytes_asm)
void mld_keccak_f1600_x4_state_extract_bytes_asm(void *state, uint8_t *data0,
                                                 uint8_t *data1, uint8_t *data2,
                                                 uint8_t *data3,
                                                 unsigned offset,
                                                 unsigned length);

#endif /* !MLD_FIPS202_NATIVE_ARMV81M_SRC_FIPS202_NATIVE_ARMV81M_H */
