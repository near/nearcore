/*
 * Copyright (c) The mlkem-native project authors
 * Copyright (c) The mldsa-native project authors
 * SPDX-License-Identifier: Apache-2.0 OR ISC OR MIT
 */

#ifndef MLD_FIPS202_NATIVE_ARMV81M_MVE_H
#define MLD_FIPS202_NATIVE_ARMV81M_MVE_H

#define MLD_FIPS202_NATIVE_ARMV81M

/* Part of backend API */
#define MLD_USE_NATIVE_FIPS202_X4
#define MLD_USE_NATIVE_FIPS202_X4_XOR_BYTES
#define MLD_USE_NATIVE_FIPS202_X4_EXTRACT_BYTES
/* Guard for assembly file */
#define MLD_FIPS202_ARMV81M_NEED_X4

#if !defined(__ASSEMBLER__)
#include "../api.h"
#include "src/fips202_native_armv81m.h"

/*
 * Native x4 permutation
 * State is kept in bit-interleaved format.
 */
#define mld_keccak_f1600_x4_native_impl \
  MLD_NAMESPACE(keccak_f1600_x4_native_impl)
MLD_INTERNAL_API
int mld_keccak_f1600_x4_native_impl(uint64_t *state);

MLD_MUST_CHECK_RETURN_VALUE
static MLD_INLINE int mld_keccak_f1600_x4_native(uint64_t *state)
{
  return mld_keccak_f1600_x4_native_impl(state);
}

/*
 * Native x4 XOR bytes (with on-the-fly bit interleaving)
 */
MLD_MUST_CHECK_RETURN_VALUE
static MLD_INLINE int mld_keccakf1600_xor_bytes_x4_native(
    uint64_t *state, const uint8_t *data0, const uint8_t *data1,
    const uint8_t *data2, const uint8_t *data3, unsigned offset,
    unsigned length)
{
  mld_keccak_f1600_x4_state_xor_bytes_asm(state, data0, data1, data2, data3,
                                          offset, length);
  return MLD_NATIVE_FUNC_SUCCESS;
}

/*
 * Native x4 extract bytes (with on-the-fly bit de-interleaving)
 */
MLD_MUST_CHECK_RETURN_VALUE
static MLD_INLINE int mld_keccakf1600_extract_bytes_x4_native(
    uint64_t *state, uint8_t *data0, uint8_t *data1, uint8_t *data2,
    uint8_t *data3, unsigned offset, unsigned length)
{
  mld_keccak_f1600_x4_state_extract_bytes_asm(state, data0, data1, data2, data3,
                                              offset, length);
  return MLD_NATIVE_FUNC_SUCCESS;
}

#endif /* !__ASSEMBLER__ */

#endif /* !MLD_FIPS202_NATIVE_ARMV81M_MVE_H */
