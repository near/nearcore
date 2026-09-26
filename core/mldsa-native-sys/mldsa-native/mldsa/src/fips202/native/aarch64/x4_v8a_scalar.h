/*
 * Copyright (c) The mlkem-native project authors
 * Copyright (c) The mldsa-native project authors
 * SPDX-License-Identifier: Apache-2.0 OR ISC OR MIT
 */

#ifndef MLD_FIPS202_NATIVE_AARCH64_X4_V8A_SCALAR_H
#define MLD_FIPS202_NATIVE_AARCH64_X4_V8A_SCALAR_H

/* Part of backend API */
#define MLD_USE_NATIVE_FIPS202_X4
/* Guard for assembly file */
#define MLD_FIPS202_AARCH64_NEED_X4_V8A_SCALAR_HYBRID

#if !defined(__ASSEMBLER__)
#include "../api.h"
#include "src/fips202_native_aarch64.h"
MLD_MUST_CHECK_RETURN_VALUE
static MLD_INLINE int mld_keccak_f1600_x4_native(uint64_t *state)
{
  if (!mld_sys_check_capability(MLD_SYS_CAP_AARCH64_NEON))
  {
    return MLD_NATIVE_FUNC_FALLBACK;
  }

  mld_keccak_f1600_x4_v8a_scalar_hybrid_aarch64_asm(
      state, mld_keccakf1600_round_constants);
  return MLD_NATIVE_FUNC_SUCCESS;
}
#endif /* !__ASSEMBLER__ */

#endif /* !MLD_FIPS202_NATIVE_AARCH64_X4_V8A_SCALAR_H */
