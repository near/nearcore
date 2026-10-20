/*
 * Copyright (c) The mlkem-native project authors
 * Copyright (c) The mldsa-native project authors
 * SPDX-License-Identifier: Apache-2.0 OR ISC OR MIT
 */

#ifndef MLD_FIPS202_NATIVE_AUTO_H
#define MLD_FIPS202_NATIVE_AUTO_H

/*
 * Default FIPS202 backend
 */
#include "../../sys.h"

#if defined(MLD_SYS_AARCH64)
#include "aarch64/auto.h"
#endif

/* The x86_64 backend requires toolchain support for the SysV ABI */
#if defined(MLD_SYS_X86_64_AVX2) && defined(MLD_SYSV_ABI_SUPPORTED) &&       \
    (!defined(MLD_CONFIG_NO_KEYPAIR_API) ||                                  \
     !defined(MLD_CONFIG_NO_SIGN_API) || !defined(MLD_CONFIG_REDUCE_RAM)) && \
    !defined(MLD_CONFIG_SERIAL_FIPS202_ONLY)
#include "x86_64/keccak_f1600_x4_avx2.h"
#endif /* MLD_SYS_X86_64_AVX2 && MLD_SYSV_ABI_SUPPORTED &&          \
          (!MLD_CONFIG_NO_KEYPAIR_API || !MLD_CONFIG_NO_SIGN_API || \
          !MLD_CONFIG_REDUCE_RAM) && !MLD_CONFIG_SERIAL_FIPS202_ONLY */

/* We do not yet include the FIPS202 backend for Armv8.1-M+MVE by default
 * as it is still experimental and undergoing review. */
/* #if defined(MLD_SYS_ARMV81M_MVE) */
/* #include "armv81m/mve.h" */
/* #endif */

#endif /* !MLD_FIPS202_NATIVE_AUTO_H */
