/*
 * Copyright (c) The mlkem-native project authors
 * Copyright (c) The mldsa-native project authors
 * SPDX-License-Identifier: Apache-2.0 OR ISC OR MIT
 */

#ifndef MLD_FIPS202_NATIVE_API_H
#define MLD_FIPS202_NATIVE_API_H
/*
 * FIPS-202 native interface
 *
 * This header is primarily for documentation purposes.
 * It should not be included by backend implementations.
 */

#include "../../cbmc.h"
#include "../../common.h"

/* Backends must return MLD_NATIVE_FUNC_SUCCESS upon success. */
#define MLD_NATIVE_FUNC_SUCCESS (0)
/* Backends may return MLD_NATIVE_FUNC_FALLBACK to signal to the frontend that
 * the target/parameters are unsupported; typically, this would be because of
 * dependencies on CPU features not detected on the host CPU. In this case,
 * the frontend falls back to the default C implementation.
 *
 * IMPORTANT: Backend implementations must ensure that the decision of whether
 * to fallback (return MLD_NATIVE_FUNC_FALLBACK) or not must never depend on
 * the input data itself. Fallback decisions may only depend on system
 * capabilities (e.g., CPU features) and, where present, length information.
 * This requirement applies to all backend functions to maintain constant-time
 * properties.
 */
#define MLD_NATIVE_FUNC_FALLBACK (-1)

/*
 * This is the C<->native interface allowing for the drop-in
 * of custom Keccak-F1600 implementations.
 *
 * A _backend_ is a specific implementation of parts of this interface.
 *
 * You can replace 1-fold or 4-fold batched Keccak-F1600.
 * To enable, set MLD_USE_NATIVE_FIPS202_X1 or MLD_USE_NATIVE_FIPS202_X4
 * in your backend, and define the inline wrappers mld_keccak_f1600_x1_native()
 * and/or mld_keccak_f1600_x4_native(), respectively, to forward to your
 * implementation.
 */

#if defined(MLD_USE_NATIVE_FIPS202_X1)
MLD_MUST_CHECK_RETURN_VALUE
static MLD_INLINE int mld_keccak_f1600_x1_native(uint64_t *state)
__contract__(
    requires(memory_no_alias(state, sizeof(uint64_t) * 25 * 1))
    assigns(memory_slice(state, sizeof(uint64_t) * 25 * 1))
    ensures(return_value == MLD_NATIVE_FUNC_FALLBACK || return_value == MLD_NATIVE_FUNC_SUCCESS)
    ensures((return_value == MLD_NATIVE_FUNC_FALLBACK) ==> array_unchanged_u64(state, 25 * 1))
);
#endif /* MLD_USE_NATIVE_FIPS202_X1 */
#if defined(MLD_USE_NATIVE_FIPS202_X4)
MLD_MUST_CHECK_RETURN_VALUE
static MLD_INLINE int mld_keccak_f1600_x4_native(uint64_t *state)
__contract__(
    requires(memory_no_alias(state, sizeof(uint64_t) * 25 * 4))
    assigns(memory_slice(state, sizeof(uint64_t) * 25 * 4))
    ensures(return_value == MLD_NATIVE_FUNC_FALLBACK || return_value == MLD_NATIVE_FUNC_SUCCESS)
    ensures((return_value == MLD_NATIVE_FUNC_FALLBACK) ==> array_unchanged_u64(state, 25 * 4))
);
#endif /* MLD_USE_NATIVE_FIPS202_X4 */

/*
 * Native x4 XOR bytes and extract bytes interface.
 *
 * These functions allow backends to provide optimized implementations for
 * XORing input data into the state and extracting output data from the state.
 * This is particularly useful for backends that use a different internal state
 * representation (e.g., bit-interleaved), as conversion can happen during
 * XOR/extract rather than before/after each permutation.
 *
 * NOTE: We assume that the custom representation of the zero state is the
 * all-zero state.
 *
 * MLD_USE_NATIVE_FIPS202_X4_XOR_BYTES: Backend provides native XOR bytes
 * MLD_USE_NATIVE_FIPS202_X4_EXTRACT_BYTES: Backend provides native extract
 * bytes
 */

#if defined(MLD_USE_NATIVE_FIPS202_X4_XOR_BYTES)
MLD_MUST_CHECK_RETURN_VALUE
static MLD_INLINE int mld_keccakf1600_xor_bytes_x4_native(
    uint64_t *state, const unsigned char *data0, const unsigned char *data1,
    const unsigned char *data2, const unsigned char *data3, unsigned offset,
    unsigned length)
__contract__(
  requires(0 <= offset && offset <= 25 * sizeof(uint64_t) &&
           0 <= length && length <= 25 * sizeof(uint64_t) - offset)
  requires(memory_no_alias(state, sizeof(uint64_t) * 25 * 4))
  requires(memory_no_alias(data0, length))
  requires((data0 == data1 &&
            data0 == data2 &&
            data0 == data3) ||
           (memory_no_alias(data1, length) &&
            memory_no_alias(data2, length) &&
            memory_no_alias(data3, length)))
  assigns(memory_slice(state, sizeof(uint64_t) * 25 * 4))
  ensures(return_value == MLD_NATIVE_FUNC_FALLBACK || return_value == MLD_NATIVE_FUNC_SUCCESS)
  ensures((return_value == MLD_NATIVE_FUNC_FALLBACK) ==> array_unchanged_u64(state, 25 * 4)));
#endif /* MLD_USE_NATIVE_FIPS202_X4_XOR_BYTES */

#if defined(MLD_USE_NATIVE_FIPS202_X4_EXTRACT_BYTES)
MLD_MUST_CHECK_RETURN_VALUE
static MLD_INLINE int mld_keccakf1600_extract_bytes_x4_native(
    uint64_t *state, unsigned char *data0, unsigned char *data1,
    unsigned char *data2, unsigned char *data3, unsigned offset,
    unsigned length)
__contract__(
  requires(0 <= offset && offset <= 25 * sizeof(uint64_t) &&
           0 <= length && length <= 25 * sizeof(uint64_t) - offset)
  requires(memory_no_alias(state, sizeof(uint64_t) * 25 * 4))
  requires(memory_no_alias(data0, length))
  requires(memory_no_alias(data1, length))
  requires(memory_no_alias(data2, length))
  requires(memory_no_alias(data3, length))
  assigns(memory_slice(data0, length))
  assigns(memory_slice(data1, length))
  assigns(memory_slice(data2, length))
  assigns(memory_slice(data3, length))
  ensures(return_value == MLD_NATIVE_FUNC_FALLBACK || return_value == MLD_NATIVE_FUNC_SUCCESS));
#endif /* MLD_USE_NATIVE_FIPS202_X4_EXTRACT_BYTES */

#endif /* !MLD_FIPS202_NATIVE_API_H */
