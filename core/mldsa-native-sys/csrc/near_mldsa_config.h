/* Configuration for the vendored mldsa-native build used by near-crypto. */
#ifndef NEAR_MLDSA_CONFIG_H
#define NEAR_MLDSA_CONFIG_H

#define MLD_CONFIG_PARAMETER_SET 65
#if !defined(MLD_CONFIG_NAMESPACE_PREFIX)
#define MLD_CONFIG_NAMESPACE_PREFIX mldsa65
#endif

/* Randomness (keygen seed, signing rnd) is supplied from Rust, so the C code
 * never needs a platform RNG. This keeps wasm32-unknown-unknown buildable. */
#define MLD_CONFIG_NO_RANDOMIZED_API

#if defined(NEAR_MLDSA_NATIVE)
#define MLD_CONFIG_USE_NATIVE_BACKEND_ARITH
#define MLD_CONFIG_ARITH_BACKEND_FILE "near_mldsa_arith_backend.h"
#if defined(NEAR_MLDSA_NATIVE_FIPS202)
#define MLD_CONFIG_USE_NATIVE_BACKEND_FIPS202
#define MLD_CONFIG_FIPS202_BACKEND_FILE "near_mldsa_fips202_backend.h"
#endif

/* Runtime CPU feature detection, like AWS-LC, so a generic x86_64 binary
 * uses AVX2 only where the CPU and OS support it. */
#define MLD_CONFIG_CUSTOM_CAPABILITY_FUNC
#if !defined(__ASSEMBLER__)
#include "src/sys.h"
#if defined(MLD_SYS_X86_64)
extern int near_mldsa_native_x86_64_has_avx2(void);
#endif
static MLD_INLINE int mld_sys_check_capability(mld_sys_cap cap)
{
#if defined(MLD_SYS_X86_64)
  if (cap == MLD_SYS_CAP_X86_64_AVX2)
  {
    return near_mldsa_native_x86_64_has_avx2();
  }
#elif defined(MLD_SYS_AARCH64)
  if (cap == MLD_SYS_CAP_AARCH64_NEON)
  {
    return 1;
  }
#endif
  (void)cap;
  return 0;
}
#endif /* !__ASSEMBLER__ */
#endif /* NEAR_MLDSA_NATIVE */

#endif /* NEAR_MLDSA_CONFIG_H */
