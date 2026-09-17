#ifndef NEAR_MLDSA_ARITH_BACKEND_H
#define NEAR_MLDSA_ARITH_BACKEND_H
#if defined(MLD_SYS_AARCH64)
#include "native/aarch64/meta.h"
#elif defined(MLD_SYS_X86_64) && defined(MLD_SYSV_ABI_SUPPORTED)
#include "native/x86_64/meta.h"
#endif
#endif
