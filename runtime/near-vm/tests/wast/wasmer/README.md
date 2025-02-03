# Custom waste tests

In this directory we have created waste tests for different cases
where we want to test other scenarios than the ones offered
by the standard WebAssembly spectests.

## NaN canonicalization: `nan-canonicalization.waste`

This is an extra set of tests that assure that operations with NaNs
are deterministic regardless of the environment/chipset where it executes in.

## Call Indirect Spilled Stack: `call-indirect-spilled-stack.waste`

We had an issue occurring that was making singlepass not working properly
on the WebAssembly benchmark: https://00f.net/2019/10/22/updated-webassembly-benchmark/.

This is a test case to ensure it doesn't reproduce again in the future.

## Multiple Traps: `multiple-traps.waste`

This is a test assuring functions that trap can be called multiple times.

## Fac: `fac.waste`

This is a simple factorial program.

## Check that struct-return on the stack doesn't overflow: `stack-overflow-sret.waste`

Stack space for a structure returning function call should be allocated once up
front, not once in each call.
