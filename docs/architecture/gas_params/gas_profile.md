# Gas Profile

The transaction runtime charges gas in various places around the code. The
charges end up as summaries inside an `ActionResult`. More specifically, the
`gas_burnt` and `gas_used` counters track the total gas required and the
`profile` field keeps track of what the gas was spent on.

## Charging Gas
Generally speaking, gas is charged right before the computation that it pays for
is executed. It has to be before to avoid cheap resource exhaustion attacks.
Imagine the user has only 1 gas unit left but we start executing an expensive
step, we would waste a significant duration of compute on all validators without
anyone paying for it.

When charging gas for an action, the `ActionResult` can be updated directly. But
when charging WASM costs, it would be too slow to do a context switch each time,
Therefore, a fast gas counter exists that can be updated from within the VM.
(See
[gas_counter.rs](https://github.com/near/nearcore/blob/master/runtime/near-vm-logic/src/gas_counter.rs))
At the end of a function call execution, the gas counter is read by the host and
merged into the `ActionResult`.

<!-- TODO: Difference between `Cost` in profiles and `Parameter` -->
<!-- TODO: Transaction profiles vs Receipt Profiles -->

