# Runtime Parameter Estimator

The runtime parameter estimator is a byzantine benchmarking suite. Byzantine
benchmarking is not really commonly used term but I feel it describes it quite
well. It measures the performance assuming that up to a third of validators and
all users collude to make the system as slow as possible.

This benchmarking suite is used check that the gas parameters defined in the
protocol are correct. Correct in this context means, a chunk filled with 1 Pgas
will only take 1 second to be applied. Or more generally, per 1 Tgas of
execution, we spend no more than 1ms wall-clock time. 

For now, nearcore timing is the only one that matters. Things will become more
complicated once there are multiple client implementations. But knowing that
nearcore can serve requests fast enough proofs that it is possible to be at
least as fast. However, we should be careful to not couple costs too tightly
with the specific implementation of nearcore to allow for innovation in new
clients.

The estimator code is part of the nearcore repository in the directory
[runtime/runtime-params-estimator](https://github.com/near/nearcore/tree/master/runtime/runtime-params-estimator).

<!-- TODO: Code structure -->
<!-- TODO: wall-clock time vs icount -->
<!-- TODO: how to add a new host function estimation -->
<!-- TODO: state of IO estimations -->
<!-- TODO: CE and Warehouse -->
<!-- TODO: ... -->