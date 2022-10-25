# Gas Cost Parameters

NEAR charges gas when executing users' WASM code. The how and why is described
in other documents, such as [Gas basic
concepts](https://docs.near.org/concepts/basics/transactions/gas), [Gas advanced
concepts](https://docs.near.org/concepts/basics/transactions/gas-advanced), and
[the runtime fee specification](https://nomicon.io/RuntimeSpec/Fees/).

So-called gas cost parameters are part of the protocol definition which can
change between versions. The section on [Parameter Definitions](./parameter_definition.md)
explains how to find the source of truth for such parameter's values in the
nearcore repository and how they can be referenced in code.

The [Gas Profile](./gas_profile.md) section goes into more details on how gas
costs of a transaction are tracked in nearcore.

The [runtime parameter estimator](./estimator.md) is a separate binary within
the nearcore repository. It contains benchmarking-like code that is used to
validate existing parameters values. Or when new features are added, new code
has to be added there to estimate the safe values of new parameters. That
section is for you if you want to add new features, such as a new pre-compiled
method or other host functions.


<!-- TODO: ## Action parameters-->
<!-- TODO: ## WASM parameters-->
<!-- TODO: ## Non-gas parameters -->
<!-- TODO: - Gas economics config-->
<!-- TODO: - Gas economics config-->
<!-- TODO: - Storage usage config-->
<!-- TODO: - Smart contract limits-->