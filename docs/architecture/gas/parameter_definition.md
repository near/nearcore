# Parameter Definitions

Gas parameters are a subset of runtime parameters that are defined in
[core/primitives/res/runtime_configs/parameters.yaml](https://github.com/near/nearcore/blob/master/core/primitives/res/runtime_configs/parameters.yaml).
**IMPORTANT:** This is not the final list of parameters, it contains the base
values which can be overwritten per protocol version. For example,
[53.yaml](https://github.com/near/nearcore/blob/master/core/primitives/res/runtime_configs/53.yaml)
changes several parameters starting from version 53. You can see the final list
of parameters in
[core/primitives/res/runtime_configs/parameters.snap](https://github.com/near/nearcore/blob/master/core/primitives/res/runtime_configs/parameters.snap).
This file is automatically updated whenever any of the parameters changes. To
see all parameter values for a specific version, check out the list of JSON
snapshots generated in this directory:
[core/primitives/src/runtime/snapshots](https://github.com/near/nearcore/blob/master/core/primitives/src/runtime/snapshots).

## Using Parameters in Code

As the introduction on this page already hints at it, parameter values are
versioned. In other words, they can change if the protocol version changes. A
nearcore binary has to support multiple versions and choose the correct
parameter value at runtime.

To make this easy, there is
[`RuntimeConfigStore`](https://github.com/near/nearcore/blob/a8964d200b3938a63d389263bc39c1bcd75b1de4/core/primitives/src/runtime/config_store.rs#L43).
It contains a sparse map from protocol versions to complete runtime
configurations (`BTreeMap<ProtocolVersion, Arc<RuntimeConfig>>`).
The runtime then uses `store.get_config(protocol_version)` to access a runtime
configuration for a specific version.

It is crucial to always use this runtime config store. Never hard-code parameter
values. Never look them up in a different way.

In practice, this usually translates to a `&RuntimeConfig` argument for any
function that depends on parameter values. This config object implicitly defines
the protocol version. It should therefore not be cached. It should be read from
the store once per chunk and then passed down to all functions that need it.

## How to Add a New Parameter

First and foremost, if you are feeling lost, open a topic in our Zulip chat
([pagoda/contract-runtime](https://near.zulipchat.com/#narrow/stream/295306-pagoda.2Fcontract-runtime)).
We are here to help.

### Principles

Before adding anything, please review the basic principles for gas parameters.

- A parameter must correspond to a clearly defined workload.
- When the workload is scalable by a factor `N` that depends on user input,
  it will likely require a base parameter and a second parameter that is
  multiplied by `N`. (Example: `N` = number of bytes when reading a value from
  storage.)
- Charge gas before executing the workload.
- Parameters should be independent of specific implementation choices in
  nearcore.
- Ideally, contract developers can easily understand what the cost is simply by
  reading the name in a gas profile.

The section on [Gas Profiles](./gas_profile.md#charging-gas) explains how to
charge gas, please also consider that when defining a new parameter.

### Necessary Code Changes

Adding the parameter in code involves several steps.

1. Define the parameter by adding it to the list in `core/primitives/res/runtime_configs/parameters.yaml.`
2. Update the Rust view of parameters by adding a variant to `enum Parameter`
   in `core/primitives-core/src/parameter.rs`. In the same file, update
   `enum FeeParameter` if you add an action cost or update `ext_costs()`
   if you add a cost inside function calls.
3. Update `RuntimeConfig`, the configuration used to reference parameters in
   code. Depending on the type of parameter, you will need to update
   `RuntimeFeesConfig` (for action costs) or `ExtCostsConfig` (for gas costs).
4. Update the list used for gas profiles. This is defined by `enum Cost` in
   `core/primitives-core/src/profile.rs`. You need to add a variant to either
   `enum ActionCosts` or `enum ExtCost`. Please also update `fn index()` that
   maps each profile entry to a unique position in serialized gas profiles.
5. The parameter should be available to use in the code section you need it in.
   Now is a good time to ensure `cargo check` and `cargo test --no-run` pass.
   Most likely you have to update some testing code, such as
   `ExtCostsConfig::test()`.
6. To merge your changes into nearcore, you will have to hide your parameter
   behind a feature flag. Add the feature to the `Cargo.toml` of each crate
   touched in steps 3 and 4 and hide the code behind `#[cfg(feature =
   "protocol_feature_MY_NEW_FEATURE")]`. Do not hide code in step 2 so that
   non-nightly builds can still read `parameters.yaml`. Also, add your feature as
   a dependency on `nightly` in `core/primitives/Cargo.toml` to make sure it
   gets included when compiling for nightly. After that, check `cargo check` and
   `cargo test --no-run` with and without `features=nightly`.

### What Gas Value Should the Parameter Have?

For a first draft, the exact gas value used in the parameter is not crucial.
Make sure the right set of parameters exists and try to set a number that roughly
makes sense. This should be enough to enable discussions on the NEP around the
feasibility and usefulness of the proposed feature. If you are not sure, a good
rule of thumb is 0.1 Tgas for each disk operation and at least 1 Tgas for each
ms of CPU time. Then round it up generously.

The value will have to be refined later. This is usually the last step after
the implementation is complete and reviewed. Have a look at the section on
[estimating gas parameters](./estimator.md) in the book.
