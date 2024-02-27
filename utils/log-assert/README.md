# Log-Assert Crate for nearcore
The log-assert crate offers a specialized set of macros designed to facilitate conditional logging and assertion within Rust applications. Especially in production environments where shutting down a node or service due to non-critical assertions is undesirable.

## Key Features
**Conditional Assertions**: log_assert! complements standard Rust assertion macros by introducing conditional logging. It enables nuanced handling of assertion failures, allowing for logging errors in release builds without causing a panic.
**Debug and Release Build Compatibility**: Works seamlessly across debug and release builds, ensuring that assertions can be dynamically adjusted based on the build configuration.

## Philosophy
Maintaining a lean dependency graph is paramount for log-assert, given its critical role in the near-primitives crate and, by extension, numerous other NEAR-related projects. By keeping dependencies to an absolute minimum, log-assert ensures that its inclusion does not unduly increase compile times or bloat the dependency tree of consumer crates. Read this [issue](https://github.com/near/nearcore/issues/8888) to know more.