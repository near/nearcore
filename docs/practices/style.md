# Code Style

This document specifies the code style to use in the nearcore repository. The
primary goal here is to achieve consistency, maintain it over time, and cut down
on the mental overhead related to style choices.

Right now, `nearcore` codebase is not perfectly consistent, and the style
acknowledges this. It guides newly written code and serves as a tie breaker for
decisions. Rewriting existing code to conform 100% to the style is not a goal.
Local consistency is more important: if new code is added to a specific file,
it's more important to be consistent with the file rather than with this style
guide.

This is a live document, which intentionally starts in a minimal case. When
doing code-reviews, consider if some recurring advice you give could be moved
into this document.

## Formatting

Use `rustfmt` for minor code formatting decisions. This rule is enforced by CI

**Rationale:** `rustfmt` style is almost always good enough, even if not always
perfect. The amount of bikeshedding saved by `rustfmt` far outweighs any
imperfections.

## Idiomatic Rust

While the most important thing is to solve the problem at hand, we strive to
implement the solution in idiomatic Rust, if possible. To learn what is
considered idiomatic Rust, a good start are the
[Rust API guidelines](https://rust-lang.github.io/api-guidelines/about.html)
(but keep in mind that `nearcore` is not a library with public API, not all
advice applies literally).

When in doubt, ask question in the [Rust
🦀](https://near.zulipchat.com/#narrow/stream/300659-Rust-.F0.9F.A6.80) Zulip
stream or during code review.

**Rationale:**
- *Consistency*: there's usually only one idiomatic solution amidst many
  non-idiomatic ones.
- *Predictability*: you can use the APIs without consulting documentation.
- *Performance, ergonomics and correctness*: language idioms usually reflect
  learned truths, which might not be immediately obvious.

## Style

This section documents all micro-rules which are not otherwise enforced by
`rustfmt`.

### Avoid `AsRef::as_ref`

When you have some concrete type, prefer `.as_str`, `.as_bytes`, `.as_path` over
generic `.as_ref`. Only use `.as_ref` when the type in question is a generic
`T: AsRef<U>`.

```rust
// GOOD
fn log_validator(account_id: AccountId) {
    metric_for(account_id.as_str())
       .increment()
}

// BAD
fn log_validator(account_id: AccountId) {
    metric_for(account_id.as_ref())
       .increment()
}
```

Note that `Option::as_ref`, `Result::as_ref` are great, do use them!

**Rationale:** Readability and churn-resistance. There might be more than one
`AsRef<U>` implementation for a given type (with different `U`s). If a new
implementation is added, some of the `.as_ref()` calls might break. See also
this [issue](https://github.com/rust-lang/rust/issues/62586).

### Avoid references to `Copy`-types

Various generic APIs in Rust often return references to data (`&T`). When `T` is
a small `Copy` type like `i32`, you end up with `&i32` while many API expect
`i32`, so dereference has to happen _somewhere_. Prefer dereferencing as early
as possible, typically in a pattern:

```rust
// GOOD
fn compute(map: HashMap<&'str, i32>) {
    if let Some(&value) = map.get("key") {
        process(value)
    }
}
fn process(value: i32) { ... }

// BAD
fn compute(map: HashMap<&'str, i32>) {
    if let Some(value) = map.get("key") {
        process(*value)
    }
}
fn process(value: i32) { ... }
```

**Rationale:** If the value is used multiple times, dereferencing in the pattern
saves keystrokes. If the value is used exactly once, we just want to be
consistent. Additional benefit of early deref is reduced scope of borrow.

Note that for some *big* `Copy` types, notably `CryptoHash`, we sometimes use
references for performance reasons. As a rule of thumb, `T` is considered *big* if
`size_of::<T>() > 2 * size_of::<usize>()`.

### Prefer for loops over `for_each` and `try_for_each` methods

Iterators offer `for_each` and `try_for_each` methods which allow executing
a closure over all items of the iterator. This is similar to using a for loop
but comes with various complications and may lead to less readable code.  Prefer
using a loop rather than those methods, for example:

```rust
// GOOD
for outcome_with_id in result? {
    *total_gas_burnt =
        safe_add_gas(*total_gas_burnt, outcome_with_id.outcome.gas_burnt)?;
    outcomes.push(outcome_with_id);
}

// BAD
result?.into_iter().try_for_each(
    |outcome_with_id: ExecutionOutcomeWithId| -> Result<(), RuntimeError> {
        *total_gas_burnt =
            safe_add_gas(*total_gas_burnt, outcome_with_id.outcome.gas_burnt)?;
        outcomes.push(outcome_with_id);
        Ok(())
    },
)?;
```

**Rationale:** The `for_each` and `try_for_each` method don’t play nice with
`break` and `continue` statements nor do they mesh well with async IO (since
`.await` inside of the closure isn’t possible). And while `try_for_each` allows
for the use of question mark operator, one may end up having to uses it twice:
once inside the closure and second time outside the call to `try_for_each`.
Furthermore, usage of the functions often introduce some minor syntax noise.

There are situations when those methods may lead to more readable code. Common
example are long call chains. Even then such code may evolve with the closure
growing and leading to less readable code. If advantages of using the methods
aren’t clear cut, it’s usually better to err on side of more imperative style.

Lastly, anecdotally the methods (e.g. when used with `chain` or `flat_map`) may
lead to faster code. This intuitively makes sense but it’s worth to keep in
mind that compilers are pretty good at optimising and in practice may generate
optimal code anyway. Furthermore, optimising code for readability may be more
important (especially outside of hot path) than small performance gains.

### Prefer `to_string` to `format!("{}")`

Prefer calling `to_string` method on an object rather than passing it through
`format!("{}")` if all you’re doing is converting it to a `String`.

```rust
// GOOD
let hash = block_hash.to_string();
let msg = format!("{}: failed to open", path.display());

// BAD
let hash = format!("{block_hash}");
let msg = path.display() + ": failed to open";
```

**Rationale:** `to_string` is shorter to type and also faster.

### Import Granularity

Group imports by module, but not deeper:

```rust
// GOOD
use std::collections::{hash_map, BTreeSet};
use std::sync::Arc;

// BAD - nested groups.
use std::{
    collections::{hash_map, BTreeSet},
    sync::Arc,
};

// BAD - not grouped together.
use std::collections::BTreeSet;
use std::collections::hash_map;
use std::sync::Arc;
```

This corresponds to `"rust-analyzer.assist.importGranularity": "module"` setting
in rust-analyzer
([docs](https://rust-analyzer.github.io/manual.html#rust-analyzer.assist.importGranularity)).

**Rationale:** Consistency, matches existing practice.

### Import Blocks

Do not separate imports into groups with blank lines. Write a single block of
imports and rely on `rustfmt` to sort them.

```rust
// GOOD
use crate::types::KnownPeerState;
use borsh::BorshSerialize;
use near_primitives::utils::to_timestamp;
use near_store::{DBCol::Peers, Store};
use rand::seq::SliceRandom;
use std::collections::HashMap;
use std::net::SocketAddr;

// BAD -- several groups of imports
use std::collections::HashMap;
use std::net::SocketAddr;

use borsh::BorshSerialize;
use rand::seq::SliceRandom;

use near_primitives::utils::to_timestamp;
use near_store::{DBCol::Peers, Store};

use crate::types::KnownPeerState;
```

**Rationale:** Consistency, ease of automatic enforcement. Today, stable rustfmt
can't split imports into groups automatically, and doing that manually
consistently is a chore.

### Derives

When deriving an implementation of a trait, specify a full path to the traits provided by the
external libraries:

```rust
// GOOD
#[derive(Copy, Clone, serde::Serialize, thiserror::Error, strum::Display)]
struct Grapefruit;

// BAD
use serde::Serialize;
use thiserror::Error;
use strum::Display;

#[derive(Copy, Clone, Serialize, Error, Display)]
struct Banana;
```

As an exception to this rule, it is okay to use either style when the derived trait already
includes the name of the library (as would be the case for `borsh::BorshSerialize`.)

**Rationale:** Specifying a full path to the externally provided derivations here makes it
straightforward to differentiate between the built-in derivations and those provided by the
external crates. The surprise factor for derivations sharing a name with the standard
library traits (`Display`) is reduced and it also acts as natural mechanism to tell apart names
prone to collision (`Serialize`), all without needing to look up the list of imports.

### Arithmetic integer operations

Use methods with an appropriate overflow handling over plain arithmetic operators (`+-*/%`) when
dealing with integers.

```
// GOOD
a.wrapping_add(b);
c.saturating_sub(2);
d.widening_mul(3);   // NB: unstable at the time of writing
e.overflowing_div(5);
f.checked_rem(7);

// BAD
a + b
c - 2
d * 3
e / 5
f % 7
```

If you’re confident the arithmetic operation cannot fail,
`x.checked_[add|sub|mul|div](y).expect("explanation why the operation is safe")` is a great
alternative, as it neatly documents not just the infallibility, but also _why_ that is the case.

This convention may be enforced by the `clippy::arithmetic_side_effects` and
`clippy::integer_arithmetic` lints.

**Rationale:** By default the outcome of an overflowing computation in Rust depends on a few
factors, most notably the compilation flags used. The quick explanation is that in debug mode the
computations may panic (cause side effects) if the result has overflowed, and when built with
optimizations enabled, these computations will wrap-around instead.

For nearcore and neard we have opted to enable the panicking behaviour regardless of the
optimization level. By doing it this we hope to prevent accidental stabilization of protocol
mis-features that depend on incorrect handling of these overflows or similarly scary silent bugs.
The downside to this approach is that any such arithmetic operation now may cause a node to crash,
much like indexing a vector with `a[idx]` may cause a crash when `idx` is out-of-bounds. Unlike
indexing, however, developers and reviewers are not used to treating integer arithmetic operations
with the due suspicion. Having to make a choice, and explicitly spell out, how an overflow case
ought to be handled will result in an easier to review and understand code and a more resilient
project overall.

## Standard Naming

* Use `-` rather than `_` in crate names and in corresponding folder names.
* Avoid single-letter variable names especially in long functions.  Common `i`,
  `j` etc. loop variables are somewhat of an exception but since Rust encourages
  use of iterators those cases aren’t that common anyway.
* Follow standard [Rust naming patterns](https://rust-lang.github.io/api-guidelines/naming.html) such as:
  * Don’t use `get_` prefix for getter methods.  A getter method is one which
    returns (a reference to) a field of an object.
  * Use `set_` prefix for setter methods.  An exception are builder objects
    which may use different a naming style.
  * Use `into_` prefix for methods which consume `self` and `to_` prefix for
    methods which don’t.
* Use `get_block_header` rather than `get_header` for methods which return
  a block header.
* Don’t use `_by_hash` suffix for methods which lookup chain objects (blocks,
  chunks, block headers etc.) by their hash (i.e. their primary identifier).
* Use `_by_height` and similar suffixes for methods which lookup chain objects
  (blocks, chunks, block headers etc.) by their height or other property which
  is not their hash.

**Rationale:** Consistency.

## Documentation

When writing documentation in `.md` files, wrap lines at approximately 80
columns.

```markdown
<!-- GOOD -->
Manually reflowing paragraphs is tedious. Luckily, most editors have this
functionality built in or available via extensions. For example, in Emacs you
can use `fill-paragraph` (<kbd>M-q</kbd>), (neo)vim allows rewrapping with `gq`,
and VS Code has `stkb.rewrap` extension.

<!-- BAD -->
One sentence per-line is also occasionally used for technical writing.
We avoid that format though.
While convenient for editing, it may be poorly legible in unrendered form

<!-- BAD -->
Definitely don't use soft-wrapping. While markdown mostly ignores source level line breaks, relying on soft wrap makes the source completely unreadable, especially on modern wide displays.
```

## [Tracing](https://tracing.rs)

When emitting events and spans with `tracing` prefer adding variable data via
[`tracing`'s field mechanism](https://docs.rs/tracing/latest/tracing/#recording-fields).

```rust
// GOOD
debug!(
    target: "client",
    validator_id = self.client.validator_signer.as_ref().map(|vs| {
        tracing::field::display(vs.validator_id())
    }),
    %hash,
    "block.previous_hash" = %block.header().prev_hash(),
    "block.height" = block.header().height(),
    %peer_id,
    was_requested,
    "Received block",
);
```

Most apparent violation of this rule will be when the event message utilizes any
form of formatting, as seen in the following example:

```rust
// BAD
debug!(
    target: "client",
    "{:?} Received block {} <- {} at {} from {}, requested: {}",
    self.client.validator_signer.as_ref().map(|vs| vs.validator_id()),
    hash,
    block.header().prev_hash(),
    block.header().height(),
    peer_id,
    was_requested
);
```

Always specify the `target` explicitly. A good default value to use is the crate
name, or the module path (e.g. `chain::client`) so that events and spans common
to a topic can be grouped together. This grouping can later be used for
customizing which events to output.

**Rationale:** This makes the events structured – one of the major value add
propositions of the tracing ecosystem. Structured events allow for immediately
actionable data without additional post-processing, especially when using some
of the more advanced tracing subscribers. Of particular interest would be those
that output events as JSON, or those that publish data to distributed event
collection systems such as opentelemetry. Maintaining this rule will also
usually result in faster execution (when logs at the relevant level are enabled.)

### Spans

Use the [spans](https://docs.rs/tracing/latest/tracing/#spans) to introduce
context and grouping to and between events instead of manually adding such
information as part of the events themselves. Most of the subscribers ingesting
spans also provide a built-in timing facility, so prefer using spans for measuring
the amount of time a section of code needs to execute.

Give spans simple names that make them both easy to trace back to code, and to
find a particular span in logs or other tools ingesting the span data. If a
span begins at the top of a function, prefer giving it a name of that function,
otherwise prefer a `snake_case` name.

Use the regular span API over convenience macros such as `#[instrument]`, as
this allows instrumenting portions of a function without affecting the code
structure:

```rust
fn compile_and_serialize_wasmer(code: &[u8]) -> Result<wasmer::Module> {
    let _span = tracing::debug_span!(target: "vm", "compile_and_serialize_wasmer").entered();
    // ...
    // _span will be dropped when this scope ends, terminating the span created above.
    // You can also `drop` it manually, to end the span early with `drop(_span)`.
}
```

**Rationale:** Much as with events, this makes the information provided by spans
structured and contextual. This information can then be output to tooling in an
industry standard format, and can be interpreted by an extensive ecosystem of
`tracing` subscribers.

### Event and span levels

The `INFO` level is enabled by default, use it for information useful for node
operators. The `DEBUG` level is enabled on the canary nodes, use it for
information useful in debugging testnet failures. The `TRACE` level is not
generally enabled, use it for arbitrary debug output.

## Metrics

Consider adding metrics to new functionality. For example, how often each type
of error was triggered, how often each message type was processed.

**Rationale:** Metrics are cheap to increment, and they often provide a significant
insight into operation of the code, almost as much as logging. But unlike logging
metrics don't incur a significant runtime cost.

### Naming

Prefix all `nearcore` metrics with `near_`. Follow the
[Prometheus naming convention](https://prometheus.io/docs/practices/naming/)
for new metrics.

**Rationale:** The `near_` prefix makes it trivial to separate metrics exported
by `nearcore` from other metrics, such as metrics about the state of the machine
that runs `neard`.

### Performance

In most cases incrementing a metric is cheap enough never to give it a second
thought. However accessing a metric with labels on a hot path needs to be done
carefully.

If a label is based on an integer, use a faster way of converting an integer
to the label, such as the `itoa` crate.

For hot code paths, re-use results of `with_label_values()` as much as possible.

**Rationale:** We've encountered issues caused by the runtime costs of
incrementing metrics before. Avoid runtime costs of incrementing metrics too
often.
