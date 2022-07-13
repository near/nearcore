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
considered idiomatic Rust, a good start are the Rust API guidelines (but keep in
mind that `nearcore` is not a library with public API, not all advice applies
literally):

https://rust-lang.github.io/api-guidelines/about.html

When in doubt, ask question in the [Rust
🦀](https://near.zulipchat.com/#narrow/stream/300659-Rust-.F0.9F.A6.80) Zulip
stream or during code review.

**Rationale:** Consistency, as there's usually only one idiomatic solution
amidst many non-idiomatic ones. Predictability, you can use the APIs without
consulting documentation. Performance, ergonomics and correctness: language
idioms usually reflect learned truths, which might not be immediately obvious.

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

Note that `Option::as_ref`, `Result::as_ref` are great, use do use them!

**Rationale:** readability and churn-resistance. There might be more than one
`AsRef<U>` implementation for a given type (with different `U`s). If a new
implementation is added, some of the `.as_ref()` calls might break. See also
https://github.com/rust-lang/rust/issues/62586.


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

**Rationale:** if the value is used multiple times, dereferencing in the pattern
saves keystrokes. If the value is used exactly once, we just want to be
consistent. Additional benefit of early deref is reduced scope of borrow.

Note that for some *big* `Copy` types, notably `CryptoHash`, we sometimes use
references for performance reasons. As a rule of thumb, `T` is considered *big* if
`size_of::<T>() > 2 * size_of::<usize>()`.

### Import Granularity

Group import by module, but not deeper:

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

**Rationale:** Consistency, ease of automatic enforcement. Today stable rustfmt
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

### Visibility

"Minimize visibility" is a standard best practice, but rust has some extra
specifics. In Rust, there are two granularities for visibility: visibility of
items within a single crate, and visibility of items across the crates.
Cross-crate APIs often form important architectural boundaries affecting the
whole application, while what happens inside a crate concerns only the crate
itself.

The difference between `pub` (making items visible to other crates) and `pub(crate)` (making items visible within the crate) is big, while the difference
between `pub(crate)` and private is relatively small. Be mindful when exposing
APIs across crates with `pub`. Similarly, be mindful when introducing new
cross-crate dependencies by editing `Cargo.toml`.

Use `pub` only for items reachable outside of the crate, don't rely on effective
visibility:

```rust
// GOOD
pub(crate) struct Person {
    pub(crate) first_name: String,
}

pub(crate) mod auth {
    pub(crate) check_password(password: &str) -> Result<(), ()>
}

// BAD
pub(crate) struct Person {
    pub first_name: String,
}

pub(crate) mod auth {
    pub check_password(password: &str) -> Result<(), ()>
}
```

**Rationale:** it's useful to tell at a glance that `pub` items form crate's
public API without checking all the ancestor modules.

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
[`tracing`'s field mechanism][fields].

[fields]: https://docs.rs/tracing/latest/tracing/#recording-fields

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
    was_requested
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
customizing of which events to output.

**Rationale:** This makes the events structured – one of the major value
propositions of the tracing ecosystem. Structured events allow for immediately
actionable data without additional post-processing, especially when using some
of the more advanced tracing subscribers. Of particular interest would be those
that output events as JSON, or those that publish data to distributed event
collection systems such as opentelemetry. Maintaining this rule will also
usually result in faster execution (when logs at the relevant level are enabled.)

### Spans

Use the [spans][spans] to introduce context and grouping to and between events
instead of manually adding such information as part of the events themselves.
Most of the subscribers ingesting spans also provide a built-in timing facility,
so prefer using spans for measuring the amount of time a section of code needs
to execute.

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

[spans]: https://docs.rs/tracing/latest/tracing/#spans

**Rationale:** Much as with events, this makes the information provided by spans
structured and contextual. This information can then be output to tooling in an
industry standard format, and can be interpreted by an extensive ecosystem of
`tracing` subscribers.

### Event and span levels

The `INFO` level is enabled by default, use it for information useful for node
operators. The `DEBUG` level is enabled on the canary nodes, use it for
information useful in debugging testnet failures. The `TRACE` level is not
generally enabled, use it for arbitrary debug output.
