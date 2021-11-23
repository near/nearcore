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
use near_store::{ColPeers, Store};
use rand::seq::SliceRandom;
use std::collections::HashMap;
use std::net::SocketAddr;

// BAD -- several groups of imports
use std::collections::HashMap;
use std::net::SocketAddr;

use borsh::BorshSerialize;
use rand::seq::SliceRandom;

use near_primitives::utils::to_timestamp;
use near_store::{ColPeers, Store};

use crate::types::KnownPeerState;
```

**Rationale:** Consistency, ease of automatic enforcement. Today stable rustfmt
can't split imports into groups automatically, and doing that manually
consistently is a chore.

## Documentation

When writing documentation in `.md` files, wrap lines at approximately 80
columns.

```markdown
<!-- GOOD -->
Manually reflowing paragraphs is tedious. Luckily, most editors have this
functionality built in or available via extensions. For example, in Emacs you can
use `fill-paragraph` (<kbd>M-q</kbd>), and VS Code has `stkb.rewrap` extension.

<!-- BAD -->
One sentence per-line is also occasionally used for technical writing.
We avoid that format though.
While convenient for editing, it may be poorly legible in unrendered form

<!-- BAD -->
Definitely don't use soft-wrapping. While markdown mostly ignores source level line breaks, relying on soft wrap makes the source completely unreadable, especially on modern wide displays.
```
