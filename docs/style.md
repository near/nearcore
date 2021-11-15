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
use near_network::test_utils::{convert_boot_nodes, open_port};
use near_network::types::NetworkViewClientMessages;

// BAD
use near_network::{
    test_utils::{convert_boot_nodes, open_port},
    types::NetworkViewClientMessages,
};

// BAD
use near_network::test_utils::convert_boot_nodes;
use near_network::test_utils::open_port;
use near_network::types::NetworkViewClientMessages;
```

This corresponds to `"rust-analyzer.assist.importGranularity": "module"` setting
in rust-analyzer
([docs](https://rust-analyzer.github.io/manual.html#rust-analyzer.assist.importGranularity)).

**Rationale:** Consistency, matches existing practice.
