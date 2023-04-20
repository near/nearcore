# Rust 🦀

This short chapter collects various useful general resources about the Rust
programming language. If you are already familiar with Rust, skip this
chapter. Otherwise, this chapter is for you!

## Getting Help

Rust community actively encourages beginners to ask questions, take advantage of that!

We have a dedicated stream for Rust questions on our Zulip: [Rust
🦀](https://near.zulipchat.com/#narrow/stream/300659-Rust-.F0.9F.A6.80).

There's a general Rust forum at <https://users.rust-lang.org>.

For a more interactive chat, take a look at Discord:
<https://discord.com/invite/rust-lang>.

## Reference Material

Rust is _very_ well documented. It's possible to learn the whole language and
most of the idioms by just reading the official docs. Starting points are

* [The Rust Book](https://doc.rust-lang.org/book/) (any resemblance to "Guide to
  Nearcore Development" is purely coincidental)
* [Standard Library API](https://doc.rust-lang.org/stable/std/)

Alternatives are:

* [Programming
  Rust](https://www.amazon.com/Programming-Rust-Fast-Systems-Development/dp/1491927283)
  is an alternative book that moves a bit faster.
* [Rust By Example](https://doc.rust-lang.org/rust-by-example/) is a great
  resource for learning by doing.

Rust has some great tooling, which is also documented:

* [Cargo](https://doc.rust-lang.org/cargo/), the build system. Worth at least skimming through!
* For IDE support, see [IntelliJ Rust](https://www.jetbrains.com/rust/) if you
  like JetBrains products or
  [rust-analyzer](https://rust-analyzer.github.io/manual.html) if you use any
  other editor (fun fact: NEAR was one of the sponsors of rust-analyzer!).
* [Rustup](https://rust-lang.github.io/rustup/) manages versions of Rust
  itself. It's unobtrusive, so feel free to skip this.

## Cheat Sheet

This is a thing in its category, do check it out:

<https://cheats.rs>

## Language Mastery

* [Rust for Rustaceans](https://nostarch.com/rust-rustaceans) — the book to read
  after "The Book".
* [Tokio docs](https://tokio.rs/tokio/tutorial) explain asynchronous programming
  in Rust (async/await).
* [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/about.html)
  codify rules for idiomatic Rust APIs. Note that guidelines apply to _semver
  surface_ of libraries, and most of the code in nearcore is not on the semver
  boundary. Still, a lot of insight there!
* [Rustonomicon](https://doc.rust-lang.org/nomicon/) explains `unsafe`. (any
  resemblance to <https://nomicon.io> is purely coincidental)

## Selected Blog Posts

A lot of finer knowledge is hidden away in various dusty corners of Web-2.0.
Here are some favorites:

* <https://docs.rs/dtolnay/latest/dtolnay/macro._02__reference_types.html>
* <https://limpet.net/mbrubeck/2019/02/07/rust-a-unique-perspective.html>
* <https://smallcultfollowing.com/babysteps/blog/2018/02/01/in-rust-ordinary-vectors-are-values/>
* <https://smallcultfollowing.com/babysteps/blog/2016/10/02/observational-equivalence-and-unsafe-code/>
* <https://matklad.github.io/2021/09/05/Rust100k.html>

And on the easiest topic of error handling specifically:

* <http://sled.rs/errors.html>
* <https://kazlauskas.me/entries/errors>
* <http://joeduffyblog.com/2016/02/07/the-error-model/>
* <https://blog.burntsushi.net/rust-error-handling/>

Finally, as a dessert, the first rust slide deck:
<http://venge.net/graydon/talks/rust-2012.pdf>.
