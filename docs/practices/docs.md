# Documentation

This chapter describes nearcore approach to develope documentation. In general,
there are three primary types of documentation to keep in mind:

* **Protocol specification** ([source](https://github.com/near/NEPs),
  [rendered](https://nomicon.io)) is a formal description of the NEAR protocol.
  It can be used to implement alternative NEAR clients.
* **User Docs** ([rendered](https://docs.near.org)) explain how to use the near
  network. User docs are also split into documentation for validators (that is,
  how to run your own near node) and documentation for smart contract
  developers.
* **Internal Development Docs** ([rendered](https://near.github.io/nearcore/),
  [source](https://github.com/near/nearcore/tree/master/docs)) is the book you
  are reading right now! The target audience here are developers of nearcore.

Here, we focus on the last sort -- internal docs.

## Overview

The bulk of the internal docs is this book. If you want to write some kind of a
document, add it here! The [architecture](../architecture/) and
[practices](../practices/) chapter are intended for somewhat up-to-date
normative documents, but in the [misc](../misc/) anything goes.

These are internal docs, not user-facing ones, so don't worry about proper
English, typos, or beautiful diagrams -- just write stuff! It can easily be
improved over time with pull requests. For docs, we use a light-weight review
process and try to merge any improvement as quickly as possible. Rather than
blocking a PR on some stylistic changes, just merge it and submit a follow up.

Note the "edit" button at the top-right corner -- super useful for fixing any
typos you spot!

In addition to the book, we also have some "inline" documentation in the code.
For Rust, it is customary to have a per-crate `README.md` file and include it as
a doc comment via `#![doc = include_str!("../README.md")]` in `lib.rs`. We don't
*require* every `struct` and `function` to be documented, but we certainly
encourage documenting as much as possible. If you spend some time refactoring or
fixing a function, consider adding a doc comment (`///`) to it as a drive-by
improvement.

We currently don't render `rustdoc`, see
[#7836](https://github.com/near/nearcore/issues/7836).

## Book How To

We use mdBook to render a bunch of markdown files as a nice doc with table of
contents, search and themes. Full docs are
[here](https://rust-lang.github.io/mdBook/), but the basics are very simple.

To add a new page:

1. Add an `.md` file somewhere in the
   [`./docs`](https://github.com/near/nearcore/tree/master/docs) folder.
2. Add an entry to
   [`SUMMARY.md`](https://github.com/near/nearcore/blob/master/docs/SUMMARY.md)
   file.
3. Submit a PR (again, we promise to merge it without much ceremony).

The doc itself is vanilla markdown.

To render documentation locally:

```console
# Install mdBook
$ cargo install mdbook
$ mdbook serve --open ./docs
```

This will convert `.md` file from the docs folder to `.html`, open a browser,
and start a file watcher to rebuild and reload on change.

Note that GitHub's default rendering mostly just works as well, so you don't
need special preview when reviewing pull requests to docs.

The book deployed via this GitHub Action:
[.github/workflows/book.yml](https://github.com/near/nearcore/blob/master/.github/workflows/book.yml).
It just runs mdBook and then deploys the result to
https://near.github.io/nearcore/.

For internal docs, you often want to have pretty pictures. We don't currently
have a recommended workflow, but here are some tips:

* Don't add binary media files to Git. Rather, upload images as comments to this
  super-secret issue [#7821](https://github.com/near/nearcore/issues/7821), and
  then link to the images as

  ```
  ![image](https://user-images.githubusercontent.com/1711539/195626792-7697129b-7f9c-4953-b939-0b9bcacaf72c.png)
  ```

  Use single comment per page with multiple images.

* Google Doc is an OK way to create technical drawings, you can add a link to
  the doc with source to that secret issue as well.

* There's some momentum around using mermaid.js for diagramming, and there's
  appropriate plugin for that: https://github.com/badboy/mdbook-mermaid.
  Consider if that's something you might want to use.
