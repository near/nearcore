# Documentation

This chapter describes nearcore's approach to documentation. There are three
primary types of documentation to keep in mind:

* [**The NEAR Protocol Specification**](https://nomicon.io)
  ([source code](https://github.com/near/NEPs)) is the formal description of
  the NEAR protocol. The reference nearcore implementation and any other NEAR
  client implementations must follow this specification.
* [**User docs**](https://docs.near.org) ([source code](https://github.com/near/docs))
  explain what is NEAR and how to participate in the network. In particular,
  they contain information pertinent to the users of NEAR: validators and
  smart contract developers.
* [**Documentation for nearcore developers**](https://near.github.io/nearcore/)
  ([source code](https://github.com/near/nearcore/tree/master/docs)) is the
  book you are reading right now! The target audience here is the contributors
  to the main implementation of the NEAR protocol (nearcore).

## Overview

The bulk of the internal docs is within this book. If you want to write some
kind of document, add it here! The [architecture](../architecture/) and
[practices](../practices/) chapters are intended for somewhat up-to-date
normative documents. The [misc](../misc/) chapter holds everything else.

This book is not intended for user-facing documentation, so don't worry about
proper English, typos, or beautiful diagrams -- just write stuff! It can easily
be improved over time with pull requests. For docs, we use a lightweight review
process and try to merge any improvement as quickly as possible. Rather than
blocking a PR on some stylistic changes, just merge it and submit a follow-up.

Note the "edit" button at the top-right corner -- super useful for fixing any
typos you spot!

In addition to the book, we also have some "inline" documentation in the code.
For Rust, it is customary to have a per-crate `README.md` file and include it as
a doc comment via `#![doc = include_str!("../README.md")]` in `lib.rs`. We don't
*require* every item to be documented, but we certainly encourage documenting as
much as possible. If you spend some time refactoring or fixing a function,
consider adding a doc comment (`///`) to it as a drive-by improvement.

We currently don't render `rustdoc`, see [#7836](https://github.com/near/nearcore/issues/7836).

## Book How To

We use mdBook to render a bunch of markdown files as a static website with a table
of contents, search and themes. Full docs are [here](https://rust-lang.github.io/mdBook/),
but the basics are very simple.

To add a new page to the book:

1. Add a `.md` file somewhere in the
   [`./docs`](https://github.com/near/nearcore/tree/master/docs) folder.
2. Add a link to that page to the
   [`SUMMARY.md`](https://github.com/near/nearcore/blob/master/docs/SUMMARY.md).
3. Submit a PR (again, we promise to merge it without much ceremony).

The doc itself is in vanilla markdown.

To render documentation locally:

```console
# Install mdBook
$ cargo install mdbook
$ mdbook serve --open ./docs
```

This will generate the book from the docs folder, open it in a browser and
start a file watcher to rebuild the book every time the source files change.

Note that GitHub's default rendering mostly works just as well, so you don't
need to go out of your way to preview your changes when drafting a page or
reviewing pull requests to this book.

The book is deployed via the
[book GitHub Action workflow](https://github.com/near/nearcore/blob/master/.github/workflows/book.yml).
This workflow runs mdBook and then deploys the result to
[GitHub Pages](https://docs.github.com/en/pages/getting-started-with-github-pages/about-github-pages).

For internal docs, you often want to have pretty pictures. We don't currently
have a recommended workflow, but here are some tips:

* Don't add binary media files to Git to avoid inflating repository size.
  Rather, upload images as comments to this super-secret issue
  [#7821](https://github.com/near/nearcore/issues/7821), and then link to
  the images as

  ```
  ![image](https://user-images.githubusercontent.com/1711539/195626792-7697129b-7f9c-4953-b939-0b9bcacaf72c.png)
  ```

  Use a single comment per page with multiple images.

* Google Docs is an OK way to create technical drawings, you can add a link to
  the doc with source to that secret issue as well.

* There's some momentum around using mermaid.js for diagramming, and there's
  an appropriate [plugin](https://github.com/badboy/mdbook-mermaid) for that.
  Consider if that's something you might want to use.
