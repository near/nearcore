# Introduction

Welcome to nearcore development guide!

The target audience of this guide are developers of nearcore itself. If you are
a user of NEAR (either a contract developer, or validator running a node),
please refer to the user docs at <https://docs.near.org>.

This guide is build with [mdBook](https://rust-lang.github.io/mdBook/index.html)
from sources in the [nearcore repository](https://github.com/near/nearcore/).
You can edit it by pressing the "edit" icon in the top right corner, so feel
free to correct all the typos and such. The guide is hosted at
<https://near.github.io/nearcore/>.

The guide is organized as a collection of loosely coupled chapters -- you don't
need to read it in order, feel free to peruse the TOC and focus on interesting
bits. The chapters are classified into three parts:

* [**Architecture**](./architecture/index.html) talks about how the code works.
  So, for example, if you are interested in how a transaction flows through the
  system, look there!
* [**Practices**](./practices/index.html) describe, broadly, how we write the code.
  For example, if you want to learn about code style, issue tracking, or
  debugging performance problems, this is a chapter for you.
* Finally, the [**Misc**](./misc/index.html) part holds various assorted bit and
  pieces. We are trying to bias ourself towards writing more docs, so, if you
  want to document something and it doesn't cleanly map to any category, just
  put it in misc!

If you are unsure, start with the next chapter, [Architecture Overview](./architecture/index.html)
