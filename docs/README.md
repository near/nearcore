# Introduction

Welcome to the nearcore development guide!

The target audience of this guide are developers of nearcore itself. If you are
a user of NEAR (either a contract developer, or validator running a node),
please refer to the user docs at <https://docs.near.org>.

This guide is built with [mdBook](https://rust-lang.github.io/mdBook/)
from sources in the [nearcore repository](https://github.com/near/nearcore/).
You can edit it by pressing the "edit" icon in the top right corner, we welcome
all contributions. The guide is hosted at <https://near.github.io/nearcore/>.

The guide is organized as a collection of loosely coupled chapters -- you don't
need to read them in order, feel free to peruse the TOC, and focus on
the interesting bits. The chapters are classified into three parts:

* [**Architecture**](./architecture/) talks about how the code works.
  So, for example, if you are interested in how a transaction flows through the
  system, look there!
* [**Practices**](./practices/) describe, broadly, how we write code.
  For example, if you want to learn about code style, issue tracking, or
  debugging performance problems, this is the chapter for you.
* Finally, the [**Misc**](./misc/) part holds various assorted bits
  and pieces. We are trying to bias ourselves towards writing more docs, so, if
  you want to document something and it doesn't cleanly map to a category above,
  just put it in misc!

If you are unsure, start with [Architecture Overview](./architecture/) and then
read [Run a Node](./practices/workflows/run_a_node.md)
