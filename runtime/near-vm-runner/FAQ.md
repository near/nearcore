# FAQ for using virtual machines in the blockchain.

## Preface

This document is formed as an FAQ for the new blockchain architect and implementer,
documenting possible contract runtime design decisions and the logic behind them.
In some cases it’s really an answer, and in some others -  musings on possible ways
of thinking about the problem.

## Higher level architectural questions

### Should I use a special purpose virtual machine best suited for my blockchain, or rely on the general purpose VM?

First blockchains, such as Bitcoin and Ethereum 1.0, were in favour of the
special purpose VMs, while more recent blockchains, partially due to broader
Wasm implementations availability tend to use general purpose VM,
providing blockchain APIs as external/host functions.
Advantage of general purpose VM is mostly due to the broader developers community
behind such VMs, and wider availability of the development tools, such as language
compilers, debuggers, static analyzers, IDEs, etc.

### How do I make blockchain implementation economically sustainable?

Due to the decentralized nature of the blockchain, users shall be motivated
to lend their computational resources to others. The widely accepted approach here is
to rely on gas, the financial equivalent of computation cost,
whenever computation is performed. Gas can be bought using blockchain tokens
and is spent per used computational resources. Unfortunately, it is not possible
to estimate the amount of the computational cost that the contract can incur without
executing it, due to the halting problem. This means that the user must have an
upper bound estimate on how much gas they are willing to spend that they would
declare in the transaction. This makes performing general computations on the
blockchain harder to reason about, as developers must estimate maximum computational
requirements of the contract before its execution.


### What gas is supposed to represent? Complexity of computations performed, hardware resources used, power consumed, smth else?

Most practical blockchains make gas represent the number of the raw VM instructions
executed for managed code execution, and some synthetic and usually precomputed cost
for external native functions executed by the blockchain runtime.
For example, the number of actual machine instructions could be used as gas metrics
for host functions. Some blockchains however, approximate the gas as the elapsed time.

### What does the gas price reflect? How should I price the gas in my system? Pure price of hardware for validators or some economic meaning?

Gas price can either represent the cost of hardware maintenance for validators,
probably with some multiplier to make validation economically attractive,
or be used as a mechanism for improving blockchain functioning, i.e. load
balancing and attracting new users (i.e. validators could agree to work below
margin for some time to attract developers and users).

### How will my developers know the amount of gas to attach to their contract?

Prediction of the gas amount to be attached to the complex contract may become a
non-trivial problem, if execution has unbound loops. A proper gas metering better
be assisted by the developers tools.

### Can gas metering have theoretical limitations wrt impact on execution performance?

Gas metering, being computation itself, has its cost. There are few options, such as
hardware assisted gas metering, or fully precomputed gas cost if the contract execution
path is fully predictable, but in general case it’s rather hard to make it free for
the system.

### Why is gas metering not part of the Wasm standard? What shall be done with external/host functions then?

It probably shall be, and we as the blockchain community shall push forward adding gas
metering to the Wasm standard. Gas costs for the external functions likely to be part
of the Wasm runtime embedding mechanism, actual prices to be defined empirically,
unless there's a better mechanism provided by the OS/hardware.

### Shall gas be one scalar quantity, or vector (i.e. for CPU, memory, disk, network resources)?

There’s no definitive answer to this question yet, however, complex gas metering/pricing
strategies would make development experience even more complex, as one has to estimate
not one quantity, but the whole vector, and the token cost of different vector
components need to be decided as well.

### How consumed gas shall depend on the consumed resources?

The most straightforward approach is the linear dependency, i.e. program with twice
as many executed instructions shall consume twice as much gas, however nonlinear
dependencies, such as quadratic or even exponential, may be of interest in scenarios,
when too extensive computations in the contract may affect performance or stability
of the blockchain.

## Practical aspects of the execution engines

### Shall I use an interpreter, non-optimizing JIT, optimizing JIT, non-optimizing AOT, optimizing AOT compiler in my blockchain?
The answer depends on the required features, and the following table will try to help.
Every solution is subjectively measured on a bad/ok/good/great scale.


| **Metric**        | **Interpreter**|**Non-opt JIT**|**Opt JIT**|**Non-opt AOT**|**Opt AOT**|
| ----------------- |:--------------:| -------------:|----------:|--------------:|----------:|
| correctness       |  great         | good          |  ok       | good          |  ok       |
| performance       |  bad           | ok            |  good     | ok            | great     |
| security          |  great         | ok            | bad       | ok            | bad       |
| additional per-contract storage | great | great    | great     | bad           |  ok       |
| additional per-contract computations | great | ok  | bad       | good          | good      |
| Gas counting in VM | great         | ok            | mostly ok | ok            | mostly ok |

With an optimizing compiler, it’s possible to have decent performance, at the cost of spending
more time in the runtime in case of JIT, or more storage and compilation time on execution
of the contract first time in case of AOT compilers. Another potential limitation of optimizing
compilers is the possibility of JIT bombs and other security vulnerabilities coming from running
such complex pieces of software as optimizing compilers on the untrusted input. However,
with the careful selection of optimization passes and VM implementation it shall be possible
to limit the security impact while keeping decent performance.

### How do I do gas metering in presence of the compiler?

In case of JITs execution of the compiler can be either ignored, metered based on input size with
fixed coefficient, or metered precisely. In the latter case, JIT bombing is less of the problem,
as JIT bombs presumably will be prohibitively expensive. However, metering the compiler requires
cooperation from its authors, or ability to run the compiler itself in a metered environment
(i.e. if JIT is executed in VM itself). In case of AOT compilers, compilation is once per contract,
and technically compilation cost here could be included into the cost of contract deployment.

### What are the dangers of bugs in compilers/VMs?

Unlike traditional software development, bugs and UB in the contract runtime could be pretty
devastating for the network coherence, as they may trigger inconsistency between nodes, and
lead to undesired blockchain forks. Thus, whenever there’s a risk of behavioral discrepancy
between nodes executing contract code - it shall be mitigated. No visible state shall rely
upon timing taken for the certain operation, compilation or execution alike, and if an
execution correctness problem exists - it must be the same on all nodes.
Thus compiler crashes are always preferred to potential hiding of undefined behavior.

Another even more dangerous scenario of a compiler bug is when the bug is consistent for
all nodes, but it leads to incorrect or vulnerable contract execution. In that case the bug
can go unnoticed for many months until it manifests or gets exploited by someone.

### What is the cold cache attack and how do I avoid it?

Cold cache attack is a generic attack not specific to the contract runtime.
Consider the following scenario. Suppose some blockchain has the full governance mechanism that allows
participants to decide on the configuration parameters of the system. Suppose someone discovered
that the node would consume less resources if they add a caching layer somewhere, e.g. if
they decide to cache compilation of the contracts. Suppose everyone incorporates this change
into their nodes, then through the governance mechanism they lower the fee of the cached operation,
to represent the average cost of executing it. But average cost does not represent the worst
case scenario. So someone might find a way to execute computation on the system in such a way
that they would miss the cache. In case of the compiled contract this could be executing
a “cold” contract. As a result they will incur high computational cost to the system by paying
for an average scenario. This could lead to the node slowdown, dropout of the participants, etc.

There is currently no clear way how to avoid cold cache attack for the case when we cache contract compilation.

### What contracts can be executed without the actual blockchain? How to automatically test contracts?

Contracts are state machines, transferring the input state to the output state, so it’s not required
to have the traditional blockchain notions, such as network consensus and global transaction history
to be present to just execute the contract on known state. Thus, running certain contracts, such as tests,
in predefined blockchain state is an extremely desirable feature of the contract runtime. It is also useful
to dry-run contracts inside the devtools, e.g. to estimate the gas usage.

### Can Wasm clients be used for gas estimation?

Up to a certain extent, the pure computations part (such as compiler algorithms) is easy, when host
functions (external to Wasm functions provided by the blockchain) are involved it becomes harder.

## Development tools.

### What programming languages shall I support in my blockchain?

This is a rather broad question, so some aspects of that will be covered in next questions.
The most basic dichotomy is to support the general purpose language(s) such as Rust, AssemblyScript
vs. the specialized contract language, such as Solidity, Move, etc. Supporting custom language
usually induces cost of the toolchain maintenance, and it not only includes the compiler,
but also editor support, static analyzers, IDE and so on.  Another important problem is the higher
barriers for developers, since they should learn a new language, potentially with new paradigms,
and a few of online resources to learn. Using general purpose language seems preferable,
if your blockchain aims at providing rich smart contract functionality, especially if third party
libraries are expected to be used in the contract.

### What to do with the garbage collection in general purpose programming languages?

As one could see from the previous question, high level languages may require GC,
and the contract runtime may not have such a feature available.
There are few options:
   * Not support such a languages
   * Support such languages but not implement object reclamation and GC as many contracts are short lived it could be a viable option
   * Implement automated memory management as part of the language runtime and ship it with every program or once per language runtime version
   * Use runtime with existing GC, such as JVM or JS VM
   * Move forward with the [WebAssembly GC proposal](https://github.com/WebAssembly/gc/blob/master/proposals/gc/Overview.md)

### Shall I support pure functional language, such as Haskell, in contracts?

It depends on the target audience of your blockchain. Implementation wise, runtime for the functional
programming languages is not that different from runtime for an object oriented language,
and typically needs the GC. Thus, see the previous question.

### Is it important for the smart contract to be formally verifiable?

As with any other software, formal verification on smart contracts checks a certain behavioral aspects
of the program, and as such, could be a useful tool for finding bugs and potential misbehavior of
the program. However, it is not a silver bullet, and does not guarantee that the smart contract is
“correct”. Formal verification could be used as one of the static analysis tools helping developers
to write the better code.

### Is it important for the smart contract language to be Turing-complete?

There is no definitive answer to that question yet, however, for many practical aspects Turing
incompleteness means lack of the unbound loops/recursion, which limits the language expressive
and computational power. Many practical algorithms, such as Dijkstra’s algorithm or even simple
BFS/DFS are expressed in a form that is hard to formally prove to be bound, and so hard to
express in a Turing incomplete language. However, in some cases, Turing-incomplete contracts
could be useful as they are much easier statically analyzed and formally verified.

### Should my contracts use DSL or metaprogramming?

Both a Domain Specific Language (DSL) or a certain metaprogramming technique, such as the
template generator, on top of the general purpose language (such as Rust) seems to be a
good compromise between somewhat excessive expressive power of the general  purpose language
and ability to use tooling (compilers, debuggers) for such a language. Exact choice of the
technology mostly depends on the underlying general purpose language. In the case of Rust,
DSL approach could be implemented using standard Rust AST macroses.

### Should my embedded DSL (eDSL) be blackbox or glassbox?

If the eDSL shall be blackbox and not allow underlying language primitives invocation, or be
glassbox and allow the full power of the underlying general purpose language is an important
design decision. Blackbox approach produces less powerful, but more analyzable and verifiable
language, while glassbox is mostly a convenience to simplify coding, not really controlling
the expressive power of the language. See
[this question](#is-it-important-for-the-smart-contract-language-to-be-turing-complete)
for further discussion.

### Should we make it impossible to write unsafe contracts?

Dichotomy here is either taking a paternalistic approach for the users, and trying to constraint
them as much as possible through eDSL to the point where we minimize the chance of them making
some common contract-specific mistake. Or we take a liberal approach and consider typical
contract mistakes to be under full users’ responsibility. Most of the modern blockchains
follow the latter approach, sometimes augmented with the static analysis tools to help users
avoid certain classes of mistakes.

### Who is responsible for the safety of contract execution in the blockchain?

The question of responsibility of what’s going on in a decentralized distributed system is a very
complex topic. From contract runtime point of view, it can only provide an execution environment
which behaves according to the specification, and allows chain to proceed with any contract given
as input, by either consistently executing it and modifying the ledger per contract’s execution
result or refusing execution with a sensible error message and making no changes in the ledger.
Correctness and responsibility beyond that shall be usually decided outside of the contract runtime.

### Who is responsible for the safety of the assets programming?

Blockchain runtime has an option to provide primitive for safe asset-like manipulation,
however they must be carefully designed to avoid limiting functionality, or repeating generic
data manipulation operations available in general purpose programming language by default.

### Should all contracts be optimal in terms of performance?

The most important metric of performance in the blockchain is the rate of blocks/transactions processed
by the chain as the whole, and contract runtime performance is a part of the equation, responsible for
the raw execution speed. So contract optimizations are critical only if it makes the whole chain stall,
and if performance is dominated by the other components, optimizations in contract runtimes may be
ignored. When discussing the compiled contract size, huge (>1MiB) contracts are generally considered
harmful. Shall we optimize 100KiB contracts to be as small as possible, at the cost of worse
debuggability and less applicability of the general purpose libraries is not as clear. We generally
tend to consider such optimizations worsening development experience as excessive. Also, there are
certain best practises, such as avoiding JSON and using more efficient binary serialization for
transferring parameters to contracts, which should be communicated to the blockchain developers.

### Real financial world is asynchronous. Should DeFi (decentralized finance) also be?

While a pretty broad question, when projected on smart contract context, it means few rather simple questions.
   * Shall cross-contract calls be represented as:
       * sync operations
       * async operations
       * continuations
   * Shall some/all host functions be asynchronous?

Answers to those questions depend on the blockchain architecture, however general purpose
contract runtime likely shall support both modes of operation.


### How to think about the computational model for smart contracts, especially if they do I/O ?

The basic dichotomy here is the following: shall you expose the ability of performing callbacks driven
by the external operation in the same execution round (i.e. not recorded by the blockchain), or all
the operations performed by the smart contract must be time-bound synchronous, and all async operations
are represented as subsequent invocations of the same contract at different entry points/states.
Second approach seems to be better suitable for the practical applications, as otherwise the contract
must cope with the changed state of the blockchain during its execution, which may be not straightforward,
and the contract runtime can no longer rely on predictable/bound execution time of the contract.

### Should there be a wasi-blockchain spec like for instance the upcoming wasi-crypto spec?

WASI per se mostly provides the interface to the system operations, like native or FFI calls,
while metering naturally accounts for resources consumed both inside and outside of the VM
during arbitrary contract execution. However, API to control metering could be handled as
a WASI operation.
